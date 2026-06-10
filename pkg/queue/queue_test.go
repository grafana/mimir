// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/queue_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
package queue

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/queue/tree"
	util_test "github.com/grafana/mimir/pkg/util/test"
)

func TestMain(m *testing.M) {
	util_test.VerifyNoLeakTestMain(m)
}

// Dimension labels mirror those the scheduler attaches to its requests. The
// queue treats them as opaque routing labels; tests redeclare them here so
// the queue test suite has no dependency on the scheduler package.
const (
	ingesterQueueDimension                = "ingester"
	storeGatewayQueueDimension            = "store-gateway"
	ingesterAndStoreGatewayQueueDimension = "ingester-and-store-gateway"
)

var secondQueueDimensionOptions = []string{
	ingesterQueueDimension,
	storeGatewayQueueDimension,
	ingesterAndStoreGatewayQueueDimension,
	UnknownDimension,
}

// randAdditionalQueueDimension is the basic implementation of additionalQueueDimensionFunc,
// used to assign the expected query component queue dimensions to test requests
// before they are enqueued by the queue producer groups utilized in benchmark tests.
// This version ignores the tenant ID, giving all tenants the same distribution of query components.
func randAdditionalQueueDimension(_ string) []string {
	maxIdx := len(secondQueueDimensionOptions)

	idx := rand.Intn(maxIdx)
	if idx == len(secondQueueDimensionOptions) {
		return nil
	}
	return secondQueueDimensionOptions[idx : idx+1]
}

// testItem is a generic stand-in for the scheduler's SchedulerRequest.
// The queue treats values as an opaque Item; tests use this type so
// the queue's own test suite has no dependency on the scheduler package.
//
// The field shape mirrors the scheduler's SchedulerRequest so that
// memory-usage benchmarks produce realistic allocation profiles: the
// HttpRequest field holds a heap-allocated struct with its own strings and
// header pointers, rather than a single inline byte filler that would have
// misleadingly cheap copy semantics.
type testItem struct {
	Ctx                       context.Context
	FrontendAddr              string
	UserID                    string
	HttpRequest               *httpgrpc.HTTPRequest
	EnqueueTime               time.Time
	AdditionalQueueDimensions []string
}

// ExpectedQueryComponentName mirrors SchedulerRequest.ExpectedQueryComponentName.
func (r *testItem) ExpectedQueryComponentName() string {
	if len(r.AdditionalQueueDimensions) > 0 {
		return r.AdditionalQueueDimensions[0]
	}
	return UnknownDimension
}

// makeTestItem creates a request whose in-memory size and shape
// approximate a real query request (a representative URL, headers, and
// frontend address). The point is for benchmarks of a non-trivial queue
// depth to be dominated by request payload rather than queue mechanics.
func makeTestItem(tenantID string, additionalQueueDimensions []string) *testItem {
	return &testItem{
		Ctx:          context.Background(),
		FrontendAddr: "http://query-frontend:8007",
		UserID:       tenantID,
		HttpRequest: &httpgrpc.HTTPRequest{
			Method: "GET",
			Headers: []*httpgrpc.Header{
				{Key: "QueryId", Values: []string{"12345678901234567890"}},
				{Key: "Accept", Values: []string{"application/vnd.mimir.queryresponse+protobuf", "application/json"}},
				{Key: "X-Scope-OrgId", Values: []string{tenantID}},
				{Key: "uber-trace-id", Values: []string{"48475050943e8e05:70e8b02d28e4337b:077cd9b649b6ac02:1"}},
			},
			Url: "/prometheus/api/v1/query_range?end=1701720000&query=rate%28go_goroutines%7Bcluster%3D%22docker-compose-local%22%2Cjob%3D%22mimir-microservices-mode%2Fquery-scheduler%22%2Cnamespace%3D%22mimir-microservices-mode%22%7D%5B10m15s%5D%29&start=1701648000&step=60",
		},
		EnqueueTime:               time.Now(),
		AdditionalQueueDimensions: additionalQueueDimensions,
	}
}

func BenchmarkConcurrentQueueOperations(b *testing.B) {
	maxConsumersPerTenant := 0 // disable shuffle sharding
	forgetConsumerDelay := time.Duration(0)
	maxOutstandingRequestsPerTenant := 100

	for _, numTenants := range []int{1, 10, 1000} {
		b.Run(fmt.Sprintf("%v tenants", numTenants), func(b *testing.B) {

			// Query-frontends run 5 parallel streams per scheduler by default,
			// and we typically see 2-5 frontends running at any one time.
			for _, numProducers := range []int{10, 25} {
				b.Run(fmt.Sprintf("%v concurrent producers", numProducers), func(b *testing.B) {

					// Consumers run with parallelism of 16 when query sharding is enabled.
					for _, numConsumers := range []int{16, 160, 1600} {
						b.Run(fmt.Sprintf("%v concurrent consumers", numConsumers), func(b *testing.B) {
							queue, err := New(
								log.NewNopLogger(),
								maxOutstandingRequestsPerTenant,
								forgetConsumerDelay,
								promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
								promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
								promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
							)
							require.NoError(b, err)

							ctx := context.Background()

							require.NoError(b, queue.starting(ctx))
							b.Cleanup(func() {
								require.NoError(b, queue.stop(nil))
							})

							startProducersChan, producersErrGroup := makeQueueProducerGroup(
								queue, maxConsumersPerTenant, b.N, numProducers, numTenants, randAdditionalQueueDimension,
							)

							queueConsumerErrGroup, startConsumersChan := makeQueueConsumerGroup(
								context.Background(), queue, b.N, numConsumers, 1, nil,
							)

							b.ResetTimer()
							close(startProducersChan) // run producers
							close(startConsumersChan) // run consumers

							err = producersErrGroup.Wait()
							require.NoError(b, err)

							err = queueConsumerErrGroup.Wait()
							require.NoError(b, err)
						})
					}
				})
			}
		})
	}
}
func queueActorIterationCount(totalIters int, numActors int, actorIdx int) int {
	actorIters := totalIters / numActors
	remainderIters := totalIters % numActors

	if remainderIters == 0 {
		// iterations are spread equally across actors without a remainder
		return actorIters
	}

	// If we can't perfectly spread iterations across all actors,
	// assign remaining iterations to the actors at the beginning of the list.
	if actorIdx < remainderIters {
		// this actor is early enough in the list to get one of the remaining iterations
		return actorIters + 1
	}

	return actorIters
}

func makeQueueProducerGroup(
	queue *Queue,
	maxConsumersPerTenant int,
	totalRequests int,
	numProducers int,
	numTenants int,
	queueDimensionFunc func(string) []string,
) (chan struct{}, *errgroup.Group) {
	startProducersChan := make(chan struct{})
	producersErrGroup, _ := errgroup.WithContext(context.Background())

	runProducer := runQueueProducerIters(
		queue,
		maxConsumersPerTenant,
		totalRequests,
		numProducers,
		numTenants,
		startProducersChan,
		queueDimensionFunc,
	)
	for producerIdx := 0; producerIdx < numProducers; producerIdx++ {
		producerIdx := producerIdx
		producersErrGroup.Go(func() error {
			return runProducer(producerIdx)
		})
	}
	return startProducersChan, producersErrGroup
}

func runQueueProducerIters(
	queue *Queue,
	maxConsumersPerTenant int,
	totalIters int,
	numProducers int,
	numTenants int,
	start chan struct{},
	additionalQueueDimensionFunc func(tenantID string) []string,
) func(producerIdx int) error {
	return func(producerIdx int) error {
		producerIters := queueActorIterationCount(totalIters, numProducers, producerIdx)
		tenantID := producerIdx % numTenants
		tenantIDStr := strconv.Itoa(tenantID)
		<-start

		for i := 0; i < producerIters; i++ {
			err := queueProduce(queue, maxConsumersPerTenant, tenantIDStr, additionalQueueDimensionFunc)
			if err != nil {
				return err
			}

			tenantID = (tenantID + 1) % numTenants
		}

		return nil
	}
}

func queueProduce(
	queue *Queue,
	maxConsumersPerTenant int,
	tenantID string,
	additionalQueueDimensionFunc func(tenantID string) []string,
) error {
	var additionalQueueDimensions []string
	if additionalQueueDimensionFunc != nil {
		additionalQueueDimensions = additionalQueueDimensionFunc(tenantID)
	}
	req := makeTestItem(tenantID, additionalQueueDimensions)
	for {
		err := queue.SubmitItemToEnqueue(tenantID, req, req.ExpectedQueryComponentName(), maxConsumersPerTenant, func() {})
		if err == nil {
			break
		}
		// Keep retrying if we've hit the max queue length, otherwise give up immediately.
		if !errors.Is(err, ErrTooManyRequests) {
			return err
		}
	}
	return nil
}

func makeQueueConsumerGroup(
	ctx context.Context,
	queue *Queue,
	totalRequests int,
	numConsumers int,
	numWorkersPerConsumer int,
	consumeFunc consumeItem,
) (*errgroup.Group, chan struct{}) {
	queueConsumerErrGroup, ctx := errgroup.WithContext(ctx)
	consumedRequestsCounter := make(chan struct{}, totalRequests)
	startConsumersChan := make(chan struct{})
	stopConsumersChan := make(chan struct{})
	runConsumer := runQueueConsumerUntilEmpty(ctx, totalRequests, queue, consumeFunc, consumedRequestsCounter, startConsumersChan, stopConsumersChan)

	for consumerIdx := 0; consumerIdx < numConsumers; consumerIdx++ {
		for workerIdx := 0; workerIdx < numWorkersPerConsumer; workerIdx++ {
			consumerIdx := consumerIdx
			queueConsumerErrGroup.Go(func() error {
				return runConsumer(consumerIdx)
			})
		}
	}
	return queueConsumerErrGroup, startConsumersChan
}

func runQueueConsumerUntilEmpty(
	ctx context.Context,
	totalRequests int,
	requestQueue *Queue,
	consumeFunc consumeItem,
	consumedRequestsCounter chan struct{},
	start chan struct{},
	stop chan struct{},
) func(consumerIdx int) error {
	return func(consumerIdx int) error {
		lastTenantIndex := FirstTenant()
		consumerID := fmt.Sprintf("consumer-%v", consumerIdx)

		consumerWorkerConn := NewUnregisteredConsumerWorkerConn(context.Background(), consumerID)
		err := requestQueue.AwaitRegisterConsumerWorkerConn(consumerWorkerConn)
		if err != nil {
			return err
		}
		defer requestQueue.SubmitUnregisterConsumerWorkerConn(consumerWorkerConn)

		consumedRequest := make(chan struct{})
		loopQueueConsume := func() error {
			for {
				idx, err := queueConsume(requestQueue, consumerWorkerConn, lastTenantIndex, consumeFunc)
				if err != nil {
					return err
				}

				consumedRequest <- struct{}{}
				lastTenantIndex = idx
			}
		}
		loopQueueConsumeErrGroup, _ := errgroup.WithContext(ctx)

		<-start
		loopQueueConsumeErrGroup.Go(loopQueueConsume)

		for {
			select {
			case <-stop:
				return nil
			case <-consumedRequest:
				consumedRequestsCounter <- struct{}{}
				if len(consumedRequestsCounter) == totalRequests {
					close(stop)
				}
			}
		}
	}
}

type consumeItem func(request Item) error

func queueConsume(
	queue *Queue, consumerWorkerConn *ConsumerWorkerConn, lastTenantIdx TenantIndex, consumeFunc consumeItem,
) (TenantIndex, error) {
	dequeueReq := NewConsumerWorkerDequeueRequest(consumerWorkerConn, lastTenantIdx)
	request, idx, err := queue.AwaitItemForConsumer(dequeueReq)
	if err != nil {
		return lastTenantIdx, err
	}
	lastTenantIdx = idx

	if consumeFunc != nil {
		err = consumeFunc(request)
	}
	return lastTenantIdx, err
}

func (q *Queue) EmptyQueue(t *testing.T) {
	lastTenantIndex := FirstTenant()
	consumerID := "emptying-consumer"

	consumerWorkerConn := NewUnregisteredConsumerWorkerConn(context.Background(), consumerID)
	require.NoError(t, q.AwaitRegisterConsumerWorkerConn(consumerWorkerConn))
	defer q.SubmitUnregisterConsumerWorkerConn(consumerWorkerConn)

	consumer := func(request Item) error {
		return nil
	}

	for {
		if q.queueBroker.isEmpty() {
			return
		}

		idx, err := queueConsume(q, consumerWorkerConn, lastTenantIndex, consumer)
		require.NoError(t, err)
		lastTenantIndex = idx
	}
}

// TestDispatchToWaitingDequeueRequestForUnregisteredConsumerWorker tests a scenario which previously caused a panic.
// When a consumer-worker submits a dequeue request while there are no queue items sharded to that consumer,
// The waiting dequeue request is held in an internal queue until a reshard or enqueue operation occurs.
// A reshard or enqueue operation triggers an attempt to dispatch queue items to those waiting dequeue requests.
//
// If the consumer-worker associated with the dequeue request has since crashed or otherwise been deregistered,
// the queue should skip that dequeue request drop it from the internal queue so it will not be retried.
func TestDispatchToWaitingDequeueRequestForUnregisteredConsumerWorker(t *testing.T) {
	const forgetDelay = 2 * time.Second
	const testTimeout = 10 * time.Second

	queue, err := New(
		log.NewNopLogger(),
		2,
		forgetDelay,
		promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
		promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
		promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
	)
	require.NoError(t, err)

	// Start the queue service.
	ctx := context.Background()
	require.NoError(t, services.StartAndAwaitRunning(ctx, queue))

	// Two consumers connect, allowing us to enqueue requests sharded to only one of the two.
	consumer1Ctx, consumer1CtxCancel := context.WithCancelCause(context.Background())
	consumer1Conn := NewUnregisteredConsumerWorkerConn(consumer1Ctx, "consumer-1")
	assert.NoError(t, queue.AwaitRegisterConsumerWorkerConn(consumer1Conn))
	consumer2Conn := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-2")
	assert.NoError(t, queue.AwaitRegisterConsumerWorkerConn(consumer2Conn))

	// A third consumer to be registered later to trigger request dispatch
	consumer3Conn := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-3")

	t.Cleanup(func() {
		// if the test has failed and the queue does not get cleared,
		// we must send a shutdown signal for the remaining connected consumer
		// or else StopAndAwaitTerminated will never complete.
		queue.SubmitUnregisterConsumerWorkerConn(consumer2Conn)
		queue.SubmitUnregisterConsumerWorkerConn(consumer3Conn)

		queue.EmptyQueue(t)

		assert.NoError(t, services.StopAndAwaitTerminated(ctx, queue))
	})

	assertControlFlowError := func(t *testing.T, err error) {
		// Error received by the consumer connections waiting for a dequeue should be one of:
		// * ErrConsumerWorkerDisconnected: the waiting conn was crashed or otherwise deregistered but the whole consumer is not yet deregistered
		// * context.Canceled: context cancellation also propagates on waiting conn crash/deregister and races with ErrConsumerWorkerDisconnected
		// * ErrConsumerShuttingDown: consumer has initiated shutdown or all conns deregistered
		// * ErrStopped: The scheduler has received a shutdown signal and cleared its queue or lost all consumer connections.
		assert.True(t, errors.Is(err, ErrConsumerWorkerDisconnected) || errors.Is(err, context.Canceled) || errors.Is(err, ErrConsumerShuttingDown) || errors.Is(err, ErrStopped))
	}

	// Enqueue requests which will NOT be sharded to consumer-1.
	// NOTE: "user-1" shuffle shard always chooses the first consumer ("consumer-1" in this case)
	// when there are only one or two consumers in the sorted list of connected consumers.
	//
	// These requests will sit in the queue -
	// consumer-2 is the only consumer sharded to user-1, but consumer-2 has not requested to dequeue yet.
	// >1 queue dimensions must exist in the queue to reproduce a potential panic condition (dims % unregisteredWorkerID).
	reqNotShardedToConsumer1 := &testItem{
		Ctx:                       context.Background(),
		HttpRequest:               &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
		AdditionalQueueDimensions: []string{"ingester"},
	}
	assert.NoError(t, queue.SubmitItemToEnqueue("user-2", reqNotShardedToConsumer1, reqNotShardedToConsumer1.ExpectedQueryComponentName(), 1, nil))
	reqNotShardedToConsumer1 = &testItem{
		Ctx:                       context.Background(),
		HttpRequest:               &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
		AdditionalQueueDimensions: []string{"store-gateway"},
	}
	assert.NoError(t, queue.SubmitItemToEnqueue("user-2", reqNotShardedToConsumer1, reqNotShardedToConsumer1.ExpectedQueryComponentName(), 1, nil))

	// Consumer-1 submits a request to dequeue;
	// it will not receive anything as the only requests in the queue are not sharded to consumer-1.
	consumer1SubmitDequeueReqWG := sync.WaitGroup{}
	consumer1SubmitDequeueReqWG.Add(1)
	consumer1DequeueReq := NewConsumerWorkerDequeueRequest(consumer1Conn, FirstTenant())
	go func() {
		defer consumer1SubmitDequeueReqWG.Done()
		_, _, err := queue.AwaitItemForConsumer(consumer1DequeueReq)
		// This blocks until it receives one of the control flow errors;
		// it will not receive a request as those in the queue are only sharded to consumer-2.
		assertControlFlowError(t, err)
	}()

	// Wait for the waiting dequeue request to be enqueued internally.
	for queue.waitingDequeueRequestsToDispatchCount.Load() < 1 {
		time.Sleep(1 * time.Millisecond)
	}

	// Simulate a crash of consumer-1's only connection, with no graceful shutdown notification from the consumer.
	// The entire consumer is not removed until forgetDelay passes, but the individual connection is marked as unregistered.
	// A waiting dequeue request must have its connection ID set to -1 to complete the potential panic condition.
	consumer1CtxCancel(errors.New("crash"))
	queue.SubmitUnregisterConsumerWorkerConn(consumer1Conn) // normally done in the defers of the grpc loop

	// Wait for the queue to process the de-registration of the consumer-1 connection.
	for queue.connectedConsumerWorkers.Load() > 1 {
		time.Sleep(1 * time.Millisecond)
	}

	// Register another consumer to trigger a re-shard and ensure we attempt to dispatch the requests.
	// The queue will first try to dispatch a queue item to its internally-queued waiting dequeue requests.
	// The first waiting dequeue request in that list is from the crashed and deregistered consumer-1 -
	// This test is to ensure that is handled gracefully.
	assert.NoError(t, queue.AwaitRegisterConsumerWorkerConn(consumer3Conn))

	// The dequeue request should have been dropped from the waiting list
	// when the queue received an error from the queue broker's dequeueItemForConsumer.
	assert.Eventually(
		t,
		func() bool { return queue.waitingDequeueRequestsToDispatchCount.Load() == 0 },
		testTimeout,
		1*time.Second,
	)

	// This should be long gone but ensure we have no hanging goroutines
	consumer1SubmitDequeueReqWG.Wait()
}

func TestQueue_RegisterAndUnregisterConsumerWorkerConnections(t *testing.T) {
	const forgetDelay = 3 * time.Second

	queue, err := New(
		log.NewNopLogger(),
		1,
		forgetDelay,
		promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
		promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
		promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
	)
	require.NoError(t, err)

	// start the queue service.
	ctx := context.Background()
	require.NoError(t, services.StartAndAwaitRunning(ctx, queue))

	t.Cleanup(func() {
		// we must send a shutdown signal for any remaining connected consumers
		// or else StopAndAwaitTerminated will never complete.
		queue.SubmitNotifyConsumerShutdown(ctx, "consumer-1")
		queue.SubmitNotifyConsumerShutdown(ctx, "consumer-2")
		require.NoError(t, services.StopAndAwaitTerminated(ctx, queue))
	})

	// 2 consumers open 3 connections each.
	consumer1Conn1 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-1")
	require.NoError(t, queue.AwaitRegisterConsumerWorkerConn(consumer1Conn1))
	require.Equal(t, 0, consumer1Conn1.WorkerID)
	require.Equal(t, 1, int(queue.connectedConsumerWorkers.Load()))

	consumer1Conn2 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-1")
	require.NoError(t, queue.AwaitRegisterConsumerWorkerConn(consumer1Conn2))
	require.Equal(t, 1, consumer1Conn2.WorkerID)
	require.Equal(t, 2, int(queue.connectedConsumerWorkers.Load()))

	consumer1Conn3 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-1")
	require.NoError(t, queue.AwaitRegisterConsumerWorkerConn(consumer1Conn3))
	require.Equal(t, 2, consumer1Conn3.WorkerID)
	require.Equal(t, 3, int(queue.connectedConsumerWorkers.Load()))

	consumer2Conn1 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-2")
	require.NoError(t, queue.AwaitRegisterConsumerWorkerConn(consumer2Conn1))
	require.Equal(t, 0, consumer2Conn1.WorkerID)
	require.Equal(t, 4, int(queue.connectedConsumerWorkers.Load()))

	consumer2Conn2 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-2")
	require.NoError(t, queue.AwaitRegisterConsumerWorkerConn(consumer2Conn2))
	require.Equal(t, 1, consumer2Conn2.WorkerID)
	require.Equal(t, 5, int(queue.connectedConsumerWorkers.Load()))

	consumer2Conn3 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-2")
	require.NoError(t, queue.AwaitRegisterConsumerWorkerConn(consumer2Conn3))
	require.Equal(t, 2, consumer2Conn3.WorkerID)
	require.Equal(t, 6, int(queue.connectedConsumerWorkers.Load()))

	// if consumer-worker disconnects and reconnects before any other consumer-worker changes,
	// the consumer-worker connect will get its same worker ID back
	queue.SubmitUnregisterConsumerWorkerConn(consumer2Conn2)
	require.NoError(t, queue.AwaitRegisterConsumerWorkerConn(consumer2Conn2))
	require.Equal(t, 1, consumer2Conn2.WorkerID)
	require.Equal(t, 6, int(queue.connectedConsumerWorkers.Load()))

	// if a consumer-worker disconnects and another consumer-worker connects before the first reconnects
	// the second consumer-worker will have taken the worker ID of the first consumer-worker,
	// and the first consumer-worker will get issued a new worker ID

	// even though some operations are awaited
	// and some are just submitted without waiting for completion,
	// all consumer-worker operations are processed in the order of the submit/await calls.
	queue.SubmitUnregisterConsumerWorkerConn(consumer1Conn2)
	// we cannot be sure the worker ID is unregistered yet,
	// but once we await the next worker register call, we can be sure.
	consumer1Conn4 := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-1")
	require.NoError(t, queue.AwaitRegisterConsumerWorkerConn(consumer1Conn4))
	require.False(t, consumer1Conn2.IsRegistered())
	require.Equal(t, 1, consumer1Conn4.WorkerID)
	require.Equal(t, 6, int(queue.connectedConsumerWorkers.Load()))
	// re-connect from the first consumer-worker and get a completely new worker ID
	require.NoError(t, queue.AwaitRegisterConsumerWorkerConn(consumer1Conn2))
	require.Equal(t, 3, consumer1Conn2.WorkerID)
	require.Equal(t, 7, int(queue.connectedConsumerWorkers.Load()))
}

func TestQueue_GetNextRequestForConsumer_ShouldGetRequestAfterReshardingBecauseConsumerHasBeenForgotten(t *testing.T) {
	const forgetDelay = 3 * time.Second
	const testTimeout = 10 * time.Second

	queue, err := New(
		log.NewNopLogger(),
		1,
		forgetDelay,
		promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
		promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
		promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
	)
	require.NoError(t, err)

	// Start the queue service.
	ctx := context.Background()
	require.NoError(t, services.StartAndAwaitRunning(ctx, queue))

	// Two consumers connect.
	consumer1Conn := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-1")
	require.NoError(t, queue.AwaitRegisterConsumerWorkerConn(consumer1Conn))
	consumer2Conn := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-2")
	require.NoError(t, queue.AwaitRegisterConsumerWorkerConn(consumer2Conn))

	t.Cleanup(func() {
		// if the test has failed and the queue does not get cleared,
		// we must send a shutdown signal for the remaining connected consumer
		// or else StopAndAwaitTerminated will never complete.
		queue.SubmitUnregisterConsumerWorkerConn(consumer2Conn)
		require.NoError(t, services.StopAndAwaitTerminated(ctx, queue))
	})

	// Consumer-2 waits for a new request.
	consumer2wg := sync.WaitGroup{}
	consumer2wg.Add(1)
	go func() {
		defer consumer2wg.Done()
		dequeueReq := NewConsumerWorkerDequeueRequest(consumer2Conn, FirstTenant())
		_, _, err := queue.AwaitItemForConsumer(dequeueReq)
		require.NoError(t, err)
	}()

	// Consumer-1 crashes (no graceful shutdown notification).
	queue.SubmitUnregisterConsumerWorkerConn(consumer1Conn)

	// Enqueue a request from an user which would be assigned to consumer-1.
	// NOTE: "user-1" shuffle shard always chooses the first consumer ("consumer-1" in this case)
	// when there are only one or two consumers in the sorted list of connected consumers
	req := &testItem{
		Ctx:                       context.Background(),
		HttpRequest:               &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
		AdditionalQueueDimensions: randAdditionalQueueDimension(""),
	}
	require.NoError(t, queue.SubmitItemToEnqueue("user-1", req, req.ExpectedQueryComponentName(), 1, nil))

	startTime := time.Now()
	done := make(chan struct{})
	go func() {
		consumer2wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		waitTime := time.Since(startTime)
		// We expect that consumer-2 got the request only after forget delay is passed.
		assert.GreaterOrEqual(t, waitTime.Milliseconds(), forgetDelay.Milliseconds())
	case <-time.After(testTimeout):
		t.Fatal("timeout: consumer-2 did not receive the request expected to be resharded to consumer-2")
	}
}

func TestQueue_GetNextRequestForConsumer_ReshardNotifiedCorrectlyForMultipleConsumerForget(t *testing.T) {
	const forgetDelay = 3 * time.Second
	const testTimeout = 10 * time.Second

	queue, err := New(
		log.NewNopLogger(),
		1,
		forgetDelay,
		promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
		promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
		promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
	)
	require.NoError(t, err)

	// Start the queue service.
	ctx := context.Background()
	require.NoError(t, services.StartAndAwaitRunning(ctx, queue))

	// Three consumers connect.
	// We will submit the enqueue request with maxConsumers: 2.
	//
	// Whenever forgetDisconnectedConsumers runs, all consumers which reached zero connections since the last
	// run of forgetDisconnectedConsumers will all be removed in from the shuffle shard in the same run.
	//
	// In this case two consumers are forgotten in the same run, but only the first forgotten consumer triggers a reshard.
	// In the first reshard, the tenant goes from a shuffled subset of consumers to a state of
	// "tenant can use all consumers", as connected consumers is now <= tenant.maxConsumers.
	// The second forgotten consumer won't trigger a reshard, as connected consumers is already <= tenant.maxConsumers.
	//
	// We are testing that the occurrence of a reshard is reported correctly
	// when not all consumer forget operations in a single run of forgetDisconnectedConsumers caused a reshard.
	// Two consumers connect.
	consumer1Conn := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-1")
	require.NoError(t, queue.AwaitRegisterConsumerWorkerConn(consumer1Conn))
	consumer2Conn := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-2")
	require.NoError(t, queue.AwaitRegisterConsumerWorkerConn(consumer2Conn))
	consumer3Conn := NewUnregisteredConsumerWorkerConn(context.Background(), "consumer-3")
	require.NoError(t, queue.AwaitRegisterConsumerWorkerConn(consumer3Conn))

	t.Cleanup(func() {
		// if the test has failed and the queue does not get cleared,
		// we must send a shutdown signal for the remaining connected consumer
		// or else StopAndAwaitTerminated will never complete.
		queue.SubmitUnregisterConsumerWorkerConn(consumer2Conn)
		require.NoError(t, services.StopAndAwaitTerminated(ctx, queue))
	})

	// consumer-2 waits for a new request.
	consumer2wg := sync.WaitGroup{}
	consumer2wg.Add(1)
	go func() {
		defer consumer2wg.Done()
		dequeueReq := NewConsumerWorkerDequeueRequest(consumer2Conn, FirstTenant())
		_, _, err := queue.AwaitItemForConsumer(dequeueReq)
		require.NoError(t, err)
	}()

	// consumer-1 and consumer-3 crash (no graceful shutdown notification).
	queue.SubmitUnregisterConsumerWorkerConn(consumer1Conn)
	queue.SubmitUnregisterConsumerWorkerConn(consumer3Conn)

	// Enqueue a request from a tenant which would be assigned to consumer-1.
	// NOTE: "user-1" shuffle shard always chooses the first consumer ("consumer-1" in this case)
	// when there are only one or two consumers in the sorted list of connected consumers
	req := &testItem{
		Ctx:                       context.Background(),
		HttpRequest:               &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
		AdditionalQueueDimensions: randAdditionalQueueDimension(""),
	}
	require.NoError(t, queue.SubmitItemToEnqueue("user-1", req, req.ExpectedQueryComponentName(), 2, nil))

	startTime := time.Now()
	done := make(chan struct{})
	go func() {
		consumer2wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		waitTime := time.Since(startTime)
		// We expect that consumer-2 got the request only after forget delay is passed.
		assert.GreaterOrEqual(t, waitTime.Milliseconds(), forgetDelay.Milliseconds())
	case <-time.After(testTimeout):
		t.Fatal("timeout: consumer-2 did not receive the request expected to be resharded to consumer-2")
	}
}

func TestQueue_GetNextRequestForConsumer_ShouldReturnAfterContextCancelled(t *testing.T) {
	const forgetDelay = 3 * time.Second
	const consumerID = "consumer-1"

	queue, err := New(
		log.NewNopLogger(),
		1,
		forgetDelay,
		promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
		promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
		promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
	)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), queue))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), queue))
	})

	consumer1Conn := NewUnregisteredConsumerWorkerConn(context.Background(), consumerID)
	require.NoError(t, queue.AwaitRegisterConsumerWorkerConn(consumer1Conn))

	// Calling AwaitItemForConsumer with a context that is already cancelled should fail immediately.
	deadCtx, cancel := context.WithCancel(context.Background())
	cancel()
	consumer1Conn.ctx = deadCtx

	dequeueReq := NewConsumerWorkerDequeueRequest(consumer1Conn, FirstTenant())
	r, tenant, err := queue.AwaitItemForConsumer(dequeueReq)
	assert.Nil(t, r)
	assert.Equal(t, FirstTenant(), tenant)
	assert.ErrorIs(t, err, context.Canceled)

	// Further, a context canceled after AwaitItemForConsumer publishes a request should also fail.
	errChan := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	consumer1Conn.ctx = ctx

	go func() {
		dequeueReq := NewConsumerWorkerDequeueRequest(consumer1Conn, FirstTenant())
		_, _, err := queue.AwaitItemForConsumer(dequeueReq)
		errChan <- err
	}()

	time.Sleep(20 * time.Millisecond) // Wait for AwaitItemForConsumer to be waiting for a query.
	cancel()

	select {
	case err := <-errChan:
		require.Equal(t, context.Canceled, err)
	case <-time.After(time.Second):
		require.Fail(t, "gave up waiting for GetNextRequestForConsumerToReturn")
	}
}

func TestQueue_GetNextRequestForConsumer_ShouldReturnImmediatelyIfConsumerIsAlreadyShuttingDown(t *testing.T) {
	const forgetDelay = 3 * time.Second
	const consumerID = "consumer-1"

	queue, err := New(
		log.NewNopLogger(),
		1,
		forgetDelay,
		promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
		promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
		promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
	)
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, services.StartAndAwaitRunning(ctx, queue))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, queue))
	})

	consumerConn := NewUnregisteredConsumerWorkerConn(context.Background(), consumerID)
	require.NoError(t, queue.AwaitRegisterConsumerWorkerConn(consumerConn))

	queue.SubmitNotifyConsumerShutdown(ctx, consumerID)

	dequeueReq := NewConsumerWorkerDequeueRequest(consumerConn, FirstTenant())
	_, _, err = queue.AwaitItemForConsumer(dequeueReq)
	require.EqualError(t, err, "consumer has informed the scheduler it is shutting down")
}

func TestQueue_tryDispatchRequestToConsumer_ShouldReEnqueueAfterFailedSendToConsumer(t *testing.T) {
	const forgetDelay = 3 * time.Second
	const consumerID = "consumer-1"

	queue, err := New(
		log.NewNopLogger(),
		1,
		forgetDelay,
		promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
		promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
		promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
	)
	require.NoError(t, err)

	// bypassing queue dispatcher loop for direct usage of the queueBroker and
	// passing a ConsumerWorkerDequeueRequest for a canceled consumer connection
	qb := newQueueBroker(queue.maxOutstandingPerTenant, queue.forgetDelay)
	qb.addConsumerWorkerConn(NewUnregisteredConsumerWorkerConn(context.Background(), consumerID))

	tenantMaxConsumers := 0 // no sharding
	queueDim := randAdditionalQueueDimension("")
	req := &testItem{
		Ctx:                       context.Background(),
		HttpRequest:               &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
		AdditionalQueueDimensions: queueDim,
	}
	tr := tenantItem{
		tenantID:       "tenant-1",
		queueDimension: req.ExpectedQueryComponentName(),
		item:           req,
	}

	var multiAlgorithmTreeQueuePath tree.QueuePath
	if queueDim == nil {
		queueDim = []string{UnknownDimension}
	}
	multiAlgorithmTreeQueuePath = append(append(multiAlgorithmTreeQueuePath, queueDim...), "tenant-1")

	require.Nil(t, qb.tree.GetNode(multiAlgorithmTreeQueuePath))
	require.NoError(t, qb.enqueueItemBack(&tr, tenantMaxConsumers))
	require.False(t, qb.tree.GetNode(multiAlgorithmTreeQueuePath).IsEmpty())

	ctx, cancel := context.WithCancel(context.Background())
	call := &ConsumerWorkerDequeueRequest{
		ConsumerWorkerConn: &ConsumerWorkerConn{
			ctx:        ctx,
			ConsumerID: consumerID,
			WorkerID:   0,
		},
		lastTenantIndex: FirstTenant(),
		recvChan:        make(chan consumerWorkerDequeueResponse),
	}
	cancel() // ensure consumer context done before send is attempted

	// send to consumer will fail but method returns true,
	// indicating not to re-submit a request for ConsumerWorkerDequeueRequest for the consumer
	require.True(t, queue.trySendNextItemForConsumer(call))
	// assert request was re-enqueued for tenant after failed send
	require.False(t, qb.tree.GetNode(multiAlgorithmTreeQueuePath).IsEmpty())
}

// This test ensures that even if the queue has no pending requests, we still wait until any inflight requests
// have been returned before existing
func TestQueue_ShutdownWithPendingRequests_ShouldDrainRequests(t *testing.T) {
	ctx := context.Background()
	tenantID := "testTenant"
	consumerID := "consumer1"

	// So we create a queue
	queue, err := New(
		log.NewNopLogger(),
		1,
		0,
		promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
		promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
		promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
	)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, queue))

	// With a worker
	conn := NewUnregisteredConsumerWorkerConn(context.Background(), consumerID)
	require.NoError(t, queue.AwaitRegisterConsumerWorkerConn(conn))

	// And push a request to the queue
	req := makeTestItem(tenantID, []string{})
	require.NotNil(t, req)
	err = queue.SubmitItemToEnqueue(tenantID, req, req.ExpectedQueryComponentName(), 1, func() {})
	require.NoError(t, err)

	// And make sure it got to the queue
	require.Equal(t, queue.queueBroker.tree.ItemCount(), 1)

	// Stop the Queue
	queue.StopAsync()

	// Consume the existing request from the queue and ensure it matches the one we queued
	dequeueReq := NewConsumerWorkerDequeueRequest(conn, FirstTenant())
	r, _, err := queue.AwaitItemForConsumer(dequeueReq)
	require.NoError(t, err)
	require.NotNil(t, r)
	require.Equal(t, r, req)

	// Ensure the request has been removed from the queue
	require.Equal(t, queue.queueBroker.tree.ItemCount(), 0)

	// And finally make sure it stops within the timeout
	require.NoError(t, queue.AwaitTerminated(ctx))
}
