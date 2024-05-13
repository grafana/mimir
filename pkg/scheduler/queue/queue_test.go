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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	util_test "github.com/grafana/mimir/pkg/util/test"
)

func TestMain(m *testing.M) {
	util_test.VerifyNoLeakTestMain(m)
}

var secondQueueDimensionOptions = []string{
	ingesterQueueDimension,
	storeGatewayQueueDimension,
	ingesterAndStoreGatewayQueueDimension,
}

func randAdditionalQueueDimension(allowEmpty bool) []string {
	maxIdx := len(secondQueueDimensionOptions)
	if allowEmpty {
		maxIdx++
	}

	idx := rand.Intn(maxIdx)
	if idx == len(secondQueueDimensionOptions) {
		return nil
	}
	return secondQueueDimensionOptions[idx : idx+1]
}

// makeSchedulerRequest is intended to create a query request with a nontrivial size.
//
// When running benchmarks for memory usage, we want a relatively representative request size.
// The size of the requests in a queue of nontrivial depth should significantly outweigh the memory
// used by the queue mechanics, in order get a more meaningful % delta between competing queue implementations.
func makeSchedulerRequest(tenantID string, additionalQueueDimensions []string) *SchedulerRequest {
	return &SchedulerRequest{
		Ctx:             context.Background(),
		FrontendAddress: "http://query-frontend:8007",
		UserID:          tenantID,
		Request: &httpgrpc.HTTPRequest{
			Method: "GET",
			Headers: []*httpgrpc.Header{
				{Key: "QueryId", Values: []string{"12345678901234567890"}},
				{Key: "Accept", Values: []string{"application/vnd.mimir.queryresponse+protobuf", "application/json"}},
				{Key: "X-Scope-OrgId", Values: []string{tenantID}},
				{Key: "uber-trace-id", Values: []string{"48475050943e8e05:70e8b02d28e4337b:077cd9b649b6ac02:1"}},
			},
			Url: "/prometheus/api/v1/query_range?end=1701720000&query=rate%28go_goroutines%7Bcluster%3D%22docker-compose-local%22%2Cjob%3D%22mimir-microservices-mode%2Fquery-scheduler%22%2Cnamespace%3D%22mimir-microservices-mode%22%7D%5B10m15s%5D%29&start=1701648000&step=60",
		},
		AdditionalQueueDimensions: additionalQueueDimensions,
		EnqueueTime:               time.Now(),
	}
}

// TestMultiDimensionalQueueFairnessSlowConsumerEffects emulates a simplified queue slowdown scenario
// which the scheduler's additional queue dimensions features are intended to solve for.
//
// In this scenario, one category of queue item causes the queue consumer to slow down, introducing a
// significant delay while the queue consumer processes it and before the consumer can dequeue the next item.
// This emulates a situation where one of the query components - the ingesters or store-gateways - is under load.
//
// If queue items belonging to the slow category are in the same queue in front of the normal queue items,
// the normal queue items must wait for all slow queue items to be cleared before they can be serviced.
// In this way, the degraded performance of the slow query component equally degrades the performance of the
// queries which *could* be serviced quickly, but are waiting behind the slow queries in the queue.
//
// With the additional queue dimensions enabled, the queues are split by which query component the query will utilize.
// The queue broker then round-robins between the split queues, which has the effect of alternating between
// dequeuing the slow queries and normal queries rather than blocking normal queries behind slow queries.
func TestMultiDimensionalQueueFairnessSlowConsumerEffects(t *testing.T) {
	promRegistry := prometheus.NewPedanticRegistry()

	maxQueriersPerTenant := 0 // disable shuffle sharding
	forgetQuerierDelay := time.Duration(0)
	maxOutstandingRequestsPerTenant := 1000

	totalRequests := 100
	numTenants := 1
	numProducers := 10
	numConsumers := 1

	normalQueueDimension := "normal-request"
	slowConsumerLatency := 20 * time.Millisecond
	slowConsumerQueueDimension := "slow-request"
	normalQueueDimensionFunc := func() []string { return []string{normalQueueDimension} }
	slowQueueDimensionFunc := func() []string { return []string{slowConsumerQueueDimension} }

	additionalQueueDimensionsEnabledCases := []bool{false, true}
	queueDurationTotals := map[bool]map[string]float64{
		false: {normalQueueDimension: 0.0, slowConsumerQueueDimension: 0.0},
		true:  {normalQueueDimension: 0.0, slowConsumerQueueDimension: 0.0},
	}

	for _, additionalQueueDimensionsEnabled := range additionalQueueDimensionsEnabledCases {

		// Scheduler code uses a histogram for queue duration, but a counter is a more direct metric
		// for this test, as we are concerned with the total or average wait time for all queue items.
		// Prometheus histograms also lack support for test assertions via prometheus/testutil.
		queueDuration := promauto.With(promRegistry).NewCounterVec(prometheus.CounterOpts{
			Name: "test_query_scheduler_queue_duration_total_seconds",
			Help: "[test] total time spent by items in queue before getting picked up by a consumer",
		}, []string{"additional_queue_dimensions"})

		queue := NewRequestQueue(
			log.NewNopLogger(),
			maxOutstandingRequestsPerTenant,
			additionalQueueDimensionsEnabled,
			forgetQuerierDelay,
			promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"tenant"}),
			promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"tenant"}),
			promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
		)

		ctx := context.Background()
		require.NoError(t, queue.starting(ctx))
		t.Cleanup(func() {
			require.NoError(t, queue.stop(nil))
		})

		// fill queue first with the slow queries, then the normal queries
		for _, queueDimensionFunc := range []func() []string{slowQueueDimensionFunc, normalQueueDimensionFunc} {
			startProducersChan := make(chan struct{})
			producersErrGroup, _ := errgroup.WithContext(ctx)

			runProducer := runQueueProducerIters(
				queue, maxQueriersPerTenant, totalRequests/2, numProducers, numTenants, startProducersChan, queueDimensionFunc,
			)
			for producerIdx := 0; producerIdx < numProducers; producerIdx++ {
				producerIdx := producerIdx
				producersErrGroup.Go(func() error {
					return runProducer(producerIdx)
				})
			}
			close(startProducersChan)
			err := producersErrGroup.Wait()
			require.NoError(t, err)
		}

		// emulate delay when consuming the slow queries
		consumeFunc := func(request Request) error {
			schedulerRequest := request.(*SchedulerRequest)
			if schedulerRequest.AdditionalQueueDimensions[0] == slowConsumerQueueDimension {
				time.Sleep(slowConsumerLatency)
			}

			queueTime := time.Since(schedulerRequest.EnqueueTime)
			additionalQueueDimensionLabels := strings.Join(schedulerRequest.AdditionalQueueDimensions, ":")
			queueDuration.With(prometheus.Labels{"additional_queue_dimensions": additionalQueueDimensionLabels}).Add(queueTime.Seconds())
			return nil
		}

		// consume queries
		queueConsumerErrGroup, ctx := errgroup.WithContext(ctx)
		startConsumersChan := make(chan struct{})
		runConsumer := runQueueConsumerIters(ctx, queue, totalRequests, numConsumers, startConsumersChan, consumeFunc)

		for consumerIdx := 0; consumerIdx < numConsumers; consumerIdx++ {
			consumerIdx := consumerIdx
			queueConsumerErrGroup.Go(func() error {
				return runConsumer(consumerIdx)
			})
		}

		close(startConsumersChan)
		err := queueConsumerErrGroup.Wait()
		require.NoError(t, err)

		// record total queue duration by queue dimensions and whether the queue splitting was enabled
		for _, queueDimension := range []string{normalQueueDimension, slowConsumerQueueDimension} {
			queueDurationTotals[additionalQueueDimensionsEnabled][queueDimension] = promtest.ToFloat64(
				queueDuration.With(prometheus.Labels{"additional_queue_dimensions": queueDimension}),
			)
		}

		promRegistry.Unregister(queueDuration)
	}

	// total or average time in queue for a normal queue item should be roughly cut in half
	// when queue splitting is enabled, as the average normal queue item waits behind
	// half of the slow queue items, instead of waiting behind all the slow queue items.
	expected := queueDurationTotals[false][normalQueueDimension] / 2
	actual := queueDurationTotals[true][normalQueueDimension]
	// some variance allowed due to actual time processing needed beyond the slow consumer delay;
	// variance is also a function of the number of consumers and the consumer delay chosen.
	// variance can be tighter if the test runs longer but there is a tradeoff for testing and CI speed
	delta := expected * 0.10
	require.InDelta(t, expected, actual, delta)

}

func BenchmarkConcurrentQueueOperations(b *testing.B) {
	maxQueriersPerTenant := 0 // disable shuffle sharding
	forgetQuerierDelay := time.Duration(0)
	maxOutstandingRequestsPerTenant := 100

	for _, numTenants := range []int{1, 10, 1000} {
		b.Run(fmt.Sprintf("%v tenants", numTenants), func(b *testing.B) {

			// Query-frontends run 5 parallel streams per scheduler by default,
			// and we typically see 2-5 frontends running at any one time.
			for _, numProducers := range []int{10, 25} {
				b.Run(fmt.Sprintf("%v concurrent producers", numProducers), func(b *testing.B) {

					// Queriers run with parallelism of 16 when query sharding is enabled.
					for _, numConsumers := range []int{16, 160, 1600} {
						b.Run(fmt.Sprintf("%v concurrent consumers", numConsumers), func(b *testing.B) {
							queue := NewRequestQueue(
								log.NewNopLogger(),
								maxOutstandingRequestsPerTenant,
								true,
								forgetQuerierDelay,
								promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"tenant"}),
								promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"tenant"}),
								promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
							)

							startSignalChan := make(chan struct{})
							queueActorsErrGroup, ctx := errgroup.WithContext(context.Background())

							require.NoError(b, queue.starting(ctx))
							b.Cleanup(func() {
								require.NoError(b, queue.stop(nil))
							})

							runProducer := runQueueProducerIters(
								queue, maxQueriersPerTenant, b.N, numProducers, numTenants, startSignalChan, nil,
							)

							for producerIdx := 0; producerIdx < numProducers; producerIdx++ {
								producerIdx := producerIdx
								queueActorsErrGroup.Go(func() error {
									return runProducer(producerIdx)
								})
							}

							runConsumer := runQueueConsumerIters(ctx, queue, b.N, numConsumers, startSignalChan, nil)

							for consumerIdx := 0; consumerIdx < numConsumers; consumerIdx++ {
								consumerIdx := consumerIdx
								queueActorsErrGroup.Go(func() error {
									return runConsumer(consumerIdx)
								})
							}

							b.ResetTimer()
							close(startSignalChan)
							err := queueActorsErrGroup.Wait()
							if err != nil {
								require.NoError(b, err)
							}
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

func runQueueProducerIters(
	queue *RequestQueue,
	maxQueriersPerTenant int,
	totalIters int,
	numProducers int,
	numTenants int,
	start chan struct{},
	additionalQueueDimensionFunc func() []string,
) func(producerIdx int) error {
	return func(producerIdx int) error {
		producerIters := queueActorIterationCount(totalIters, numProducers, producerIdx)
		tenantID := producerIdx % numTenants
		tenantIDStr := strconv.Itoa(tenantID)
		<-start

		for i := 0; i < producerIters; i++ {
			err := queueProduce(queue, maxQueriersPerTenant, tenantIDStr, additionalQueueDimensionFunc)
			if err != nil {
				return err
			}

			tenantID = (tenantID + 1) % numTenants
		}

		return nil
	}
}

func queueProduce(
	queue *RequestQueue, maxQueriersPerTenant int, tenantID string, additionalQueueDimensionFunc func() []string,
) error {
	var additionalQueueDimensions []string
	if additionalQueueDimensionFunc != nil {
		additionalQueueDimensions = additionalQueueDimensionFunc()
	}
	req := makeSchedulerRequest(tenantID, additionalQueueDimensions)
	for {
		err := queue.EnqueueRequestToDispatcher(tenantID, req, maxQueriersPerTenant, func() {})
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

func runQueueConsumerIters(
	ctx context.Context,
	queue *RequestQueue,
	totalIters int,
	numConsumers int,
	start chan struct{},
	consumeFunc consumeRequest,
) func(consumerIdx int) error {
	return func(consumerIdx int) error {
		consumerIters := queueActorIterationCount(totalIters, numConsumers, consumerIdx)
		lastTenantIndex := FirstTenant()
		querierID := fmt.Sprintf("consumer-%v", consumerIdx)
		queue.SubmitRegisterQuerierConnection(querierID)
		defer queue.SubmitUnregisterQuerierConnection(querierID)

		<-start

		for i := 0; i < consumerIters; i++ {
			idx, err := queueConsume(ctx, queue, querierID, lastTenantIndex, consumeFunc)
			if err != nil {
				return err
			}

			lastTenantIndex = idx
		}

		return nil
	}
}

type consumeRequest func(request Request) error

func queueConsume(
	ctx context.Context, queue *RequestQueue, querierID string, lastTenantIndex TenantIndex, consumeFunc consumeRequest,
) (TenantIndex, error) {
	request, idx, err := queue.GetNextRequestForQuerier(ctx, lastTenantIndex, querierID)
	if err != nil {
		return lastTenantIndex, err
	}
	lastTenantIndex = idx

	if consumeFunc != nil {
		err = consumeFunc(request)
	}
	return lastTenantIndex, err
}

func TestRequestQueue_GetNextRequestForQuerier_ShouldGetRequestAfterReshardingBecauseQuerierHasBeenForgotten(t *testing.T) {
	const forgetDelay = 3 * time.Second
	const testTimeout = 10 * time.Second

	queue := NewRequestQueue(
		log.NewNopLogger(),
		1, true,
		forgetDelay,
		promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
		promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
		promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
	)

	// Start the queue service.
	ctx := context.Background()
	require.NoError(t, services.StartAndAwaitRunning(ctx, queue))
	t.Cleanup(func() {
		// if the test has failed and the queue does not get cleared,
		// we must send a shutdown signal for the remaining connected querier
		// or else StopAndAwaitTerminated will never complete.
		queue.SubmitUnregisterQuerierConnection("querier-2")
		require.NoError(t, services.StopAndAwaitTerminated(ctx, queue))
	})

	// Two queriers connect.
	queue.SubmitRegisterQuerierConnection("querier-1")
	queue.SubmitRegisterQuerierConnection("querier-2")

	// Querier-2 waits for a new request.
	querier2wg := sync.WaitGroup{}
	querier2wg.Add(1)
	go func() {
		defer querier2wg.Done()
		_, _, err := queue.GetNextRequestForQuerier(ctx, FirstTenant(), "querier-2")
		require.NoError(t, err)
	}()

	// Querier-1 crashes (no graceful shutdown notification).
	queue.SubmitUnregisterQuerierConnection("querier-1")

	// Enqueue a request from an user which would be assigned to querier-1.
	// NOTE: "user-1" shuffle shard always chooses the first querier ("querier-1" in this case)
	// when there are only one or two queriers in the sorted list of connected queriers
	req := &SchedulerRequest{
		Ctx:                       context.Background(),
		Request:                   &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
		AdditionalQueueDimensions: randAdditionalQueueDimension(true),
	}
	require.NoError(t, queue.EnqueueRequestToDispatcher("user-1", req, 1, nil))

	startTime := time.Now()
	done := make(chan struct{})
	go func() {
		querier2wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		waitTime := time.Since(startTime)
		// We expect that querier-2 got the request only after forget delay is passed.
		assert.GreaterOrEqual(t, waitTime.Milliseconds(), forgetDelay.Milliseconds())
	case <-time.After(testTimeout):
		t.Fatal("timeout: querier-2 did not receive the request expected to be resharded to querier-2")
	}
}

func TestRequestQueue_GetNextRequestForQuerier_ReshardNotifiedCorrectlyForMultipleQuerierForget(t *testing.T) {
	const forgetDelay = 3 * time.Second
	const testTimeout = 10 * time.Second

	queue := NewRequestQueue(
		log.NewNopLogger(),
		1, true,
		forgetDelay,
		promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
		promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
		promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
	)

	// Start the queue service.
	ctx := context.Background()
	require.NoError(t, services.StartAndAwaitRunning(ctx, queue))
	t.Cleanup(func() {
		// if the test has failed and the queue does not get cleared,
		// we must send a shutdown signal for the remaining connected querier
		// or else StopAndAwaitTerminated will never complete.
		queue.SubmitUnregisterQuerierConnection("querier-2")
		require.NoError(t, services.StopAndAwaitTerminated(ctx, queue))
	})

	// Three queriers connect.
	// We will submit the enqueue request with maxQueriers: 2.
	//
	// Whenever forgetDisconnectedQueriers runs, all queriers which reached zero connections since the last
	// run of forgetDisconnectedQueriers will all be removed in from the shuffle shard in the same run.
	//
	// In this case two queriers are forgotten in the same run, but only the first forgotten querier triggers a reshard.
	// In the first reshard, the tenant goes from a shuffled subset of queriers to a state of
	// "tenant can use all queriers", as connected queriers is now <= tenant.maxQueriers.
	// The second forgotten querier won't trigger a reshard, as connected queriers is already <= tenant.maxQueriers.
	//
	// We are testing that the occurrence of a reshard is reported correctly
	// when not all querier forget operations in a single run of forgetDisconnectedQueriers caused a reshard.
	queue.SubmitRegisterQuerierConnection("querier-1")
	queue.SubmitRegisterQuerierConnection("querier-2")
	queue.SubmitRegisterQuerierConnection("querier-3")

	// querier-2 waits for a new request.
	querier2wg := sync.WaitGroup{}
	querier2wg.Add(1)
	go func() {
		defer querier2wg.Done()
		_, _, err := queue.GetNextRequestForQuerier(ctx, FirstTenant(), "querier-2")
		require.NoError(t, err)
	}()

	// querier-1 and querier-3 crash (no graceful shutdown notification).
	queue.SubmitUnregisterQuerierConnection("querier-1")
	queue.SubmitUnregisterQuerierConnection("querier-3")

	// Enqueue a request from a tenant which would be assigned to querier-1.
	// NOTE: "user-1" shuffle shard always chooses the first querier ("querier-1" in this case)
	// when there are only one or two queriers in the sorted list of connected queriers
	req := &SchedulerRequest{
		Ctx:                       context.Background(),
		Request:                   &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
		AdditionalQueueDimensions: randAdditionalQueueDimension(true),
	}
	require.NoError(t, queue.EnqueueRequestToDispatcher("user-1", req, 2, nil))

	startTime := time.Now()
	done := make(chan struct{})
	go func() {
		querier2wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		waitTime := time.Since(startTime)
		// We expect that querier-2 got the request only after forget delay is passed.
		assert.GreaterOrEqual(t, waitTime.Milliseconds(), forgetDelay.Milliseconds())
	case <-time.After(testTimeout):
		t.Fatal("timeout: querier-2 did not receive the request expected to be resharded to querier-2")
	}
}

func TestRequestQueue_GetNextRequestForQuerier_ShouldReturnAfterContextCancelled(t *testing.T) {
	const forgetDelay = 3 * time.Second
	const querierID = "querier-1"

	queue := NewRequestQueue(
		log.NewNopLogger(),
		1,
		true,
		forgetDelay,
		promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
		promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
		promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
	)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), queue))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), queue))
	})

	queue.SubmitRegisterQuerierConnection(querierID)
	errChan := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		_, _, err := queue.GetNextRequestForQuerier(ctx, FirstTenant(), querierID)
		errChan <- err
	}()

	time.Sleep(20 * time.Millisecond) // Wait for GetNextRequestForQuerier to be waiting for a query.
	cancel()

	select {
	case err := <-errChan:
		require.Equal(t, context.Canceled, err)
	case <-time.After(time.Second):
		require.Fail(t, "gave up waiting for GetNextRequestForQuerierToReturn")
	}
}

func TestRequestQueue_GetNextRequestForQuerier_ShouldReturnImmediatelyIfQuerierIsAlreadyShuttingDown(t *testing.T) {
	const forgetDelay = 3 * time.Second
	const querierID = "querier-1"

	queue := NewRequestQueue(
		log.NewNopLogger(),
		1,
		true,
		forgetDelay,
		promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
		promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
		promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
	)

	ctx := context.Background()
	require.NoError(t, services.StartAndAwaitRunning(ctx, queue))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, queue))
	})

	queue.SubmitRegisterQuerierConnection(querierID)
	queue.SubmitNotifyQuerierShutdown(querierID)

	_, _, err := queue.GetNextRequestForQuerier(context.Background(), FirstTenant(), querierID)
	require.EqualError(t, err, "querier has informed the scheduler it is shutting down")
}

func TestRequestQueue_tryDispatchRequestToQuerier_ShouldReEnqueueAfterFailedSendToQuerier(t *testing.T) {
	const forgetDelay = 3 * time.Second
	const querierID = "querier-1"

	queue := NewRequestQueue(
		log.NewNopLogger(),
		1,
		true,
		forgetDelay,
		promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
		promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
		promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
	)

	// bypassing queue dispatcher loop for direct usage of the queueBroker and
	// passing a nextRequestForQuerierCall for a canceled querier connection
	queueBroker := newQueueBroker(queue.maxOutstandingPerTenant, queue.additionalQueueDimensionsEnabled, queue.forgetDelay)
	queueBroker.addQuerierConnection(querierID)

	tenantMaxQueriers := 0 // no sharding
	req := &SchedulerRequest{
		Ctx:                       context.Background(),
		Request:                   &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
		AdditionalQueueDimensions: randAdditionalQueueDimension(true),
	}
	tr := tenantRequest{
		tenantID: TenantID("tenant-1"),
		req:      req,
	}

	require.Nil(t, queueBroker.tenantQueuesTree.getNode(QueuePath{"tenant-1"}))
	require.NoError(t, queueBroker.enqueueRequestBack(&tr, tenantMaxQueriers))
	require.False(t, queueBroker.tenantQueuesTree.getNode(QueuePath{"tenant-1"}).IsEmpty())

	ctx, cancel := context.WithCancel(context.Background())
	call := &nextRequestForQuerierCall{
		ctx:             ctx,
		querierID:       QuerierID(querierID),
		lastTenantIndex: FirstTenant(),
		resultChan:      make(chan nextRequestForQuerier),
	}
	cancel() // ensure querier context done before send is attempted

	// send to querier will fail but method returns true,
	// indicating not to re-submit a request for nextRequestForQuerierCall for the querier
	require.True(t, queue.trySendNextRequestForQuerier(call))
	// assert request was re-enqueued for tenant after failed send
	require.False(t, queueBroker.tenantQueuesTree.getNode(QueuePath{"tenant-1"}).IsEmpty())
}
