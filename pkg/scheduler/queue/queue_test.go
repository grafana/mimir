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

	util_test "github.com/grafana/mimir/pkg/util/test"
)

func TestMain(m *testing.M) {
	util_test.VerifyNoLeakTestMain(m)
}

// cannot import constants from frontend/v2 due to import cycle,
// but content of the strings should not matter as much as the number of options
var secondQueueDimensionOptions = []string{
	"ingester",
	"store-gateway",
	"ingester-and-store-gateway",
}

func randAdditionalQueueDimension() []string {
	idx := rand.Intn(len(secondQueueDimensionOptions) + 1)
	if idx == len(secondQueueDimensionOptions) {
		// randomly don't add a second queue dimension at all to ensure the items still get dequeued
		return nil
	}
	return secondQueueDimensionOptions[idx : idx+1]
}

func makeSchedulerRequest(tenantID string) *SchedulerRequest {
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
		AdditionalQueueDimensions: randAdditionalQueueDimension(),
	}
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

							runProducer := runQueueProducerForBenchmark(b, queue, maxQueriersPerTenant, numProducers, numTenants, startSignalChan)

							for producerIdx := 0; producerIdx < numProducers; producerIdx++ {
								producerIdx := producerIdx
								queueActorsErrGroup.Go(func() error {
									return runProducer(producerIdx)
								})
							}

							runConsumer := runQueueConsumerForBenchmark(ctx, b, queue, numConsumers, startSignalChan)

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

func queueActorIterationCount(benchmarkIters int, numActors int, actorIdx int) int {
	actorIters := benchmarkIters / numActors
	remainderIters := benchmarkIters % numActors

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

func runQueueProducerForBenchmark(b *testing.B, queue *RequestQueue, maxQueriersPerTenant int, numProducers int, numTenants int, start chan struct{}) func(producerIdx int) error {
	return func(producerIdx int) error {
		producerIters := queueActorIterationCount(b.N, numProducers, producerIdx)
		tenantID := producerIdx % numTenants
		tenantIDStr := strconv.Itoa(tenantID)
		<-start

		for i := 0; i < producerIters; i++ {
			for {
				// when running this benchmark for memory usage comparison,
				// we want to have a relatively representative size of request
				// in order not to skew the % delta between queue implementations.
				// Unless the request starts to get copied, the size of the requests in the queue
				// should significantly outweigh the memory used to implement the queue mechanics.
				req := makeSchedulerRequest(tenantIDStr)
				//req.AdditionalQueueDimensions = randAdditionalQueueDimension()
				err := queue.EnqueueRequestToDispatcher(tenantIDStr, req, maxQueriersPerTenant, func() {})
				if err == nil {
					break
				}

				// Keep retrying if we've hit the max queue length, otherwise give up immediately.
				if !errors.Is(err, ErrTooManyRequests) {
					return err
				}
			}

			tenantID = (tenantID + 1) % numTenants
		}

		return nil
	}
}

func runQueueConsumerForBenchmark(ctx context.Context, b *testing.B, queue *RequestQueue, numConsumers int, start chan struct{}) func(consumerIdx int) error {
	return func(consumerIdx int) error {
		consumerIters := queueActorIterationCount(b.N, numConsumers, consumerIdx)
		lastTenantIndex := FirstUser()
		querierID := fmt.Sprintf("consumer-%v", consumerIdx)
		queue.RegisterQuerierConnection(querierID)
		defer queue.UnregisterQuerierConnection(querierID)

		<-start

		for i := 0; i < consumerIters; i++ {
			_, idx, err := queue.GetNextRequestForQuerier(ctx, lastTenantIndex, querierID)
			if err != nil {
				return err
			}

			lastTenantIndex = idx
		}

		return nil
	}
}

func TestRequestQueue_GetNextRequestForQuerier_ShouldGetRequestAfterReshardingBecauseQuerierHasBeenForgotten(t *testing.T) {
	const forgetDelay = 3 * time.Second

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
		require.NoError(t, services.StopAndAwaitTerminated(ctx, queue))
	})

	// Two queriers connect.
	queue.RegisterQuerierConnection("querier-1")
	queue.RegisterQuerierConnection("querier-2")

	// Querier-2 waits for a new request.
	querier2wg := sync.WaitGroup{}
	querier2wg.Add(1)
	go func() {
		defer querier2wg.Done()
		_, _, err := queue.GetNextRequestForQuerier(ctx, FirstUser(), "querier-2")
		require.NoError(t, err)
	}()

	// Querier-1 crashes (no graceful shutdown notification).
	queue.UnregisterQuerierConnection("querier-1")

	// Enqueue a request from an user which would be assigned to querier-1.
	// NOTE: "user-1" hash falls in the querier-1 shard.
	req := &SchedulerRequest{
		Ctx:                       context.Background(),
		Request:                   &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
		AdditionalQueueDimensions: randAdditionalQueueDimension(),
	}
	require.NoError(t, queue.EnqueueRequestToDispatcher("user-1", req, 1, nil))

	startTime := time.Now()
	querier2wg.Wait()
	waitTime := time.Since(startTime)

	// We expect that querier-2 got the request only after querier-1 forget delay is passed.
	assert.GreaterOrEqual(t, waitTime.Milliseconds(), forgetDelay.Milliseconds())
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

	queue.RegisterQuerierConnection(querierID)
	errChan := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		_, _, err := queue.GetNextRequestForQuerier(ctx, FirstUser(), querierID)
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

	queue.RegisterQuerierConnection(querierID)
	queue.NotifyQuerierShutdown(querierID)

	_, _, err := queue.GetNextRequestForQuerier(context.Background(), FirstUser(), querierID)
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
		AdditionalQueueDimensions: randAdditionalQueueDimension(),
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
		ctx:           ctx,
		querierID:     QuerierID(querierID),
		lastUserIndex: FirstUser(),
		processed:     make(chan nextRequestForQuerier),
	}
	cancel() // ensure querier context done before send is attempted

	// send to querier will fail but method returns true,
	// indicating not to re-submit a request for nextRequestForQuerierCall for the querier
	require.True(t, queue.tryDispatchRequestToQuerier(queueBroker, call))
	// assert request was re-enqueued for tenant after failed send
	require.False(t, queueBroker.tenantQueuesTree.getNode(QueuePath{"tenant-1"}).IsEmpty())
}
