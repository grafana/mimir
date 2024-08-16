// // SPDX-License-Identifier: AGPL-3.0-only
// // Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/queue_test.go
// // Provenance-includes-license: Apache-2.0
// // Provenance-includes-copyright: The Cortex Authors.
package queue

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	util_test "github.com/grafana/mimir/pkg/util/test"
)

// // TODO (casie): Write tests for prioritizeQueryComponents is true
func buildTreeTestsStruct() []struct {
	name                  string
	useMultiAlgoTreeQueue bool
} {
	return []struct {
		name                  string
		useMultiAlgoTreeQueue bool
	}{
		{"legacy tree queue", false},
		{"integrated tree queue", true},
	}
}

func TestMain(m *testing.M) {
	util_test.VerifyNoLeakTestMain(m)
}

func testQuerierInflightRequestsMetric() *prometheus.SummaryVec {
	return promauto.With(prometheus.NewPedanticRegistry()).NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "test_cortex_query_scheduler_querier_inflight_requests",
			Help:       "[test] Number of inflight requests being processed on all querier-scheduler connections. Quantile buckets keep track of inflight requests over the last 60s.",
			Objectives: map[float64]float64{0.5: 0.05, 0.75: 0.02, 0.8: 0.02, 0.9: 0.01, 0.95: 0.01, 0.99: 0.001},
			MaxAge:     time.Minute,
			AgeBuckets: 6,
		},
		[]string{"query_component"},
	)
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
		Ctx:          context.Background(),
		FrontendAddr: "http://query-frontend:8007",
		UserID:       tenantID,
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
// This simulates a situation where one of the query components - the ingesters or store-gateways - is under load.
//
// If queue items belonging to the slow category are in the same queue ("normal-channel") in front of the normal queue
// items, the normal queue items must wait for all slow queue items to be cleared before they can be serviced.
// In this way, the degraded performance of the slow query component equally degrades the performance of the
// queries which *could* be serviced quickly, but are waiting behind the slow queries in the queue.
//
// When using multiple queue dimensions, the queues are split by which "component" the query will utilize -- in this
// test, those components are called "normal-channel" and "slow-channel" for clarity. The queue broker then
// round-robins between the multiple queues, which has the effect of alternately dequeuing from the slow queries
// and normal queries rather than blocking normal queries behind slow queries.
//func TestMultiDimensionalQueueFairnessSlowConsumerEffects(t *testing.T) {
//	treeTypes := buildTreeTestsStruct()
//
//	for _, tt := range treeTypes {
//		t.Run(tt.name, func(t *testing.T) {
//			promRegistry := prometheus.NewPedanticRegistry()
//
//			maxQueriersPerTenant := 0 // disable shuffle sharding
//			forgetQuerierDelay := time.Duration(0)
//			maxOutstandingRequestsPerTenant := 1000
//
//			totalRequests := 100
//			numTenants := 1
//			numProducers := 10
//			numConsumers := 1
//
//			normalQueueDimension := "normal-request"
//			slowConsumerLatency := 20 * time.Millisecond
//			slowConsumerQueueDimension := "slow-request"
//			normalQueueDimensionFunc := func(_ bool) []string { return []string{"normal-channel"} }
//			slowQueueDimensionFunc := func(usingMultipleDimensions bool) []string {
//				if usingMultipleDimensions {
//					return []string{"slow-channel"}
//				}
//				return []string{"normal-channel"}
//			}
//
//			useMultipleDimensions := []bool{false, true}
//			queueDurationTotals := map[bool]map[string]float64{
//				false: {normalQueueDimension: 0.0, slowConsumerQueueDimension: 0.0},
//				true:  {normalQueueDimension: 0.0, slowConsumerQueueDimension: 0.0},
//			}
//
//			for _, multipleDimensionsUsed := range useMultipleDimensions {
//
//				// Scheduler code uses a histogram for queue duration, but a counter is a more direct metric
//				// for this test, as we are concerned with the total or average wait time for all queue items.
//				// Prometheus histograms also lack support for test assertions via prometheus/testutil.
//				queueDuration := promauto.With(promRegistry).NewCounterVec(prometheus.CounterOpts{
//					Name: "test_query_scheduler_queue_duration_total_seconds",
//					Help: "[test] total time spent by items in queue before getting picked up by a consumer",
//				}, []string{"additional_queue_dimensions"})
//
//				queue, err := NewRequestQueue(
//					log.NewNopLogger(),
//					maxOutstandingRequestsPerTenant,
//					tt.useMultiAlgoTreeQueue,
//					forgetQuerierDelay,
//					promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
//					promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
//					promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
//					promauto.With(nil).NewSummaryVec(prometheus.SummaryOpts{}, []string{"query_component"}),
//				)
//				require.NoError(t, err)
//
//				ctx := context.Background()
//				require.NoError(t, queue.starting(ctx))
//				t.Cleanup(func() {
//					require.NoError(t, queue.stop(nil))
//				})
//
//				// fill queue first with the slow queries, then the normal queries
//				for _, queueDimensionFunc := range []func(bool) []string{slowQueueDimensionFunc, normalQueueDimensionFunc} {
//					startProducersChan := make(chan struct{})
//					producersErrGroup, _ := errgroup.WithContext(ctx)
//
//					runProducer := runQueueProducerIters(
//						queue, maxQueriersPerTenant, totalRequests/2, numProducers, numTenants, startProducersChan, multipleDimensionsUsed, queueDimensionFunc,
//					)
//					for producerIdx := 0; producerIdx < numProducers; producerIdx++ {
//						producerIdx := producerIdx
//						producersErrGroup.Go(func() error {
//							return runProducer(producerIdx)
//						})
//					}
//					close(startProducersChan)
//					err := producersErrGroup.Wait()
//					require.NoError(t, err)
//				}
//
//				// emulate delay when consuming the slow queries
//				consumeFunc := func(request Request) error {
//					schedulerRequest := request.(*SchedulerRequest)
//					if schedulerRequest.AdditionalQueueDimensions[0] == slowConsumerQueueDimension {
//						time.Sleep(slowConsumerLatency)
//					}
//
//					queueTime := time.Since(schedulerRequest.EnqueueTime)
//					additionalQueueDimensionLabels := strings.Join(schedulerRequest.AdditionalQueueDimensions, ":")
//					queueDuration.With(prometheus.Labels{"additional_queue_dimensions": additionalQueueDimensionLabels}).Add(queueTime.Seconds())
//					return nil
//				}
//
//				// consume queries
//				queueConsumerErrGroup, ctx := errgroup.WithContext(ctx)
//				startConsumersChan := make(chan struct{})
//				runConsumer := runQueueConsumerIters(ctx, queue, totalRequests, numConsumers, startConsumersChan, consumeFunc)
//
//				for consumerIdx := 0; consumerIdx < numConsumers; consumerIdx++ {
//					consumerIdx := consumerIdx
//					queueConsumerErrGroup.Go(func() error {
//						return runConsumer(consumerIdx)
//					})
//				}
//
//				close(startConsumersChan)
//				err = queueConsumerErrGroup.Wait()
//				require.NoError(t, err)
//
//				// record total queue duration by queue dimensions and whether the queue splitting was enabled
//				for _, queueDimension := range []string{normalQueueDimension, slowConsumerQueueDimension} {
//					queueDurationTotals[multipleDimensionsUsed][queueDimension] = promtest.ToFloat64(
//						queueDuration.With(prometheus.Labels{"additional_queue_dimensions": queueDimension}),
//					)
//				}
//
//				promRegistry.Unregister(queueDuration)
//			}
//
//			// total or average time in queue for a normal queue item should be roughly cut in half
//			// when queue splitting is enabled, as the average normal queue item waits behind
//			// half of the slow queue items, instead of waiting behind all the slow queue items.
//			expected := queueDurationTotals[false][normalQueueDimension] / 2
//			actual := queueDurationTotals[true][normalQueueDimension]
//			// some variance allowed due to actual time processing needed beyond the slow consumer delay;
//			// variance is also a function of the number of consumers and the consumer delay chosen.
//			// variance can be tighter if the test runs longer but there is a tradeoff for testing and CI speed
//			delta := expected * 0.10
//			require.InDelta(t, expected, actual, delta)
//		})
//	}
//
//}

func BenchmarkConcurrentQueueOperations(b *testing.B) {
	treeTypes := buildTreeTestsStruct()

	for _, t := range treeTypes {
		b.Run(t.name, func(b *testing.B) {
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
									queue, err := NewRequestQueue(
										log.NewNopLogger(),
										maxOutstandingRequestsPerTenant,
										t.useMultiAlgoTreeQueue,
										forgetQuerierDelay,
										promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
										promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
										promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
										promauto.With(nil).NewSummaryVec(prometheus.SummaryOpts{}, []string{"query_component"}),
									)
									require.NoError(b, err)

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
									err = queueActorsErrGroup.Wait()
									if err != nil {
										require.NoError(b, err)
									}
								})
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
	additionalQueueDimensionFunc func(tenantID string) []string,
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
	queue *RequestQueue,
	maxQueriersPerTenant int,
	tenantID string,
	additionalQueueDimensionFunc func(tenantID string) []string,
) error {
	var additionalQueueDimensions []string
	if additionalQueueDimensionFunc != nil {
		additionalQueueDimensions = additionalQueueDimensionFunc(tenantID)
	}
	req := makeSchedulerRequest(tenantID, additionalQueueDimensions)
	for {
		err := queue.SubmitRequestToEnqueue(tenantID, req, maxQueriersPerTenant, func() {})
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
		querierWorkerConn := NewUnregisteredQuerierWorkerConn(context.Background(), QuerierID(querierID))
		err := queue.AwaitRegisterQuerierWorkerConn(querierWorkerConn)
		if err != nil {
			return err
		}
		defer queue.SubmitUnregisterQuerierWorkerConn(querierWorkerConn)

		<-start

		for i := 0; i < consumerIters; i++ {
			idx, err := queueConsume(ctx, queue, querierID, consumerIdx, lastTenantIndex, consumeFunc)
			if err != nil {
				return err
			}

			lastTenantIndex = idx
		}

		return nil
	}
}

func makeQueueConsumerGroup(
	ctx context.Context,
	queue *RequestQueue,
	totalRequests int,
	numConsumers int,
	consumeFunc consumeRequest,
) (*errgroup.Group, chan struct{}) {
	queueConsumerErrGroup, ctx := errgroup.WithContext(ctx)
	consumedRequestsCounter := make(chan struct{}, totalRequests)
	startConsumersChan := make(chan struct{})
	stopConsumersChan := make(chan struct{})
	runConsumer := runQueueConsumerUntilEmpty(ctx, totalRequests, queue, consumeFunc, consumedRequestsCounter, startConsumersChan, stopConsumersChan)

	for consumerIdx := 0; consumerIdx < numConsumers; consumerIdx++ {
		consumerIdx := consumerIdx
		queueConsumerErrGroup.Go(func() error {
			return runConsumer(consumerIdx)
		})
	}
	return queueConsumerErrGroup, startConsumersChan
}

func runQueueConsumerUntilEmpty(
	ctx context.Context,
	totalRequests int,
	requestQueue *RequestQueue,
	consumeFunc consumeRequest,
	consumedRequestsCounter chan struct{},
	start chan struct{},
	stop chan struct{},
) func(consumerIdx int) error {
	return func(consumerIdx int) error {
		lastTenantIndex := FirstTenant()
		querierID := fmt.Sprintf("consumer-%v", consumerIdx)
		querierWorkerConn := NewUnregisteredQuerierWorkerConn(context.Background(), QuerierID(querierID))
		err := requestQueue.AwaitRegisterQuerierWorkerConn(querierWorkerConn)
		if err != nil {
			return err
		}
		defer requestQueue.SubmitUnregisterQuerierWorkerConn(querierWorkerConn)

		consumedRequest := make(chan struct{})
		loopQueueConsume := func() error {
			for {
				idx, err := queueConsume(ctx, requestQueue, querierID, consumerIdx, lastTenantIndex, consumeFunc)
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

type consumeRequest func(request Request) error

func queueConsume(
	ctx context.Context, queue *RequestQueue, querierID string, workerID int, lastTenantIndex TenantIndex, consumeFunc consumeRequest,
) (TenantIndex, error) {
	request, idx, err := queue.WaitForRequestForQuerier(ctx, lastTenantIndex, querierID, workerID)
	if err != nil {
		return lastTenantIndex, err
	}
	lastTenantIndex = idx

	if consumeFunc != nil {
		err = consumeFunc(request)
	}
	return lastTenantIndex, err
}

func TestRequestQueue_RegisterAndUnregisterQuerierWorkerConnections(t *testing.T) {
	const forgetDelay = 3 * time.Second

	treeTypes := buildTreeTestsStruct()
	for _, tt := range treeTypes {
		t.Run(tt.name, func(t *testing.T) {
			queue, err := NewRequestQueue(
				log.NewNopLogger(),
				1,
				tt.useMultiAlgoTreeQueue,
				forgetDelay,
				promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
				promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
				promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
				promauto.With(nil).NewSummaryVec(prometheus.SummaryOpts{}, []string{"query_component"}),
			)
			require.NoError(t, err)

			// start the queue service.
			ctx := context.Background()
			require.NoError(t, services.StartAndAwaitRunning(ctx, queue))

			t.Cleanup(func() {
				// we must send a shutdown signal for any remaining connected queriers
				// or else StopAndAwaitTerminated will never complete.
				queue.SubmitNotifyQuerierShutdown(ctx, "querier-1")
				queue.SubmitNotifyQuerierShutdown(ctx, "querier-2")
				require.NoError(t, services.StopAndAwaitTerminated(ctx, queue))
			})

			// 2 queriers open 3 connections each.
			querier1Conn1 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-1")
			require.NoError(t, queue.AwaitRegisterQuerierWorkerConn(querier1Conn1))
			require.Equal(t, 0, querier1Conn1.WorkerID)
			require.Equal(t, 1, int(queue.connectedQuerierWorkers.Load()))

			querier1Conn2 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-1")
			require.NoError(t, queue.AwaitRegisterQuerierWorkerConn(querier1Conn2))
			require.Equal(t, 1, querier1Conn2.WorkerID)
			require.Equal(t, 2, int(queue.connectedQuerierWorkers.Load()))

			querier1Conn3 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-1")
			require.NoError(t, queue.AwaitRegisterQuerierWorkerConn(querier1Conn3))
			require.Equal(t, 2, querier1Conn3.WorkerID)
			require.Equal(t, 3, int(queue.connectedQuerierWorkers.Load()))

			querier2Conn1 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-2")
			require.NoError(t, queue.AwaitRegisterQuerierWorkerConn(querier2Conn1))
			require.Equal(t, 0, querier2Conn1.WorkerID)
			require.Equal(t, 4, int(queue.connectedQuerierWorkers.Load()))

			querier2Conn2 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-2")
			require.NoError(t, queue.AwaitRegisterQuerierWorkerConn(querier2Conn2))
			require.Equal(t, 1, querier2Conn2.WorkerID)
			require.Equal(t, 5, int(queue.connectedQuerierWorkers.Load()))

			querier2Conn3 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-2")
			require.NoError(t, queue.AwaitRegisterQuerierWorkerConn(querier2Conn3))
			require.Equal(t, 2, querier2Conn3.WorkerID)
			require.Equal(t, 6, int(queue.connectedQuerierWorkers.Load()))

			// if querier-worker disconnects and reconnects before any other querier-worker changes,
			// the querier-worker connect will get its same worker ID back
			queue.SubmitUnregisterQuerierWorkerConn(querier2Conn2)
			require.NoError(t, queue.AwaitRegisterQuerierWorkerConn(querier2Conn2))
			require.Equal(t, 1, querier2Conn2.WorkerID)
			require.Equal(t, 6, int(queue.connectedQuerierWorkers.Load()))

			// if a querier-worker disconnects and another querier-worker connects before the first reconnects
			// the second querier-worker will have taken the worker ID of the first querier-worker,
			// and the first querier-worker will get issued a new worker ID

			// even though some operations are awaited
			// and some are just submitted without waiting for completion,
			// all querier-worker operations are processed in the order of the submit/await calls.
			queue.SubmitUnregisterQuerierWorkerConn(querier1Conn2)
			// we cannot be sure the worker ID is unregistered yet,
			// but once we await the next worker register call, we can be sure.
			querier1Conn4 := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-1")
			require.NoError(t, queue.AwaitRegisterQuerierWorkerConn(querier1Conn4))
			require.False(t, querier1Conn2.IsRegistered())
			require.Equal(t, 1, querier1Conn4.WorkerID)
			require.Equal(t, 6, int(queue.connectedQuerierWorkers.Load()))
			// re-connect from the first querier-worker and get a completely new worker ID
			require.NoError(t, queue.AwaitRegisterQuerierWorkerConn(querier1Conn2))
			require.Equal(t, 3, querier1Conn2.WorkerID)
			require.Equal(t, 7, int(queue.connectedQuerierWorkers.Load()))
		})
	}
}

func TestRequestQueue_GetNextRequestForQuerier_ShouldGetRequestAfterReshardingBecauseQuerierHasBeenForgotten(t *testing.T) {
	const forgetDelay = 3 * time.Second
	const testTimeout = 10 * time.Second

	treeTypes := buildTreeTestsStruct()

	for _, tt := range treeTypes {
		t.Run(tt.name, func(t *testing.T) {
			queue, err := NewRequestQueue(
				log.NewNopLogger(),
				1,
				tt.useMultiAlgoTreeQueue,
				forgetDelay,
				promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
				promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
				promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
				promauto.With(nil).NewSummaryVec(prometheus.SummaryOpts{}, []string{"query_component"}),
			)
			require.NoError(t, err)

			// Start the queue service.
			ctx := context.Background()
			require.NoError(t, services.StartAndAwaitRunning(ctx, queue))

			// Two queriers connect.
			querier1Conn := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-1")
			require.NoError(t, queue.AwaitRegisterQuerierWorkerConn(querier1Conn))
			querier2Conn := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-2")
			require.NoError(t, queue.AwaitRegisterQuerierWorkerConn(querier2Conn))

			t.Cleanup(func() {
				// if the test has failed and the queue does not get cleared,
				// we must send a shutdown signal for the remaining connected querier
				// or else StopAndAwaitTerminated will never complete.
				queue.SubmitUnregisterQuerierWorkerConn(querier2Conn)
				require.NoError(t, services.StopAndAwaitTerminated(ctx, queue))
			})

			// Querier-2 waits for a new request.
			querier2wg := sync.WaitGroup{}
			querier2wg.Add(1)
			go func() {
				defer querier2wg.Done()
				_, _, err := queue.WaitForRequestForQuerier(ctx, FirstTenant(), "querier-2", 0)
				require.NoError(t, err)
			}()

			// Querier-1 crashes (no graceful shutdown notification).
			queue.SubmitUnregisterQuerierWorkerConn(querier1Conn)

			// Enqueue a request from an user which would be assigned to querier-1.
			// NOTE: "user-1" shuffle shard always chooses the first querier ("querier-1" in this case)
			// when there are only one or two queriers in the sorted list of connected queriers
			req := &SchedulerRequest{
				Ctx:                       context.Background(),
				Request:                   &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
				AdditionalQueueDimensions: randAdditionalQueueDimension(true),
			}
			require.NoError(t, queue.SubmitRequestToEnqueue("user-1", req, 1, nil))

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

		})
	}
}

func TestRequestQueue_GetNextRequestForQuerier_ReshardNotifiedCorrectlyForMultipleQuerierForget(t *testing.T) {
	const forgetDelay = 3 * time.Second
	const testTimeout = 10 * time.Second
	treeTypes := buildTreeTestsStruct()

	for _, tt := range treeTypes {
		t.Run(tt.name, func(t *testing.T) {
			queue, err := NewRequestQueue(
				log.NewNopLogger(),
				1,
				tt.useMultiAlgoTreeQueue,
				forgetDelay,
				promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
				promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
				promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
				promauto.With(nil).NewSummaryVec(prometheus.SummaryOpts{}, []string{"query_component"}),
			)
			require.NoError(t, err)

			// Start the queue service.
			ctx := context.Background()
			require.NoError(t, services.StartAndAwaitRunning(ctx, queue))

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
			// Two queriers connect.
			querier1Conn := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-1")
			require.NoError(t, queue.AwaitRegisterQuerierWorkerConn(querier1Conn))
			querier2Conn := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-2")
			require.NoError(t, queue.AwaitRegisterQuerierWorkerConn(querier2Conn))
			querier3Conn := NewUnregisteredQuerierWorkerConn(context.Background(), "querier-3")
			require.NoError(t, queue.AwaitRegisterQuerierWorkerConn(querier3Conn))

			t.Cleanup(func() {
				// if the test has failed and the queue does not get cleared,
				// we must send a shutdown signal for the remaining connected querier
				// or else StopAndAwaitTerminated will never complete.
				queue.SubmitUnregisterQuerierWorkerConn(querier2Conn)
				require.NoError(t, services.StopAndAwaitTerminated(ctx, queue))
			})

			// querier-2 waits for a new request.
			querier2wg := sync.WaitGroup{}
			querier2wg.Add(1)
			go func() {
				defer querier2wg.Done()
				_, _, err := queue.WaitForRequestForQuerier(ctx, FirstTenant(), "querier-2", 0)
				require.NoError(t, err)
			}()

			// querier-1 and querier-3 crash (no graceful shutdown notification).
			queue.SubmitUnregisterQuerierWorkerConn(querier1Conn)
			queue.SubmitUnregisterQuerierWorkerConn(querier3Conn)

			// Enqueue a request from a tenant which would be assigned to querier-1.
			// NOTE: "user-1" shuffle shard always chooses the first querier ("querier-1" in this case)
			// when there are only one or two queriers in the sorted list of connected queriers
			req := &SchedulerRequest{
				Ctx:                       context.Background(),
				Request:                   &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
				AdditionalQueueDimensions: randAdditionalQueueDimension(true),
			}
			require.NoError(t, queue.SubmitRequestToEnqueue("user-1", req, 2, nil))

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

		})
	}
}

func TestRequestQueue_GetNextRequestForQuerier_ShouldReturnAfterContextCancelled(t *testing.T) {
	const forgetDelay = 3 * time.Second
	const querierID = "querier-1"
	treeTypes := buildTreeTestsStruct()

	for _, tt := range treeTypes {
		t.Run(tt.name, func(t *testing.T) {
			queue, err := NewRequestQueue(
				log.NewNopLogger(),
				1,
				tt.useMultiAlgoTreeQueue,
				forgetDelay,
				promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
				promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
				promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
				promauto.With(nil).NewSummaryVec(prometheus.SummaryOpts{}, []string{"query_component"}),
			)
			require.NoError(t, err)

			require.NoError(t, services.StartAndAwaitRunning(context.Background(), queue))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), queue))
			})

			querier1Conn := NewUnregisteredQuerierWorkerConn(context.Background(), querierID)
			require.NoError(t, queue.AwaitRegisterQuerierWorkerConn(querier1Conn))

			// Calling WaitForRequestForQuerier with a context that is already cancelled should fail immediately.
			deadCtx, cancel := context.WithCancel(context.Background())
			cancel()
			r, tenant, err := queue.WaitForRequestForQuerier(deadCtx, FirstTenant(), querierID, 0)
			assert.Nil(t, r)
			assert.Equal(t, FirstTenant(), tenant)
			assert.ErrorIs(t, err, context.Canceled)

			// Further, a context canceled after WaitForRequestForQuerier publishes a request should also fail.
			errChan := make(chan error)
			ctx, cancel := context.WithCancel(context.Background())

			go func() {
				_, _, err := queue.WaitForRequestForQuerier(ctx, FirstTenant(), querierID, 0)
				errChan <- err
			}()

			time.Sleep(20 * time.Millisecond) // Wait for WaitForRequestForQuerier to be waiting for a query.
			cancel()

			select {
			case err := <-errChan:
				require.Equal(t, context.Canceled, err)
			case <-time.After(time.Second):
				require.Fail(t, "gave up waiting for GetNextRequestForQuerierToReturn")
			}

		})
	}

}

func TestRequestQueue_GetNextRequestForQuerier_ShouldReturnImmediatelyIfQuerierIsAlreadyShuttingDown(t *testing.T) {
	const forgetDelay = 3 * time.Second
	const querierID = "querier-1"

	treeTypes := buildTreeTestsStruct()

	for _, tt := range treeTypes {
		t.Run(tt.name, func(t *testing.T) {
			queue, err := NewRequestQueue(
				log.NewNopLogger(),
				1,
				tt.useMultiAlgoTreeQueue,
				forgetDelay,
				promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
				promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
				promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
				promauto.With(nil).NewSummaryVec(prometheus.SummaryOpts{}, []string{"query_component"}),
			)
			require.NoError(t, err)

			ctx := context.Background()
			require.NoError(t, services.StartAndAwaitRunning(ctx, queue))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, queue))
			})

			querierConn := NewUnregisteredQuerierWorkerConn(context.Background(), querierID)
			require.NoError(t, queue.AwaitRegisterQuerierWorkerConn(querierConn))

			queue.SubmitNotifyQuerierShutdown(ctx, querierID)

			_, _, err = queue.WaitForRequestForQuerier(context.Background(), FirstTenant(), querierID, 0)
			require.EqualError(t, err, "querier has informed the scheduler it is shutting down")
		})
	}

}

func TestRequestQueue_tryDispatchRequestToQuerier_ShouldReEnqueueAfterFailedSendToQuerier(t *testing.T) {
	const forgetDelay = 3 * time.Second
	const querierID = "querier-1"

	treeTypes := buildTreeTestsStruct()
	for _, tt := range treeTypes {
		t.Run(tt.name, func(t *testing.T) {
			queue, err := NewRequestQueue(
				log.NewNopLogger(),
				1,
				tt.useMultiAlgoTreeQueue,
				forgetDelay,
				promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
				promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
				promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
				promauto.With(nil).NewSummaryVec(prometheus.SummaryOpts{}, []string{"query_component"}),
			)
			require.NoError(t, err)

			// bypassing queue dispatcher loop for direct usage of the queueBroker and
			// passing a waitingQuerierConn for a canceled querier connection
			queueBroker := newQueueBroker(queue.maxOutstandingPerTenant, false, queue.forgetDelay)
			queueBroker.addQuerierWorkerConn(NewUnregisteredQuerierWorkerConn(context.Background(), querierID))

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

			// TODO (casie): Clean this up when deprecating legacy tree queue
			if tq, ok := queueBroker.tree.(*TreeQueue); ok {
				require.Nil(t, tq.getNode(QueuePath{"tenant-1"}))
				require.NoError(t, queueBroker.enqueueRequestBack(&tr, tenantMaxQueriers))
				require.False(t, tq.getNode(QueuePath{"tenant-1"}).IsEmpty())
			} else if itq, ok := queueBroker.tree.(*MultiQueuingAlgorithmTreeQueue); ok {
				require.Nil(t, itq.GetNode(QueuePath{"tenant-1"}))
				require.NoError(t, queueBroker.enqueueRequestBack(&tr, tenantMaxQueriers))
				require.False(t, itq.GetNode(QueuePath{"tenant-1"}).IsEmpty())
			}

			ctx, cancel := context.WithCancel(context.Background())
			call := &waitingQuerierConn{
				querierConnCtx:  ctx,
				querierID:       QuerierID(querierID),
				lastTenantIndex: FirstTenant(),
				recvChan:        make(chan requestForQuerier),
			}
			cancel() // ensure querier context done before send is attempted

			// send to querier will fail but method returns true,
			// indicating not to re-submit a request for waitingQuerierConn for the querier
			require.True(t, queue.trySendNextRequestForQuerier(call))
			// assert request was re-enqueued for tenant after failed send
			// TODO (casie): Clean this up when deprecating legacy tree queue
			if tq, ok := queueBroker.tree.(*TreeQueue); ok {
				require.False(t, tq.getNode(QueuePath{"tenant-1"}).IsEmpty())
			} else if itq, ok := queueBroker.tree.(*MultiQueuingAlgorithmTreeQueue); ok {
				require.False(t, itq.GetNode(QueuePath{"tenant-1"}).IsEmpty())
			}

		})
	}

}
