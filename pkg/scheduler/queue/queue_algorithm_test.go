// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"container/list"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
)

func newTQA() *tenantQuerierAssignments {
	currentQuerier := QuerierID("")
	return &tenantQuerierAssignments{
		queriersByID:       map[QuerierID]*querierConn{},
		querierIDsSorted:   nil,
		querierForgetDelay: 0,
		tenantIDOrder:      nil,
		tenantsByID:        map[TenantID]*queueTenant{},
		tenantQuerierIDs:   map[TenantID]map[QuerierID]struct{}{},
		tenantNodes:        map[string][]*Node{},
		currentQuerier:     currentQuerier,
		tenantOrderIndex:   localQueueIndex,
	}
}

func newBenchmarkRequestQueue(log log.Logger,
	maxOutstandingPerTenant int,
	additionalQueueDimensionsEnabled bool,
	useMultiAlgoQueue bool,
	queryComponentUtilization *QueryComponentUtilization,
	tenantQuerierAssignments *tenantQuerierAssignments,
	tree Tree,
	forgetDelay time.Duration,
	queueLength *prometheus.GaugeVec,
	discardedRequests *prometheus.CounterVec,
	enqueueDuration prometheus.Histogram,
) (*RequestQueue, error) {

	q := &RequestQueue{
		// settings
		log:                              log,
		maxOutstandingPerTenant:          maxOutstandingPerTenant,
		additionalQueueDimensionsEnabled: additionalQueueDimensionsEnabled,
		forgetDelay:                      forgetDelay,

		// metrics for reporting
		connectedQuerierWorkers: atomic.NewInt64(0),
		queueLength:             queueLength,
		discardedRequests:       discardedRequests,
		enqueueDuration:         enqueueDuration,

		// channels must not be buffered so that we can detect when dispatcherLoop() has finished.
		stopRequested: make(chan struct{}),
		stopCompleted: make(chan struct{}),

		requestsToEnqueue:             make(chan requestToEnqueue),
		requestsSent:                  make(chan *SchedulerRequest),
		requestsCompleted:             make(chan *SchedulerRequest),
		querierOperations:             make(chan querierOperation),
		waitingQuerierConns:           make(chan *waitingQuerierConn),
		waitingQuerierConnsToDispatch: list.New(),

		QueryComponentUtilization: queryComponentUtilization,
		queueBroker: &queueBroker{
			tree:                             tree,
			tenantQuerierAssignments:         tenantQuerierAssignments,
			maxTenantQueueSize:               maxOutstandingPerTenant,
			additionalQueueDimensionsEnabled: true,
			prioritizeQueryComponents:        true,
		},
	}

	q.Service = services.NewBasicService(q.starting, q.running, q.stop).WithName("request queue")

	return q, nil
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
func TestMultiDimensionalQueueAlgorithmSlowConsumerEffects(t *testing.T) {
	connectedWorkers := 10
	// with 10 connected workers, if queue len >= waiting workers,
	// no component can have more than 6 inflight requests.
	testReservedCapacity := 0.4

	var err error
	queryComponentUtilization, err := NewQueryComponentUtilization(
		testReservedCapacity, testQuerierInflightRequestsGauge(),
	)
	require.NoError(t, err)

	utilizationCheckImpl := queryComponentUtilizationReserveConnections{
		utilization:      queryComponentUtilization,
		connectedWorkers: connectedWorkers,
		waitingWorkers:   1,
	}
	queryComponentUtilizationQueueAlgo := queryComponentUtilizationDequeueSkipOverThreshold{
		queryComponentUtilizationThreshold: &utilizationCheckImpl,
		currentNodeOrderIndex:              -1,
	}
	tqa := newTQA()
	tree, err := NewTree(&queryComponentUtilizationQueueAlgo, tqa, &roundRobinState{})
	require.NoError(t, err)

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

	queueDurationTotals := map[string]float64{
		normalQueueDimension: 0.0, slowConsumerQueueDimension: 0.0,
	}

	// Scheduler code uses a histogram for queue duration, but a counter is a more direct metric
	// for this test, as we are concerned with the total or average wait time for all queue items.
	// Prometheus histograms also lack support for test assertions via prometheus/testutil.
	queueDuration := promauto.With(promRegistry).NewCounterVec(prometheus.CounterOpts{
		Name: "test_query_scheduler_queue_duration_total_seconds",
		Help: "[test] total time spent by items in queue before getting picked up by a consumer",
	}, []string{"additional_queue_dimensions"})

	additionalQueueDimensionsEnabled := true
	useMultiAlgoQueue := true
	queue, err := newBenchmarkRequestQueue(
		log.NewNopLogger(),
		maxOutstandingRequestsPerTenant,
		additionalQueueDimensionsEnabled,
		useMultiAlgoQueue,
		queryComponentUtilization,
		tqa,
		tree,
		forgetQuerierDelay,
		promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
		promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
		promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
	)
	require.NoError(t, err)

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
	err = queueConsumerErrGroup.Wait()
	require.NoError(t, err)

	// record total queue duration by queue dimensions and whether the queue splitting was enabled
	for _, queueDimension := range []string{normalQueueDimension, slowConsumerQueueDimension} {
		queueDurationTotals[queueDimension] = promtest.ToFloat64(
			queueDuration.With(prometheus.Labels{"additional_queue_dimensions": queueDimension}),
		)
	}

	promRegistry.Unregister(queueDuration)

	fmt.Println(queueDurationTotals)
}
