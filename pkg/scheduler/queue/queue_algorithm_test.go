// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"container/list"
	"context"
	"fmt"
	"math/rand"
	"strconv"
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

const querierForgetDelay = 0
const maxOutStandingPerTenant = 1000

func newTQA() *tenantQuerierAssignments {
	currentQuerier := QuerierID("")
	return &tenantQuerierAssignments{
		queriersByID:       map[QuerierID]*querierConn{},
		querierIDsSorted:   nil,
		querierForgetDelay: querierForgetDelay,
		tenantIDOrder:      nil,
		tenantsByID:        map[TenantID]*queueTenant{},
		tenantQuerierIDs:   map[TenantID]map[QuerierID]struct{}{},
		tenantNodes:        map[string][]*Node{},
		currentQuerier:     currentQuerier,
		tenantOrderIndex:   localQueueIndex,
	}
}

func newBenchmarkRequestQueue(
	queryComponentUtilization *QueryComponentUtilization,
	tenantQuerierAssignments *tenantQuerierAssignments,
	tree Tree,
) (*RequestQueue, error) {

	q := &RequestQueue{
		// settings
		log:                              log.NewNopLogger(),
		maxOutstandingPerTenant:          maxOutStandingPerTenant,
		additionalQueueDimensionsEnabled: true,
		forgetDelay:                      time.Duration(querierForgetDelay),

		// metrics for reporting
		connectedQuerierWorkers: atomic.NewInt64(0),
		queueLength:             promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
		discardedRequests:       promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
		enqueueDuration:         promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),

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
			maxTenantQueueSize:               maxOutStandingPerTenant,
			additionalQueueDimensionsEnabled: true,
			prioritizeQueryComponents:        true,
		},
	}
	q.Service = services.NewBasicService(q.starting, q.running, q.stop).WithName("request queue")
	return q, nil
}

type dimensionWeight struct {
	dimension string
	weight    int
}

func weightedRandAdditionalQueueDimension(dimensionWeights []dimensionWeight) string {
	totalWeight := 0
	for _, dimensionWeight := range dimensionWeights {
		totalWeight += dimensionWeight.weight
	}

	randInt := rand.Intn(totalWeight)

	sum := 0
	for _, dimensionWeight := range dimensionWeights {
		sum += dimensionWeight.weight
		if randInt < sum {
			return dimensionWeight.dimension
		}
	}

	panic("no dimension selected")
}

func makeWeightedRandAdditionalQueueDimensionFunc(
	tenantDimensionWeights map[string][]dimensionWeight,
) func(tenantID string) []string {
	return func(tenantID string) []string {
		return []string{weightedRandAdditionalQueueDimension(tenantDimensionWeights[tenantID])}
	}
}

//
//type QueryComponentUtilizationCheckThresholdQueueAlgorithmBenchmarkScenario struct {
//	utilizationTriggerCheckQueueLenMultiple   int
//	utilizationCheckThresholdReservedCapacity float64
//}

func makeQueueProducerGroup(
	queue *RequestQueue,
	maxQueriersPerTenant int,
	totalRequests int,
	numProducers int,
	numTenants int,
	queueDimensionFunc func(string) []string,
) (chan struct{}, *errgroup.Group) {
	startProducersChan := make(chan struct{})
	producersErrGroup, _ := errgroup.WithContext(context.Background())

	runProducer := runQueueProducerIters(
		queue, maxQueriersPerTenant, totalRequests, numProducers, numTenants, startProducersChan, queueDimensionFunc,
	)
	for producerIdx := 0; producerIdx < numProducers; producerIdx++ {
		producerIdx := producerIdx
		producersErrGroup.Go(func() error {
			return runProducer(producerIdx)
		})
	}
	return startProducersChan, producersErrGroup
}

func makeQueueConsumeFunc(
	queue *RequestQueue,
	queueDuration *prometheus.CounterVec,
	slowConsumerQueueDimension string,
	slowConsumerLatency time.Duration,
) consumeRequest {
	return func(request Request) error {
		schedulerRequest := request.(*SchedulerRequest)
		queryComponent := schedulerRequest.ExpectedQueryComponentName()

		queue.QueryComponentUtilization.MarkRequestSent(schedulerRequest)
		if queryComponent == slowConsumerQueueDimension {
			time.Sleep(slowConsumerLatency)
		}
		queue.QueryComponentUtilization.MarkRequestCompleted(schedulerRequest)

		counterLabels := prometheus.Labels{
			"user":            schedulerRequest.UserID,
			"query_component": queryComponent,
		}
		queueTime := time.Since(schedulerRequest.EnqueueTime)
		queueDuration.With(counterLabels).Add(queueTime.Seconds())
		return nil
	}
}

func makeQueueConsumerGroup(queue *RequestQueue, totalRequests int, numConsumers int, consumeFunc consumeRequest) (*errgroup.Group, chan struct{}) {
	queueConsumerErrGroup, ctx := errgroup.WithContext(context.Background())
	startConsumersChan := make(chan struct{})
	runConsumer := runQueueConsumerIters(ctx, queue, totalRequests, numConsumers, startConsumersChan, consumeFunc)

	for consumerIdx := 0; consumerIdx < numConsumers; consumerIdx++ {
		consumerIdx := consumerIdx
		queueConsumerErrGroup.Go(func() error {
			return runConsumer(consumerIdx)
		})
	}
	return queueConsumerErrGroup, startConsumersChan
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
func TestMultiDimensionalQueueAlgorithmSlowConsumerEffects(t *testing.T) {

	testCases := []struct {
		name                         string
		tenantQueueDimensionsWeights map[string][]dimensionWeight
	}{
		{
			name: "single tenant, 1% slow queries",
			tenantQueueDimensionsWeights: map[string][]dimensionWeight{
				"0": {
					{ingesterQueueDimension, 99},
					{storeGatewayQueueDimension, 1},
				},
			},
		},
		{
			name: "single tenant, 5% slow queries",
			tenantQueueDimensionsWeights: map[string][]dimensionWeight{
				"0": {
					{ingesterQueueDimension, 95},
					{storeGatewayQueueDimension, 5},
				},
			},
		},
		{
			name: "single tenant, 10% slow queries",
			tenantQueueDimensionsWeights: map[string][]dimensionWeight{
				"0": {
					{ingesterQueueDimension, 90},
					{storeGatewayQueueDimension, 10},
				},
			},
		},
		{
			name: "single tenant, 25% slow queries",
			tenantQueueDimensionsWeights: map[string][]dimensionWeight{
				"0": {
					{ingesterQueueDimension, 75},
					{storeGatewayQueueDimension, 25},
				},
			},
		},
		{
			name: "single tenant, 50% slow queries",
			tenantQueueDimensionsWeights: map[string][]dimensionWeight{
				"0": {
					{ingesterQueueDimension, 50},
					{storeGatewayQueueDimension, 50},
				},
			},
		},
		{
			name: "single tenant, 75% slow queries",
			tenantQueueDimensionsWeights: map[string][]dimensionWeight{
				"0": {
					{ingesterQueueDimension, 25},
					{storeGatewayQueueDimension, 75},
				},
			},
		},
		{
			name: "single tenant, 90% slow queries",
			tenantQueueDimensionsWeights: map[string][]dimensionWeight{
				"0": {
					{ingesterQueueDimension, 10},
					{storeGatewayQueueDimension, 90},
				},
			},
		},
		{
			name: "single tenant, 95% slow queries",
			tenantQueueDimensionsWeights: map[string][]dimensionWeight{
				"0": {
					{ingesterQueueDimension, 5},
					{storeGatewayQueueDimension, 95},
				},
			},
		},
		{
			name: "single tenant, 99% slow queries",
			tenantQueueDimensionsWeights: map[string][]dimensionWeight{
				"0": {
					{ingesterQueueDimension, 1},
					{storeGatewayQueueDimension, 99},
				},
			},
		},
	}

	maxQueriersPerTenant := 0 // disable shuffle sharding

	totalRequests := 1000
	numTenants := 1
	numProducers := 10
	numConsumers := 10

	normalQueueDimension := ingesterQueueDimension
	slowConsumerLatency := 200 * time.Millisecond
	slowConsumerQueueDimension := storeGatewayQueueDimension

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// with 10 connected workers, if queue len >= waiting workers,
			// no component can have more than 6 inflight requests.
			testReservedCapacity := 0.4

			var err error
			queryComponentUtilization, err := NewQueryComponentUtilization(testQuerierInflightRequestsGauge())
			require.NoError(t, err)

			utilizationTriggerCheckImpl := NewQueryComponentUtilizationTriggerCheckByQueueLenAndWaitingConns(1)
			utilizationCheckThresholdImpl, err := NewQueryComponentUtilizationReserveConnections(
				queryComponentUtilization, testReservedCapacity,
			)
			require.NoError(t, err)

			queryComponentUtilizationQueueAlgo := queryComponentUtilizationDequeueSkipOverThreshold{
				queryComponentUtilizationTriggerCheck:   utilizationTriggerCheckImpl,
				queryComponentUtilizationCheckThreshold: utilizationCheckThresholdImpl,
				currentNodeOrderIndex:                   -1,
			}
			tqa := newTQA()
			tree, err := NewTree(&queryComponentUtilizationQueueAlgo, tqa, &roundRobinState{})
			require.NoError(t, err)

			promRegistry := prometheus.NewPedanticRegistry()

			queueDimensionFunc := makeWeightedRandAdditionalQueueDimensionFunc(
				testCase.tenantQueueDimensionsWeights,
			)

			// Scheduler code uses a histogram for queue duration, but a counter is a more direct metric
			// for this test, as we are concerned with the total or average wait time for all queue items.
			// Prometheus histograms also lack support for test assertions via prometheus/testutil.
			queueDuration := promauto.With(promRegistry).NewCounterVec(prometheus.CounterOpts{
				Name: "test_query_scheduler_queue_duration_total_seconds",
				Help: "[test] total time spent by items in queue before getting picked up by a consumer",
			}, []string{"user", "query_component"})

			queue, err := newBenchmarkRequestQueue(queryComponentUtilization, tqa, tree)
			require.NoError(t, err)

			ctx := context.Background()
			require.NoError(t, queue.starting(ctx))
			t.Cleanup(func() {
				require.NoError(t, queue.stop(nil))
			})

			// set up queue producers
			startProducersChan, producersErrGroup := makeQueueProducerGroup(
				queue, maxQueriersPerTenant, totalRequests, numProducers, numTenants, queueDimensionFunc,
			)
			// configure queue consumers with sleep for processing slow consumer queue items
			consumeFunc := makeQueueConsumeFunc(
				queue, queueDuration, slowConsumerQueueDimension, slowConsumerLatency,
			)
			// set up queue consumers to be started by closing startConsumersChan
			queueConsumerErrGroup, startConsumersChan := makeQueueConsumerGroup(
				queue, totalRequests, numConsumers, consumeFunc,
			)

			// run producers to fill queue
			close(startProducersChan)
			err = producersErrGroup.Wait()
			require.NoError(t, err)

			// start queue consumers and wait for completion
			close(startConsumersChan)
			err = queueConsumerErrGroup.Wait()
			require.NoError(t, err)

			// record total queue duration by queue dimensions and whether the queue splitting was enabled
			queueDurationTotals := map[string]map[string]float64{}
			for tenantIdx := 0; tenantIdx < numTenants; tenantIdx++ {
				tenantID := strconv.Itoa(tenantIdx)
				queueDurationTotals[tenantID] = map[string]float64{}
				for _, queueDimension := range []string{normalQueueDimension, slowConsumerQueueDimension} {
					counterLabels := prometheus.Labels{"user": tenantID, "query_component": queueDimension}
					queueDurationTotals[tenantID][queueDimension] = promtest.ToFloat64(queueDuration.With(counterLabels))
				}
			}

			promRegistry.Unregister(queueDuration)

			fmt.Println(queueDurationTotals)
		})
	}
}
