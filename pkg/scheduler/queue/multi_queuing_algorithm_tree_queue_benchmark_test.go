// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

const querierForgetDelay = 0
const maxOutStandingPerTenant = 1000000

func weightedRandAdditionalQueueDimension(dimensionWeights map[string]float64) string {
	totalWeight := float64(0)
	for _, dimensionWeight := range dimensionWeights {
		totalWeight += dimensionWeight
	}
	roundTotalWeight := math.Round(totalWeight*10) / 10

	if roundTotalWeight != 1.0 {
		panic("dimension weights must sum to 1.0")
	}

	randInt := rand.Float64()

	sum := float64(0)
	for dimension, dimensionWeight := range dimensionWeights {
		sum += dimensionWeight
		if randInt < sum {
			return dimension
		}
	}

	panic("no dimension selected")
}

func makeWeightedRandAdditionalQueueDimensionFunc(
	tenantDimensionWeights map[string]map[string]float64,
) func(tenantID string) []string {
	return func(tenantID string) []string {
		return []string{weightedRandAdditionalQueueDimension(tenantDimensionWeights[tenantID])}
	}
}

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
		queue,
		maxQueriersPerTenant,
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

func makeQueueConsumeFuncWithSlowQueryComponent(
	queue *RequestQueue,
	slowConsumerQueueDimension string,
	slowConsumerLatency time.Duration,
	normalConsumerLatency time.Duration,
	report *testScenarioQueueDurationObservations,
) consumeRequest {
	return func(request Request) error {
		schedulerRequest := request.(*SchedulerRequest)
		queryComponent := schedulerRequest.ExpectedQueryComponentName()
		if queryComponent == ingesterAndStoreGatewayQueueDimension {
			queryComponent = storeGatewayQueueDimension
		}
		report.Observe(schedulerRequest.UserID, queryComponent, time.Since(schedulerRequest.EnqueueTime).Seconds())

		queue.QueryComponentUtilization.MarkRequestSent(schedulerRequest)
		if queryComponent == slowConsumerQueueDimension {
			time.Sleep(slowConsumerLatency)
		} else {
			time.Sleep(normalConsumerLatency)
		}

		queue.QueryComponentUtilization.MarkRequestCompleted(schedulerRequest)
		return nil
	}
}

type testScenarioQueueDurationObservations struct {
	testCaseName                            string
	mu                                      sync.Mutex
	tenantIDQueueDurationObservations       map[string][]float64
	queryComponentQueueDurationObservations map[string][]float64
}

func (o *testScenarioQueueDurationObservations) Observe(tenantID, queryComponent string, queueDuration float64) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.tenantIDQueueDurationObservations[tenantID] = append(
		o.tenantIDQueueDurationObservations[tenantID], queueDuration,
	)
	o.queryComponentQueueDurationObservations[queryComponent] = append(
		o.queryComponentQueueDurationObservations[queryComponent], queueDuration,
	)
}

func (o *testScenarioQueueDurationObservations) Report() *testScenarioQueueDurationReport {
	var tenantIDs []string
	for tenantID := range o.tenantIDQueueDurationObservations {
		tenantIDs = append(tenantIDs, tenantID)
	}
	slices.Sort(tenantIDs)
	var queryComponents []string
	for queryComponent := range o.queryComponentQueueDurationObservations {
		queryComponents = append(queryComponents, queryComponent)
	}
	slices.Sort(queryComponents)

	report := &testScenarioQueueDurationReport{
		testCaseName: o.testCaseName,
	}

	for _, queryComponent := range queryComponents {
		meanDur := mean(o.queryComponentQueueDurationObservations[queryComponent])
		stdDevDur := stddev(o.queryComponentQueueDurationObservations[queryComponent], meanDur)
		report.queryComponentReports = append(
			report.queryComponentReports,
			testDimensionReport{
				dimensionName:        queryComponent,
				meanSecondsInQueue:   meanDur,
				stdDevSecondsInQueue: stdDevDur,
			},
		)
	}
	return report
}

type testScenarioQueueDurationReport struct {
	testCaseName          string
	queryComponentReports []testDimensionReport
}

func (r *testScenarioQueueDurationReport) String() string {
	var queryComponentReports []string
	for _, queryComponentReport := range r.queryComponentReports {
		queryComponentReports = append(
			queryComponentReports,
			fmt.Sprintf(
				"%s: mean: %.4f stddev: %.2f",
				queryComponentReport.dimensionName,
				queryComponentReport.meanSecondsInQueue,
				queryComponentReport.stdDevSecondsInQueue),
		)
	}
	return fmt.Sprintf(
		"seconds in queue: %v", queryComponentReports,
	)
}

type testDimensionReport struct {
	dimensionName        string
	meanSecondsInQueue   float64
	stdDevSecondsInQueue float64
}

func mean(numbers []float64) float64 {
	sum := 0.0
	for _, number := range numbers {
		sum += number
	}
	return sum / float64(len(numbers))
}

func stddev(numbers []float64, mean float64) float64 {
	sumOfSquares := 0.0
	for _, number := range numbers {
		sumOfSquares += math.Pow(number-mean, 2)
	}
	meanOfSquares := sumOfSquares / float64(len(numbers))
	return math.Sqrt(meanOfSquares)
}

// TestMultiDimensionalQueueFairnessSlowConsumerEffects emulates a simplified queue slowdown scenario
// which the scheduler's additional queue dimensions features are intended to solve for.
//
// In this scenario, one category of queue item causes the queue consumer to slow down, introducing a
// significant delay while the queue consumer processes it and before the consumer can dequeue the next item.
// This emulates a situation where one of the query components - the ingesters or store-gateways - is under load.
//
// In a single-queue or single-queue-per-tenant scenario, the slow query component will cause the queue to back up.
// When queue items belonging to the slow category are in the same queue in front of the normal queue items,
// the normal queue items must wait for all slow queue items to be cleared before they can be serviced.
// In this way, the degraded performance of the slow query component equally degrades the performance of the
// queries which *could* be serviced quickly, but are waiting behind the slow queries in the queue.
func TestMultiDimensionalQueueAlgorithmSlowConsumerEffects(t *testing.T) {

	weightedQueueDimensionTestCases := []struct {
		name                         string
		tenantQueueDimensionsWeights map[string]map[string]float64
	}{
		{
			name: "1 tenant, 05pct slow queries",
			tenantQueueDimensionsWeights: map[string]map[string]float64{
				"0": {
					ingesterQueueDimension:                .95,
					storeGatewayQueueDimension:            .025,
					ingesterAndStoreGatewayQueueDimension: .025,
				},
			},
		},
		{
			name: "1 tenant, 10pct slow queries",
			tenantQueueDimensionsWeights: map[string]map[string]float64{
				"0": {
					ingesterQueueDimension:                .90,
					storeGatewayQueueDimension:            .05,
					ingesterAndStoreGatewayQueueDimension: .05,
				},
			},
		},
		{
			name: "1 tenant, 25pct slow queries",
			tenantQueueDimensionsWeights: map[string]map[string]float64{
				"0": {
					ingesterQueueDimension:                .75,
					storeGatewayQueueDimension:            .125,
					ingesterAndStoreGatewayQueueDimension: .125,
				},
			},
		},
		{
			name: "1 tenant, 50pct slow queries",
			tenantQueueDimensionsWeights: map[string]map[string]float64{
				"0": {
					ingesterQueueDimension:                .50,
					storeGatewayQueueDimension:            .25,
					ingesterAndStoreGatewayQueueDimension: .25,
				},
			},
		},
		{
			name: "1 tenant, 75pct slow queries",
			tenantQueueDimensionsWeights: map[string]map[string]float64{
				"0": {
					ingesterQueueDimension:                .25,
					storeGatewayQueueDimension:            .375,
					ingesterAndStoreGatewayQueueDimension: .375,
				},
			},
		},
		{
			name: "1 tenant, 90pct slow queries",
			tenantQueueDimensionsWeights: map[string]map[string]float64{
				"0": {
					ingesterQueueDimension:                .10,
					storeGatewayQueueDimension:            .45,
					ingesterAndStoreGatewayQueueDimension: .45,
				},
			},
		},
		{
			name: "1 tenant, 95pct slow queries",
			tenantQueueDimensionsWeights: map[string]map[string]float64{
				"0": {
					ingesterQueueDimension:                .05,
					storeGatewayQueueDimension:            .475,
					ingesterAndStoreGatewayQueueDimension: .475,
				},
			},
		},
	}

	maxQueriersPerTenant := 0 // disable shuffle sharding

	// Increase totalRequests to tighten up variations when running locally, but do not commit higher values;
	// the later test cases with a higher percentage of slow queries will take a long time to run.
	totalRequests := 1000
	numProducers := 10
	// Number of consumers must be divisible by the number of query component queue types
	// for even distribution of workers to queues and fair comparison between algorithms.
	// Query component queue types can up to 4: ingester, store-gateway, ingester-and-store-gateway, and unknown.
	// The unknown queue type is not used in this test, but 12 ensures divisibility by 4, 3, 2, and 1.
	numConsumers := 12

	normalConsumerLatency := 1 * time.Millisecond
	slowConsumerQueueDimension := storeGatewayQueueDimension
	// slow request approximately 100x longer than the fast request seems fair;
	// an ingester can respond in 0.3 seconds while a slow store-gateway query can take 30 seconds
	slowConsumerLatency := 100 * time.Millisecond

	var testCaseNames []string
	testCaseReports := map[string]*testScenarioQueueDurationReport{}

	for _, weightedQueueDimensionTestCase := range weightedQueueDimensionTestCases {
		numTenants := len(weightedQueueDimensionTestCase.tenantQueueDimensionsWeights)

		tqa := newTenantQuerierAssignments(0)

		nonFlippedRoundRobinTree, err := NewTree(tqa, &roundRobinState{}, &roundRobinState{})
		require.NoError(t, err)

		querierWorkerPrioritizationTree, err := NewTree(NewQuerierWorkerQueuePriorityAlgo(), tqa, &roundRobinState{})
		require.NoError(t, err)

		trees := []struct {
			name string
			tree Tree
		}{
			// keeping these names the same length keeps logged results aligned
			{
				"non-flipped round-robin tree",
				nonFlippedRoundRobinTree,
			},
			{
				"querier-worker priority tree",
				querierWorkerPrioritizationTree,
			},
		}
		for _, tree := range trees {
			testCaseName := fmt.Sprintf(
				"tree: %s, %s",
				tree.name,
				weightedQueueDimensionTestCase.name,
			)
			testCaseObservations := &testScenarioQueueDurationObservations{
				testCaseName:                            testCaseName,
				tenantIDQueueDurationObservations:       map[string][]float64{},
				queryComponentQueueDurationObservations: map[string][]float64{},
			}

			// only the non-flipped tree uses the old tenant -> query component hierarchy
			prioritizeQueryComponents := tree.tree != nonFlippedRoundRobinTree

			t.Run(testCaseName, func(t *testing.T) {
				queue, err := NewRequestQueue(
					log.NewNopLogger(),
					maxOutStandingPerTenant,
					true,
					prioritizeQueryComponents,
					querierForgetDelay,
					promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
					promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
					promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
					promauto.With(nil).NewSummaryVec(prometheus.SummaryOpts{}, []string{"query_component"}),
				)
				require.NoError(t, err)

				// NewRequestQueue constructor does not allow passing in a tree or tenantQuerierAssignments
				// so we have to override here to use the same structures as the test case
				queue.queueBroker.tenantQuerierAssignments = tqa
				queue.queueBroker.tree = tree.tree

				ctx := context.Background()
				require.NoError(t, queue.starting(ctx))
				t.Cleanup(func() {
					require.NoError(t, queue.stop(nil))
				})

				// configure queue producers to enqueue requests with the query component
				// randomly assigned according to the distribution defined in the test case
				queueDimensionFunc := makeWeightedRandAdditionalQueueDimensionFunc(
					weightedQueueDimensionTestCase.tenantQueueDimensionsWeights,
				)
				producersChan, producersErrGroup := makeQueueProducerGroup(
					queue, maxQueriersPerTenant, totalRequests, numProducers, numTenants, queueDimensionFunc,
				)

				// configure queue consumers with respective latencies for processing requests
				// which were assigned the "normal" or "slow" query component
				consumeFunc := makeQueueConsumeFuncWithSlowQueryComponent(
					queue, slowConsumerQueueDimension, slowConsumerLatency, normalConsumerLatency, testCaseObservations,
				)
				queueConsumerErrGroup, startConsumersChan := makeQueueConsumerGroup(
					context.Background(), queue, totalRequests, numConsumers, consumeFunc,
				)

				// run queue consumers and producers and wait for completion

				// run producers to fill queue
				close(producersChan)
				err = producersErrGroup.Wait()
				require.NoError(t, err)

				// run consumers to until queue is empty
				close(startConsumersChan)
				err = queueConsumerErrGroup.Wait()
				require.NoError(t, err)

				report := testCaseObservations.Report()
				t.Logf(testCaseName + ": " + report.String())
				// collect results in order
				testCaseNames = append(testCaseNames, testCaseName)
				testCaseReports[testCaseName] = report

				// ensure everything was dequeued
				path, val := tree.tree.Dequeue()
				assert.Nil(t, val)
				assert.Equal(t, path, QueuePath{})
			})
		}
	}
	// log results in order
	for _, testCaseName := range testCaseNames {
		t.Logf("%s: %s", testCaseName, testCaseReports[testCaseName])
	}

}
