// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"context"
	"fmt"
	"github.com/grafana/mimir/pkg/scheduler/queue/tree"
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

const slowConsumerQueueDimension = storeGatewayQueueDimension

func makeQueueConsumeFuncWithSlowQueryComponent(
	queue *RequestQueue,
	slowConsumerLatency time.Duration,
	normalConsumerLatency time.Duration,
	report *testScenarioQueueDurationObservations,
) consumeRequest {
	return func(request QueryRequest) error {
		schedulerRequest := request.(*SchedulerRequest)
		queryComponent := schedulerRequest.ExpectedQueryComponentName()
		if queryComponent == ingesterAndStoreGatewayQueueDimension {
			// we expect the latency of a query hitting both a normal and a slowed-down query component
			// will be constrained by the latency of the slowest query component;
			// we enqueued with the "both" query component to observe the queue algorithm behavior
			// but we re-assign query component here for simplicity in logic & reporting
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

	o.queryComponentQueueDurationObservations[queryComponent] = append(
		o.queryComponentQueueDurationObservations[queryComponent], queueDuration,
	)

	if queryComponent != slowConsumerQueueDimension {
		// when analyzing the statistics for the tenant-specific experience of queue duration,
		// we only care about the queue duration for the non-slowed-down query component,
		// as in the real-world scenario, queries to a severely slowed-down query component are expected
		// to be a lost cause, largely timing out before they can be fully serviced.
		o.tenantIDQueueDurationObservations[tenantID] = append(
			o.tenantIDQueueDurationObservations[tenantID], queueDuration,
		)
	}

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

	for _, tenantID := range tenantIDs {
		meanDur := mean(o.tenantIDQueueDurationObservations[tenantID])
		stdDevDur := stddev(o.tenantIDQueueDurationObservations[tenantID], meanDur)
		report.tenantIDReports = append(
			report.tenantIDReports,
			testDimensionReport{
				dimensionName:        "tenant-" + tenantID,
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
	tenantIDReports       []testDimensionReport
}

func (r *testScenarioQueueDurationReport) QueryComponentReportString() string {
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

func (r *testScenarioQueueDurationReport) TenantIDReportString() string {
	var tenantIDReports []string
	for _, tenantIDReport := range r.tenantIDReports {
		tenantIDReports = append(
			tenantIDReports,
			fmt.Sprintf(
				"%s: mean: %.4f stddev: %.2f",
				tenantIDReport.dimensionName,
				tenantIDReport.meanSecondsInQueue,
				tenantIDReport.stdDevSecondsInQueue),
		)
	}
	return fmt.Sprintf(
		"seconds in queue:%v", tenantIDReports,
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
//
// In this benchmark, queries for the store-gateway query component are artificially slowed down.
// We assume that in such a slowdown scenario that queries hitting both query components (ingester and store-gateway)
// will be constrained by the latency of the slowest query component, so we treat them as store-gateway queries.
// The goal of the queueing algorithms is to enable ingester-only queries to continue to be serviced quickly
// when the store-gateway queries are slow; we are not concerned with de-loading the store-gateways
// or otherwise improving the performance for the store-gateway queries.
// Benchmark results should be interpreted primarily in terms of the performance of the ingester-only queries.
func TestMultiDimensionalQueueAlgorithmSlowConsumerEffects(t *testing.T) {

	weightedQueueDimensionTestCases := []struct {
		name                         string
		tenantQueueDimensionsWeights map[string]map[string]float64
	}{
		// single-tenant scenarios
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

		// multi-tenant scenarios
		{
			name: "2 tenants, first with 10pct slow queries, second with 90pct slow queries",
			tenantQueueDimensionsWeights: map[string]map[string]float64{
				"0": {
					ingesterQueueDimension:                .90,
					storeGatewayQueueDimension:            .05,
					ingesterAndStoreGatewayQueueDimension: .05,
				},
				"1": {
					ingesterQueueDimension:                .10,
					storeGatewayQueueDimension:            .45,
					ingesterAndStoreGatewayQueueDimension: .45,
				},
			},
		},
		{
			name: "2 tenants, first with 25pct slow queries, second with 75pct slow queries",
			tenantQueueDimensionsWeights: map[string]map[string]float64{
				"0": {
					ingesterQueueDimension:                .75,
					storeGatewayQueueDimension:            .125,
					ingesterAndStoreGatewayQueueDimension: .125,
				},
				"1": {
					ingesterQueueDimension:                .25,
					storeGatewayQueueDimension:            .375,
					ingesterAndStoreGatewayQueueDimension: .375,
				},
			},
		},
		{
			name: "2 tenants, first with 50pct slow queries, second with 50pct slow queries",
			tenantQueueDimensionsWeights: map[string]map[string]float64{
				"0": {
					ingesterQueueDimension:                .50,
					storeGatewayQueueDimension:            .25,
					ingesterAndStoreGatewayQueueDimension: .25,
				},
				"1": {
					ingesterQueueDimension:                .50,
					storeGatewayQueueDimension:            .25,
					ingesterAndStoreGatewayQueueDimension: .25,
				},
			},
		},
	}

	// Increase totalRequests to tighten up variations when running locally, but do not commit higher values;
	// the later test cases with a higher percentage of slow queries will take a long time to run.
	totalRequests := 1000
	numProducers := 10
	numConsumers := 8
	// Number of workers must be divisible by the number of query component queue types
	// for even distribution of workers to queues and fair comparison between algorithms.
	// Query component queue types can up to 4: ingester, store-gateway, ingester-and-store-gateway, and unknown.
	// The unknown queue type is not used in this test, but 12 ensures divisibility by 4, 3, 2, and 1.
	numWorkersPerConsumer := 12

	normalConsumerLatency := 1 * time.Millisecond
	// slow request approximately 100x longer than the fast request seems fair;
	// an ingester can respond in 0.3 seconds while a slow store-gateway query can take 30 seconds
	slowConsumerLatency := 100 * time.Millisecond

	// enable shuffle sharding; we cannot be too restrictive with only two tenants,
	// or some consumers will not get sharded to any of the two tenants.
	// enabling shuffle sharding ensures we will hit cases where a querier-worker
	// does not find any tenant leaf nodes it can work on under its prioritized query component node
	maxQueriersPerTenant := numConsumers - 1

	var testCaseNames []string
	testCaseReports := map[string]*testScenarioQueueDurationReport{}

	for _, weightedQueueDimensionTestCase := range weightedQueueDimensionTestCases {
		numTenants := len(weightedQueueDimensionTestCase.tenantQueueDimensionsWeights)

		tqaNonFlipped := tree.NewTenantQuerierQueuingAlgorithm()
		tqaFlipped := tree.NewTenantQuerierQueuingAlgorithm()
		tqaQuerierWorkerPrioritization := tree.NewTenantQuerierQueuingAlgorithm()

		nonFlippedRoundRobinTree, err := tree.NewTree(tqaNonFlipped, tree.NewRoundRobinState())
		require.NoError(t, err)

		flippedRoundRobinTree, err := tree.NewTree(tree.NewRoundRobinState(), tqaFlipped)
		require.NoError(t, err)

		querierWorkerPrioritizationTree, err := tree.NewTree(tree.NewQuerierWorkerQueuePriorityAlgo(), tqaQuerierWorkerPrioritization)
		require.NoError(t, err)

		treeScenarios := []struct {
			name string
			tree tree.Tree
			tqa  *tree.TenantQuerierQueuingAlgorithm
		}{
			// keeping these names the same length keeps logged results aligned
			{
				"tenant-querier -> query component round-robin tree",
				nonFlippedRoundRobinTree,
				tqaNonFlipped,
			},
			{
				"query component round-robin -> tenant-querier tree",
				flippedRoundRobinTree,
				tqaFlipped,
			},
			{
				"worker-queue prioritization -> tenant-querier tree",
				querierWorkerPrioritizationTree,
				tqaQuerierWorkerPrioritization,
			},
		}
		for _, scenario := range treeScenarios {
			testCaseName := fmt.Sprintf(
				"tree: %s, %s",
				scenario.name,
				weightedQueueDimensionTestCase.name,
			)
			testCaseObservations := &testScenarioQueueDurationObservations{
				testCaseName:                            testCaseName,
				tenantIDQueueDurationObservations:       map[string][]float64{},
				queryComponentQueueDurationObservations: map[string][]float64{},
			}

			// only the non-flipped tree uses the old tenant -> query component hierarchy
			prioritizeQueryComponents := scenario.tree != nonFlippedRoundRobinTree

			t.Run(testCaseName, func(t *testing.T) {
				queue, err := NewRequestQueue(
					log.NewNopLogger(),
					maxOutStandingPerTenant,
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
				queue.queueBroker.tenantQuerierAssignments = &tenantQuerierAssignments{
					querierIDsSorted: make([]tree.QuerierID, 0),
					tenantsByID:      make(map[string]*queueTenant),
					queuingAlgorithm: scenario.tqa,
				}
				queue.queueBroker.prioritizeQueryComponents = prioritizeQueryComponents
				queue.queueBroker.tree = scenario.tree

				ctx := context.Background()
				require.NoError(t, queue.starting(ctx))

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
					queue, slowConsumerLatency, normalConsumerLatency, testCaseObservations,
				)
				queueConsumerErrGroup, startConsumersChan := makeQueueConsumerGroup(
					context.Background(), queue, totalRequests, numConsumers, numWorkersPerConsumer, consumeFunc,
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
				t.Logf("%s: %s", testCaseName, report.QueryComponentReportString())
				t.Logf("%s: %s", testCaseName, report.TenantIDReportString())
				// collect results in order
				testCaseNames = append(testCaseNames, testCaseName)
				testCaseReports[testCaseName] = report

				require.NoError(t, queue.stop(nil))
				assert.NotEqual(t, "", tree.CurrentQuerier(scenario.tqa))
				// ensure everything was dequeued; we can pass a nil DequeueArgs because we don't
				// want to update any state before doing this (i.e., we're dequeuing for _any_ querier,
				// just to make sure the tree is empty).
				path, val := scenario.tree.Dequeue(nil)
				assert.Nil(t, val)
				assert.Equal(t, path, tree.QueuePath{})
			})
		}
	}

	t.Log("Results by query component:")
	for _, testCaseName := range testCaseNames {
		t.Logf("%s: %s", testCaseName, testCaseReports[testCaseName].QueryComponentReportString())
	}

	t.Log("Results for ingester-only queries by tenant ID:")
	for _, testCaseName := range testCaseNames {
		t.Logf("%s: %s", testCaseName, testCaseReports[testCaseName].TenantIDReportString())
	}

}
