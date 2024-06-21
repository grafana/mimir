// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
)

func testQuerierInflightRequestsGauge() *prometheus.GaugeVec {
	return promauto.With(prometheus.NewPedanticRegistry()).NewGaugeVec(prometheus.GaugeOpts{
		Name: "test_query_scheduler_querier_inflight_requests",
		Help: "[test] Number of inflight requests being processed on a querier-scheduler connection.",
	}, []string{"query_component"})
}

func TestExceedsUtilizationThresholdForQueryComponents(t *testing.T) {
	connectedWorkers := int64(10)
	// with 10 connected workers, if queue len >= waiting workers,
	// no component can have more than 6 inflight requests.
	testReservedCapacity := 0.4

	testCases := []struct {
		name string

		queueLen                     int
		waitingWorkers               int
		ingesterInflightRequests     int
		storeGatewayInflightRequests int

		queryComponentName         string
		thresholdExceededComponent QueryComponent
	}{
		// No queue backlog, threshold met or exceeded cases: we do not signal to skip the request.
		// In these cases, one component's inflight requests meet or exceed the utilization threshold,
		// but there are more waiting workers than items in the queue.
		{
			name:                         "ingester only, no backlog, ingester utilization above threshold",
			queueLen:                     1,
			waitingWorkers:               2,
			ingesterInflightRequests:     8,
			storeGatewayInflightRequests: 0,
			queryComponentName:           ingesterQueueDimension,
			thresholdExceededComponent:   "",
		},
		{
			name:                         "store-gateway only, no backlog, store-gateway utilization above threshold",
			queueLen:                     1,
			waitingWorkers:               2,
			ingesterInflightRequests:     0,
			storeGatewayInflightRequests: 8,
			queryComponentName:           storeGatewayQueueDimension,
			thresholdExceededComponent:   "",
		},
		{
			name:                         "ingester and store-gateway, no backlog, ingester utilization above threshold",
			queueLen:                     2,
			waitingWorkers:               3,
			ingesterInflightRequests:     7,
			storeGatewayInflightRequests: 0,
			queryComponentName:           ingesterAndStoreGatewayQueueDimension,
			thresholdExceededComponent:   "",
		},
		{
			name:                         "ingester and store-gateway, no backlog, store gateway utilization above threshold",
			queueLen:                     2,
			waitingWorkers:               3,
			ingesterInflightRequests:     0,
			storeGatewayInflightRequests: 7,
			queryComponentName:           ingesterAndStoreGatewayQueueDimension,
			thresholdExceededComponent:   "",
		},
		{
			name:                         "uncategorized queue component, no backlog, ingester utilization above threshold",
			queueLen:                     3,
			waitingWorkers:               4,
			ingesterInflightRequests:     6,
			storeGatewayInflightRequests: 0,
			queryComponentName:           "abc",
			thresholdExceededComponent:   "",
		},
		{
			name:                         "uncategorized queue component, no backlog, store gateway utilization above threshold",
			queueLen:                     3,
			waitingWorkers:               4,
			ingesterInflightRequests:     0,
			storeGatewayInflightRequests: 6,
			queryComponentName:           "xyz",
			thresholdExceededComponent:   "",
		},

		// Queue backlog, threshold not exceeded cases: we do not signal to skip the request.
		// In these cases, there are more items in the queue than waiting workers,
		// but no component's inflight requests meet or exceed the utilization threshold.
		{
			name:                         "ingester only, queue backlog, ingester utilization below threshold",
			queueLen:                     10,
			waitingWorkers:               1,
			ingesterInflightRequests:     5,
			storeGatewayInflightRequests: 4,
			queryComponentName:           ingesterQueueDimension,
			thresholdExceededComponent:   "",
		},
		{
			name:                         "store-gateway only, queue backlog, store-gateway utilization below threshold",
			queueLen:                     10,
			waitingWorkers:               1,
			ingesterInflightRequests:     4,
			storeGatewayInflightRequests: 5,
			queryComponentName:           storeGatewayQueueDimension,
			thresholdExceededComponent:   "",
		},
		{
			name:                         "ingester and store-gateway, queue backlog, all utilization below threshold",
			queueLen:                     10,
			waitingWorkers:               2,
			ingesterInflightRequests:     4,
			storeGatewayInflightRequests: 4,
			queryComponentName:           ingesterAndStoreGatewayQueueDimension,
			thresholdExceededComponent:   "",
		},
		{
			name:                         "uncategorized queue component, queue backlog, all utilization below threshold",
			queueLen:                     10,
			waitingWorkers:               2,
			ingesterInflightRequests:     4,
			storeGatewayInflightRequests: 4,
			queryComponentName:           "abc",
			thresholdExceededComponent:   "",
		},

		// Queue backlog, threshold exceeded cases: we signal to skip the request.
		// In these cases, there are more items in the queue than waiting workers,
		// and one component's inflight requests meet or exceed the utilization threshold.
		{
			name:                         "ingester only, queue backlog, ingester utilization at threshold",
			queueLen:                     10,
			waitingWorkers:               1,
			ingesterInflightRequests:     6,
			storeGatewayInflightRequests: 3,
			queryComponentName:           ingesterQueueDimension,
			thresholdExceededComponent:   Ingester,
		},
		{
			name:                         "store-gateway only, queue backlog, store-gateway utilization at threshold",
			queueLen:                     10,
			waitingWorkers:               1,
			ingesterInflightRequests:     3,
			storeGatewayInflightRequests: 6,
			queryComponentName:           storeGatewayQueueDimension,
			thresholdExceededComponent:   StoreGateway,
		},
		{
			name:                         "ingester and store-gateway, queue backlog, ingester utilization above threshold",
			queueLen:                     10,
			waitingWorkers:               1,
			ingesterInflightRequests:     6,
			storeGatewayInflightRequests: 3,
			queryComponentName:           ingesterAndStoreGatewayQueueDimension,
			thresholdExceededComponent:   Ingester,
		},
		{
			name:                         "ingester and store-gateway, queue backlog, store gateway utilization above threshold",
			queueLen:                     10,
			waitingWorkers:               1,
			ingesterInflightRequests:     3,
			storeGatewayInflightRequests: 6,
			queryComponentName:           ingesterAndStoreGatewayQueueDimension,
			thresholdExceededComponent:   StoreGateway,
		},
		{
			name:                         "uncategorized queue component, queue backlog, ingester utilization above threshold",
			queueLen:                     10,
			waitingWorkers:               1,
			ingesterInflightRequests:     7,
			storeGatewayInflightRequests: 2,
			queryComponentName:           "abc",
			thresholdExceededComponent:   Ingester,
		},
		{
			name:                         "uncategorized queue component, queue backlog, store gateway utilization above threshold",
			queueLen:                     10,
			waitingWorkers:               1,
			ingesterInflightRequests:     2,
			storeGatewayInflightRequests: 7,
			queryComponentName:           "xyz",
			thresholdExceededComponent:   StoreGateway,
		},

		// corner cases
		{
			name:                         "uncategorized queue component, queue backlog, store gateway utilization above threshold",
			queueLen:                     10,
			waitingWorkers:               1,
			ingesterInflightRequests:     2,
			storeGatewayInflightRequests: 7,
			queryComponentName:           "xyz",
			thresholdExceededComponent:   StoreGateway,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var err error
			queryComponentUtilization, err := NewQueryComponentUtilization(
				testReservedCapacity, testQuerierInflightRequestsGauge(),
			)
			require.NoError(t, err)

			disabledComponentUtilization, err := NewQueryComponentUtilization(
				0, testQuerierInflightRequestsGauge(),
			)
			require.NoError(t, err)

			for i := 0; i < testCase.ingesterInflightRequests; i++ {
				queryComponentUtilization.IncrementForComponentName(ingesterQueueDimension)
			}

			for i := 0; i < testCase.storeGatewayInflightRequests; i++ {
				queryComponentUtilization.IncrementForComponentName(storeGatewayQueueDimension)
			}

			exceedsThreshold, queryComponent := queryComponentUtilization.ExceedsThresholdForComponentName(
				testCase.queryComponentName,
				int(connectedWorkers),
				testCase.queueLen,
				testCase.waitingWorkers,
			)
			require.Equal(t, queryComponent, testCase.thresholdExceededComponent)
			// we should only return a component when exceedsThreshold is true and vice versa
			require.Equal(t, exceedsThreshold, testCase.thresholdExceededComponent != "")

			// with 1 connected worker, a component should never be marked as exceeding the threshold
			exceedsThreshold, queryComponent = queryComponentUtilization.ExceedsThresholdForComponentName(
				testCase.queryComponentName,
				1,
				testCase.queueLen,
				testCase.waitingWorkers,
			)
			require.False(t, exceedsThreshold)
			require.Equal(t, queryComponent, QueryComponent(""))

			// a component utilization with reserved capacity 0 disables capacity checks
			exceedsThreshold, queryComponent = disabledComponentUtilization.ExceedsThresholdForComponentName(
				testCase.queryComponentName,
				int(connectedWorkers),
				testCase.queueLen,
				testCase.waitingWorkers,
			)
			require.False(t, exceedsThreshold)
			require.Equal(t, queryComponent, QueryComponent(""))
		})
	}
}
