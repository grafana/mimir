// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
)

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

func TestIsOverUtilizedForQueryComponents(t *testing.T) {
	// with reserved capacity at 40% and 10 connected workers,
	// no component can have more than 6 inflight requests.
	testReservedCapacity := 0.4
	connectedWorkers := 10

	testCases := []struct {
		name string

		ingesterInflightRequests     int
		storeGatewayInflightRequests int

		queryComponentName         string
		thresholdExceededComponent QueryComponent
	}{
		// Threshold met or exceeded cases:
		// one component's inflight requests meet or exceed the utilization limit,
		// so we signal the queue algorithm to attempt to skip the request.
		{
			name:                         "ingester only, ingester utilization at threshold",
			ingesterInflightRequests:     6,
			storeGatewayInflightRequests: 3,
			queryComponentName:           ingesterQueueDimension,
			thresholdExceededComponent:   Ingester,
		},
		{
			name:                         "store-gateway only, store-gateway utilization at threshold",
			ingesterInflightRequests:     5,
			storeGatewayInflightRequests: 6,
			queryComponentName:           storeGatewayQueueDimension,
			thresholdExceededComponent:   StoreGateway,
		},
		{
			name:                         "ingester and store-gateway, ingester utilization at threshold",
			ingesterInflightRequests:     6,
			storeGatewayInflightRequests: 6,
			queryComponentName:           ingesterAndStoreGatewayQueueDimension,
			thresholdExceededComponent:   Ingester,
		},
		{
			name:                         "ingester only, ingester utilization above threshold",
			ingesterInflightRequests:     7,
			storeGatewayInflightRequests: 5,
			queryComponentName:           ingesterQueueDimension,
			thresholdExceededComponent:   Ingester,
		},
		{
			name:                         "store-gateway only, store-gateway utilization above threshold",
			ingesterInflightRequests:     1,
			storeGatewayInflightRequests: 7,
			queryComponentName:           storeGatewayQueueDimension,
			thresholdExceededComponent:   StoreGateway,
		},
		{
			name:                         "ingester and store-gateway, ingester utilization above threshold",
			ingesterInflightRequests:     8,
			storeGatewayInflightRequests: 3,
			queryComponentName:           ingesterAndStoreGatewayQueueDimension,
			thresholdExceededComponent:   Ingester,
		},
		{
			name:                         "ingester and store-gateway, store gateway utilization above threshold",
			ingesterInflightRequests:     0,
			storeGatewayInflightRequests: 8,
			queryComponentName:           ingesterAndStoreGatewayQueueDimension,
			thresholdExceededComponent:   StoreGateway,
		},
		{
			name:                         "uncategorized queue component, ingester utilization above threshold",
			ingesterInflightRequests:     9,
			storeGatewayInflightRequests: 2,
			queryComponentName:           "abc",
			thresholdExceededComponent:   Ingester,
		},
		{
			name:                         "uncategorized queue component, store gateway utilization above threshold",
			ingesterInflightRequests:     4,
			storeGatewayInflightRequests: 9,
			queryComponentName:           "xyz",
			thresholdExceededComponent:   StoreGateway,
		},

		// Threshold not met cases:
		// component's inflight requests may meet but do not exceed the utilization limit,
		// so we do not signal the queue algorithm to attempt to skip the request.
		{
			name:                         "ingester and store-gateway, all utilization below threshold",
			ingesterInflightRequests:     5,
			storeGatewayInflightRequests: 5,
			queryComponentName:           ingesterAndStoreGatewayQueueDimension,
			thresholdExceededComponent:   "",
		},
		{
			name:                         "uncategorized queue component, all utilization below threshold",
			ingesterInflightRequests:     5,
			storeGatewayInflightRequests: 5,
			queryComponentName:           "abc",
			thresholdExceededComponent:   "",
		},
	}

	for _, testCase := range testCases {
		var err error
		queryComponentUtilization, err := NewQueryComponentUtilization(testQuerierInflightRequestsMetric())
		require.NoError(t, err)

		queryComponentUtilizationLimitByConnections, err := NewQueryComponentUtilizationLimitByConnections(testReservedCapacity)
		require.NoError(t, err)

		// setting reserved capacity to 0 disables the query component utilization checks
		queryComponentUtilizationLimitDisabled, err := NewQueryComponentUtilizationLimitByConnections(0)
		require.NoError(t, err)

		queryComponentUtilizationLimitByConnections.SetConnectedWorkers(connectedWorkers)
		queryComponentUtilizationLimitDisabled.SetConnectedWorkers(connectedWorkers)

		t.Run(testCase.name, func(t *testing.T) {
			// increment component utilization tracking to the test case values
			for i := 0; i < testCase.ingesterInflightRequests; i++ {
				ingesterInflightRequest := &SchedulerRequest{
					FrontendAddr:              "frontend-a",
					QueryID:                   uint64(i),
					AdditionalQueueDimensions: []string{ingesterQueueDimension},
				}
				queryComponentUtilization.MarkRequestSent(ingesterInflightRequest)
			}
			for i := 0; i < testCase.storeGatewayInflightRequests; i++ {
				storeGatewayInflightRequest := &SchedulerRequest{
					FrontendAddr:              "frontend-b",
					QueryID:                   uint64(i),
					AdditionalQueueDimensions: []string{storeGatewayQueueDimension},
				}
				queryComponentUtilization.MarkRequestSent(storeGatewayInflightRequest)
			}

			isOverUtilized, queryComponent := queryComponentUtilizationLimitByConnections.IsOverUtilized(
				queryComponentUtilization, testCase.queryComponentName,
			)
			require.Equal(t, queryComponent, testCase.thresholdExceededComponent)
			// we should only return a component when exceedsThreshold is true and vice versa
			require.Equal(t, isOverUtilized, testCase.thresholdExceededComponent != "")

			// a component utilization with reserved capacity 0 disables capacity checks
			isOverUtilized, queryComponent = queryComponentUtilizationLimitDisabled.IsOverUtilized(
				queryComponentUtilization, testCase.queryComponentName,
			)
			require.False(t, isOverUtilized)
			require.Equal(t, queryComponent, QueryComponent(""))
		})
	}
}

func TestIsOverUtilizedForQueryComponents_CornerCases(t *testing.T) {
	queryComponentNames := []string{
		ingesterQueueDimension,
		storeGatewayQueueDimension,
		ingesterAndStoreGatewayQueueDimension,
		"some uncategorized string queue dimension",
		"", // also uncategorized
	}

	var err error
	queryComponentUtilization, err := NewQueryComponentUtilization(testQuerierInflightRequestsMetric())
	require.NoError(t, err)

	//////// corner case 1: do not reserve capacity if there is only one connected worker
	// reserved capacity does not matter for this case as long as it is valid and not zero
	queryComponentUtilizationLimitByConnections, err := NewQueryComponentUtilizationLimitByConnections(0.1)
	require.NoError(t, err)
	// with 1 connected worker, a component should never be marked as exceeding the threshold
	queryComponentUtilizationLimitByConnections.SetConnectedWorkers(1)
	// set utilization to be maxed out for both components
	queryComponentUtilization.ingesterInflightRequests = 1
	queryComponentUtilization.storeGatewayInflightRequests = 1

	// no component should be marked as exceeding the threshold with only one connected worker
	for _, queryComponentName := range queryComponentNames {
		isOverUtilized, queryComponent := queryComponentUtilizationLimitByConnections.IsOverUtilized(
			queryComponentUtilization, queryComponentName,
		)
		require.False(t, isOverUtilized)
		require.Equal(t, queryComponent, QueryComponent(""))
	}

	//////// corner case 2: reserved connections is rounded up so one connection is reserved
	//////// even if (connected workers) * (reserved capacity) is less than one
	queryComponentUtilizationLimitByConnections, err = NewQueryComponentUtilizationLimitByConnections(0.01)
	require.NoError(t, err)
	// with 10 connected workers and reserved capacity .01, the un-rounded reserved connections would be 0.1
	queryComponentUtilizationLimitByConnections.SetConnectedWorkers(10)
	// without rounding up the reserved connections, utilizing more than 9 connections would be allowed
	queryComponentUtilization.ingesterInflightRequests = 9
	queryComponentUtilization.storeGatewayInflightRequests = 9

	// every component should be marked as exceeding the threshold
	for _, queryComponentName := range queryComponentNames {
		isOverUtilized, _ := queryComponentUtilizationLimitByConnections.IsOverUtilized(
			queryComponentUtilization, queryComponentName,
		)
		require.True(t, isOverUtilized)
	}
}
