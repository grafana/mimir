// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func testQuerierInflightRequestsGauge() *prometheus.GaugeVec {
	return promauto.With(prometheus.NewPedanticRegistry()).NewGaugeVec(prometheus.GaugeOpts{
		Name: "test_query_scheduler_querier_inflight_requests",
		Help: "[test] Number of inflight requests being processed on a querier-scheduler connection.",
	}, []string{"query_component"})
}

func TestQueryComponentCapacity_Concurrency(t *testing.T) {

	requestCount := 100
	queryComponentLoad, err := NewQueryComponentCapacity(
		DefaultReservedQueryComponentCapacity, testQuerierInflightRequestsGauge(),
	)
	require.NoError(t, err)

	mockForwardRequestToQuerier := func(t *testing.T, capacity *QueryComponentCapacity) {
		expectedQueryComponent := randAdditionalQueueDimension(false)[0]

		capacity.IncrementForComponentName(expectedQueryComponent)
		require.GreaterOrEqual(t, capacity.ingesterInflightRequests.Load(), int64(0))
		require.GreaterOrEqual(t, capacity.storeGatewayInflightRequests.Load(), int64(0))

		capacity.DecrementForComponentName(expectedQueryComponent)
		require.GreaterOrEqual(t, capacity.ingesterInflightRequests.Load(), int64(0))
		require.GreaterOrEqual(t, capacity.storeGatewayInflightRequests.Load(), int64(0))
	}

	wg := sync.WaitGroup{}
	start := make(chan struct{})
	for i := 0; i < requestCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			mockForwardRequestToQuerier(t, queryComponentLoad)
		}()
	}
	close(start)
	wg.Wait()
	require.Equal(t, int64(0), queryComponentLoad.ingesterInflightRequests.Load())
	require.Equal(t, int64(0), queryComponentLoad.storeGatewayInflightRequests.Load())
	require.Equal(t, int64(0), queryComponentLoad.querierInflightRequestsTotal.Load())
}

func TestExceedsReservedCapacityForQueryComponents(t *testing.T) {
	connectedWorkers := atomic.NewInt64(10)
	// with 10 connected workers, if queue len >= waiting workers,
	// no component can have more than 6 inflight requests.
	testReservedCapacity := 0.4

	testCases := []struct {
		name string

		queueLen                     int
		waitingWorkers               int
		ingesterInflightRequests     int
		storegatewayInflightRequests int

		queryComponentName         string
		thresholdExceededComponent QueryComponent
	}{
		// No queue backlog, threshold met or exceeded cases: we do not signal to skip the request.
		// In these cases, one component's inflight requests meet or exceed the capacity threshold,
		// but there are more waiting workers than items in the queue.
		{
			name:                         "ingester only, no backlog, ingester utilization above threshold",
			queueLen:                     1,
			waitingWorkers:               2,
			ingesterInflightRequests:     8,
			storegatewayInflightRequests: 0,
			queryComponentName:           ingesterQueueDimension,
			thresholdExceededComponent:   "",
		},
		{
			name:                         "store-gateway only, no backlog, store-gateway utilization above threshold",
			queueLen:                     1,
			waitingWorkers:               2,
			ingesterInflightRequests:     0,
			storegatewayInflightRequests: 8,
			queryComponentName:           storeGatewayQueueDimension,
			thresholdExceededComponent:   "",
		},
		{
			name:                         "ingester and store-gateway, no backlog, ingester utilization above threshold",
			queueLen:                     2,
			waitingWorkers:               3,
			ingesterInflightRequests:     7,
			storegatewayInflightRequests: 0,
			queryComponentName:           ingesterQueueDimension,
			thresholdExceededComponent:   "",
		},
		{
			name:                         "ingester and store-gateway, no backlog, store gateway utilization above threshold",
			queueLen:                     2,
			waitingWorkers:               3,
			ingesterInflightRequests:     0,
			storegatewayInflightRequests: 7,
			queryComponentName:           storeGatewayQueueDimension,
			thresholdExceededComponent:   "",
		},
		{
			name:                         "uncategorized queue component, no backlog, ingester utilization above threshold",
			queueLen:                     3,
			waitingWorkers:               4,
			ingesterInflightRequests:     6,
			storegatewayInflightRequests: 0,
			queryComponentName:           "abc",
			thresholdExceededComponent:   "",
		},
		{
			name:                         "uncategorized queue component, no backlog, store gateway utilization above threshold",
			queueLen:                     3,
			waitingWorkers:               4,
			ingesterInflightRequests:     0,
			storegatewayInflightRequests: 6,
			queryComponentName:           "xyz",
			thresholdExceededComponent:   "",
		},

		// Queue backlog, threshold not exceeded cases: we do not signal to skip the request.
		// In these cases, there are more items in the queue than waiting workers,
		// but no component's inflight requests meet or exceed the capacity threshold.
		{
			name:                         "ingester only, queue backlog, ingester utilization below threshold",
			queueLen:                     10,
			waitingWorkers:               1,
			ingesterInflightRequests:     5,
			storegatewayInflightRequests: 4,
			queryComponentName:           ingesterQueueDimension,
			thresholdExceededComponent:   "",
		},
		{
			name:                         "store-gateway only, queue backlog, store-gateway utilization below threshold",
			queueLen:                     10,
			waitingWorkers:               1,
			ingesterInflightRequests:     4,
			storegatewayInflightRequests: 5,
			queryComponentName:           storeGatewayQueueDimension,
			thresholdExceededComponent:   "",
		},
		{
			name:                         "ingester and store-gateway, queue backlog, all utilization below threshold",
			queueLen:                     10,
			waitingWorkers:               2,
			ingesterInflightRequests:     4,
			storegatewayInflightRequests: 4,
			queryComponentName:           ingesterAndStoreGatewayQueueDimension,
			thresholdExceededComponent:   "",
		},
		{
			name:                         "uncategorized queue component, queue backlog, all utilization below threshold",
			queueLen:                     10,
			waitingWorkers:               2,
			ingesterInflightRequests:     4,
			storegatewayInflightRequests: 4,
			queryComponentName:           "abc",
			thresholdExceededComponent:   "",
		},

		// Queue backlog, threshold exceeded cases: we signal to skip the request.
		// In these cases, there are more items in the queue than waiting workers,
		// and one component's inflight requests meet or exceed the capacity threshold.
		{
			name:                         "ingester only, queue backlog, ingester utilization at threshold",
			queueLen:                     10,
			waitingWorkers:               1,
			ingesterInflightRequests:     6,
			storegatewayInflightRequests: 3,
			queryComponentName:           ingesterQueueDimension,
			thresholdExceededComponent:   Ingester,
		},
		{
			name:                         "store-gateway only, queue backlog, store-gateway utilization at threshold",
			queueLen:                     10,
			waitingWorkers:               1,
			ingesterInflightRequests:     3,
			storegatewayInflightRequests: 6,
			queryComponentName:           storeGatewayQueueDimension,
			thresholdExceededComponent:   StoreGateway,
		},
		{
			name:                         "ingester and store-gateway, queue backlog, ingester utilization above threshold",
			queueLen:                     10,
			waitingWorkers:               1,
			ingesterInflightRequests:     6,
			storegatewayInflightRequests: 3,
			queryComponentName:           ingesterAndStoreGatewayQueueDimension,
			thresholdExceededComponent:   Ingester,
		},
		{
			name:                         "ingester and store-gateway, queue backlog, store gateway utilization above threshold",
			queueLen:                     10,
			waitingWorkers:               1,
			ingesterInflightRequests:     3,
			storegatewayInflightRequests: 6,
			queryComponentName:           ingesterAndStoreGatewayQueueDimension,
			thresholdExceededComponent:   StoreGateway,
		},
		{
			name:                         "uncategorized queue component, queue backlog, ingester utilization above threshold",
			queueLen:                     10,
			waitingWorkers:               1,
			ingesterInflightRequests:     7,
			storegatewayInflightRequests: 2,
			queryComponentName:           "abc",
			thresholdExceededComponent:   Ingester,
		},
		{
			name:                         "uncategorized queue component, queue backlog, store gateway utilization above threshold",
			queueLen:                     10,
			waitingWorkers:               1,
			ingesterInflightRequests:     2,
			storegatewayInflightRequests: 7,
			queryComponentName:           "xyz",
			thresholdExceededComponent:   StoreGateway,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			queryComponentLoad, err := NewQueryComponentCapacity(
				testReservedCapacity, testQuerierInflightRequestsGauge(),
			)
			require.NoError(t, err)

			for i := 0; i < testCase.ingesterInflightRequests; i++ {
				queryComponentLoad.IncrementForComponentName(ingesterQueueDimension)
			}

			for i := 0; i < testCase.storegatewayInflightRequests; i++ {
				queryComponentLoad.IncrementForComponentName(storeGatewayQueueDimension)
			}

			exceedsCapacity, queryComponent := queryComponentLoad.ExceedsCapacityForComponentName(
				testCase.queryComponentName,
				connectedWorkers,
				testCase.queueLen,
				testCase.waitingWorkers,
			)
			require.Equal(t, queryComponent, testCase.thresholdExceededComponent)
			// we should only return a component when exceedsCapacity is true and vice versa
			require.Equal(t, exceedsCapacity, testCase.thresholdExceededComponent != "")
		})
	}
}
