// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// getForComponent is a test utility, not intended for use by consumers of QueryComponentLoad
func (qcl *QueryComponentLoad) getForComponent(queryComponent QueryComponent) int {
	qcl.inflightRequestsMu.RLock()
	defer qcl.inflightRequestsMu.RUnlock()
	return qcl.querierInflightRequestsByComponent[queryComponent]
}

func TestQueryComponentLoad_Concurrency(t *testing.T) {

	requestCount := 100
	testOverloadFactor := 2.0
	queryComponentLoad, err := NewQueryComponentLoad(testOverloadFactor, prometheus.NewPedanticRegistry())
	require.NoError(t, err)

	mockForwardRequestToQuerier := func(t *testing.T, load *QueryComponentLoad) {
		expectedQueryComponent := randAdditionalQueueDimension(false)[0]

		load.IncrementForComponentName(expectedQueryComponent)
		require.GreaterOrEqual(t, load.getForComponent(Ingester), 0)
		require.GreaterOrEqual(t, load.getForComponent(StoreGateway), 0)

		load.DecrementForComponentName(expectedQueryComponent)
		require.GreaterOrEqual(t, load.getForComponent(Ingester), 0)
		require.GreaterOrEqual(t, load.getForComponent(StoreGateway), 0)
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
}

func TestIsOverloadedForQueryComponents(t *testing.T) {
	testOverloadFactor := 2.0

	testCases := []struct {
		name                         string
		ingesterInflightRequests     int
		storegatewayInflightRequests int
		queryComponentName           string
		expectedOverloadedComponent  QueryComponent
	}{
		// single-component request, not overloaded cases
		{
			name:                         "ingester only, ingester below overload threshold: not overloaded",
			ingesterInflightRequests:     16,
			storegatewayInflightRequests: 9,
			queryComponentName:           ingesterQueueDimension,
			expectedOverloadedComponent:  "",
		},
		{
			name:                         "store-gateway only, store-gateway below overload threshold: not overloaded",
			ingesterInflightRequests:     10,
			storegatewayInflightRequests: 19,
			queryComponentName:           storeGatewayQueueDimension,
			expectedOverloadedComponent:  "",
		},

		// single-component request, overloaded cases
		{
			name:                         "ingester only, ingester at overload threshold: is overloaded",
			ingesterInflightRequests:     14,
			storegatewayInflightRequests: 7,
			queryComponentName:           ingesterQueueDimension,
			expectedOverloadedComponent:  Ingester,
		},
		{
			name:                         "store-gateway only, store-gateway at overload threshold: is overloaded",
			ingesterInflightRequests:     10,
			storegatewayInflightRequests: 20,
			queryComponentName:           storeGatewayQueueDimension,

			expectedOverloadedComponent: StoreGateway,
		},

		// dual-component request, not overloaded cases
		{
			name:                         "ingester and store-gateway, ingester below threshold: not overloaded",
			ingesterInflightRequests:     15,
			storegatewayInflightRequests: 8,
			queryComponentName:           ingesterAndStoreGatewayQueueDimension,
			expectedOverloadedComponent:  "",
		},
		{
			name:                         "ingester and store-gateway, store-gateway below overload threshold: not overloaded",
			ingesterInflightRequests:     5,
			storegatewayInflightRequests: 9,
			queryComponentName:           ingesterAndStoreGatewayQueueDimension,

			expectedOverloadedComponent: "",
		},

		// dual-component request, overloaded cases
		{
			name:                         "ingester and store-gateway, ingester above overload threshold: is overloaded",
			ingesterInflightRequests:     18,
			storegatewayInflightRequests: 8,
			queryComponentName:           ingesterAndStoreGatewayQueueDimension,
			expectedOverloadedComponent:  Ingester,
		},
		{
			name:                         "ingester and store-gateway, store-gateway above overload threshold: is overloaded",
			ingesterInflightRequests:     5,
			storegatewayInflightRequests: 11,
			queryComponentName:           ingesterAndStoreGatewayQueueDimension,
			expectedOverloadedComponent:  StoreGateway,
		},

		// possible corner cases
		{
			name:                         "ingester only, equal load: not overloaded",
			ingesterInflightRequests:     1,
			storegatewayInflightRequests: 1,
			queryComponentName:           ingesterQueueDimension,
			expectedOverloadedComponent:  "",
		},
		{
			name:                         "store-gateway only, equal load: not overloaded",
			ingesterInflightRequests:     2,
			storegatewayInflightRequests: 2,
			queryComponentName:           storeGatewayQueueDimension,
			expectedOverloadedComponent:  "",
		},
		{
			name:                         "ingester and store-gateway, equal load: not overloaded",
			ingesterInflightRequests:     3,
			storegatewayInflightRequests: 3,
			queryComponentName:           ingesterAndStoreGatewayQueueDimension,
			expectedOverloadedComponent:  "",
		},
		{
			name:                         "ingester only, zero load: not overloaded",
			ingesterInflightRequests:     0,
			storegatewayInflightRequests: 0,
			queryComponentName:           ingesterQueueDimension,
			expectedOverloadedComponent:  "",
		},
		{
			name:                         "store-gateway only, zero load: not overloaded",
			ingesterInflightRequests:     0,
			storegatewayInflightRequests: 0,
			queryComponentName:           storeGatewayQueueDimension,
			expectedOverloadedComponent:  "",
		},
		{
			name:                         "ingester and store-gateway, zero load: not overloaded",
			ingesterInflightRequests:     0,
			storegatewayInflightRequests: 0,
			queryComponentName:           storeGatewayQueueDimension,
			expectedOverloadedComponent:  "",
		},
		{
			name:                         "ingester only, other component with zero load: not overloaded",
			ingesterInflightRequests:     10000,
			storegatewayInflightRequests: 0,
			queryComponentName:           ingesterQueueDimension,
			expectedOverloadedComponent:  "",
		},
		{
			name:                         "store-gateway only, other component with zero load: not overloaded",
			ingesterInflightRequests:     0,
			storegatewayInflightRequests: 10000,
			queryComponentName:           storeGatewayQueueDimension,
			expectedOverloadedComponent:  "",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			queryComponentLoad, err := NewQueryComponentLoad(testOverloadFactor, prometheus.NewPedanticRegistry())
			require.NoError(t, err)

			alwaysNotOverloaded := &QueryComponentLoad{
				querierInflightRequestsByComponent: make(map[QueryComponent]int),
				querierInflightRequestsTotal:       0,
				// constructor disallows anything <=1; set it manually here
				overloadFactor: rand.Float64(),
			}

			for i := 0; i < testCase.ingesterInflightRequests; i++ {
				queryComponentLoad.IncrementForComponentName(ingesterQueueDimension)
			}

			for i := 0; i < testCase.storegatewayInflightRequests; i++ {
				queryComponentLoad.IncrementForComponentName(storeGatewayQueueDimension)
			}

			_, overloadedComponent := queryComponentLoad.IsOverloadedForComponentName(testCase.queryComponentName)
			require.Equal(t, overloadedComponent, testCase.expectedOverloadedComponent)

			// if overloadFactor is somehow set below 1, it should be ignored and always return not overloaded
			isOverloaded, _ := alwaysNotOverloaded.IsOverloadedForComponentName(testCase.queryComponentName)
			require.False(t, isOverloaded)
		})
	}
}
