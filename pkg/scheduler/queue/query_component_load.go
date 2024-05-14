// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"math"
	"sync"

	"github.com/pkg/errors"
)

type QueryComponent int

const (
	StoreGateway QueryComponent = iota
	Ingester
)

// cannot import constants from frontend/v2 due to import cycle
// other options do not need to be checked explicitly
const ingesterQueueDimension = "ingester"
const storeGatewayQueueDimension = "store-gateway"
const ingesterAndStoreGatewayQueueDimension = "ingester-and-store-gateway"

// QueryComponentFlagsForRequest wraps QueryComponentFlags to parse the expected query component from a request.
// nolint: unused
func QueryComponentFlagsForRequest(req *SchedulerRequest) (isIngester, isStoreGateway bool) {
	var expectedQueryComponent string
	if len(req.AdditionalQueueDimensions) > 0 {
		expectedQueryComponent = req.AdditionalQueueDimensions[0]
	}

	return QueryComponentFlags(expectedQueryComponent)
}

// QueryComponentFlags interprets annotations by the frontend for the expected query component,
// and flags whether a query request is expected to be served by the ingesters, store-gateways, or both.
// nolint: unused
func QueryComponentFlags(expectedQueryComponent string) (isIngester, isStoreGateway bool) {
	// Annotations from the frontend representing "both" or "unknown" do not need to be checked explicitly.
	// Conservatively, we assume a query request will be served by both query components
	// as we prefer to overestimate query component load rather than underestimate.
	isIngester, isStoreGateway = true, true

	switch expectedQueryComponent {
	case ingesterQueueDimension:
		isStoreGateway = false
	case storeGatewayQueueDimension:
		isIngester = false
	}
	return isIngester, isStoreGateway
}

type QueryComponentLoad struct {
	inflightRequestsMu sync.RWMutex //nolint: unused
	// schedulerQuerierInflightRequestsByQueryComponent tracks requests from the time they are forwarded to a querier
	// to the time are completed by the querier or failed due to cancel, timeout, or disconnect.
	// Unlike the Scheduler's schedulerInflightRequests, tracking begins only when the request is sent to a querier.
	// Scheduler-Querier inflight requests are broken out by the query component,
	// representing whether the query request will be served by the ingesters, store-gateways, or both.
	// Query requests utilizing both ingesters and store-gateways are tracked in the map entry for both keys.
	schedulerQuerierInflightRequestsByQueryComponent map[QueryComponent]int //nolint: unused
	schedulerQuerierTotalInflightRequests            int                    //nolint: unused

	overloadFactor float64 // nolint: unused
}

// nolint: unused
const defaultOverloadFactor = 2.0 // component is overloaded if it has double the inflight requests as the other

func NewQueryComponentLoad(overloadFactor float64) (*QueryComponentLoad, error) {
	if overloadFactor <= 1 {
		return nil, errors.New("overloadFactor must be greater than 1")
	}
	return &QueryComponentLoad{
		schedulerQuerierInflightRequestsByQueryComponent: make(map[QueryComponent]int),
		schedulerQuerierTotalInflightRequests:            0,
		overloadFactor:                                   overloadFactor,
	}, nil
}

// IsOverloadedForQueryComponents checks if TODO
// nolint: unused
func (qcl *QueryComponentLoad) IsOverloadedForQueryComponents(isIngester, isStoreGateway bool) bool {
	overloadThreshold := qcl.overloadThreshold()

	if overloadThreshold <= 0 {
		return false
	}

	qcl.inflightRequestsMu.RLock()
	defer qcl.inflightRequestsMu.RUnlock()

	if isIngester {
		if qcl.schedulerQuerierInflightRequestsByQueryComponent[Ingester] >= overloadThreshold {
			return true
		}
	}
	if isStoreGateway {
		if qcl.schedulerQuerierInflightRequestsByQueryComponent[StoreGateway] >= overloadThreshold {
			return true
		}
	}
	return false
}

// nolint: unused
func (qcl *QueryComponentLoad) overloadThreshold() int {
	// overload factor is expected to be > 1; any misconfiguration of this should be a no-op
	if qcl.overloadFactor <= 1 {
		return 0
	}

	qcl.inflightRequestsMu.RLock()
	defer qcl.inflightRequestsMu.RUnlock()

	// no overloaded component if there are no inflight requests at all.
	if qcl.schedulerQuerierTotalInflightRequests == 0 {
		return 0
	}

	// we only intend to operate with two query components for now,
	// but we generalize the calculation to apply the overload factor to the second-most-loaded component
	highest, secondHighest := 0, 0

	for _, componentInflightRequests := range qcl.schedulerQuerierInflightRequestsByQueryComponent {
		if componentInflightRequests > highest {
			secondHighest = highest
			highest = componentInflightRequests
		} else if componentInflightRequests > secondHighest && componentInflightRequests < highest {
			secondHighest = componentInflightRequests
		}
	}

	if secondHighest <= 0 {
		return 0
	}

	// compute the overload threshold.
	overloadThreshold := int(math.Ceil(float64(secondHighest) * qcl.overloadFactor))
	return overloadThreshold
}

// IncrementForComponentName is called when a request is sent to a querier
// nolint: unused
func (qcl *QueryComponentLoad) IncrementForComponentName(expectedQueryComponent string) {
	qcl.updateForComponentName(expectedQueryComponent, 1)
}

// DecrementForComponentName is called when a querier completes or fails a request
// nolint: unused
func (qcl *QueryComponentLoad) DecrementForComponentName(expectedQueryComponent string) {
	qcl.updateForComponentName(expectedQueryComponent, -1)
}

// nolint: unused
func (qcl *QueryComponentLoad) updateForComponentName(expectedQueryComponent string, increment int) {
	isIngester, isStoreGateway := QueryComponentFlags(expectedQueryComponent)

	qcl.inflightRequestsMu.Lock()
	defer qcl.inflightRequestsMu.Unlock()
	if isIngester {
		qcl.schedulerQuerierInflightRequestsByQueryComponent[Ingester] += increment
	}
	if isStoreGateway {
		qcl.schedulerQuerierInflightRequestsByQueryComponent[StoreGateway] += increment
	}
	qcl.schedulerQuerierTotalInflightRequests += increment
}
