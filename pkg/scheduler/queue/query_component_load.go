// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"math"
	"sync"
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

// QueryComponentsForRequest interprets annotations by the frontend for the expected query component,
// and flags whether a query request is expected to be served by the ingesters, store-gateways, or both.
// nolint: unused
func QueryComponentsForRequest(req *SchedulerRequest) (isIngester, isStoreGateway bool) {
	var expectedQueryComponent string
	if len(req.AdditionalQueueDimensions) > 0 {
		expectedQueryComponent = req.AdditionalQueueDimensions[0]
	}

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

// IsOverloadedForQueryComponents checks if TODO
// nolint: unused
func (qcl *QueryComponentLoad) IsOverloadedForQueryComponents(isIngester, isStoreGateway bool, overloadThreshold int) bool {
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

	// compute average inflight requests per component.
	avg := float64(qcl.schedulerQuerierTotalInflightRequests) / float64(len(qcl.schedulerQuerierInflightRequestsByQueryComponent))

	// compute the overload threshold.
	overloadThreshold := int(math.Ceil(avg * qcl.overloadFactor))
	return overloadThreshold
}

// IncrementQueryComponentInflightRequests is called when a request is sent to a querier
// nolint: unused
func (qcl *QueryComponentLoad) IncrementQueryComponentInflightRequests(req *SchedulerRequest) {
	qcl.updateQueryComponentInflightRequests(req, 1)
}

// DecrementQueryComponentInflightRequests is called when a querier completes or fails a request
// nolint: unused
func (qcl *QueryComponentLoad) DecrementQueryComponentInflightRequests(req *SchedulerRequest) {
	qcl.updateQueryComponentInflightRequests(req, -1)
}

// nolint: unused
func (qcl *QueryComponentLoad) updateQueryComponentInflightRequests(req *SchedulerRequest, increment int) {
	isIngester, isStoreGateway := QueryComponentsForRequest(req)

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
