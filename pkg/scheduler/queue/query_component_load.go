// SPDX-License-Identifier: AGPL-3.0-only

package queue

import "sync"

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

// nolint: unused
// queryComponents interprets annotations by the frontend for the expected query component,
// and flags whether a query request is expected to be served by the ingesters, store-gateways, or both.
func queryComponents(req *SchedulerRequest) (isIngester, isStoreGateway bool) {
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
	// Unlike the Scheduler's schedulerInflightRequests, this does not begin until the request is sent to a querier.
	// Scheduler-Querier inflight requests are broken out by the query component,
	// representing whether the query request will be served by the ingesters, store-gateways, or both.
	// Query requests utilizing both ingesters and store-gateways are tracked in the map entry for both keys.
	schedulerQuerierInflightRequestsByQueryComponent map[QueryComponent]int //nolint: unused
	schedulerQuerierTotalInflightRequests            int                    //nolint: unused
}

// nolint: unused
func (qcl *QueryComponentLoad) incrementQueryComponentInflightRequests(req *SchedulerRequest) {
	qcl.updateQueryComponentInflightRequests(req, 1)
}

// nolint: unused
func (qcl *QueryComponentLoad) decrementQueryComponentInflightRequests(req *SchedulerRequest) {
	qcl.updateQueryComponentInflightRequests(req, -1)
}

// nolint: unused
func (qcl *QueryComponentLoad) updateQueryComponentInflightRequests(req *SchedulerRequest, increment int) {
	isIngester, isStoreGateway := queryComponents(req)

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
