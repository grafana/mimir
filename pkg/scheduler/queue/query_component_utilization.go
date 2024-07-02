// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"math"
	"sync"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type QueryComponent string

const (
	StoreGateway QueryComponent = "store-gateway"
	Ingester     QueryComponent = "ingester"
)

// cannot import constants from frontend/v2 due to import cycle
// these are attached to the request's AdditionalQueueDimensions by the frontend.
const ingesterQueueDimension = "ingester"
const storeGatewayQueueDimension = "store-gateway"
const ingesterAndStoreGatewayQueueDimension = "ingester-and-store-gateway"

// queryComponentFlags interprets annotations by the frontend for the expected query component,
// and flags whether a query request is expected to be served by the ingesters, store-gateways, or both.
func queryComponentFlags(queryComponentName string) (isIngester, isStoreGateway bool) {
	// Annotations from the frontend representing "both" or "unknown" do not need to be checked explicitly.
	// Conservatively, we assume a query request will be served by both query components
	// as we prefer to overestimate query component capacity utilization rather than underestimate.
	isIngester, isStoreGateway = true, true

	switch queryComponentName {
	case ingesterQueueDimension:
		isStoreGateway = false
	case storeGatewayQueueDimension:
		isIngester = false
	}
	return isIngester, isStoreGateway
}

// QueryComponentUtilization tracks requests from the time they are forwarded to a querier
// to the time are completed by the querier or failed due to cancel, timeout, or disconnect.
// Unlike the Scheduler's schedulerInflightRequests, tracking begins only when the request is sent to a querier.
//
// Scheduler-Querier inflight requests are broken out by the query component flags,
// representing whether the query request will be served by the ingesters, store-gateways, or both.
// Query requests utilizing both ingesters and store-gateways are tracked in the atomics for both component,
// therefore the sum of inflight requests by component is likely to exceed the inflight requests total.
type QueryComponentUtilization struct {
	// targetReservedCapacity sets the portion of querier-worker connections we attempt to reserve
	// for queries to the less-loaded query component when the query queue becomes backlogged.
	targetReservedCapacity float64

	inflightRequestsMu sync.RWMutex
	// inflightRequests tracks requests from the time the request was successfully sent to a querier
	// to the time the request was completed by the querier or failed due to cancel, timeout, or disconnect.
	inflightRequests             map[RequestKey]*SchedulerRequest
	ingesterInflightRequests     int
	storeGatewayInflightRequests int
	querierInflightRequestsTotal int

	querierInflightRequestsMetric *prometheus.SummaryVec
}

// DefaultReservedQueryComponentCapacity reserves 1 / 3 of querier-worker connections
// for the query component utilizing fewer of the available connections.
// Chosen to represent an even balance between the three possible combinations of query components:
// ingesters only, store-gateways only, or both ingesters and store-gateways.
const DefaultReservedQueryComponentCapacity = 0.33

// MaxReservedQueryComponentCapacity is an exclusive upper bound on the targetReservedCapacity.
// The threshold for a QueryComponent's utilization of querier-worker connections
// can only be exceeded by one QueryComponent at a time as long as targetReservedCapacity is < 0.5.
// Therefore, one of the components will always be given the OK to dequeue queries for.
const MaxReservedQueryComponentCapacity = 0.5

func NewQueryComponentUtilization(
	targetReservedCapacity float64,
	querierInflightRequestsMetric *prometheus.SummaryVec,
) (*QueryComponentUtilization, error) {

	if targetReservedCapacity >= MaxReservedQueryComponentCapacity {
		return nil, errors.New("invalid targetReservedCapacity")
	}

	return &QueryComponentUtilization{
		targetReservedCapacity: targetReservedCapacity,

		inflightRequests:             map[RequestKey]*SchedulerRequest{},
		ingesterInflightRequests:     0,
		storeGatewayInflightRequests: 0,
		querierInflightRequestsTotal: 0,

		querierInflightRequestsMetric: querierInflightRequestsMetric,
	}, nil
}

func (qcl *QueryComponentUtilization) ExceedsThresholdForComponentName(
	name string, connectedWorkers int,
) (bool, QueryComponent) {
	if connectedWorkers <= 1 {
		// corner case; cannot reserve capacity with only one worker available
		return false, ""
	}

	// allow the functionality to be turned off via setting targetReservedCapacity to 0
	minReservedConnections := 0
	if qcl.targetReservedCapacity > 0 {
		// reserve at least one connection in case (connected workers) * (reserved capacity) is less than one
		minReservedConnections = int(
			math.Ceil(
				math.Max(qcl.targetReservedCapacity*float64(connectedWorkers), 1),
			),
		)
	}

	isIngester, isStoreGateway := queryComponentFlags(name)
	qcl.inflightRequestsMu.RLock()
	defer qcl.inflightRequestsMu.RUnlock()

	if isIngester {
		if connectedWorkers-(qcl.ingesterInflightRequests) <= minReservedConnections {
			return true, Ingester
		}
	}
	if isStoreGateway {
		if connectedWorkers-(qcl.storeGatewayInflightRequests) <= minReservedConnections {
			return true, StoreGateway
		}
	}
	return false, ""
}

func (qcl *QueryComponentUtilization) TriggerUtilizationCheck(queueLen, waitingWorkers int) bool {
	if waitingWorkers > queueLen {
		// excess querier-worker capacity; no need to reserve any for now
		return false
	}
	return true
}

// MarkRequestSent is called when a request is sent to a querier
func (qcl *QueryComponentUtilization) MarkRequestSent(req *SchedulerRequest) {
	if req != nil {
		qcl.inflightRequestsMu.Lock()
		defer qcl.inflightRequestsMu.Unlock()

		qcl.inflightRequests[req.Key()] = req
		queryComponent := req.ExpectedQueryComponentName()
		qcl.incrementForComponentName(queryComponent)

	}
}

// MarkRequestCompleted is called when a querier completes or fails a request
func (qcl *QueryComponentUtilization) MarkRequestCompleted(req *SchedulerRequest) {
	if req != nil {
		qcl.inflightRequestsMu.Lock()
		defer qcl.inflightRequestsMu.Unlock()

		reqKey := req.Key()
		if req, ok := qcl.inflightRequests[reqKey]; ok {
			queryComponent := req.ExpectedQueryComponentName()
			qcl.decrementForComponentName(queryComponent)
		} else {
		}
		delete(qcl.inflightRequests, reqKey)
	}
}

func (qcl *QueryComponentUtilization) incrementForComponentName(expectedQueryComponent string) {
	qcl.updateForComponentName(expectedQueryComponent, 1)
}

func (qcl *QueryComponentUtilization) decrementForComponentName(expectedQueryComponent string) {
	qcl.updateForComponentName(expectedQueryComponent, -1)
}

func (qcl *QueryComponentUtilization) updateForComponentName(expectedQueryComponent string, increment int) {
	isIngester, isStoreGateway := queryComponentFlags(expectedQueryComponent)
	// lock is expected to be obtained by the calling method to mark the request as sent or completed
	if isIngester {
		qcl.ingesterInflightRequests += increment
	}
	if isStoreGateway {
		qcl.storeGatewayInflightRequests += increment
	}
	qcl.querierInflightRequestsTotal += increment
}

func (qcl *QueryComponentUtilization) GetInflightRequests() (ingester, storeGateway int) {
	qcl.inflightRequestsMu.RLock()
	defer qcl.inflightRequestsMu.RUnlock()
	return qcl.ingesterInflightRequests, qcl.storeGatewayInflightRequests
}

func (qcl *QueryComponentUtilization) ObserveInflightRequests() {
	qcl.inflightRequestsMu.RLock()
	defer qcl.inflightRequestsMu.RUnlock()
	qcl.querierInflightRequestsMetric.WithLabelValues(string(Ingester)).Observe(float64(qcl.ingesterInflightRequests))
	qcl.querierInflightRequestsMetric.WithLabelValues(string(StoreGateway)).Observe(float64(qcl.storeGatewayInflightRequests))
}
