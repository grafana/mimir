// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type QueryComponent string

const (
	StoreGateway QueryComponent = "store-gateway"
	Ingester     QueryComponent = "ingester"
)

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
	inflightRequestsMu sync.RWMutex
	// inflightRequests tracks requests from the time the request was successfully sent to a querier
	// to the time the request was completed by the querier or failed due to cancel, timeout, or disconnect.
	inflightRequests             map[RequestKey]*SchedulerRequest
	ingesterInflightRequests     int
	storeGatewayInflightRequests int
	querierInflightRequestsTotal int

	querierInflightRequestsMetric *prometheus.SummaryVec
}

func NewQueryComponentUtilization(
	querierInflightRequestsMetric *prometheus.SummaryVec,
) (*QueryComponentUtilization, error) {

	return &QueryComponentUtilization{
		inflightRequests:             map[RequestKey]*SchedulerRequest{},
		ingesterInflightRequests:     0,
		storeGatewayInflightRequests: 0,
		querierInflightRequestsTotal: 0,

		querierInflightRequestsMetric: querierInflightRequestsMetric,
	}, nil
}

// MarkRequestSent is called when a request is sent to a querier
func (qcl *QueryComponentUtilization) MarkRequestSent(req *SchedulerRequest) {
	if req != nil {
		qcl.inflightRequestsMu.Lock()
		defer qcl.inflightRequestsMu.Unlock()

		qcl.inflightRequests[req.Key()] = req
		qcl.incrementForComponentName(req.ExpectedQueryComponentName())
	}
}

// MarkRequestCompleted is called when a querier completes or fails a request
func (qcl *QueryComponentUtilization) MarkRequestCompleted(req *SchedulerRequest) {
	if req != nil {
		qcl.inflightRequestsMu.Lock()
		defer qcl.inflightRequestsMu.Unlock()

		reqKey := req.Key()
		if req, ok := qcl.inflightRequests[reqKey]; ok {
			qcl.decrementForComponentName(req.ExpectedQueryComponentName())
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

func (qcl *QueryComponentUtilization) ObserveInflightRequests() {
	qcl.inflightRequestsMu.RLock()
	defer qcl.inflightRequestsMu.RUnlock()
	qcl.querierInflightRequestsMetric.WithLabelValues(string(Ingester)).Observe(float64(qcl.ingesterInflightRequests))
	qcl.querierInflightRequestsMetric.WithLabelValues(string(StoreGateway)).Observe(float64(qcl.storeGatewayInflightRequests))
}

// GetForComponent is a test-only util
func (qcl *QueryComponentUtilization) GetForComponent(component QueryComponent) int {
	qcl.inflightRequestsMu.RLock()
	defer qcl.inflightRequestsMu.RUnlock()
	switch component {
	case Ingester:
		return qcl.ingesterInflightRequests
	case StoreGateway:
		return qcl.storeGatewayInflightRequests
	default:
		return 0
	}
}
