// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"math"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
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

// QueryComponentNameForRequest parses the expected query component from annotations by the frontend.
func QueryComponentNameForRequest(req *SchedulerRequest) string {
	var expectedQueryComponent string
	if len(req.AdditionalQueueDimensions) > 0 {
		expectedQueryComponent = req.AdditionalQueueDimensions[0]
	}

	return expectedQueryComponent
}

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

// QueryComponentCapacity tracks requests from the time they are forwarded to a querier
// to the time are completed by the querier or failed due to cancel, timeout, or disconnect.
// Unlike the Scheduler's schedulerInflightRequests, tracking begins only when the request is sent to a querier.
//
// Scheduler-Querier inflight requests are broken out by the query component flags,
// representing whether the query request will be served by the ingesters, store-gateways, or both.
// Query requests utilizing both ingesters and store-gateways are tracked in the atomics for both component,
// therefore the sum of inflight requests by component is likely to exceed the inflight requests total.
type QueryComponentCapacity struct {
	// targetReservedCapacity sets the portion of querier-worker connections we attempt to reserve
	// for queries to the less-loaded query component when the query queue becomes backlogged.
	targetReservedCapacity float64

	ingesterInflightRequests     *atomic.Int64
	storeGatewayInflightRequests *atomic.Int64
	querierInflightRequestsTotal *atomic.Int64

	querierInflightRequestsGauge *prometheus.GaugeVec
}

// DefaultReservedQueryComponentCapacity reserves 1 / 3 of querier-worker connections
// for the query component utilizing fewer of the available connections.
const DefaultReservedQueryComponentCapacity = 0.33

const MinReservedQueryComponentCapacity = 0.1
const MaxReservedQueryComponentCapacity = 0.5

func NewQueryComponentCapacity(
	targetReservedCapacity float64,
	querierInflightRequests *prometheus.GaugeVec,
) (*QueryComponentCapacity, error) {

	if targetReservedCapacity < MinReservedQueryComponentCapacity || targetReservedCapacity >= MaxReservedQueryComponentCapacity {
		return nil, errors.New("invalid targetReservedCapacity")
	}

	return &QueryComponentCapacity{
		targetReservedCapacity: targetReservedCapacity,

		ingesterInflightRequests:     atomic.NewInt64(0),
		storeGatewayInflightRequests: atomic.NewInt64(0),
		querierInflightRequestsTotal: atomic.NewInt64(0),

		querierInflightRequestsGauge: querierInflightRequests,
	}, nil
}

// ExceedsCapacityForComponentName checks whether a query component has exceeded the capacity utilization threshold.
// This enables the dequeuing algorithm to skip requests for a query component experiencing heavy load,
// reserving querier-worker connection capacity to continue servicing requests for the other component.
// If there are no requests in queue for the other, less-utilized component, the dequeuing algorithm may choose
// to continue servicing requests for the component which has exceeded the reserved capacity threshold.
//
// Capacity utilization for a QueryComponent is defined by the portion of the querier-worker connections
// which are currently in flight processing a query which requires that QueryComponent.
//
// The component name can indicate the usage of one or both of ingesters and store-gateways,
// and will be indicated as exceeding the threshold if either ingesters or store-gateways
// are currently in excess of the reserved capacity.
//
// The threshold for a QueryComponent's utilization of querier-worker connections
// can only be exceeded by one QueryComponent at a time as long as targetReservedCapacity is < 0.5.
// Therefore, one of the components will always be given the OK to dequeue queries for.
//
// Capacity reservation only occurs when the queue backlogged, where backlogged is defined as
// (length of the query queue) >= (number of querier-worker connections waiting for a query).
//
// A QueryComponent's utilization is allowed to exceed the reserved capacity when the queue is not backlogged.
// If an influx of queries then creates a backlog, this method will indicate to skip queries for the component.
// As the inflight queries complete or fail, the component's utilization will naturally decrease.
// This method will continue to indicate to skip queries for the component until it is back under the threshold.
func (qcl *QueryComponentCapacity) ExceedsCapacityForComponentName(
	name string, connectedWorkers *atomic.Int64, queueLen, waitingWorkers int,
) (bool, QueryComponent) {
	if waitingWorkers > queueLen {
		// excess querier-worker capacity; no need to reserve any for now
		return false, ""
	}
	if connectedWorkers.Load() <= 1 {
		// corner case; cannot reserve capacity with only one worker available
		return false, ""
	}

	// reserve at least one connection in case (connected workers) * (reserved capacity) is less than one
	minReservedConnections := int64(
		math.Ceil(
			math.Max(qcl.targetReservedCapacity*float64(connectedWorkers.Load()), 1),
		),
	)

	isIngester, isStoreGateway := queryComponentFlags(name)
	if isIngester {
		if connectedWorkers.Load()-(qcl.ingesterInflightRequests.Load()) <= minReservedConnections {
			return true, Ingester
		}
	}
	if isStoreGateway {
		if connectedWorkers.Load()-(qcl.storeGatewayInflightRequests.Load()) <= minReservedConnections {
			return true, StoreGateway
		}
	}
	return false, ""
}

// IncrementForComponentName is called when a request is sent to a querier
func (qcl *QueryComponentCapacity) IncrementForComponentName(expectedQueryComponent string) {
	qcl.updateForComponentName(expectedQueryComponent, 1)
}

// DecrementForComponentName is called when a querier completes or fails a request
func (qcl *QueryComponentCapacity) DecrementForComponentName(expectedQueryComponent string) {
	qcl.updateForComponentName(expectedQueryComponent, -1)
}

func (qcl *QueryComponentCapacity) updateForComponentName(expectedQueryComponent string, increment int64) {
	isIngester, isStoreGateway := queryComponentFlags(expectedQueryComponent)

	if isIngester {
		qcl.ingesterInflightRequests.Add(increment)
		qcl.querierInflightRequestsGauge.WithLabelValues(string(Ingester)).Add(float64(increment))
	}
	if isStoreGateway {
		qcl.storeGatewayInflightRequests.Add(increment)
		qcl.querierInflightRequestsGauge.WithLabelValues(string(StoreGateway)).Add(float64(increment))
	}
	qcl.querierInflightRequestsTotal.Add(increment)
}
