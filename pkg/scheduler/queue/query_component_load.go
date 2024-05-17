// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"math"
	"sync"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type QueryComponent string

const (
	StoreGateway QueryComponent = "store-gateway"
	Ingester     QueryComponent = "ingester"
)

var QueryComponents = []QueryComponent{
	StoreGateway,
	Ingester,
}

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
	// as we prefer to overestimate query component load rather than underestimate.
	isIngester, isStoreGateway = true, true

	switch queryComponentName {
	case ingesterQueueDimension:
		isStoreGateway = false
	case storeGatewayQueueDimension:
		isIngester = false
	}
	return isIngester, isStoreGateway
}

// QueryComponentLoad tracks requests from the time they are forwarded to a querier
// to the time are completed by the querier or failed due to cancel, timeout, or disconnect.
// Unlike the Scheduler's schedulerInflightRequests, tracking begins only when the request is sent to a querier.
//
// Scheduler-Querier inflight requests are broken out by the query component flags,
// representing whether the query request will be served by the ingesters, store-gateways, or both.
// Query requests utilizing both ingesters and store-gateways are tracked in the map entry for both keys,
// therefore the sum of inflight requests by component is likely to exceed the inflight requests total.
type QueryComponentLoad struct {
	overloadFactor float64

	inflightRequestsMu                 sync.RWMutex
	querierInflightRequestsByComponent map[QueryComponent]int
	querierInflightRequestsTotal       int

	querierInflightRequestsGauge *prometheus.GaugeVec
}

// QueryComponentDefaultOverloadFactor component is overloaded if it has double the inflight requests as the other
const QueryComponentDefaultOverloadFactor = 2.0

func NewQueryComponentLoad(overloadFactor float64, registerer prometheus.Registerer) (*QueryComponentLoad, error) {
	if overloadFactor <= 1 {
		return nil, errors.New("overloadFactor must be greater than 1")
	}

	return &QueryComponentLoad{
		querierInflightRequestsByComponent: make(map[QueryComponent]int),
		querierInflightRequestsTotal:       0,
		overloadFactor:                     overloadFactor,
		querierInflightRequestsGauge: promauto.With(registerer).NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "cortex_query_scheduler_querier_inflight_requests",
				Help: "Number of inflight requests being processed on a querier-scheduler connection.",
			},
			[]string{"query_component"},
		),
	}, nil
}

// IsOverloadedForComponentName checks if load for a query component name is at or above the overload threshold.
// The component name can indicate the usage of one or more QueryComponents
// but only one component can be considered to be overloaded at a time,
// as the threshold is determined by the second-most-loaded-component.
func (qcl *QueryComponentLoad) IsOverloadedForComponentName(name string) (bool, QueryComponent) {
	isIngester, isStoreGateway := queryComponentFlags(name)

	qcl.inflightRequestsMu.RLock()
	defer qcl.inflightRequestsMu.RUnlock()

	overloadThreshold := qcl.overloadThreshold()
	if overloadThreshold <= 0 {
		return false, ""
	}

	if isIngester {
		if qcl.querierInflightRequestsByComponent[Ingester] >= overloadThreshold {
			return true, Ingester
		}
	}
	if isStoreGateway {
		if qcl.querierInflightRequestsByComponent[StoreGateway] >= overloadThreshold {
			return true, StoreGateway
		}
	}
	return false, ""
}

// overloadThreshold calculates the minimum number of inflight requests to qualify a query component as overloaded.
// This is determined by (inflight requests for second-most-loaded component) * (overload factor).
// Intended to only be called within a read lock on inflightRequestsMu.
func (qcl *QueryComponentLoad) overloadThreshold() int {
	// overload factor is expected to be > 1; any misconfiguration of this should be a no-op
	if qcl.overloadFactor <= 1 {
		return 0
	}

	// no overloaded component if there are no inflight requests at all.
	if qcl.querierInflightRequestsTotal == 0 {
		return 0
	}

	// we only intend to operate with two query components for now,
	// but we generalize the calculation to apply the overload factor to the second-most-loaded component
	highest, secondHighest := 0, 0

	for _, componentInflightRequests := range qcl.querierInflightRequestsByComponent {
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
func (qcl *QueryComponentLoad) IncrementForComponentName(expectedQueryComponent string) {
	qcl.updateForComponentName(expectedQueryComponent, 1)
}

// DecrementForComponentName is called when a querier completes or fails a request
func (qcl *QueryComponentLoad) DecrementForComponentName(expectedQueryComponent string) {
	qcl.updateForComponentName(expectedQueryComponent, -1)
}

func (qcl *QueryComponentLoad) updateForComponentName(expectedQueryComponent string, increment int) {
	isIngester, isStoreGateway := queryComponentFlags(expectedQueryComponent)

	qcl.inflightRequestsMu.Lock()
	defer qcl.inflightRequestsMu.Unlock()
	if isIngester {
		qcl.querierInflightRequestsByComponent[Ingester] += increment
		qcl.querierInflightRequestsGauge.WithLabelValues(string(Ingester)).Add(float64(increment))
	}
	if isStoreGateway {
		qcl.querierInflightRequestsByComponent[StoreGateway] += increment
		qcl.querierInflightRequestsGauge.WithLabelValues(string(StoreGateway)).Add(float64(increment))
	}
	qcl.querierInflightRequestsTotal += increment
}
