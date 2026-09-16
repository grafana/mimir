// SPDX-License-Identifier: AGPL-3.0-only

package inflight

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// MaxInflightCollector is a prometheus.Collector that reports, per tenant, the peak number
// of concurrent in-flight queries and the greatest age reached by an in-flight query, both
// measured since the last collection.
//
// A plain in-flight gauge only shows the value at scrape time, so a burst that starts and
// ends between two scrapes is invisible. MaxInflightCollector tracks the running peak as
// queries start and finish, and on collection it also folds in the current age of every
// query still in flight. A query that was running at any point during the window therefore
// contributes to the reported age, whether or not it has finished.
//
// Note that both values are reset on collection.
type MaxInflightCollector struct {
	countDesc *prometheus.Desc
	ageDesc   *prometheus.Desc
	mtx       sync.Mutex
	nextID    InflightRequest
	entries   map[InflightRequest]entry
	tenants   map[string]*tenantInflight
	pool      sync.Pool
}

// InflightRequest identifies one in-flight request to a MaxInflightCollector.
// Add returns an InflightRequest and this should be passed to Remove
type InflightRequest uint64

// entry is one query that is currently in flight.
type entry struct {
	tenant *tenantInflight
	start  time.Time
}

type tenantInflight struct {
	current  int
	maxCount int
	// maxAge is the greatest age reached by a query that finished during this window.
	// Queries still in flight are accounted for by Collect instead.
	maxAge time.Duration
}

const (
	countMetricName = "cortex_query_frontend_max_inflight_requests"
	ageMetricName   = "cortex_query_frontend_max_inflight_request_age_seconds"

	typeLabel = "type"
	userLabel = "user"

	typeHelp = "The type label is \"http\" for requests entering the query-frontend, or \"dispatched\" for the sub-requests sent on to query-schedulers."

	countMetricHelp = "Peak number of concurrent in-flight requests for a tenant since the last metric collection (reset on each scrape). " + typeHelp
	ageMetricHelp   = "Greatest age reached by an in-flight request for a tenant since the last metric collection (reset on each scrape). Requests that finished within the window are included. " + typeHelp
)

// NewMaxInflightCollector returns a MaxInflightCollector reporting the in-flight requests of
// one part of the query-frontend, identified by requestType. The variable label is always
// "user". The caller is responsible for registering it with a prometheus.Registerer.
func NewMaxInflightCollector(requestType string) *MaxInflightCollector {
	constLabels := prometheus.Labels{typeLabel: requestType}

	return &MaxInflightCollector{
		countDesc: prometheus.NewDesc(countMetricName, countMetricHelp, []string{userLabel}, constLabels),
		ageDesc:   prometheus.NewDesc(ageMetricName, ageMetricHelp, []string{userLabel}, constLabels),
		entries:   map[InflightRequest]entry{},
		tenants:   map[string]*tenantInflight{},
		pool:      sync.Pool{New: func() any { return &tenantInflight{} }},
	}
}

// Add records the start of one in-flight query for tenantID and returns the handle to pass to
// Remove when the query finishes.
func (c *MaxInflightCollector) Add(tenantID string) InflightRequest {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	t := c.tenants[tenantID]
	if t == nil {
		t = c.pool.Get().(*tenantInflight)
		c.tenants[tenantID] = t
	}

	c.nextID++
	c.entries[c.nextID] = entry{tenant: t, start: time.Now()}

	t.current++
	if t.current > t.maxCount {
		t.maxCount = t.current
	}

	return c.nextID
}

// Remove records the end of the in-flight query identified by id, folding its final age into
// the tenant's peak for this window.
//
// Remove is idempotent.
func (c *MaxInflightCollector) Remove(id InflightRequest) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	e, ok := c.entries[id]
	if !ok {
		return
	}
	delete(c.entries, id)

	e.tenant.current--
	if age := time.Since(e.start); age > e.tenant.maxAge {
		e.tenant.maxAge = age
	}
}

func (c *MaxInflightCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.countDesc
	ch <- c.ageDesc
}

func (c *MaxInflightCollector) Collect(ch chan<- prometheus.Metric) {
	type tenantPeak struct {
		tenantID string
		maxCount int
		maxAge   time.Duration
	}

	c.mtx.Lock()

	// Fold in the age of every query still in flight, so a query that has not finished yet
	// still contributes to the age reported for the window it spans.
	now := time.Now()
	for _, e := range c.entries {
		if age := now.Sub(e.start); age > e.tenant.maxAge {
			e.tenant.maxAge = age
		}
	}

	peaks := make([]tenantPeak, 0, len(c.tenants))
	for tenantID, t := range c.tenants {
		peaks = append(peaks, tenantPeak{tenantID: tenantID, maxCount: t.maxCount, maxAge: t.maxAge})

		if t.current == 0 {
			// The tenant cannot contribute to the next window until it sends another query,
			// and Add will take it from the pool then. Drop it to keep the map bounded.
			delete(c.tenants, tenantID)
			*t = tenantInflight{}
			c.pool.Put(t)
			continue
		}
		t.maxCount = t.current
		t.maxAge = 0
	}
	c.mtx.Unlock()

	for _, p := range peaks {
		ch <- prometheus.MustNewConstMetric(c.countDesc, prometheus.GaugeValue, float64(p.maxCount), p.tenantID)
		ch <- prometheus.MustNewConstMetric(c.ageDesc, prometheus.GaugeValue, p.maxAge.Seconds(), p.tenantID)
	}
}
