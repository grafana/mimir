// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// MaxQueueLengthGauge is a prometheus.Collector that reports, per tenant, the
// maximum number of queued items observed since the last collection (scrape).
//
// The instantaneous cortex_query_scheduler_queue_length gauge only reflects the
// queue depth at scrape time, so short bursts between scrapes are invisible.
// MaxQueueLengthGauge tracks the running peak as items are enqueued and emits it
// on Collect, then resets each tenant's tracked maximum to its current depth so
// the next scrape window starts from the standing queue depth rather than zero.
type MaxQueueLengthGauge struct {
	desc *prometheus.Desc

	mtx     sync.Mutex
	tenants map[string]*tenantQueueDepth
}

type tenantQueueDepth struct {
	current int
	max     int
}

// NewMaxQueueLengthGauge returns a MaxQueueLengthGauge. The caller is
// responsible for registering it with a prometheus.Registerer.
func NewMaxQueueLengthGauge() *MaxQueueLengthGauge {
	return &MaxQueueLengthGauge{
		desc: prometheus.NewDesc(
			"cortex_query_scheduler_queue_length_peak",
			"Maximum number of queries observed in a tenant's queue since the last metric collection (reset on each scrape). Captures the true peak queue depth between scrapes.",
			[]string{"user"},
			nil,
		),
		tenants: map[string]*tenantQueueDepth{},
	}
}

// inc records the enqueue of one item for tenantID, updating the running peak.
func (g *MaxQueueLengthGauge) inc(tenantID string) {
	g.mtx.Lock()
	defer g.mtx.Unlock()

	d := g.tenants[tenantID]
	if d == nil {
		d = &tenantQueueDepth{}
		g.tenants[tenantID] = d
	}
	d.current++
	if d.current > d.max {
		d.max = d.current
	}
}

// dec records the dequeue of one item for tenantID.
func (g *MaxQueueLengthGauge) dec(tenantID string) {
	g.mtx.Lock()
	defer g.mtx.Unlock()

	d := g.tenants[tenantID]
	if d == nil {
		return
	}
	if d.current > 0 {
		d.current--
	}
}

// DeleteTenant drops all tracked state for tenantID. It is called when a tenant
// becomes inactive so the map does not grow unbounded.
func (g *MaxQueueLengthGauge) DeleteTenant(tenantID string) {
	g.mtx.Lock()
	defer g.mtx.Unlock()

	delete(g.tenants, tenantID)
}

func (g *MaxQueueLengthGauge) Describe(ch chan<- *prometheus.Desc) {
	ch <- g.desc
}

// Collect emits the peak queue depth observed for each tenant since the last
// collection, then resets each tenant's tracked maximum to its current depth.
//
// The tracked peaks are snapshotted (and reset) under a short lock, then emitted
// to ch afterwards: sending to ch can block until prometheus drains the metric
// channel, and inc/dec share this lock on the query dispatcher hot path, so the
// lock must not be held across the channel sends.
func (g *MaxQueueLengthGauge) Collect(ch chan<- prometheus.Metric) {
	type tenantPeak struct {
		tenantID string
		max      int
	}

	g.mtx.Lock()
	peaks := make([]tenantPeak, 0, len(g.tenants))
	for tenantID, d := range g.tenants {
		peaks = append(peaks, tenantPeak{tenantID: tenantID, max: d.max})
		d.max = d.current
	}
	g.mtx.Unlock()

	for _, p := range peaks {
		ch <- prometheus.MustNewConstMetric(g.desc, prometheus.GaugeValue, float64(p.max), p.tenantID)
	}
}
