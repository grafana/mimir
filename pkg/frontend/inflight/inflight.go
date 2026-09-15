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
// Both values are reset on collection. The peak count resets to the number of queries
// currently in flight, so the next window starts from standing concurrency rather than
// zero. The age resets to zero, because the age of a query still running is recomputed from
// its start time on every collection.
//
// Since the reset happens in Collect, every scrape consumes the window: a second scraper,
// such as another Prometheus or a manual request to /metrics, takes a share of the peaks
// away from the first.
//
// One mutex covers all tenants, and Add and Remove run on the query hot path, so it sees
// contention proportional to query concurrency. Note that queue.MaxQueueLengthGauge looks
// like the same design but is not comparable: its inc and dec are only reached from the
// scheduler's single dispatcherLoop goroutine, so its lock only ever contends with the
// scrape. Collect additionally walks every in-flight query to fold in its current age,
// holding that same lock, so its cost grows with the number of in-flight queries. See
// BenchmarkAddRemove and BenchmarkCollect before changing either.
type MaxInflightCollector struct {
	countDesc *prometheus.Desc
	ageDesc   *prometheus.Desc

	// now returns the current time. It is overridable so unit tests can deterministically
	// stamp start times and compute ages. Production code uses time.Now.
	now func() time.Time

	mtx     sync.Mutex
	nextID  uint64
	entries map[uint64]entry
	tenants map[string]*tenantInflight
}

// entry is one query that is currently in flight. It holds the tenant's counters directly
// rather than the tenant ID, so neither Remove nor Collect has to hash a string to find
// them, and an in-flight query does not retain a reference to the tenant ID string.
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

// NewMaxInflightCollector returns a MaxInflightCollector exposing the two given metric names
// and help strings. The variable label is always "user". The caller is responsible for
// registering it with a prometheus.Registerer.
func NewMaxInflightCollector(countName, countHelp, ageName, ageHelp string) *MaxInflightCollector {
	return &MaxInflightCollector{
		countDesc: prometheus.NewDesc(countName, countHelp, []string{"user"}, nil),
		ageDesc:   prometheus.NewDesc(ageName, ageHelp, []string{"user"}, nil),
		now:       time.Now,
		entries:   map[uint64]entry{},
		tenants:   map[string]*tenantInflight{},
	}
}

// Add records the start of one in-flight query for tenantID and returns the ID to pass to
// Remove when the query finishes. The returned ID is never zero, so callers can use zero to
// mean "not tracked".
func (c *MaxInflightCollector) Add(tenantID string) uint64 {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	t := c.tenants[tenantID]
	if t == nil {
		t = &tenantInflight{}
		c.tenants[tenantID] = t
	}

	c.nextID++
	c.entries[c.nextID] = entry{tenant: t, start: c.now()}

	t.current++
	if t.current > t.maxCount {
		t.maxCount = t.current
	}

	return c.nextID
}

// Remove records the end of the in-flight query identified by id, folding its final age into
// the tenant's peak for this window.
//
// Calling Remove more than once for the same ID, or with an ID that was never issued, is a
// no-op. Callers whose cleanup can run twice rely on this.
func (c *MaxInflightCollector) Remove(id uint64) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	e, ok := c.entries[id]
	if !ok {
		return
	}
	delete(c.entries, id)

	e.tenant.current--
	if age := c.now().Sub(e.start); age > e.tenant.maxAge {
		e.tenant.maxAge = age
	}
}

func (c *MaxInflightCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.countDesc
	ch <- c.ageDesc
}

// Collect emits the peak in-flight count and the greatest in-flight query age observed for
// each tenant since the last collection, then opens a new window.
//
// The snapshot is taken, and the window reset, under a short lock and emitted to ch
// afterwards: sending to ch can block until prometheus drains the metric channel, and
// Add/Remove share this lock on the request hot path, so the lock must not be held across
// the channel sends.
func (c *MaxInflightCollector) Collect(ch chan<- prometheus.Metric) {
	type tenantPeak struct {
		tenantID string
		maxCount int
		maxAge   time.Duration
	}

	c.mtx.Lock()

	// Fold in the age of every query still in flight, so a query that has not finished yet
	// still contributes to the age reported for the window it spans.
	now := c.now()
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
			// and Add will recreate it then. Drop it to keep the map bounded.
			delete(c.tenants, tenantID)
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
