// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"github.com/prometheus/client_golang/prometheus"
)

// frozenDescCollector wraps a prometheus.Collector with a snapshot of
// its descriptors taken at construction time. Describe always emits
// the snapshot; Collect delegates to the inner collector so any
// metrics added to the inner collector after the snapshot are still
// reported during scrapes.
//
// The point is to give the wrapper a stable identity in the outer
// Registry: prometheus.Registry hashes the descriptor IDs returned by
// Describe to assign a collectorID, and Unregister recomputes the
// hash from the wrapped Collector's current Describe output to find
// the entry. If the inner collector's desc set changes between
// Register and Unregister, the hashes differ, the Unregister silently
// returns false, and the stale collectorID lives on — only to collide
// with the next Register call whose initial desc set happens to match
// the original hash ("duplicate metrics collector registration
// attempted").
//
// Wrapping at Register time and reusing the wrapper at Unregister
// time pins the collector identity for its entire lifecycle. The
// trade-off is that the outer Registry's bookkeeping (descIDs,
// dimHashesByName) is only seeded with the descs from the snapshot,
// so any later metric whose Desc was not in the snapshot is emitted
// by Collect but is not known to the outer Registry by name. The
// default (non-pedantic) Registry tolerates this at Gather time, so
// scrape output still contains every metric the inner collector
// emits.
type frozenDescCollector struct {
	inner prometheus.Collector
	descs []*prometheus.Desc
}

// newFrozenDescCollector snapshots the descriptors currently produced
// by inner.Describe and returns a wrapper that emits them
// deterministically.
//
// Callers should construct the wrapper after the inner collector has
// finished any startup-time descriptor registration; descriptors
// added to the inner collector after this call are *not* part of the
// frozen identity and won't be visible to outer-Registry bookkeeping.
func newFrozenDescCollector(inner prometheus.Collector) *frozenDescCollector {
	if inner == nil {
		return nil
	}
	// Buffer a channel large enough that the producer rarely blocks;
	// Describe is allowed to send arbitrarily many descs, so we read
	// in a goroutine and let the goroutine close the channel.
	ch := make(chan *prometheus.Desc, 32)
	go func() {
		inner.Describe(ch)
		close(ch)
	}()
	var descs []*prometheus.Desc
	for d := range ch {
		descs = append(descs, d)
	}
	return &frozenDescCollector{inner: inner, descs: descs}
}

// Describe emits the snapshot. Implements prometheus.Collector.
func (f *frozenDescCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range f.descs {
		ch <- d
	}
}

// Collect delegates to the wrapped collector so all current metrics
// flow through. Implements prometheus.Collector.
func (f *frozenDescCollector) Collect(ch chan<- prometheus.Metric) {
	f.inner.Collect(ch)
}
