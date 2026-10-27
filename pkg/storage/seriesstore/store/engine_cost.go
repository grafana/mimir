// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"time"

	promlabels "github.com/prometheus/prometheus/model/labels"
)

// CostAttribution is told which series are active, by their labels, as Mimir's cost attribution counts them. It is
// the ingester's tracker, which this replaces for counting by the series the engine has already.
type CostAttribution interface {
	// Increment is for a series that became active, with its native histogram bucket count or -1.
	Increment(lbls promlabels.Labels, now time.Time, histogramBuckets int)
	// Decrement is for one that is no longer active, with the bucket count it was incremented with.
	Decrement(lbls promlabels.Labels, histogramBuckets int)
}

// SetCostAttribution sets where the active series are reported to: every one of them again, as of the next
// ActiveSeries. A nil sink stops the reports. What the previous sink was told isn't taken back, as it is dropped.
func (e *Engine) SetCostAttribution(sink CostAttribution) {
	e.costMu.Lock()
	defer e.costMu.Unlock()
	e.cost = sink
	for index, state := range e.store.shards {
		state.Lock()
		if t, ok := e.tenantLocked(index); ok {
			t.cost = sink
			t.series.forEach(func(entry *seriesEntry) {
				entry.series.counted = false
				entry.series.setCountedBucketCount(-1)
			})
		}
		state.Unlock()
	}
}

// syncCost tells the cost attribution which series became active or stopped being, since the last time. Like the
// tracker's purge, the series idle since the cutoff stop being active at once, and the ones a clear left stay counted
// until they are ingested again, as clearing leaves the cost attribution's counts in place.
func (e *Engine) syncCost(cutoffMs int64, now time.Time) {
	e.costMu.Lock()
	defer e.costMu.Unlock()
	sink := e.cost
	if sink == nil {
		return
	}
	for index, state := range e.store.shards {
		state.Lock()
		if t, ok := e.tenantLocked(index); ok {
			t.cost = sink
			var builder promlabels.ScratchBuilder
			t.series.forEach(func(entry *seriesEntry) {
				series := &entry.series
				if series.activeCleared {
					return
				}
				active := series.lastIngestedMs >= cutoffMs
				buckets := -1
				if active && series.nativeHistogram {
					buckets = int(series.lastBucketCount)
				}
				switch {
				case active && !series.counted:
					sink.Increment(toPromLabels(entry.labels, &builder), now, buckets)
					series.counted = true
					series.setCountedBucketCount(buckets)
				case !active && series.counted:
					sink.Decrement(toPromLabels(entry.labels, &builder), series.countedBucketCount())
					series.counted = false
					series.setCountedBucketCount(-1)
				case active && series.countedBucketCount() != buckets:
					lbls := toPromLabels(entry.labels, &builder)
					sink.Decrement(lbls, series.countedBucketCount())
					sink.Increment(lbls, now, buckets)
					series.setCountedBucketCount(buckets)
				}
			})
		}
		state.Unlock()
	}
}

// uncount tells the cost attribution that a series that is leaving the index isn't active any more.
func (t *tenant) uncount(entry *seriesEntry, builder *promlabels.ScratchBuilder) {
	if t.cost == nil || !entry.series.counted {
		return
	}
	t.cost.Decrement(toPromLabels(entry.labels, builder), entry.series.countedBucketCount())
	entry.series.counted = false
}
