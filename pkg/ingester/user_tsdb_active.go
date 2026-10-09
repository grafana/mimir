// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/grafana/mimir/pkg/costattribution"
	"github.com/grafana/mimir/pkg/ingester/activeseries"
	asmodel "github.com/grafana/mimir/pkg/ingester/activeseries/model"
)

// activeSeriesHead is implemented by an engine that keeps which series are active in its series, from the samples it
// ingests, so the ingester's tracker, a map of every series, isn't needed to answer for it.
type activeSeriesHead interface {
	// ActiveSeries counts the series ingested at or after the cutoff.
	ActiveSeries(cutoff time.Time) activeCounts
	// SetActiveTrackers sets the custom trackers that ActiveSeries counts by.
	SetActiveTrackers(matchers *asmodel.Matchers)
	// SetCostAttribution sets where the series that are active are reported, by their labels, when ActiveSeries
	// is called: Increment for a series that became active and Decrement for one that isn't any more, so a tenant
	// with cost attribution doesn't need the tracker either. A nil sink stops the reports.
	SetCostAttribution(sink costAttributionSink)
	// DeactivateSeries makes the series inactive until their next samples, and DeactivateAll all of them.
	DeactivateSeries(refs []storage.SeriesRef)
	DeactivateAll()
	// ActiveRefs tells which series were ingested at or after the cutoff.
	ActiveRefs(cutoff time.Time) activeseries.ActiveRefs
}

// costAttributionSink is what an engine reports the active series to, which cost attribution's tracker is.
type costAttributionSink interface {
	// Increment is for a series that became active, with its native histogram bucket count or -1.
	Increment(lbls labels.Labels, now time.Time, nativeHistogramBucketNum int)
	// Decrement is for one that isn't active any more, with the bucket count it was incremented with.
	Decrement(lbls labels.Labels, nativeHistogramBucketNum int)
}

// setCostAttribution makes the engine report the tenant's active series to the cost attribution tracker, or stop.
func (u *userTSDB) setCostAttribution(tracker *costattribution.ActiveSeriesTracker) {
	if tracker == nil {
		u.nativeActive.SetCostAttribution(nil)
		return
	}
	u.nativeActive.SetCostAttribution(tracker)
}

// activeCounts are the active series, in all and by custom tracker, in the order of the trackers.
type activeCounts struct {
	Total, OTLP, NativeHistograms, NativeHistogramBuckets int
	Trackers                                              []trackerCounts
}

type trackerCounts struct {
	Total, NativeHistograms, NativeHistogramBuckets int
}

// trackerActive reports whether the tracker, not the engine, tells the tenant's active series: it does for an engine
// that doesn't keep them.
func (u *userTSDB) trackerActive() bool {
	// Without active series metrics nothing is active, which the tracker, that isn't fed, says.
	return u.nativeActive == nil || !u.cfg.ActiveSeriesMetrics.Enabled
}

// activeCutoff is when a series has to have been ingested to be active at now, or at the time of the last update of the
// active series if later, which is what the tracker's purge does: series it dropped stay dropped until their next samples.
func (u *userTSDB) activeCutoff(now time.Time) time.Time {
	if asOf := u.activeAsOf.Load(); asOf > now.UnixNano() {
		now = time.Unix(0, asOf)
	}
	return now.Add(-u.cfg.ActiveSeriesMetrics.IdleTimeout)
}

// activeSeriesCounts returns the active series at now, after the tracker purged the ones that aren't any more.
func (u *userTSDB) activeSeriesCounts(now time.Time) activeCounts {
	if !u.trackerActive() {
		u.activeAsOf.Store(now.UnixNano())
		return u.nativeActive.ActiveSeries(u.activeCutoff(now))
	}
	idx := mustIndex(u.Head())
	u.activeSeries.Purge(now, idx)
	_ = idx.Close()

	total, matching, otlp, histograms, matchingHistograms, buckets, matchingBuckets := u.activeSeries.ActiveWithMatchers()
	counts := activeCounts{Total: total, OTLP: otlp, NativeHistograms: histograms, NativeHistogramBuckets: buckets, Trackers: make([]trackerCounts, len(matching))}
	for i := range matching {
		counts.Trackers[i] = trackerCounts{Total: matching[i], NativeHistograms: matchingHistograms[i], NativeHistogramBuckets: matchingBuckets[i]}
	}
	return counts
}

// activeSeriesTotal returns how many series are active, as of the tracker's last purge when it answers.
func (u *userTSDB) activeSeriesTotal(now time.Time) int {
	if !u.trackerActive() {
		return u.nativeActive.ActiveSeries(u.activeCutoff(now)).Total
	}
	total, _, _, _ := u.activeSeries.Active()
	return total
}

// activeRefs tells which series are active.
func (u *userTSDB) activeRefs(now time.Time) activeseries.ActiveRefs {
	if !u.trackerActive() {
		return u.nativeActive.ActiveRefs(u.activeCutoff(now))
	}
	return u.activeSeries
}

// deactivateSeries and deactivateAll make series inactive until their next samples: the ones the ingester doesn't own.
func (u *userTSDB) deactivateSeries(refs []storage.SeriesRef) {
	if u.nativeActive != nil {
		u.nativeActive.DeactivateSeries(refs)
	}
	if u.trackerActive() {
		idx := mustIndex(u.Head())
		for _, ref := range refs {
			u.activeSeries.Delete(chunks.HeadSeriesRef(ref), idx)
		}
		_ = idx.Close()
	}
}

func (u *userTSDB) deactivateAll() {
	if u.nativeActive != nil {
		u.nativeActive.DeactivateAll()
	}
	if u.trackerActive() {
		u.activeSeries.Clear()
	}
}
