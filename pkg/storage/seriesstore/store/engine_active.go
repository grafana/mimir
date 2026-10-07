// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// The engine keeps what Mimir's active series tracker keeps in the series themselves: when each was last ingested,
// whether it was by OTLP, and its native histogram buckets. The tracker, a map of every series' reference, costs the
// ingester a quarter of its heap for what the series have already.

// ActiveCounts are the series ingested within the active window.
type ActiveCounts struct {
	Total, OTLP, NativeHistograms, NativeHistogramBuckets uint64
	// By custom tracker, in the order of ActiveTrackers.
	Trackers []TrackerCounts
}

// TrackerCounts are the active series that match a custom tracker.
type TrackerCounts struct {
	Total, NativeHistograms, NativeHistogramBuckets uint64
}

// ActiveTrackers are a tenant's custom trackers: how many there are, and which a series' labels match, by index.
type ActiveTrackers struct {
	Count int
	// Match is called concurrently.
	Match func(promlabels.Labels) []uint16
}

type activeTrackerSet struct {
	ActiveTrackers
	generation uint64
}

// SetActiveTrackers replaces the custom trackers, which series' cached matches follow by their generation.
func (e *Engine) SetActiveTrackers(trackers ActiveTrackers) {
	e.activeTrackers.Store(&activeTrackerSet{ActiveTrackers: trackers, generation: e.activeGeneration.Add(1)})
}

// IngestedSeries is a series that got samples.
type IngestedSeries struct {
	Ref storage.SeriesRef
	// The bucket count of its last native histogram, or -1 when it didn't end with one.
	HistogramBuckets int
}

// MarkIngested records, for series ingested another way than AppendFloats, when and how they were.
func (e *Engine) MarkIngested(series []IngestedSeries, atMs int64, otlp bool) {
	shards := len(e.store.shards)
	byShard := make([][]IngestedSeries, shards)
	for _, s := range series {
		if shard := refShard(uint64(s.Ref)); shard < shards {
			byShard[shard] = append(byShard[shard], s)
		}
	}
	for index, group := range byShard {
		if len(group) == 0 {
			continue
		}
		state := e.store.shards[index]
		state.Lock()
		if t, ok := e.tenantLocked(index); ok {
			for _, s := range group {
				if entry, ok := e.lookupLocked(t, uint64(s.Ref)); ok {
					entry.series.markIngested(atMs, otlp, s.HistogramBuckets)
				}
			}
		}
		state.Unlock()
	}
}

func (s *Series) markIngested(atMs int64, otlp bool, histogramBuckets int) {
	s.lastIngestedMs = atMs
	s.activeCleared = false
	s.otlp = otlp
	s.nativeHistogram = histogramBuckets >= 0
	if histogramBuckets >= 0 {
		s.lastBucketCount = uint32(histogramBuckets)
	}
}

// DeactivateSeries makes the series inactive until their next samples, like a tracker deleting them, and
// DeactivateAll like it clearing all.
func (e *Engine) DeactivateSeries(refs []storage.SeriesRef) {
	shards := len(e.store.shards)
	byShard := make([][]storage.SeriesRef, shards)
	for _, ref := range refs {
		if shard := refShard(uint64(ref)); shard < shards {
			byShard[shard] = append(byShard[shard], ref)
		}
	}
	for index, group := range byShard {
		if len(group) == 0 {
			continue
		}
		state := e.store.shards[index]
		state.Lock()
		if t, ok := e.tenantLocked(index); ok {
			for _, ref := range group {
				if entry, ok := e.lookupLocked(t, uint64(ref)); ok {
					entry.series.lastIngestedMs = math.MinInt64
				}
			}
		}
		state.Unlock()
	}
}

func (e *Engine) DeactivateAll() {
	for index, state := range e.store.shards {
		state.Lock()
		if t, ok := e.tenantLocked(index); ok {
			t.series.forEach(func(entry *seriesEntry) { entry.series.activeCleared = true })
		}
		state.Unlock()
	}
}

// ActiveSeries counts the series ingested at or after cutoffMs.
func (e *Engine) ActiveSeries(cutoffMs int64) ActiveCounts {
	trackers := e.activeTrackers.Load()
	count := 0
	if trackers != nil {
		count = trackers.Count
	}
	perShard := make([]ActiveCounts, len(e.store.shards))
	var wg sync.WaitGroup
	for index := range e.store.shards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			counts := ActiveCounts{Trackers: make([]TrackerCounts, count)}
			var stale []uint64
			var ages activeAges
			state := e.store.shards[index]
			state.RLock()
			if t, ok := e.tenantLocked(index); ok {
				t.series.forEach(func(entry *seriesEntry) {
					series := &entry.series
					ages.observe(series, cutoffMs)
					if !series.isActive(cutoffMs) {
						return
					}
					buckets := uint64(0)
					histogram := uint64(0)
					if series.nativeHistogram {
						histogram, buckets = 1, uint64(series.lastBucketCount)
					}
					counts.Total++
					counts.NativeHistograms += histogram
					counts.NativeHistogramBuckets += buckets
					if series.otlp {
						counts.OTLP++
					}
					if count == 0 {
						return
					}
					matches := series.matchedTrackers()
					if series.trackerGeneration != trackers.generation {
						matches = trackers.Match(toPromLabels(entry.labels, &promlabels.ScratchBuilder{}))
						stale = append(stale, series.ref)
					}
					for _, match := range matches {
						counts.Trackers[match].Total++
						counts.Trackers[match].NativeHistograms += histogram
						counts.Trackers[match].NativeHistogramBuckets += buckets
					}
				})
			}
			state.RUnlock()
			if len(stale) > 0 {
				e.cacheTrackerMatches(index, trackers, stale)
			}
			perShard[index] = counts
			if index == 0 {
				ages.log(cutoffMs)
			}
		}()
	}
	wg.Wait()

	total := ActiveCounts{Trackers: make([]TrackerCounts, count)}
	for _, counts := range perShard {
		total.Total += counts.Total
		total.OTLP += counts.OTLP
		total.NativeHistograms += counts.NativeHistograms
		total.NativeHistogramBuckets += counts.NativeHistogramBuckets
		for i := range counts.Trackers {
			total.Trackers[i].Total += counts.Trackers[i].Total
			total.Trackers[i].NativeHistograms += counts.Trackers[i].NativeHistograms
			total.Trackers[i].NativeHistogramBuckets += counts.Trackers[i].NativeHistogramBuckets
		}
	}
	return total
}

// cacheTrackerMatches evaluates the trackers for the series, whose matches were stale in the last pass, so the next
// passes only read them.
func (e *Engine) cacheTrackerMatches(shard int, trackers *activeTrackerSet, refs []uint64) {
	state := e.store.shards[shard]
	state.Lock()
	defer state.Unlock()
	t, ok := e.tenantLocked(shard)
	if !ok || e.activeTrackers.Load() != trackers {
		return
	}
	var builder promlabels.ScratchBuilder
	for _, ref := range refs {
		if entry, ok := e.lookupLocked(t, ref); ok && entry.series.trackerGeneration != trackers.generation {
			entry.series.setMatchedTrackers(append([]uint16(nil), trackers.Match(toPromLabels(entry.labels, &builder))...))
			entry.series.trackerGeneration = trackers.generation
		}
	}
}

// IsActive reports whether the series was ingested at or after cutoffMs, and its native histogram bucket count
// when its last samples ended with a histogram.
func (e *Engine) IsActive(ref storage.SeriesRef, cutoffMs int64) (active bool, buckets int, histogram bool) {
	e.withSeries(uint64(ref), func(series *Series) {
		if active = series.isActive(cutoffMs); active && series.nativeHistogram {
			buckets, histogram = int(series.lastBucketCount), true
		}
	})
	return active, buckets, histogram
}

// activeAges counts what the series' last ingests are, to find out why a count differs from another engine's. Temporary.
type activeAges struct {
	never, deactivated, cleared, older, active, recent int
	total                                              int
}

func (a *activeAges) observe(series *Series, cutoffMs int64) {
	a.total++
	switch {
	case series.lastIngestedMs == 0:
		a.never++
	case series.lastIngestedMs == math.MinInt64:
		a.deactivated++
	case series.activeCleared:
		a.cleared++
	case series.lastIngestedMs < cutoffMs-int64(20*60*1000):
		a.older++
	case series.lastIngestedMs < cutoffMs:
		a.recent++
	default:
		a.active++
	}
}

var lastActiveLog atomic.Int64

func (a *activeAges) log(cutoffMs int64) {
	now := time.Now().UnixNano()
	last := lastActiveLog.Load()
	if now-last < int64(3*time.Minute) || !lastActiveLog.CompareAndSwap(last, now) {
		return
	}
	fmt.Fprintf(os.Stderr, "activelog shard0 total=%d active=%d idle<20m=%d idle>20m=%d never=%d deactivated=%d cleared=%d\n", a.total, a.active, a.recent, a.older, a.never, a.deactivated, a.cleared)
}
