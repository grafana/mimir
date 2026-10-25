// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"container/heap"
	"context"
	"slices"
	"sync/atomic"

	"github.com/prometheus/common/model"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// The engine's native metric metadata that every build has. How a series keeps its values is the build's: a
// dictionary's ids by default (metadata.go), or #19563's state with the native_metadata_ablation tag
// (metadata_ablation.go), each with its seriesMetadata, shardMetadata, metadataMemo and metricMetadataValues.

// metadataBaseMargin is how far before the first observation a shard's native metadata and resources' base is:
// samples down to the out-of-order window or half a chunk range before it can still come in, and a replay after an
// unclean shutdown starts further back still, which clamps.
func (e *Engine) metadataBaseMargin() int64 {
	return max(e.oooWindow.Load(), e.store.chunkRangeMs/2)
}

// metadataRebaseAfter is how far past its shard's base a series' run may start before the shard is rebased: half the
// range of the offsets.
const metadataRebaseAfter = 1 << 31

// maxMetricMetadataBytes bounds the type, unit and help of one value, as the Prometheus TSDB's native metric metadata
// does: a bigger value isn't kept, and is counted.
const maxMetricMetadataBytes = 8 << 10

// metricMetadataStats are an engine's counters of what its native metric metadata didn't keep as it came.
type metricMetadataStats struct {
	// Values over maxMetricMetadataBytes, which aren't kept.
	oversized atomic.Uint64
	// Observations older than their shard's base, whose runs start at the base instead.
	clamped atomic.Uint64
	// Lookups of a value by a batch's memo, and the ones it didn't have yet.
	memoHits, memoMisses atomic.Uint64
	// The changes over the change limit: each coalesced range, and each out-of-order change absorbed for it.
	coalesced atomic.Uint64
	// The older runs evicted past maxMetadataRunsPerSeries.
	evictions atomic.Uint64
	// Called with each, once set at the engine's open.
	onCoalesced func()
}

func (s *metricMetadataStats) coalesce() {
	s.coalesced.Add(1)
	if s.onCoalesced != nil {
		s.onCoalesced()
	}
}

// metricMetadataOf is a series' own metadata as the native metric metadata keeps it, and whether it's kept: an empty
// one is none, and an oversized one is counted.
func metricMetadataOf(m mimirpb.SeriesMetadata, stats *metricMetadataStats) (metadata.Metadata, bool) {
	if m.IsEmpty() {
		return metadata.Metadata{}, false
	}
	return keptMetricMetadata(metadata.Metadata{Type: mimirpb.MetricMetadataMetricTypeToMetricType(m.Type), Unit: m.Unit, Help: m.Help}, stats)
}

// keptMetricMetadata is metadata an appender got as the native metric metadata keeps it: without a type, its type is
// unknown, and an oversized one isn't kept, and is counted.
func keptMetricMetadata(m metadata.Metadata, stats *metricMetadataStats) (metadata.Metadata, bool) {
	if m.IsEmpty() {
		return metadata.Metadata{}, false
	}
	if m.Type == "" {
		m.Type = model.MetricTypeUnknown
	}
	if len(m.Type)+len(m.Unit)+len(m.Help) > maxMetricMetadataBytes {
		stats.oversized.Add(1)
		return metadata.Metadata{}, false
	}
	return m, true
}

// SetNativeMetricMetadata sets whether the series' own metric metadata is kept, from the next batch on.
func (e *Engine) SetNativeMetricMetadata(enabled bool) {
	e.nativeMetadata.Store(enabled)
}

// SetMetricMetadataChangeLimit sets how many changes of its metadata a series may have in a window of sample time of
// windowMs: past them, the changes coalesce into one range until the window ends. 0 for either is no limit.
func (e *Engine) SetMetricMetadataChangeLimit(maxChanges int, windowMs int64) {
	e.metadataMaxChanges.Store(int64(maxChanges))
	e.metadataChangeWindowMs.Store(windowMs)
}

// shardMetadataLocked is the shard's part of the engine's native metric metadata, for a caller holding its lock.
func (e *Engine) shardMetadataLocked(t *tenant) *shardMetadata {
	if t.nativeMetadata == nil {
		t.nativeMetadata = newShardMetadata(e.metricMetadata, &e.metricMetadataStats)
	}
	return t.nativeMetadata
}

// MetricMetadataVersion is a series' metadata value from a time on.
type MetricMetadataVersion struct {
	EffectiveFrom int64
	Metadata      metadata.Metadata
}

// metricMetadataVersions are the series ref's metadata versions, oldest first.
func (e *Engine) metricMetadataVersions(ref uint64) []MetricMetadataVersion {
	index := refShard(ref)
	state := e.store.shards[index]
	state.RLock()
	defer state.RUnlock()
	t, ok := e.tenantLocked(index)
	if !ok || t.nativeMetadata == nil {
		return nil
	}
	entry, ok := e.lookupLocked(t, ref)
	if !ok {
		return nil
	}
	versions, _, _ := e.versionsLocked(t.nativeMetadata, &entry.series, nil)
	return versions
}

// NativeMetricMetadataSeries is a head series' native metric metadata.
type NativeMetricMetadataSeries struct {
	Labels promlabels.Labels
	// The versions, oldest first.
	Versions []MetricMetadataVersion
	// Whether a version stands for the changes over the change limit in its window, with the latest of their values.
	Coalesced bool
	// Whether older versions went past the cap on them.
	Truncated bool
}

// NativeMetricMetadata returns the native metric metadata of the head series that match any of matcherSets, in label
// order, at most limit of them with a positive limit, and whether more matched.
func (e *Engine) NativeMetricMetadata(ctx context.Context, matcherSets [][]*promlabels.Matcher, limit int) ([]NativeMetricMetadataSeries, bool, error) {
	compiled := make([][]compiledMatcher, 0, len(matcherSets))
	for _, matchers := range matcherSets {
		c, err := compilePromMatchers(nil, matchers)
		if err != nil {
			return nil, false, err
		}
		compiled = append(compiled, c)
	}
	// With a limit, the first series by labels are kept in a heap of the limit's size, largest on top, rather than all
	// of them sorted.
	selected := &metadataSelection{}
	seen := map[uint64]struct{}{}
	limited := false
	var err error
	e.perShard(func(_ int, t *tenant, _ *chunks.DiskMapper, _ *coldState) {
		if err = ctx.Err(); err != nil || t.nativeMetadata == nil {
			return
		}
		for _, matchers := range compiled {
			t.series.matching(matchers, func(entry *seriesEntry) bool {
				if !entry.series.hasMetricMetadata() {
					return true
				}
				if _, ok := seen[entry.series.ref]; ok {
					return true
				}
				seen[entry.series.ref] = struct{}{}
				if limit > 0 && selected.Len() == limit {
					limited = true
					if labels.Compare(entry.labels, selected.series[0].labels) >= 0 {
						return true
					}
					heap.Pop(selected)
				}
				versions, coalesced, truncated := e.versionsLocked(t.nativeMetadata, &entry.series, nil)
				heap.Push(selected, selectedMetadata{labels: entry.labels, versions: versions, coalesced: coalesced, truncated: truncated})
				return true
			})
		}
	})
	if err != nil {
		return nil, false, err
	}
	slices.SortFunc(selected.series, func(a, b selectedMetadata) int { return labels.Compare(a.labels, b.labels) })
	out := make([]NativeMetricMetadataSeries, 0, len(selected.series))
	var builder promlabels.ScratchBuilder
	for _, s := range selected.series {
		out = append(out, NativeMetricMetadataSeries{Labels: toPromLabels(s.labels, &builder), Versions: s.versions, Coalesced: s.coalesced, Truncated: s.truncated})
	}
	return out, limited, nil
}

// NativeMetricMetadataCoverageStart is when the native metric metadata starts: the later of the head's min time and
// the oldest time a series' metadata was first observed at since the engine opened.
func (e *Engine) NativeMetricMetadataCoverageStart() int64 {
	return max(e.MinTime(), e.metadataObservedFrom.Load())
}

type selectedMetadata struct {
	labels               labels.Labels
	versions             []MetricMetadataVersion
	coalesced, truncated bool
}

// metadataSelection is a max-heap of series by labels.
type metadataSelection struct{ series []selectedMetadata }

func (s *metadataSelection) Len() int { return len(s.series) }
func (s *metadataSelection) Less(i, j int) bool {
	return labels.Compare(s.series[i].labels, s.series[j].labels) > 0
}
func (s *metadataSelection) Swap(i, j int) { s.series[i], s.series[j] = s.series[j], s.series[i] }
func (s *metadataSelection) Push(x any)    { s.series = append(s.series, x.(selectedMetadata)) }
func (s *metadataSelection) Pop() any {
	last := s.series[len(s.series)-1]
	s.series = s.series[:len(s.series)-1]
	return last
}

// MetricMetadataStats are an engine's native metric metadata, as its metrics report it.
type MetricMetadataStats struct {
	// The series with metadata, and their versions: one each, and their older runs.
	Series, Versions int64
	// The dictionary's values, and the bytes of their type, unit and help.
	Values, ValueBytes int64
	// The entries the series' groups have room for, which every series' inline metadata is paid in.
	EntryCapacity int64
	// The values too big to keep, the runs whose start clamped to their shard's base, the batches' memo lookups, and
	// the changes over the change limit, and the older runs evicted past the cap on them.
	Oversized, Clamped, MemoHits, MemoMisses, Coalesced, Evictions uint64
}

// MetricMetadataStats returns the engine's native metric metadata as its metrics report it.
func (e *Engine) MetricMetadataStats() MetricMetadataStats {
	stats := MetricMetadataStats{
		Values:     e.metricMetadata.values.Load(),
		ValueBytes: e.metricMetadata.bytes.Load(),
		Oversized:  e.metricMetadataStats.oversized.Load(),
		Clamped:    e.metricMetadataStats.clamped.Load(),
		MemoHits:   e.metricMetadataStats.memoHits.Load(),
		MemoMisses: e.metricMetadataStats.memoMisses.Load(),
		Coalesced:  e.metricMetadataStats.coalesced.Load(),
		Evictions:  e.metricMetadataStats.evictions.Load(),
	}
	for index, state := range e.store.shards {
		state.RLock()
		if t, ok := e.tenantLocked(index); ok {
			if m := t.nativeMetadata; m != nil {
				stats.Series += int64(m.series)
				stats.Versions += int64(m.series + m.runs)
			}
			for group := range t.series.groups {
				stats.EntryCapacity += int64(cap(t.series.groups[group].entries))
			}
		}
		state.RUnlock()
	}
	return stats
}
