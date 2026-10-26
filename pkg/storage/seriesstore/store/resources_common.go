// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"slices"

	promlabels "github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// The engine's native resources' API, which every build has: the default build keeps them in its own store
// (resources.go), and the native_resources_ablation build in #17740's (resources_ablation.go).

// Without EntityRefs, the identifying attributes are service.name, service.namespace and service.instance.id, which
// job and instance derive from, as #17740 splits them.

func isIdentifyingResourceAttribute(key string) bool {
	return key == "service.name" || key == "service.namespace" || key == "service.instance.id"
}

// ResourceLimits are the native resources' soft limits, in sample time: past them, changes coalesce into a range of
// unknown resource until their window ends, and are counted, but samples are never rejected. 0 is no limit.
type ResourceLimits struct {
	// The window the limits count in.
	WindowMs int64
	// How many times a series' identity may change per window.
	MaxIdentityChangesPerSeries int
	// How many versions of its description an identity may have per window.
	MaxVersionsPerIdentity int
	// How many new identities the engine takes per window, and how many bytes the dictionaries' values may have.
	MaxNewIdentities int
	MaxBytes         int64
}

// ResourceLimit is one of the native resources' limits.
type ResourceLimit int

const (
	// A series' identity changes per window.
	ResourceIdentityChangeLimit ResourceLimit = iota
	// An identity's versions per window.
	ResourceVersionLimit
	// New identities per window, and the dictionaries' bytes.
	ResourceDictionaryLimit
)

// SetNativeResources sets whether the series' resources are kept, from the next batch on.
func (e *Engine) SetNativeResources(enabled bool) {
	e.nativeResources.Store(enabled)
}

// ResourceVersion is a series' resource from a time on.
type ResourceVersion struct {
	EffectiveFrom int64
	// The resource's attributes, the identifying ones first, each by key; nil for an unknown resource.
	Identifying, Descriptive []mimirpb.ResourceAttribute
	// Whether the version stands for changes over a limit, with the latest of their values, or none if unknown.
	Coalesced bool
}

// NativeResourcesSeries is a head series' native resources.
type NativeResourcesSeries struct {
	Labels   promlabels.Labels
	Versions []ResourceVersion
}

// NativeResources returns the native resources in force during [mint, maxt] of the head series that match any of
// matcherSets, in label order, at most limit of them with a positive limit, and whether more matched.
func (e *Engine) NativeResources(ctx context.Context, matcherSets [][]*promlabels.Matcher, mint, maxt int64, limit int) ([]NativeResourcesSeries, bool, error) {
	compiled := make([][]compiledMatcher, 0, len(matcherSets))
	for _, matchers := range matcherSets {
		c, err := compilePromMatchers(nil, matchers)
		if err != nil {
			return nil, false, err
		}
		compiled = append(compiled, c)
	}
	type selected struct {
		labels   labels.Labels
		versions []ResourceVersion
	}
	var all []selected
	seen := map[uint64]struct{}{}
	var err error
	e.perShard(func(_ int, t *tenant, _ *chunks.DiskMapper, _ *coldState) {
		if err = ctx.Err(); err != nil || t.nativeResources == nil {
			return
		}
		for _, matchers := range compiled {
			t.series.matching(matchers, func(entry *seriesEntry) bool {
				if !t.nativeResources.has(&entry.series) {
					return true
				}
				if _, ok := seen[entry.series.ref]; ok {
					return true
				}
				seen[entry.series.ref] = struct{}{}
				if versions := e.resourceVersionsLocked(t.nativeResources, &entry.series, mint, maxt, nil); len(versions) > 0 {
					all = append(all, selected{labels: entry.labels, versions: versions})
				}
				return true
			})
		}
	})
	if err != nil {
		return nil, false, err
	}
	slices.SortFunc(all, func(a, b selected) int { return labels.Compare(a.labels, b.labels) })
	limited := limit > 0 && len(all) > limit
	if limited {
		all = all[:limit]
	}
	out := make([]NativeResourcesSeries, 0, len(all))
	var builder promlabels.ScratchBuilder
	for _, s := range all {
		out = append(out, NativeResourcesSeries{Labels: toPromLabels(s.labels, &builder), Versions: s.versions})
	}
	return out, limited, nil
}

// resourceObservedAt records that a series' resource was first observed at timestamp.
func (e *Engine) resourceObservedAt(timestamp int64) {
	for observed := e.resourcesObservedFrom.Load(); timestamp < observed && !e.resourcesObservedFrom.CompareAndSwap(observed, timestamp); {
		observed = e.resourcesObservedFrom.Load()
	}
}

// NativeResourcesCoverageStart is when the native resources start: the later of the head's min time and the oldest
// time a series' resource was first observed at since the engine opened.
func (e *Engine) NativeResourcesCoverageStart() int64 {
	return max(e.MinTime(), e.resourcesObservedFrom.Load())
}

// ResourceStats are an engine's native resources, as its metrics report them.
type ResourceStats struct {
	// The series with a resource, and their runs: one each, and their older runs.
	Series, Runs int64
	// The dictionaries' values and their bytes, and the identities' versions.
	Identities, Descriptions, Bytes, Versions int64
	// The ranges coalesced over each limit, the values the limits refused, the runs whose start clamped to their
	// shard's base, the batches' memo lookups, and the runs and versions evicted past the cap on them.
	CoalescedIdentityChanges, CoalescedVersions, Refused, Clamped, MemoHits, MemoMisses, Evictions uint64
}
