// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"math"
	"slices"
	"sync/atomic"

	promlabels "github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// The engine's native resources' API, over the store resources.go keeps them in.

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

// SetNativeResources sets whether the series' resources are kept, from the next batch on. Off, the coverage starts
// again, like SetNativeMetricMetadata's.
func (e *Engine) SetNativeResources(enabled bool) {
	e.nativeResources.Store(enabled)
	if !enabled && e.resourcesObservedFrom.Load() != math.MaxInt64 {
		e.resourcesObservedFrom.Store(math.MaxInt64)
	}
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
	// The timestamp of the series' newest sample the engine holds, which its last version lasts until, or
	// math.MinInt64 when it holds none.
	NewestSample int64
}

// NativeResources returns the native resources in force during [mint, maxt] of the head series that match any of
// matcherSets, in label order, at most limit of them with a positive limit, and whether more matched.
func (e *Engine) NativeResources(ctx context.Context, matcherSets [][]*promlabels.Matcher, mint, maxt int64, limit int) ([]NativeResourcesSeries, bool, error) {
	series, limited, _, err := e.NativeResourcesCapped(ctx, matcherSets, mint, maxt, limit, 0, NativeResourcesOptions{})
	return series, limited, err
}

// NativeResourcesOptions are a native resources lookup's optional paths, which the tenant's runtime switches choose on
// every lookup, so that turning one off takes effect at once.
type NativeResourcesOptions struct {
	// SetMatching matches a large alternation of literals against a set of its values rather than the regex.
	SetMatching bool
	// Unsorted leaves the series in shard order, for a caller that orders them itself, as a lookup by target groups
	// them and sorts the targets. A positive limit, which keeps the first series in label order, sorts them anyway.
	Unsorted bool
}

// nativeResourcesPaths counts the lookups that took each optional path, for tests of the switches that choose them.
var nativeResourcesPaths struct{ set, unsorted atomic.Uint64 }

// NativeResourcesPaths returns how many native resources lookups since the process started matched a set, and left
// their series unsorted, for tests.
func NativeResourcesPaths() (set, unsorted uint64) {
	return nativeResourcesPaths.set.Load(), nativeResourcesPaths.unsorted.Load()
}

// NativeResourcesCapped is NativeResources that also returns how many series with resources matched, and with a
// positive maxSeries, fails with ErrLookupTooManySeries as soon as more than that many do, with opts' paths.
func (e *Engine) NativeResourcesCapped(ctx context.Context, matcherSets [][]*promlabels.Matcher, mint, maxt int64, limit, maxSeries int, opts NativeResourcesOptions) ([]NativeResourcesSeries, bool, int, error) {
	compiled := make([][]compiledMatcher, 0, len(matcherSets))
	usesSet := false
	for _, matchers := range matcherSets {
		c, err := compilePromMatchersWith(nil, matchers, opts.SetMatching)
		if err != nil {
			return nil, false, 0, err
		}
		for index := range c {
			usesSet = usesSet || (c[index].re != nil && c[index].re.set != nil)
		}
		compiled = append(compiled, c)
	}
	if usesSet {
		nativeResourcesPaths.set.Add(1)
	}
	type selected struct {
		labels   labels.Labels
		versions []ResourceVersion
		newest   int64
	}
	var all []selected
	seen := map[uint64]struct{}{}
	var err error
	e.perShard(phaseNativeResources, func(_ int, t *tenant, _ *chunks.DiskMapper, _ *coldState) {
		if err != nil {
			return
		}
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
				if maxSeries > 0 && len(seen) > maxSeries {
					err = ErrLookupTooManySeries
					return false
				}
				if versions := e.resourceVersionsLocked(t.nativeResources, &entry.series, mint, maxt, nil); len(versions) > 0 {
					newest, ok := entry.series.newestSample()
					if !ok {
						newest = math.MinInt64
					}
					all = append(all, selected{labels: entry.labels, versions: versions, newest: newest})
				}
				return true
			})
			if err != nil {
				return
			}
		}
	})
	if err != nil {
		return nil, false, len(seen), err
	}
	if opts.Unsorted && limit <= 0 {
		nativeResourcesPaths.unsorted.Add(1)
	} else {
		slices.SortFunc(all, func(a, b selected) int { return labels.Compare(a.labels, b.labels) })
	}
	limited := limit > 0 && len(all) > limit
	if limited {
		all = all[:limit]
	}
	out := make([]NativeResourcesSeries, 0, len(all))
	var builder promlabels.ScratchBuilder
	for _, s := range all {
		out = append(out, NativeResourcesSeries{Labels: toPromLabels(s.labels, &builder), Versions: s.versions, NewestSample: s.newest})
	}
	return out, limited, len(seen), nil
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
