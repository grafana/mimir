// SPDX-License-Identifier: AGPL-3.0-only
//go:build rustengine && cgo

package ingester

import (
	"context"
	"errors"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/rustengine"
)

// rustIndex is the index of the engine's series: of those in the head, or of all with cold.
type rustIndex struct {
	engine   *rustEngine
	headOnly bool
}

var _ tsdb.IndexReader = (*rustIndex)(nil)

// nativeFilters resolves the matchers to the values they match, which the engine selects series by: regular
// expressions are matched here, against the values the engine has.
func (e *rustEngine) nativeFilters(ctx context.Context, ms []*labels.Matcher) ([]rustengine.Filter, error) {
	filters := make([]rustengine.Filter, 0, len(ms))
	for _, m := range ms {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		filter := rustengine.Filter{Name: m.Name, MatchMissing: m.Matches("")}
		switch m.Type {
		case labels.MatchEqual:
			filter.Values = []string{m.Value}
		case labels.MatchNotEqual:
			filter.Values = []string{m.Value}
			filter.Exclude = true
		default:
			if m.Value == ".*" {
				if m.Type == labels.MatchRegexp {
					continue
				}
				// Nothing matches.
				return []rustengine.Filter{{Name: m.Name}}, nil
			}
			if m.Value == ".+" {
				filter.Values = []string{""}
				filter.Exclude = m.Type == labels.MatchRegexp
				break
			}
			if values := m.SetMatches(); values != nil {
				filter.Values = values
				filter.Exclude = m.Type == labels.MatchNotRegexp
				break
			}
			values, err := e.values.matching(e.native, m)
			if err != nil {
				return nil, err
			}
			filter.Values = values
			// The values that don't match are excluded, so a series without the label matches if "" does.
			filter.MatchMissing = m.Matches("")
		}
		filters = append(filters, filter)
	}
	return filters, nil
}

// rustValueCache keeps the values of the labels regular expressions were matched against, and what they matched,
// until the label gets or loses a value.
type rustValueCache struct {
	mu     sync.Mutex
	byName map[string]*rustValues
}

type rustValues struct {
	generation uint64
	values     []string
	memo       map[rustMatcherKey][]string
}

type rustMatcherKey struct {
	t     labels.MatchType
	value string
}

const rustMaxMemo = 256

// matching returns the values of the matcher's label that it matches.
func (c *rustValueCache) matching(native *rustengine.Engine, m *labels.Matcher) ([]string, error) {
	c.mu.Lock()
	entry := c.byName[m.Name]
	c.mu.Unlock()
	var known uint64
	if entry != nil {
		known = entry.generation
	}
	generation, values, changed, err := native.ValuesIfChanged(m.Name, known)
	if err != nil {
		return nil, err
	}
	if changed || entry == nil {
		entry = &rustValues{generation: generation, values: values, memo: map[rustMatcherKey][]string{}}
		c.mu.Lock()
		if c.byName == nil {
			c.byName = map[string]*rustValues{}
		}
		c.byName[m.Name] = entry
		c.mu.Unlock()
	}
	key := rustMatcherKey{m.Type, m.Value}
	c.mu.Lock()
	matched, ok := entry.memo[key]
	c.mu.Unlock()
	if ok {
		return matched, nil
	}
	candidates := entry.values
	// A required prefix narrows the values to try to a range of them, since they are sorted.
	if prefix := m.Prefix(); prefix != "" && m.Type == labels.MatchRegexp {
		from := sort.SearchStrings(candidates, prefix)
		to := from
		for to < len(candidates) && strings.HasPrefix(candidates[to], prefix) {
			to++
		}
		candidates = candidates[from:to]
	}
	matched = []string{}
	for _, v := range candidates {
		if m.Matches(v) {
			matched = append(matched, v)
		}
	}
	c.mu.Lock()
	if len(entry.memo) < rustMaxMemo {
		entry.memo[key] = matched
	}
	c.mu.Unlock()
	return matched, nil
}

func refsPostings(refs []uint64) index.Postings {
	out := make([]storage.SeriesRef, len(refs))
	for i, r := range refs {
		out[i] = storage.SeriesRef(r)
	}
	return index.NewListPostings(out)
}

func (r *rustIndex) Close() error { return nil }

type rustStringIter struct{ err error }

func (r *rustStringIter) Next() bool { return false }
func (r *rustStringIter) At() string { return "" }
func (r *rustStringIter) Err() error { return r.err }

func (r *rustIndex) Symbols() index.StringIter {
	names, err := r.LabelNames(context.Background())
	if err != nil {
		return &rustStringIter{err: err}
	}
	symbols := map[string]struct{}{}
	for _, n := range names {
		symbols[n] = struct{}{}
		values, err := r.engine.native.Values(n, r.headOnly)
		if err != nil {
			return &rustStringIter{err: err}
		}
		for _, v := range values {
			symbols[v] = struct{}{}
		}
	}
	out := make([]string, 0, len(symbols))
	for s := range symbols {
		out = append(out, s)
	}
	slices.Sort(out)
	return index.NewStringListIter(out)
}

func (r *rustIndex) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, ms ...*labels.Matcher) ([]string, error) {
	if len(ms) == 0 {
		values, err := r.engine.native.Values(name, r.headOnly)
		if limit := labelLimit(hints); err == nil && limit > 0 && len(values) > limit {
			values = values[:limit]
		}
		return values, err
	}
	filters, err := r.engine.nativeFilters(ctx, ms)
	if err != nil {
		return nil, err
	}
	return r.engine.native.LabelValues(name, math.MinInt64, math.MaxInt64, labelLimit(hints), filters)
}

func (r *rustIndex) SortedLabelValues(ctx context.Context, name string, hints *storage.LabelHints, ms ...*labels.Matcher) ([]string, error) {
	return r.LabelValues(ctx, name, hints, ms...)
}

func (r *rustIndex) LabelNames(ctx context.Context, ms ...*labels.Matcher) ([]string, error) {
	filters, err := r.engine.nativeFilters(ctx, ms)
	if err != nil {
		return nil, err
	}
	return r.engine.native.LabelNames(math.MinInt64, math.MaxInt64, 0, filters)
}

func (r *rustIndex) Postings(ctx context.Context, name string, values ...string) (index.Postings, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	refs, err := r.engine.native.Postings(r.headOnly, false, []rustengine.Filter{{Name: name, Values: values}})
	if err != nil {
		return nil, err
	}
	return refsPostings(refs), nil
}

func (r *rustIndex) PostingsForLabelMatching(ctx context.Context, name string, match func(string) bool) index.Postings {
	values, err := r.engine.native.Values(name, r.headOnly)
	if err != nil {
		return index.ErrPostings(err)
	}
	selected := values[:0:0]
	for _, v := range values {
		if match(v) {
			selected = append(selected, v)
		}
	}
	if len(selected) == 0 {
		return index.EmptyPostings()
	}
	p, err := r.Postings(ctx, name, selected...)
	if err != nil {
		return index.ErrPostings(err)
	}
	return p
}

func (r *rustIndex) PostingsForAllLabelValues(ctx context.Context, name string) index.Postings {
	return r.PostingsForLabelMatching(ctx, name, func(string) bool { return true })
}

func (r *rustIndex) PostingsForMatchers(ctx context.Context, _ bool, ms ...*labels.Matcher) (index.Postings, error) {
	filters, err := r.engine.nativeFilters(ctx, ms)
	if err != nil {
		return nil, err
	}
	refs, err := r.engine.native.Postings(r.headOnly, len(filters) == 0 && len(ms) == 0, filters)
	if err != nil {
		return nil, err
	}
	return refsPostings(refs), nil
}

// expand reads the postings' refs.
func expand(p index.Postings) ([]uint64, error) {
	var refs []uint64
	for p.Next() {
		refs = append(refs, uint64(p.At()))
	}
	return refs, p.Err()
}

func (r *rustIndex) SortedPostings(p index.Postings) index.Postings {
	refs, err := expand(p)
	if err != nil {
		return index.ErrPostings(err)
	}
	type entry struct {
		ref    storage.SeriesRef
		labels labels.Labels
	}
	entries := make([]entry, 0, len(refs))
	err = r.engine.native.SeriesLabels(refs, func(i int, ls []rustengine.Label) error {
		entries = append(entries, entry{storage.SeriesRef(refs[i]), rustLabels(ls)})
		return nil
	})
	if err != nil {
		return index.ErrPostings(err)
	}
	slices.SortFunc(entries, func(a, b entry) int { return labels.Compare(a.labels, b.labels) })
	out := make([]storage.SeriesRef, len(entries))
	for i, e := range entries {
		out[i] = e.ref
	}
	return index.NewListPostings(out)
}

func (r *rustIndex) ShardedPostings(p index.Postings, shard, count uint64) index.Postings {
	refs, err := expand(p)
	if err != nil {
		return index.ErrPostings(err)
	}
	refs, err = r.engine.shard(refs, shard, count)
	if err != nil {
		return index.ErrPostings(err)
	}
	return refsPostings(refs)
}

func (r *rustIndex) Series(ref storage.SeriesRef, builder *labels.ScratchBuilder, metas *[]chunks.Meta) error {
	found := false
	err := r.engine.native.SeriesLabels([]uint64{uint64(ref)}, func(_ int, ls []rustengine.Label) error {
		found = true
		builder.Reset()
		for _, l := range ls {
			builder.Add(l.Name, l.Value)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !found {
		return storage.ErrNotFound
	}
	if metas != nil {
		*metas = (*metas)[:0]
	}
	return nil
}

// VisitSeriesLabels visits the labels of the series, a page at a time.
func (r *rustIndex) VisitSeriesLabels(ctx context.Context, refs []storage.SeriesRef, visit func(storage.SeriesRef, []mimirpb.LabelAdapter) error) error {
	ids := make([]uint64, len(refs))
	for i, ref := range refs {
		ids[i] = uint64(ref)
	}
	return r.engine.native.SeriesLabels(ids, func(i int, ls []rustengine.Label) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return visit(refs[i], ls)
	})
}

func (r *rustIndex) LabelNamesFor(ctx context.Context, p index.Postings) ([]string, error) {
	refs, err := expand(p)
	if err != nil {
		return nil, err
	}
	names := map[string]struct{}{}
	err = r.engine.native.SeriesLabels(refs, func(_ int, ls []rustengine.Label) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, l := range ls {
			if _, ok := names[l.Name]; !ok {
				names[strings.Clone(l.Name)] = struct{}{}
			}
		}
		return nil
	})
	out := make([]string, 0, len(names))
	for n := range names {
		out = append(out, n)
	}
	slices.Sort(out)
	return out, err
}

func (r *rustIndex) IndexLookupPlanner() index.LookupPlanner { return index.ChainLookupPlanners() }

// LabelValuesCounts counts the series of each value of the labels, among those of the matchers.
func (r *rustIndex) LabelValuesCounts(ctx context.Context, names []string, ms []*labels.Matcher, cutoffMs int64) (map[string]map[string]uint64, error) {
	if cutoffMs != math.MinInt64 {
		return nil, errors.New("Rust active counts require the active series tracker")
	}
	filters, err := r.engine.nativeFilters(ctx, ms)
	if err != nil {
		return nil, err
	}
	refs, err := r.engine.native.Postings(r.headOnly, len(ms) == 0, filters)
	if err != nil {
		return nil, err
	}
	out := make(map[string]map[string]uint64, len(names))
	for _, n := range names {
		out[n] = map[string]uint64{}
	}
	err = r.engine.native.SeriesLabels(refs, func(_ int, ls []rustengine.Label) error {
		for _, l := range ls {
			if counts, ok := out[l.Name]; ok {
				counts[l.Value]++
			}
		}
		return nil
	})
	return out, err
}

// LabelNamesAndValues returns the values of each label of the series of the matchers.
func (r *rustIndex) LabelNamesAndValues(ctx context.Context, ms []*labels.Matcher) (map[string][]string, error) {
	names, err := r.LabelNames(ctx, ms...)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]string, len(names))
	for _, n := range names {
		values, err := r.LabelValues(ctx, n, nil, ms...)
		if err != nil {
			return nil, err
		}
		out[n] = values
	}
	return out, nil
}
