// SPDX-License-Identifier: AGPL-3.0-only
//go:build rustengine && cgo

package ingester

import (
	"container/list"
	"context"
	"encoding/binary"
	"errors"
	"maps"
	"math"
	"slices"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/storage/rustengine"
)

type rustIndex struct {
	engine *rustEngine
	cold   bool
}

type rustSelectorCache struct {
	mu         sync.Mutex
	generation uint64
	bytes      int
	postings   map[string]*list.Element
	lru        list.List
}

type rustCachedSelector struct {
	key   string
	refs  []storage.SeriesRef
	bytes int
}

var _ tsdb.IndexReader = (*rustIndex)(nil)

func (r *rustIndex) call(ctx context.Context, req rustengine.Request) (rustengine.Response, error) {
	if err := ctx.Err(); err != nil {
		return rustengine.Response{}, err
	}
	req.IncludeCold = r.cold
	return r.engine.client.Call(req)
}
func (r *rustIndex) Close() error { return nil }
func (r *rustIndex) Symbols() index.StringIter {
	names, err := r.LabelNames(context.Background())
	if err != nil {
		return &rustStringIter{err: err}
	}
	symbols := map[string]bool{}
	for _, n := range names {
		symbols[n] = true
		vs, err := r.values(context.Background(), n)
		if err != nil {
			return &rustStringIter{err: err}
		}
		for _, v := range vs {
			symbols[v] = true
		}
	}
	out := make([]string, 0, len(symbols))
	for s := range symbols {
		out = append(out, s)
	}
	slices.Sort(out)
	return index.NewStringListIter(out)
}

type rustStringIter struct{ err error }

func (r *rustStringIter) Next() bool { return false }
func (r *rustStringIter) At() string { return "" }
func (r *rustStringIter) Err() error { return r.err }
func (r *rustIndex) values(ctx context.Context, name string) ([]string, error) {
	out, err := r.call(ctx, rustengine.Request{Op: "values", Name: name})
	return out.Values, err
}
func (r *rustIndex) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, ms ...*labels.Matcher) ([]string, error) {
	limit := 0
	if hints != nil {
		limit = max(0, hints.Limit)
	}
	out, err := r.metadata(ctx, rustengine.Request{Op: "metadata_values", Name: name, Limit: limit}, ms)
	return out.Values, err
}
func (r *rustIndex) SortedLabelValues(ctx context.Context, name string, hints *storage.LabelHints, ms ...*labels.Matcher) ([]string, error) {
	return r.LabelValues(ctx, name, hints, ms...)
}
func (r *rustIndex) LabelNames(ctx context.Context, ms ...*labels.Matcher) ([]string, error) {
	out, err := r.metadata(ctx, rustengine.Request{Op: "metadata_names"}, ms)
	return out.Values, err
}

// Metadata retains idle series until the logical head window passes their cold data.
func (r *rustIndex) metadata(ctx context.Context, req rustengine.Request, ms []*labels.Matcher) (rustengine.Response, error) {
	if len(ms) > 0 {
		full := &rustIndex{engine: r.engine, cold: true}
		p, err := full.PostingsForMatchers(ctx, false, ms...)
		if err != nil {
			return rustengine.Response{}, err
		}
		refs, err := index.ExpandPostings(p)
		if err != nil {
			return rustengine.Response{}, err
		}
		req.Refs = make([]uint64, len(refs))
		for i, ref := range refs {
			req.Refs[i] = uint64(ref)
		}
		req.Filter = true
	}
	return r.call(ctx, req)
}

func (r *rustIndex) LabelValuesCounts(ctx context.Context, names []string, ms []*labels.Matcher, cutoffMs int64) (map[string]map[string]uint64, error) {
	// Active counts use the Go tracker; native counts are only used for IN_MEMORY.
	if cutoffMs != math.MinInt64 {
		return nil, errors.New("Rust active counts require the active series tracker")
	}
	out, err := r.metadata(ctx, rustengine.Request{Op: "metadata_counts", Values: names}, ms)
	if err != nil {
		return nil, err
	}
	if out.LabelCounts == nil {
		out.LabelCounts = make(map[string]map[string]uint64, len(names))
	}
	for _, name := range names {
		if out.LabelCounts[name] == nil {
			out.LabelCounts[name] = map[string]uint64{}
		}
	}
	return out.LabelCounts, nil
}

func (r *rustIndex) LabelNamesAndValues(ctx context.Context, ms []*labels.Matcher) (map[string][]string, error) {
	out, err := r.metadata(ctx, rustengine.Request{Op: "metadata_all"}, ms)
	if err != nil {
		return nil, err
	}
	values := make(map[string][]string, len(out.LabelCounts))
	for name, counts := range out.LabelCounts {
		values[name] = slices.Sorted(maps.Keys(counts))
	}
	return values, nil
}
func (r *rustIndex) Postings(ctx context.Context, name string, values ...string) (index.Postings, error) {
	response, err := r.call(ctx, rustengine.Request{Op: "postings", Name: name, Values: values})
	if err != nil {
		return nil, err
	}
	refs := make([]storage.SeriesRef, len(response.Refs))
	for i, v := range response.Refs {
		refs[i] = storage.SeriesRef(v)
	}
	return index.NewListPostings(refs), nil
}
func (r *rustIndex) PostingsForLabelMatching(ctx context.Context, name string, match func(string) bool) index.Postings {
	values, err := r.values(ctx, name)
	if err != nil {
		return index.ErrPostings(err)
	}
	selected := make([]string, 0, len(values))
	for _, v := range values {
		if match(v) {
			selected = append(selected, v)
		}
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
func (r *rustIndex) nativeFilters(ctx context.Context, ms []*labels.Matcher) ([]rustengine.LabelFilter, error) {
	filters := make([]rustengine.LabelFilter, 0, len(ms))
	valuesByName := map[string][]string{}
	for _, m := range ms {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		filter := rustengine.LabelFilter{Name: m.Name, MatchMissing: m.Matches("")}
		switch m.Type {
		case labels.MatchEqual:
			filter.Values = []string{m.Value}
		case labels.MatchNotEqual:
			filter.Values = []string{m.Value}
			filter.Exclude = true
		default:
			if values := m.SetMatches(); values != nil {
				filter.Values = values
				filter.Exclude = m.Type == labels.MatchNotRegexp
				break
			}
			values, ok := valuesByName[m.Name]
			if !ok {
				var err error
				values, err = r.values(ctx, m.Name)
				if err != nil {
					return nil, err
				}
				valuesByName[m.Name] = values
			}
			for _, value := range values {
				if m.Matches(value) {
					filter.Values = append(filter.Values, value)
				}
			}
		}
		filters = append(filters, filter)
	}
	return filters, nil
}
func (r *rustIndex) PostingsForMatchers(ctx context.Context, _ bool, ms ...*labels.Matcher) (index.Postings, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	const maxBytes, maxEntries, maxKey = 64 << 10, 128, 4 << 10
	keySize := 0
	for _, m := range ms {
		keySize += 9 + len(m.Name) + len(m.Value)
	}
	var cache *rustSelectorCache
	var generation uint64
	var key string
	if keySize <= maxKey {
		var err error
		generation, err = r.engine.client.IndexEpoch(r.cold)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, 0, keySize)
		for _, m := range ms {
			buf = append(buf, byte(m.Type))
			buf = binary.LittleEndian.AppendUint32(buf, uint32(len(m.Name)))
			buf = append(buf, m.Name...)
			buf = binary.LittleEndian.AppendUint32(buf, uint32(len(m.Value)))
			buf = append(buf, m.Value...)
		}
		key = string(buf)
		mode := 0
		if r.cold {
			mode = 1
		}
		cache = &r.engine.selectors[mode]
		cache.mu.Lock()
		if cache.postings == nil || cache.generation != generation {
			cache.postings = make(map[string]*list.Element)
			cache.lru.Init()
			cache.generation, cache.bytes = generation, 0
		}
		entry, ok := cache.postings[key]
		var refs []storage.SeriesRef
		if ok {
			cache.lru.MoveToFront(entry)
			refs = entry.Value.(*rustCachedSelector).refs
		}
		cache.mu.Unlock()
		if ok {
			return index.NewListPostings(refs), nil
		}
	}
	var p index.Postings
	var err error
	if len(ms) == 0 {
		k, v := index.AllPostingsKey()
		p, err = r.Postings(ctx, k, v)
	} else {
		var filters []rustengine.LabelFilter
		filters, err = r.nativeFilters(ctx, ms)
		if err == nil {
			var response rustengine.Response
			response, err = r.call(ctx, rustengine.Request{Op: "postings_select", Filters: filters})
			if err == nil {
				refs := make([]storage.SeriesRef, len(response.Refs))
				for i, ref := range response.Refs {
					refs[i] = storage.SeriesRef(ref)
				}
				p = index.NewListPostings(refs)
			}
		}
	}
	if err != nil || cache == nil {
		return p, err
	}
	refs, err := index.ExpandPostings(p)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	after, err := r.engine.client.IndexEpoch(r.cold)
	if err != nil {
		return nil, err
	}
	cost := 128 + len(key) + len(refs)*8
	if after == generation && cost <= maxBytes {
		cache.mu.Lock()
		// Concurrent readers may already have advanced to a newer index snapshot.
		if cache.generation == generation {
			if _, exists := cache.postings[key]; !exists {
				for len(cache.postings) >= maxEntries || cache.bytes+cost > maxBytes {
					oldest := cache.lru.Back()
					value := oldest.Value.(*rustCachedSelector)
					delete(cache.postings, value.key)
					cache.bytes -= value.bytes
					cache.lru.Remove(oldest)
				}
				cache.postings[key] = cache.lru.PushFront(&rustCachedSelector{key: key, refs: refs, bytes: cost})
				cache.bytes += cost
			}
		}
		cache.mu.Unlock()
	}
	return index.NewListPostings(refs), nil
}
func (r *rustIndex) SortedPostings(p index.Postings) index.Postings {
	refs, err := index.ExpandPostings(p)
	if err != nil {
		return index.ErrPostings(err)
	}
	sets := make(map[storage.SeriesRef]labels.Labels, len(refs))
	if err := r.seriesLabels(context.Background(), refs, func(ref storage.SeriesRef, adapters []rustengine.Label) {
		sets[ref] = rustLabels(adapters)
	}); err != nil {
		return index.ErrPostings(err)
	}
	slices.SortFunc(refs, func(a, b storage.SeriesRef) int { return labels.Compare(sets[a], sets[b]) })
	return index.NewListPostings(refs)
}
func (r *rustIndex) ShardedPostings(p index.Postings, shard, count uint64) index.Postings {
	return r.shardedPostings(context.Background(), p, shard, count)
}
func (r *rustIndex) shardedPostings(ctx context.Context, p index.Postings, shard, count uint64) index.Postings {
	if count == 0 || shard >= count {
		return index.ErrPostings(errors.New("invalid shard"))
	}
	if err := ctx.Err(); err != nil {
		return index.ErrPostings(err)
	}
	selected, err := index.ExpandPostings(p)
	if err != nil {
		return index.ErrPostings(err)
	}
	var refs []storage.SeriesRef
	if err := r.seriesLabels(ctx, selected, func(ref storage.SeriesRef, adapters []rustengine.Label) {
		if labels.StableHash(rustLabels(adapters))%count == shard {
			refs = append(refs, ref)
		}
	}); err != nil {
		return index.ErrPostings(err)
	}
	return index.NewListPostings(refs)
}
func (r *rustIndex) seriesLabels(ctx context.Context, refs []storage.SeriesRef, visit func(storage.SeriesRef, []rustengine.Label)) error {
	// Bound label payloads while amortizing the native call across the selected refs.
	var page [64]uint64
	for len(refs) > 0 {
		n := min(len(refs), len(page))
		for i, ref := range refs[:n] {
			page[i] = uint64(ref)
		}
		response, err := r.call(ctx, rustengine.Request{Op: "series_refs", Refs: page[:n]})
		if err != nil {
			return err
		}
		if len(response.Series) != n {
			return storage.ErrNotFound
		}
		for i, series := range response.Series {
			if series.Ref != page[i] {
				return errors.New("invalid Rust series label page")
			}
			visit(refs[i], series.Labels)
		}
		refs = refs[n:]
	}
	return nil
}
func (r *rustIndex) labels(ref storage.SeriesRef) (labels.Labels, error) {
	response, err := r.engine.client.Call(rustengine.Request{Op: "series", Ref: uint64(ref)})
	if err != nil {
		return labels.EmptyLabels(), err
	}
	if len(response.Series) == 0 {
		return labels.EmptyLabels(), storage.ErrNotFound
	}
	return rustLabels(response.Series[0].Labels), nil
}
func (r *rustIndex) Series(ref storage.SeriesRef, builder *labels.ScratchBuilder, metas *[]chunks.Meta) error {
	ls, err := r.labels(ref)
	if err != nil {
		return err
	}
	builder.Reset()
	ls.Range(func(l labels.Label) { builder.Add(l.Name, l.Value) })
	if metas != nil {
		*metas = (*metas)[:0]
	}
	return nil
}
func (r *rustIndex) LabelNamesFor(ctx context.Context, p index.Postings) ([]string, error) {
	return r.stringsFor(ctx, "label_names_for", "", nil, p)
}
func (r *rustIndex) stringsFor(ctx context.Context, op, name string, hints *storage.LabelHints, p index.Postings) ([]string, error) {
	refs, err := index.ExpandPostings(p)
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, len(refs))
	for i, ref := range refs {
		ids[i] = uint64(ref)
	}
	limit := 0
	if hints != nil {
		limit = max(0, hints.Limit)
	}
	result, err := r.call(ctx, rustengine.Request{Op: op, Name: name, Refs: ids, Limit: limit})
	if err != nil && err.Error() == "not_found" {
		return nil, storage.ErrNotFound
	}
	return result.Values, err
}
func (r *rustIndex) IndexLookupPlanner() index.LookupPlanner { return index.ChainLookupPlanners() }
