// SPDX-License-Identifier: AGPL-3.0-only
//go:build rustengine && cgo

package ingester

import (
	"context"
	"errors"
	"slices"

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
	if len(ms) == 0 {
		values, err := r.values(ctx, name)
		if hints != nil && hints.Limit > 0 && len(values) > hints.Limit {
			values = values[:hints.Limit]
		}
		return values, err
	}
	p, err := r.PostingsForMatchers(ctx, false, ms...)
	if err != nil {
		return nil, err
	}
	return r.stringsFor(ctx, "index_label_values", name, hints, p)
}
func (r *rustIndex) SortedLabelValues(ctx context.Context, name string, hints *storage.LabelHints, ms ...*labels.Matcher) ([]string, error) {
	return r.LabelValues(ctx, name, hints, ms...)
}
func (r *rustIndex) LabelNames(ctx context.Context, ms ...*labels.Matcher) ([]string, error) {
	if len(ms) == 0 {
		result, err := r.call(ctx, rustengine.Request{Op: "names"})
		return result.Values, err
	}
	p, err := r.PostingsForMatchers(ctx, false, ms...)
	if err != nil {
		return nil, err
	}
	return r.LabelNamesFor(ctx, p)
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
func (r *rustIndex) PostingsForMatchers(ctx context.Context, _ bool, ms ...*labels.Matcher) (index.Postings, error) {
	if len(ms) == 0 {
		k, v := index.AllPostingsKey()
		return r.Postings(ctx, k, v)
	}
	return tsdb.PostingsForMatchers(ctx, r, ms...)
}
func (r *rustIndex) SortedPostings(p index.Postings) index.Postings {
	refs, err := index.ExpandPostings(p)
	if err != nil {
		return index.ErrPostings(err)
	}
	sets := map[storage.SeriesRef]labels.Labels{}
	for _, ref := range refs {
		ls, err := r.labels(ref)
		if err != nil {
			return index.ErrPostings(err)
		}
		sets[ref] = ls
	}
	slices.SortFunc(refs, func(a, b storage.SeriesRef) int { return labels.Compare(sets[a], sets[b]) })
	return index.NewListPostings(refs)
}
func (r *rustIndex) ShardedPostings(p index.Postings, shard, count uint64) index.Postings {
	if count == 0 || shard >= count {
		return index.ErrPostings(errors.New("invalid shard"))
	}
	var refs []storage.SeriesRef
	for p.Next() {
		ref := p.At()
		ls, err := r.labels(ref)
		if err != nil {
			return index.ErrPostings(err)
		}
		if labels.StableHash(ls)%count == shard {
			refs = append(refs, ref)
		}
	}
	if err := p.Err(); err != nil {
		return index.ErrPostings(err)
	}
	return index.NewListPostings(refs)
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
