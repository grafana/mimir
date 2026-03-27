// SPDX-License-Identifier: AGPL-3.0-only

package segmentindex

import (
	"context"
	"sort"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

// segment is a frozen, read-only snapshot of a write buffer.
//
// In the production implementation these would be backed by disk with a
// sparse in-memory index. For now they hold a frozen MemPostings to
// prove the segment lifecycle.
type segment struct {
	postings *index.MemPostings
	series   map[storage.SeriesRef]labels.Labels
}

// Index is a segment-stack inverted index for the TSDB head.
//
// It replaces MemPostings with a structure that uses sublinear memory:
// a small in-memory write buffer plus immutable on-disk segments, each
// with a sparse in-memory index.
//
// Write path: Add(ref, labels) inserts into the write buffer. When the
// buffer is flushed, it becomes an immutable segment.
//
// Read path: queries fan out across the write buffer and all segments,
// merging results.
//
// Deletion: old segments are dropped whole.
type Index struct {
	mu       sync.RWMutex
	buf      *index.MemPostings
	series   map[storage.SeriesRef]labels.Labels
	segments []*segment
	dir      string
}

// NewIndex creates a new segment-stack index, storing segment files in dir.
func NewIndex(dir string) (*Index, error) {
	return &Index{
		buf:    index.NewMemPostings(),
		series: make(map[storage.SeriesRef]labels.Labels),
		dir:    dir,
	}, nil
}

// Add inserts a series into the index's write buffer.
func (idx *Index) Add(ref storage.SeriesRef, lset labels.Labels) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.buf.Add(ref, lset)
	idx.series[ref] = lset
}

// Flush freezes the current write buffer into an immutable segment and
// creates a new empty write buffer.
func (idx *Index) Flush() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	seg := &segment{
		postings: idx.buf,
		series:   idx.series,
	}
	idx.segments = append(idx.segments, seg)

	idx.buf = index.NewMemPostings()
	idx.series = make(map[storage.SeriesRef]labels.Labels)
	return nil
}

// DropOldestSegment removes the oldest segment.
func (idx *Index) DropOldestSegment() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if len(idx.segments) == 0 {
		return nil
	}
	idx.segments = idx.segments[1:]
	return nil
}

// Reader returns an IndexReader that merges the write buffer and all segments.
func (idx *Index) Reader() *Reader {
	return &Reader{idx: idx}
}

// Close releases all resources held by the index.
func (idx *Index) Close() error {
	return nil
}

// Reader implements a read-only view over the segment-stack index.
type Reader struct {
	idx *Index
}

// allPostings collects MemPostings references to query: the active buffer
// plus all frozen segments.
func (r *Reader) allPostings() []*index.MemPostings {
	result := make([]*index.MemPostings, 0, 1+len(r.idx.segments))
	for _, seg := range r.idx.segments {
		result = append(result, seg.postings)
	}
	result = append(result, r.idx.buf)
	return result
}

func (r *Reader) Postings(ctx context.Context, name string, values ...string) (index.Postings, error) {
	r.idx.mu.RLock()
	defer r.idx.mu.RUnlock()

	all := r.allPostings()
	iters := make([]index.Postings, 0, len(all))
	for _, mp := range all {
		iters = append(iters, mp.Postings(ctx, name, values...))
	}
	return index.Merge(ctx, iters...), nil
}

func (r *Reader) PostingsForLabelMatching(ctx context.Context, name string, match func(string) bool) index.Postings {
	r.idx.mu.RLock()
	defer r.idx.mu.RUnlock()

	all := r.allPostings()
	iters := make([]index.Postings, 0, len(all))
	for _, mp := range all {
		iters = append(iters, mp.PostingsForLabelMatching(ctx, name, match))
	}
	return index.Merge(ctx, iters...)
}

func (r *Reader) PostingsForAllLabelValues(ctx context.Context, name string) index.Postings {
	r.idx.mu.RLock()
	defer r.idx.mu.RUnlock()

	all := r.allPostings()
	iters := make([]index.Postings, 0, len(all))
	for _, mp := range all {
		iters = append(iters, mp.PostingsForAllLabelValues(ctx, name))
	}
	return index.Merge(ctx, iters...)
}

func (r *Reader) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, error) {
	r.idx.mu.RLock()
	defer r.idx.mu.RUnlock()

	seen := make(map[string]struct{})
	for _, mp := range r.allPostings() {
		for _, v := range mp.LabelValues(ctx, name, hints) {
			seen[v] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return nil, nil
	}
	result := make([]string, 0, len(seen))
	for v := range seen {
		result = append(result, v)
	}
	sort.Strings(result)
	return result, nil
}

func (r *Reader) LabelNames(_ context.Context, matchers ...*labels.Matcher) ([]string, error) {
	r.idx.mu.RLock()
	defer r.idx.mu.RUnlock()

	seen := make(map[string]struct{})
	for _, mp := range r.allPostings() {
		for _, n := range mp.LabelNames() {
			seen[n] = struct{}{}
		}
	}
	result := make([]string, 0, len(seen))
	for n := range seen {
		result = append(result, n)
	}
	sort.Strings(result)
	return result, nil
}

func (r *Reader) Series(ref storage.SeriesRef, builder *labels.ScratchBuilder, chks *[]chunks.Meta) error {
	r.idx.mu.RLock()
	defer r.idx.mu.RUnlock()

	if lset, ok := r.idx.series[ref]; ok {
		builder.Assign(lset)
		return nil
	}
	for _, seg := range r.idx.segments {
		if lset, ok := seg.series[ref]; ok {
			builder.Assign(lset)
			return nil
		}
	}
	return storage.ErrNotFound
}

func (r *Reader) Close() error {
	return nil
}
