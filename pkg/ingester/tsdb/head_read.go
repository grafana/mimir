// Copyright 2021 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"context"
	"math"
	"sort"

	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/tsdb"
	"golang.org/x/exp/slices"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
)

func (h *Head) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return NoopExemplarQuerier(), nil
}

type noopExemplarQuerierQuerier struct{}

func (n noopExemplarQuerierQuerier) Select(start, end int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	return nil, nil
}

// NoopExemplarQuerier is a storage.ExemplarQuerier that does nothing.
func NoopExemplarQuerier() storage.ExemplarQuerier {
	return noopExemplarQuerierQuerier{}
}

// Index returns an IndexReader against the block.
func (h *Head) Index() (tsdb.IndexReader, error) {
	return h.indexRange(math.MinInt64, math.MaxInt64), nil
}

func (h *Head) indexRange(mint, maxt int64) *headIndexReader {
	if hmin := h.MinTime(); hmin > mint {
		mint = hmin
	}
	return &headIndexReader{head: h, mint: mint, maxt: maxt}
}

func (h *Head) Querier(_ context.Context, mint, maxt int64) (storage.Querier, error) {
	if maxt < h.MinTime() {
		return storage.NoopQuerier(), nil
	}

	rh := NewRangeHead(h, mint, maxt)
	querier, err := tsdb.NewBlockQuerier(rh, mint, maxt)
	if err != nil {
		return nil, errors.Wrapf(err, "open block querier for head %s", rh)
	}

	// Getting the querier above registers itself in the queue that the truncation waits on.
	// So if the querier is currently not colliding with any truncation, we can continue to use it and still
	// won't run into a race later since any truncation that comes after will wait on this querier if it overlaps.
	shouldClose, getNew, newMint := h.IsQuerierCollidingWithTruncation(mint, maxt)
	if shouldClose {
		if err := querier.Close(); err != nil {
			return nil, errors.Wrapf(err, "closing head block querier %s", rh)
		}
		querier = nil
	}
	if getNew {
		rh := NewRangeHead(h, newMint, maxt)
		querier, err = tsdb.NewBlockQuerier(rh, newMint, maxt)
		if err != nil {
			return nil, errors.Wrapf(err, "open block querier for head while getting new querier %s", rh)
		}
	}
	if querier == nil {
		querier = storage.NoopQuerier()
	}
	return querier, nil
}

func (h *Head) ChunkQuerier(ctx context.Context, mint int64, maxt int64) (storage.ChunkQuerier, error) {
	if maxt < h.MinTime() {
		return storage.NoopChunkedQuerier(), nil
	}

	rh := NewRangeHead(h, mint, maxt)
	querier, err := tsdb.NewBlockChunkQuerier(rh, mint, maxt)
	if err != nil {
		return nil, errors.Wrapf(err, "open querier for head %s", rh)
	}

	// Getting the querier above registers itself in the queue that the truncation waits on.
	// So if the querier is currently not colliding with any truncation, we can continue to use it and still
	// won't run into a race later since any truncation that comes after will wait on this querier if it overlaps.
	shouldClose, getNew, newMint := h.IsQuerierCollidingWithTruncation(mint, maxt)
	if shouldClose {
		if err := querier.Close(); err != nil {
			return nil, errors.Wrapf(err, "closing head querier %s", rh)
		}
		querier = nil
	}
	if getNew {
		rh := NewRangeHead(h, newMint, maxt)
		querier, err = tsdb.NewBlockChunkQuerier(rh, newMint, maxt)
		if err != nil {
			return nil, errors.Wrapf(err, "open querier for head while getting new querier %s", rh)
		}
	}

	if querier == nil {
		querier = storage.NoopChunkedQuerier()
	}
	return querier, nil
}

type headIndexReader struct {
	head       *Head
	mint, maxt int64
}

func (h *headIndexReader) Close() error {
	return nil
}

func (h *headIndexReader) Symbols() index.StringIter {
	return h.head.postings.Symbols()
}

// SortedLabelValues returns label values present in the head for the
// specific label name that are within the time range mint to maxt.
// If matchers are specified the returned result set is reduced
// to label values of metrics matching the matchers.
func (h *headIndexReader) SortedLabelValues(name string, matchers ...*labels.Matcher) ([]string, error) {
	values, err := h.LabelValues(name, matchers...)
	if err == nil {
		slices.Sort(values)
	}
	return values, err
}

// LabelValues returns label values present in the head for the
// specific label name that are within the time range mint to maxt.
// If matchers are specified the returned result set is reduced
// to label values of metrics matching the matchers.
func (h *headIndexReader) LabelValues(name string, matchers ...*labels.Matcher) ([]string, error) {
	if h.maxt < h.head.MinTime() || h.mint > h.head.MaxTime() {
		return []string{}, nil
	}

	if len(matchers) == 0 {
		return h.head.postings.LabelValues(name), nil
	}

	return labelValuesWithMatchers(h, name, matchers...)
}

// LabelNames returns all the unique label names present in the head
// that are within the time range mint to maxt.
func (h *headIndexReader) LabelNames(matchers ...*labels.Matcher) ([]string, error) {
	if h.maxt < h.head.MinTime() || h.mint > h.head.MaxTime() {
		return []string{}, nil
	}

	if len(matchers) == 0 {
		labelNames := h.head.postings.LabelNames()
		slices.Sort(labelNames)
		return labelNames, nil
	}

	return labelNamesWithMatchers(h, matchers...)
}

// Postings returns the postings list iterator for the label pairs.
func (h *headIndexReader) Postings(name string, values ...string) (index.Postings, error) {
	switch len(values) {
	case 0:
		return index.EmptyPostings(), nil
	case 1:
		return h.head.postings.Get(name, values[0]), nil
	default:
		res := make([]index.Postings, 0, len(values))
		for _, value := range values {
			res = append(res, h.head.postings.Get(name, value))
		}
		return index.Merge(res...), nil
	}
}

func (h *headIndexReader) PostingsForMatchers(concurrent bool, ms ...*labels.Matcher) (index.Postings, error) {
	return h.head.pfmc.PostingsForMatchers(h, concurrent, ms...)
}

func (h *headIndexReader) SortedPostings(p index.Postings) index.Postings {
	series := make([]*memSeries, 0, 128)

	// Fetch all the series only once.
	for p.Next() {
		s := h.head.series.getByID(chunks.HeadSeriesRef(p.At()))
		if s == nil {
			level.Debug(h.head.logger).Log("msg", "Looked up series not found")
		} else {
			series = append(series, s)
		}
	}
	if err := p.Err(); err != nil {
		return index.ErrPostings(errors.Wrap(err, "expand postings"))
	}

	sort.Slice(series, func(i, j int) bool {
		return labels.Compare(series[i].lset, series[j].lset) < 0
	})

	// Convert back to list.
	ep := make([]storage.SeriesRef, 0, len(series))
	for _, p := range series {
		ep = append(ep, storage.SeriesRef(p.ref))
	}
	return index.NewListPostings(ep)
}

func (h *headIndexReader) ShardedPostings(p index.Postings, shardIndex, shardCount uint64) index.Postings {
	out := make([]storage.SeriesRef, 0, 128)

	for p.Next() {
		s := h.head.series.getByID(chunks.HeadSeriesRef(p.At()))
		if s == nil {
			level.Debug(h.head.logger).Log("msg", "Looked up series not found")
			continue
		}

		// Check if the series belong to the shard.
		if s.hash%shardCount != shardIndex {
			continue
		}

		out = append(out, storage.SeriesRef(s.ref))
	}

	return index.NewListPostings(out)
}

// Series returns the series for the given reference.
// Chunks are skipped if chks is nil.
func (h *headIndexReader) Series(ref storage.SeriesRef, lbls *labels.Labels, chks *[]chunks.Meta) error {
	s := h.head.series.getByID(chunks.HeadSeriesRef(ref))

	if s == nil {
		h.head.metrics.seriesNotFound.Inc()
		return storage.ErrNotFound
	}
	*lbls = append((*lbls)[:0], s.lset...)

	if chks == nil {
		return nil
	}

	s.Lock()
	defer s.Unlock()

	*chks = (*chks)[:0]

	for i, c := range s.finishedChunks {
		// Do not expose chunks that are outside of the specified range.
		if !c.OverlapsClosedInterval(h.mint, h.maxt) {
			continue
		}
		*chks = append(*chks, chunks.Meta{
			MinTime: c.minTime,
			MaxTime: c.maxTime,
			Ref:     chunks.ChunkRef(chunks.NewHeadChunkRef(s.ref, s.headChunkID(i))),
		})
	}
	if s.headChunk != nil && s.headChunk.OverlapsClosedInterval(h.mint, h.maxt) {
		*chks = append(*chks, chunks.Meta{
			MinTime: s.headChunk.minTime,
			MaxTime: math.MaxInt64, // Set the head chunks as open (being appended to).
			Ref:     chunks.ChunkRef(chunks.NewHeadChunkRef(s.ref, s.headChunkID(len(s.finishedChunks)))),
		})
	}

	return nil
}

// headChunkID returns the HeadChunkID referred to by the given position.
// * 0 <= pos < len(s.mmappedChunks) refer to s.mmappedChunks[pos]
// * pos == len(s.mmappedChunks) refers to s.headChunk
func (s *memSeries) headChunkID(pos int) chunks.HeadChunkID {
	return chunks.HeadChunkID(pos) + s.firstChunkID
}

// LabelValueFor returns label value for the given label name in the series referred to by ID.
func (h *headIndexReader) LabelValueFor(id storage.SeriesRef, label string) (string, error) {
	memSeries := h.head.series.getByID(chunks.HeadSeriesRef(id))
	if memSeries == nil {
		return "", storage.ErrNotFound
	}

	value := memSeries.lset.Get(label)
	if value == "" {
		return "", storage.ErrNotFound
	}

	return value, nil
}

// LabelNamesFor returns all the label names for the series referred to by IDs.
// The names returned are sorted.
func (h *headIndexReader) LabelNamesFor(ids ...storage.SeriesRef) ([]string, error) {
	namesMap := make(map[string]struct{})
	for _, id := range ids {
		memSeries := h.head.series.getByID(chunks.HeadSeriesRef(id))
		if memSeries == nil {
			return nil, storage.ErrNotFound
		}
		for _, lbl := range memSeries.lset {
			namesMap[lbl.Name] = struct{}{}
		}
	}
	names := make([]string, 0, len(namesMap))
	for name := range namesMap {
		names = append(names, name)
	}
	slices.Sort(names)
	return names, nil
}

// Chunks returns a ChunkReader against the block.
func (h *Head) Chunks() (tsdb.ChunkReader, error) {
	return h.chunksRange(math.MinInt64, math.MaxInt64, h.iso.State(math.MinInt64, math.MaxInt64))
}

func (h *Head) chunksRange(mint, maxt int64, is *isolationState) (*headChunkReader, error) {
	h.closedMtx.Lock()
	defer h.closedMtx.Unlock()
	if h.closed {
		return nil, errors.New("can't read from a closed head")
	}
	if hmin := h.MinTime(); hmin > mint {
		mint = hmin
	}
	return &headChunkReader{
		head:     h,
		mint:     mint,
		maxt:     maxt,
		isoState: is,
	}, nil
}

type headChunkReader struct {
	head       *Head
	mint, maxt int64
	isoState   *isolationState
}

func (h *headChunkReader) Close() error {
	if h.isoState != nil {
		h.isoState.Close()
	}
	return nil
}

// Chunk returns the chunk for the reference number.
func (h *headChunkReader) Chunk(meta chunks.Meta) (chunkenc.Chunk, error) {
	sid, cid := chunks.HeadChunkRef(meta.Ref).Unpack()

	s := h.head.series.getByID(sid)
	// This means that the series has been garbage collected.
	if s == nil {
		return nil, storage.ErrNotFound
	}

	s.Lock()
	c, err := s.chunk(cid)
	if err != nil {
		s.Unlock()
		return nil, err
	}

	// This means that the chunk is outside the specified range.
	if !c.OverlapsClosedInterval(h.mint, h.maxt) {
		s.Unlock()
		return nil, storage.ErrNotFound
	}
	s.Unlock()

	return &safeChunk{
		Chunk: c.chunk,
		s:     s,
		cid:   cid,
	}, nil
}

// chunk returns the chunk for the HeadChunkID from memory or by m-mapping it from the disk.
// If garbageCollect is true, it means that the returned *memChunk
// (and not the chunkenc.Chunk inside it) can be garbage collected after its usage.
func (s *memSeries) chunk(id chunks.HeadChunkID) (chunk *memChunk, err error) {
	// ix represents the index of chunk in the s.mmappedChunks slice. The chunk id's are
	// incremented by 1 when new chunk is created, hence (id - firstChunkID) gives the slice index.
	// The max index for the s.mmappedChunks slice can be len(s.mmappedChunks)-1, hence if the ix
	// is len(s.mmappedChunks), it represents the next chunk, which is the head chunk.
	ix := int(id) - int(s.firstChunkID)
	if ix < 0 || ix > len(s.finishedChunks) {
		return nil, storage.ErrNotFound
	}
	if ix == len(s.finishedChunks) {
		if s.headChunk == nil {
			return nil, errors.New("invalid head chunk")
		}
		return s.headChunk, nil
	}
	return s.finishedChunks[ix], nil
}

var _ chunkenc.Chunk = &boundedChunk{}

// boundedChunk is an implementation of chunkenc.Chunk that uses a
// boundedIterator that only iterates through samples which timestamps are
// >= minT and <= maxT
type boundedChunk struct {
	chunkenc.Chunk
	minT int64
	maxT int64
}

func (b boundedChunk) Bytes() []byte {
	xor := chunkenc.NewXORChunk()
	a, _ := xor.Appender()
	it := b.Iterator(nil)
	for it.Next() {
		t, v := it.At()
		a.Append(t, v)
	}
	return xor.Bytes()
}

func (b boundedChunk) Iterator(iterator chunkenc.Iterator) chunkenc.Iterator {
	it := b.Chunk.Iterator(iterator)
	if it == nil {
		panic("iterator shouldn't be nil")
	}
	return boundedIterator{it, b.minT, b.maxT}
}

var _ chunkenc.Iterator = &boundedIterator{}

// boundedIterator is an implementation of Iterator that only iterates through
// samples which timestamps are >= minT and <= maxT
type boundedIterator struct {
	chunkenc.Iterator
	minT int64
	maxT int64
}

// Next the first time its called it will advance as many positions as necessary
// until its able to find a sample within the bounds minT and maxT.
// If there are samples within bounds it will advance one by one amongst them.
// If there are no samples within bounds it will return false.
func (b boundedIterator) Next() bool {
	for b.Iterator.Next() {
		t, _ := b.Iterator.At()
		if t < b.minT {
			continue
		} else if t > b.maxT {
			return false
		}
		return true
	}
	return false
}

func (b boundedIterator) Seek(t int64) bool {
	if t < b.minT {
		// We must seek at least up to b.minT if it is asked for something before that.
		ok := b.Iterator.Seek(b.minT)
		if !ok {
			return false
		}
		t, _ := b.Iterator.At()
		return t <= b.maxT
	}
	if t > b.maxT {
		// We seek anyway so that the subsequent Next() calls will also return false.
		b.Iterator.Seek(t)
		return false
	}
	return b.Iterator.Seek(t)
}

// safeChunk makes sure that the chunk can be accessed without a race condition
type safeChunk struct {
	chunkenc.Chunk
	s   *memSeries
	cid chunks.HeadChunkID
}

func (c *safeChunk) Iterator(reuseIter chunkenc.Iterator) chunkenc.Iterator {
	c.s.Lock()
	it := c.s.iterator(c.cid, reuseIter)
	c.s.Unlock()
	return it
}

// iterator returns a chunk iterator for the requested chunkID, or a NopIterator if the requested ID is out of range.
// It is unsafe to call this concurrently with s.append(...) without holding the series lock.
func (s *memSeries) iterator(id chunks.HeadChunkID, it chunkenc.Iterator) chunkenc.Iterator {
	c, err := s.chunk(id)
	// TODO(fabxc): Work around! An error will be returns when a querier have retrieved a pointer to a
	// series's chunk, which got then garbage collected before it got
	// accessed.  We must ensure to not garbage collect as long as any
	// readers still hold a reference.
	if err != nil {
		return chunkenc.NewNopIterator()
	}

	numSamples := c.chunk.NumSamples()
	stopAfter := numSamples

	if stopAfter == 0 {
		return chunkenc.NewNopIterator()
	}
	if stopAfter == numSamples {
		return c.chunk.Iterator(it)
	}
	return makeStopIterator(c.chunk, it, stopAfter)
}

func makeStopIterator(c chunkenc.Chunk, it chunkenc.Iterator, stopAfter int) chunkenc.Iterator {
	// Re-use the Iterator object if it is a stopIterator.
	if stopIter, ok := it.(*stopIterator); ok {
		stopIter.Iterator = c.Iterator(stopIter.Iterator)
		stopIter.i = -1
		stopIter.stopAfter = stopAfter
		return stopIter
	}
	return &stopIterator{
		Iterator:  c.Iterator(it),
		i:         -1,
		stopAfter: stopAfter,
	}
}

// stopIterator wraps an Iterator, but only returns the first
// stopAfter values, if initialized with i=-1.
type stopIterator struct {
	chunkenc.Iterator

	i, stopAfter int
}

func (it *stopIterator) Next() bool {
	if it.i+1 >= it.stopAfter {
		return false
	}
	it.i++
	return it.Iterator.Next()
}
