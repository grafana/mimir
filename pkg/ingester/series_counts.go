// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/ingester/client"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

// SeriesCounts counts the head's series that match the request's matchers
// and have a chunk in its window, grouped by one label, from the series'
// chunk metas. No sample is read. The matching postings are resolved first
// and priced at mimir_tsdb.EstimatedSeriesP99Size per series, so a request
// over max_index_bytes, bounded by the tenant's
// -querier.cardinality-estimate-max-index-bytes, is refused before any
// series is walked.
//
// The head, its out-of-order chunks included, and the ingester's compacted
// local blocks that overlap the window are counted; a series in several of
// them counts once. The response's min_time_ms is the oldest time the
// ingester holds, and shard_size_unchanged_since_ms is since when the
// ownedSeries service has seen the tenant's shard size unchanged. A request
// with shard_size_unchanged_before_ms is answered with shard_size_changed and
// nothing counted when that time is not before it, or not known. Experimental.
func (i *Ingester) SeriesCounts(req *client.SeriesCountsRequest, stream client.Ingester_SeriesCountsServer) error {
	resp, err := i.seriesCountsWithStatus(stream.Context(), req)
	if err != nil {
		return err
	}
	return client.SendSeriesCountsResponse(stream, resp, seriesCountsMessageSize)
}

// seriesCountsMessageSize is about how large each message of a streamed
// SeriesCounts answer is.
const seriesCountsMessageSize = queryStreamBatchMessageSize

// seriesCountsWithStatus answers a SeriesCounts request as one response, the
// errors carrying their gRPC status.
func (i *Ingester) seriesCountsWithStatus(ctx context.Context, req *client.SeriesCountsRequest) (*client.SeriesCountsResponse, error) {
	resp, err := i.seriesCounts(ctx, req)
	if err == nil {
		return resp, nil
	}
	// The request's own errors, InvalidArgument and the max_index_bytes
	// refusal, keep their codes; the caller maps them to 400 and 422.
	if st, ok := status.FromError(err); ok && st.Code() != codes.Unknown {
		return nil, err
	}
	return nil, i.mapReadErrorToErrorWithStatus(err)
}

func (i *Ingester) seriesCounts(ctx context.Context, req *client.SeriesCountsRequest) (resp *client.SeriesCountsResponse, err error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	counter, err := cardinality.NewSeriesCounter(cardinality.CounterOptions{
		MinT:        req.StartTimestampMs,
		MaxT:        req.EndTimestampMs,
		Step:        req.StepMs,
		GroupBy:     req.GroupBy,
		Hashes:      req.Hashes,
		SketchAbove: req.SketchAbove,
	})
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if req.MaxIndexBytes < 0 {
		return nil, status.Error(codes.InvalidArgument, "max_index_bytes can't be negative")
	}
	// The tenant's limit also bounds a call that sets no max_index_bytes,
	// or a larger one, whoever sends it.
	maxIndexBytes := cardinality.BoundMaxIndexBytes(req.MaxIndexBytes, i.limits.CardinalityEstimateMaxIndexBytes(userID))
	matchers, err := client.FromLabelMatchers(req.Matchers)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Enforce read consistency before getting TSDB (covers the case the tenant's data has not been ingested
	// in this ingester yet, but there's some to ingest in the backlog).
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return nil, err
	}
	db := i.getTSDB(userID)
	if db == nil {
		return &client.SeriesCountsResponse{}, nil
	}
	head := db.Head()
	resp = &client.SeriesCountsResponse{HeadMinTimeMs: head.MinTime(), HeadMaxTimeMs: head.MaxTime(), ShardSizeUnchangedSinceMs: db.shardSizeUnchangedSince()}
	if before := req.ShardSizeUnchangedBeforeMs; before > 0 && (resp.ShardSizeUnchangedSinceMs == 0 || resp.ShardSizeUnchangedSinceMs >= before) {
		resp.ShardSizeChanged = true
		return resp, nil
	}
	sources, err := countSources(db, req.StartTimestampMs, req.EndTimestampMs)
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, src := range sources {
			src.close()
		}
	}()
	if len(sources) == 0 {
		return resp, nil
	}
	resp.MinTimeMs = sources[0].minT
	for _, src := range sources {
		resp.MinTimeMs = min(resp.MinTimeMs, src.minT)
	}

	// Resolve every source's postings first, so max_index_bytes is checked
	// against the whole request before any series is walked.
	total := 0
	for _, src := range sources {
		var postings index.Postings
		if len(matchers) == 0 {
			name, value := index.AllPostingsKey()
			postings, err = src.idx.Postings(ctx, name, value)
		} else {
			postings, err = tsdb.PostingsForMatchers(ctx, src.idx, matchers...)
		}
		if err != nil {
			return nil, err
		}
		if src.refs, err = index.ExpandPostings(postings); err != nil {
			return nil, err
		}
		total += len(src.refs)
	}
	if maxIndexBytes > 0 {
		if priced := int64(total) * mimir_tsdb.EstimatedSeriesP99Size; priced > maxIndexBytes {
			return nil, status.Error(codes.ResourceExhausted, fmt.Sprintf("the request would walk %d series in %d head and local blocks, about %d bytes at %d bytes each, more than max_index_bytes %d", total, len(sources), priced, mimir_tsdb.EstimatedSeriesP99Size, maxIndexBytes))
		}
	}

	// One source is counted as it is walked. Several hold the same series
	// over different times, so each series' chunk ranges are gathered
	// across them first, by label-set hash, and the series is counted once.
	type gathered struct {
		value  string
		ranges []cardinality.ChunkRange
	}
	var (
		byHash     map[uint64]*gathered
		builder    labels.ScratchBuilder
		metas      []chunks.Meta
		ranges     []cardinality.ChunkRange
		oooBuilder labels.ScratchBuilder
		oooMetas   []chunks.Meta
	)
	if len(sources) > 1 {
		byHash = map[uint64]*gathered{}
	}
	for _, src := range sources {
		for n, ref := range src.refs {
			if n%1024 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			if err := src.idx.Series(ref, &builder, &metas); err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					// Removed by a head compaction since the postings were read.
					continue
				}
				return nil, err
			}
			resp.SeriesWalked++
			ranges = ranges[:0]
			if src.ooo != nil {
				// The same head series' out-of-order chunks in the window.
				// Their metas carry their real end, merged where they
				// overlap.
				if err := src.ooo.Series(ref, &oooBuilder, &oooMetas); err != nil && !errors.Is(err, storage.ErrNotFound) {
					return nil, err
				}
				for _, m := range oooMetas {
					ranges = append(ranges, cardinality.ChunkRange{MinTime: m.MinTime, MaxTime: m.MaxTime})
				}
			}
			for _, m := range metas {
				maxT := m.MaxTime
				if maxT == math.MaxInt64 {
					// The open head chunk has no end yet: its metas say
					// MaxInt64. Read its last sample when the end matters,
					// which is when the chunk starts before the window or
					// the count is per bucket.
					if m.MinTime < req.StartTimestampMs || req.StepMs > 0 {
						if maxT, err = lastSampleTime(src.chunkr, m); err != nil {
							return nil, err
						}
					} else {
						maxT = req.EndTimestampMs - 1
					}
				}
				ranges = append(ranges, cardinality.ChunkRange{MinTime: m.MinTime, MaxTime: maxT})
			}
			if byHash == nil {
				counter.Add(builder.Labels(), ranges)
				continue
			}
			lset := builder.Labels()
			h := labels.StableHash(lset)
			g := byHash[h]
			if g == nil {
				g = &gathered{value: lset.Get(counter.GroupBy())}
				byHash[h] = g
			}
			g.ranges = append(g.ranges, ranges...)
		}
	}
	for h, g := range byHash {
		counter.AddGroup(g.value, h, g.ranges)
	}

	groups, err := counter.Groups()
	if err != nil {
		return nil, err
	}
	resp.SeriesCounted = counter.Counted()
	resp.Groups = make([]*client.SeriesCountGroup, 0, len(groups))
	for _, g := range groups {
		resp.Groups = append(resp.Groups, &client.SeriesCountGroup{Value: g.Value, Counts: g.Counts, Hashes: g.Hashes, Sketch: g.Sketch})
	}
	return resp, nil
}

// lastSampleTime is the timestamp of the last sample in the head chunk
// meta points to. An empty chunk reports its start.
func lastSampleTime(chunkr tsdb.ChunkReader, meta chunks.Meta) (int64, error) {
	chk, iterable, err := chunkr.ChunkOrIterable(meta)
	if err != nil {
		return 0, err
	}
	var it chunkenc.Iterator
	if chk != nil {
		it = chk.Iterator(nil)
	} else {
		it = iterable.Iterator(nil)
	}
	last := meta.MinTime
	for it.Next() != chunkenc.ValNone {
		last = it.AtT()
	}
	return last, it.Err()
}

// countSource is one place an ingester holds a tenant's series: its head or
// one of its compacted local blocks.
type countSource struct {
	minT   int64
	idx    tsdb.IndexReader
	chunkr tsdb.ChunkReader // head only, to find the open chunk's end
	// ooo reads the head's out-of-order chunks, head only, when the
	// out-of-order head overlaps the window. Its refs are the head's.
	ooo   tsdb.IndexReader
	refs  []storage.SeriesRef
	close func()
}

// countSourcesBetweenReadsHook, if set, runs after countSources opens the
// head and before it lists the blocks. Tests use it to run a head
// compaction between the two reads.
var countSourcesBetweenReadsHook func()

// countSources opens the index of every local block and of the head that
// overlaps [minT, maxT), oldest first. The head's index keeps only the
// chunks inside the window; a block's index returns all of a series'
// chunks, which the counter filters.
func countSources(db *userTSDB, minT, maxT int64) (out []*countSource, err error) {
	defer func() {
		if err != nil {
			for _, src := range out {
				src.close()
			}
			out = nil
		}
	}()
	// Read the head first. Opening its chunk reader registers a read in the
	// head's isolation state, and every head truncation or series eviction
	// waits for overlapping reads before it removes data
	// (WaitForPendingReadersInTimeRange). Any compaction whose block is not
	// in the list read below therefore can't remove its data from the head
	// until this request is done; one whose block is listed may already
	// have, and the block holds it. A series in both counts once.
	var headSrc *countSource
	head := db.Head()
	inOrder := head.MaxTime() >= minT && head.MinTime() < maxT
	outOfOrder := head.MinOOOTime() < maxT && head.MaxOOOTime() >= minT
	if head.NumSeries() > 0 && (inOrder || outOfOrder) {
		// The window end is exclusive and the range head's is not.
		rh := tsdb.NewRangeHead(head, minT, maxT-1)
		chunkr, err := rh.Chunks()
		if err != nil {
			return out, err
		}
		idx, err := rh.Index()
		if err != nil {
			_ = chunkr.Close()
			return out, err
		}
		headSrc = &countSource{minT: head.MinTime(), idx: idx, chunkr: chunkr, close: func() { _ = idx.Close(); _ = chunkr.Close() }}
		if outOfOrder {
			// In-order chunks are left out: an in-order start of MaxInt64
			// selects none, so only out-of-order chunks come back. Only
			// chunk metas are read, not chunk data, so no out-of-order
			// isolation is taken; a chunk already compacted into a local
			// block is listed in both and the series counts once.
			// min_time_ms stays the in-order head's start: the out-of-order
			// head's start is not raised when it is compacted.
			headSrc.ooo = tsdb.NewHeadAndOOOIndexReader(head, math.MaxInt64, minT, maxT-1, 0)
		}
		out = append(out, headSrc)
	}
	if countSourcesBetweenReadsHook != nil {
		countSourcesBetweenReadsHook()
	}
	for _, b := range db.Blocks() {
		if b.MaxTime() <= minT || b.MinTime() >= maxT {
			continue
		}
		idx, err := b.Index()
		if err != nil {
			return out, err
		}
		out = append(out, &countSource{minT: b.MinTime(), idx: idx, close: func() { _ = idx.Close() }})
	}
	return out, nil
}
