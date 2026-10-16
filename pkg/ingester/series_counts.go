// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
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
// over max_index_bytes is refused before any series is walked.
//
// Only the in-order head is counted: out-of-order samples and the
// ingester's compacted blocks are not, which the response's head time range
// lets the caller account for. Experimental.
func (i *Ingester) SeriesCounts(ctx context.Context, req *client.SeriesCountsRequest) (*client.SeriesCountsResponse, error) {
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
	resp = &client.SeriesCountsResponse{HeadMinTimeMs: head.MinTime(), HeadMaxTimeMs: head.MaxTime()}

	// The range head keeps only the chunks inside the window when a series
	// is read; the window end is exclusive and the range head's is not.
	rh := tsdb.NewRangeHead(head, req.StartTimestampMs, req.EndTimestampMs-1)
	idx, err := rh.Index()
	if err != nil {
		return nil, err
	}
	defer idx.Close()
	chunkr, err := rh.Chunks()
	if err != nil {
		return nil, err
	}
	defer chunkr.Close()

	var postings index.Postings
	if len(matchers) == 0 {
		name, value := index.AllPostingsKey()
		postings, err = idx.Postings(ctx, name, value)
	} else {
		postings, err = tsdb.PostingsForMatchers(ctx, idx, matchers...)
	}
	if err != nil {
		return nil, err
	}
	refs, err := index.ExpandPostings(postings)
	if err != nil {
		return nil, err
	}
	if req.MaxIndexBytes > 0 {
		if priced := int64(len(refs)) * mimir_tsdb.EstimatedSeriesP99Size; priced > req.MaxIndexBytes {
			return nil, status.Error(codes.ResourceExhausted, fmt.Sprintf("the request would walk %d head series, about %d bytes at %d bytes each, more than max_index_bytes %d", len(refs), priced, mimir_tsdb.EstimatedSeriesP99Size, req.MaxIndexBytes))
		}
	}

	var (
		builder labels.ScratchBuilder
		metas   []chunks.Meta
		ranges  []cardinality.ChunkRange
	)
	for n, ref := range refs {
		if n%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if err := idx.Series(ref, &builder, &metas); err != nil {
			if err == storage.ErrNotFound {
				// Removed by a head compaction since the postings were read.
				continue
			}
			return nil, err
		}
		resp.SeriesWalked++
		ranges = ranges[:0]
		for _, m := range metas {
			maxT := m.MaxTime
			if maxT == math.MaxInt64 {
				// The open head chunk has no end yet: its metas say
				// MaxInt64. Read its last sample when the end matters,
				// which is when the chunk starts before the window or the
				// count is per bucket.
				if m.MinTime < req.StartTimestampMs || req.StepMs > 0 {
					if maxT, err = lastSampleTime(chunkr, m); err != nil {
						return nil, err
					}
				} else {
					maxT = req.EndTimestampMs - 1
				}
			}
			ranges = append(ranges, cardinality.ChunkRange{MinTime: m.MinTime, MaxTime: maxT})
		}
		counter.Add(builder.Labels(), ranges)
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
