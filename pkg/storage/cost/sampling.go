// SPDX-License-Identifier: AGPL-3.0-only

package cost

import (
	"context"
	"fmt"
	"math"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
)

const (
	// MaxSeries bounds series inspected by each backend sampling RPC.
	MaxSeries = 50
	// MaxChunks bounds chunks fetched by each backend sampling RPC.
	MaxChunks = 50
	// MaxChunkBytes bounds encoded chunk bytes fetched by each backend sampling RPC.
	MaxChunkBytes = 1024 * 1024
	// MaxBlocks bounds blocks sampled by a store-gateway RPC.
	MaxBlocks = 8
)

// Sampler is implemented by storage backends that enforce sampling budgets.
type Sampler interface {
	SampleQueryCost(context.Context, *SamplingRequest) (*SamplingResponse, error)
}

// NewRequest translates selector matchers for backend sampling.
func NewRequest(mint, maxt int64, skipHistogramBuckets bool, matchers []*labels.Matcher) *SamplingRequest {
	r := &SamplingRequest{Start: mint, End: maxt, SkipHistogramBuckets: skipHistogramBuckets}
	for _, m := range matchers {
		r.Matchers = append(r.Matchers, Matcher{Type: int32(m.Type), Name: m.Name, Value: m.Value})
	}
	return r
}

// Validate checks the request before any storage is opened.
func (r *SamplingRequest) Validate() ([]*labels.Matcher, error) {
	if r == nil || r.End < r.Start {
		return nil, fmt.Errorf("invalid sampling time range")
	}
	matchers := make([]*labels.Matcher, 0, len(r.Matchers))
	for _, m := range r.Matchers {
		if m.Type < int32(labels.MatchEqual) || m.Type > int32(labels.MatchNotRegexp) {
			return nil, fmt.Errorf("invalid sampling matcher type %d", m.Type)
		}
		matcher, err := labels.NewMatcher(labels.MatchType(m.Type), m.Name, m.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid sampling matcher: %w", err)
		}
		matchers = append(matchers, matcher)
	}
	return matchers, nil
}

// Merge combines additive measurements without claiming complete-series coverage.
func (r *SamplingResponse) Merge(other *SamplingResponse) {
	r.Intervals += other.Intervals
	r.IntervalMilliseconds += other.IntervalMilliseconds
	r.Points += other.Points
	r.PointCost += other.PointCost
	r.SeriesExamined += other.SeriesExamined
	r.ChunksExamined += other.ChunksExamined
	r.ChunkBytes += other.ChunkBytes
	r.Unsupported = r.Unsupported || other.Unsupported
	r.Warnings = append(r.Warnings, other.Warnings...)
	r.AvailableBlocks = append(r.AvailableBlocks, other.AvailableBlocks...)
}

// Validate checks a single backend response before merging it.
func (r *SamplingResponse) Validate() error {
	if r == nil || r.SeriesExamined > MaxSeries || r.ChunksExamined > MaxChunks || r.ChunkBytes > MaxChunkBytes || r.Points > MaxChunks {
		return fmt.Errorf("query cost sampling response exceeds backend budgets")
	}
	if math.IsNaN(r.IntervalMilliseconds) || math.IsInf(r.IntervalMilliseconds, 0) || r.IntervalMilliseconds < 0 || math.IsNaN(r.PointCost) || math.IsInf(r.PointCost, 0) || r.PointCost < 0 {
		return fmt.Errorf("invalid query cost sampling measurements")
	}
	return nil
}

// Statistics returns measured averages and preserves fallback and storage warnings.
func (r *SamplingResponse) Statistics() (storage.CostSamplingStats, annotations.Annotations, error) {
	var warnings annotations.Annotations
	for _, w := range r.Warnings {
		warnings.Add(fmt.Errorf("%s", w))
	}
	if r.Unsupported {
		return storage.CostSamplingStats{}, warnings, storage.ErrCostSamplingUnsupported
	}
	var stats storage.CostSamplingStats
	if r.Intervals > 0 {
		stats.IntervalMilliseconds = r.IntervalMilliseconds / float64(r.Intervals)
	}
	if r.Points > 0 {
		stats.AveragePointCost = r.PointCost / float64(r.Points)
	}
	return stats, warnings, nil
}

// HasBudget must be checked before advancing a storage chunk iterator or fetching a chunk.
func (r *SamplingResponse) HasBudget() bool {
	return r.ChunksExamined < MaxChunks && r.ChunkBytes < MaxChunkBytes
}

// Observe measures one chunk and decodes at most its first in-window point.
// The caller must enforce the byte budget before fetching remote chunk data.
// A false result means the remaining byte budget cannot admit this chunk.
func (r *SamplingResponse) Observe(ctx context.Context, meta chunks.Meta, req *SamplingRequest) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !r.HasBudget() {
		return false, nil
	}
	r.ChunksExamined++
	if meta.Chunk == nil {
		return true, nil
	}
	size := uint64(len(meta.Chunk.Bytes()))
	if size > MaxChunkBytes-r.ChunkBytes {
		return false, nil
	}
	r.ChunkBytes += size
	it := meta.Chunk.Iterator(nil)
	if req.SkipHistogramBuckets {
		it = promql.NewHistogramStatsIterator(it)
	}
	typ := it.Seek(req.Start)
	if err := it.Err(); err != nil {
		return false, err
	}
	if typ == chunkenc.ValNone || it.AtT() > req.End {
		return true, nil
	}
	var pointCost float64
	switch typ {
	case chunkenc.ValFloat:
		_, v := it.At()
		if !value.IsStaleNaN(v) {
			pointCost = 1
		}
	case chunkenc.ValHistogram, chunkenc.ValFloatHistogram:
		_, h := it.AtFloatHistogram(nil)
		if !value.IsStaleNaN(h.Sum) {
			pointCost = float64((h.Size() + 8) / 16)
		}
	}
	if pointCost > 0 {
		r.Points++
		r.PointCost += pointCost
		if n := meta.Chunk.NumSamples(); n > 1 && meta.MaxTime > meta.MinTime {
			r.Intervals += uint64(n - 1)
			r.IntervalMilliseconds += float64(meta.MaxTime) - float64(meta.MinTime)
		}
	}
	return true, it.Err()
}
