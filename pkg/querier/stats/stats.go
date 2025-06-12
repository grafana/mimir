// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/stats/stats.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package stats

import (
	"context"
	"sync"
	"sync/atomic" //lint:ignore faillint we can't use go.uber.org/atomic with a protobuf struct without wrapping it.
	"time"

	"github.com/grafana/dskit/httpgrpc"
)

type contextKey int

var ctxKey = contextKey(0)

// ContextWithEmptyStats returns a context with empty stats.
func ContextWithEmptyStats(ctx context.Context) (*Stats, context.Context) {
	stats := &Stats{}
	ctx = context.WithValue(ctx, ctxKey, stats)
	return stats, ctx
}

// FromContext gets the Stats out of the Context. Returns nil if stats have not
// been initialised in the context. Note that Stats methods are safe to call with
// a nil receiver.
func FromContext(ctx context.Context) *Stats {
	o := ctx.Value(ctxKey)
	if o == nil {
		return nil
	}
	return o.(*Stats)
}

// IsEnabled returns whether stats tracking is enabled in the context.
func IsEnabled(ctx context.Context) bool {
	// When query statistics are enabled, the stats object is already initialised
	// within the context, so we can just check it.
	return FromContext(ctx) != nil
}

type Stats struct {
	StatsData
	perStepStatsMx sync.Mutex
}

// AddWallTime adds some time to the counter.
func (s *Stats) AddWallTime(t time.Duration) {
	if s == nil {
		return
	}

	atomic.AddInt64((*int64)(&s.WallTime), int64(t))
}

// LoadWallTime returns current wall time.
func (s *Stats) LoadWallTime() time.Duration {
	if s == nil {
		return 0
	}

	return time.Duration(atomic.LoadInt64((*int64)(&s.WallTime)))
}

func (s *Stats) AddFetchedSeries(series uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.FetchedSeriesCount, series)
}

func (s *Stats) LoadFetchedSeries() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.FetchedSeriesCount)
}

func (s *Stats) AddFetchedChunkBytes(bytes uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.FetchedChunkBytes, bytes)
}

func (s *Stats) LoadFetchedChunkBytes() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.FetchedChunkBytes)
}

func (s *Stats) AddFetchedChunks(chunks uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.FetchedChunksCount, chunks)
}

func (s *Stats) LoadFetchedChunks() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.FetchedChunksCount)
}

func (s *Stats) AddFetchedIndexBytes(indexBytes uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.FetchedIndexBytes, indexBytes)
}

func (s *Stats) LoadFetchedIndexBytes() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.FetchedIndexBytes)
}

func (s *Stats) AddShardedQueries(num uint32) {
	if s == nil {
		return
	}

	atomic.AddUint32(&s.ShardedQueries, num)
}

func (s *Stats) LoadShardedQueries() uint32 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint32(&s.ShardedQueries)
}

func (s *Stats) AddSplitQueries(num uint32) {
	if s == nil {
		return
	}

	atomic.AddUint32(&s.SplitQueries, num)
}

func (s *Stats) LoadSplitQueries() uint32 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint32(&s.SplitQueries)
}

func (s *Stats) AddEstimatedSeriesCount(c uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.EstimatedSeriesCount, c)
}

func (s *Stats) LoadEstimatedSeriesCount() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.EstimatedSeriesCount)
}

func (s *Stats) AddQueueTime(t time.Duration) {
	if s == nil {
		return
	}

	atomic.AddInt64((*int64)(&s.QueueTime), int64(t))
}

func (s *Stats) LoadQueueTime() time.Duration {
	if s == nil {
		return 0
	}

	return time.Duration(atomic.LoadInt64((*int64)(&s.QueueTime)))
}

func (s *Stats) AddEncodeTime(t time.Duration) {
	if s == nil {
		return
	}

	atomic.AddInt64((*int64)(&s.EncodeTime), int64(t))
}

func (s *Stats) LoadEncodeTime() time.Duration {
	if s == nil {
		return 0
	}

	return time.Duration(atomic.LoadInt64((*int64)(&s.EncodeTime)))
}

func (s *Stats) AddSamplesProcessed(c uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.SamplesProcessed, c)
}

func (s *Stats) LoadSamplesProcessed() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.SamplesProcessed)
}

func (s *Stats) AddSpunOffSubqueries(num uint32) {
	if s == nil {
		return
	}
	atomic.AddUint32(&s.SpunOffSubqueries, num)
}

func (s *Stats) LoadSpunOffSubqueries() uint32 {
	if s == nil {
		return 0
	}
	return atomic.LoadUint32(&s.SpunOffSubqueries)
}

func (s *Stats) AddSamplesProcessedPerStep(points []StepStat) {
	s.perStepStatsMx.Lock()
	defer s.perStepStatsMx.Unlock()

	if s == nil {
		return
	}

	s.SamplesProcessedPerStep = points
}

func (s *Stats) LoadSamplesProcessedPerStep() []StepStat {
	s.perStepStatsMx.Lock()
	defer s.perStepStatsMx.Unlock()
	if s == nil {
		return nil
	}
	return s.SamplesProcessedPerStep
}

// Merge the provided Stats into this one.
func (s *Stats) Merge(other *Stats) {
	if s == nil || other == nil {
		return
	}
	s.AddWallTime(other.LoadWallTime())
	s.AddFetchedSeries(other.LoadFetchedSeries())
	s.AddFetchedChunkBytes(other.LoadFetchedChunkBytes())
	s.AddFetchedChunks(other.LoadFetchedChunks())
	s.AddShardedQueries(other.LoadShardedQueries())
	s.AddSplitQueries(other.LoadSplitQueries())
	s.AddFetchedIndexBytes(other.LoadFetchedIndexBytes())
	s.AddEstimatedSeriesCount(other.LoadEstimatedSeriesCount())
	s.AddQueueTime(other.LoadQueueTime())
	s.AddEncodeTime(other.LoadEncodeTime())
	s.AddSamplesProcessed(other.LoadSamplesProcessed())
	s.AddSpunOffSubqueries(other.LoadSpunOffSubqueries())
	s.mergeSamplesProcessedPerStep(other.LoadSamplesProcessedPerStep())
}

func (s *Stats) mergeSamplesProcessedPerStep(other []StepStat) {
	if s == nil {
		return
	}
	// Hold the lock for the entire merge operation to make it atomic
	s.perStepStatsMx.Lock()
	defer s.perStepStatsMx.Unlock()

	this := s.SamplesProcessedPerStep // Access directly since we hold the lock

	if len(other) == 0 {
		// Nothing to merge
		return
	}

	merged := make([]StepStat, 0, len(this)+len(other))
	i, j := 0, 0
	confilctsNum := 0
	var sum int64

	for i < len(this) && j < len(other) {
		if this[i].Timestamp < other[j].Timestamp {
			merged = append(merged, this[i])
			sum += this[i].Value
			i++
		} else if other[j].Timestamp < this[i].Timestamp {
			merged = append(merged, other[j])
			sum += other[j].Value
			j++
		} else {
			confilctsNum++
			summed := StepStat{
				Timestamp: this[i].Timestamp,
				Value:     this[i].Value + other[j].Value,
			}
			merged = append(merged, summed)
			sum += summed.Value
			i++
			j++
		}
	}

	// Append any remaining elements
	for ; i < len(this); i++ {
		merged = append(merged, this[i])
		sum += this[i].Value
	}
	for ; j < len(other); j++ {
		merged = append(merged, other[j])
		sum += other[j].Value
	}
	s.SamplesProcessedPerStep = merged // Set directly since we hold the lock
}

// Copy returns a copy of the stats. Use this rather than regular struct assignment
// to make sure atomic modifications are observed.
func (s *Stats) Copy() *Stats {
	if s == nil {
		return nil
	}
	c := &Stats{}
	c.Merge(s)
	return c
}

func ShouldTrackHTTPGRPCResponse(r *httpgrpc.HTTPResponse) bool {
	// Do no track statistics for requests failed because of a server error.
	return r.Code < 500
}
