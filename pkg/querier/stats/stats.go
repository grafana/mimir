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
func ContextWithEmptyStats(ctx context.Context) (*SafeStats, context.Context) {
	stats := &SafeStats{}
	ctx = context.WithValue(ctx, ctxKey, stats)
	return stats, ctx
}

// FromContext gets the Stats out of the Context. Returns nil if stats have not
// been initialised in the context. Note that Stats methods are safe to call with
// a nil receiver.
func FromContext(ctx context.Context) *SafeStats {
	o := ctx.Value(ctxKey)
	if o == nil {
		return nil
	}
	return o.(*SafeStats)
}

// IsEnabled returns whether stats tracking is enabled in the context.
func IsEnabled(ctx context.Context) bool {
	// When query statistics are enabled, the stats object is already initialised
	// within the context, so we can just check it.
	return FromContext(ctx) != nil
}

// SafeStats is a concurrent safe wrapper around the Stats struct.
type SafeStats struct {
	Stats
	mx sync.Mutex
}

// AddWallTime adds some time to the counter.
func (s *SafeStats) AddWallTime(t time.Duration) {
	if s == nil {
		return
	}

	atomic.AddInt64((*int64)(&s.WallTime), int64(t))
}

// LoadWallTime returns current wall time.
func (s *SafeStats) LoadWallTime() time.Duration {
	if s == nil {
		return 0
	}

	return time.Duration(atomic.LoadInt64((*int64)(&s.WallTime)))
}

func (s *SafeStats) AddFetchedSeries(series uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.FetchedSeriesCount, series)
}

func (s *SafeStats) LoadFetchedSeries() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.FetchedSeriesCount)
}

func (s *SafeStats) AddFetchedChunkBytes(bytes uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.FetchedChunkBytes, bytes)
}

func (s *SafeStats) LoadFetchedChunkBytes() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.FetchedChunkBytes)
}

func (s *SafeStats) AddFetchedChunks(chunks uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.FetchedChunksCount, chunks)
}

func (s *SafeStats) LoadFetchedChunks() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.FetchedChunksCount)
}

func (s *SafeStats) AddFetchedIndexBytes(indexBytes uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.FetchedIndexBytes, indexBytes)
}

func (s *SafeStats) LoadFetchedIndexBytes() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.FetchedIndexBytes)
}

func (s *SafeStats) AddShardedQueries(num uint32) {
	if s == nil {
		return
	}

	atomic.AddUint32(&s.ShardedQueries, num)
}

func (s *SafeStats) LoadShardedQueries() uint32 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint32(&s.ShardedQueries)
}

func (s *SafeStats) AddSplitQueries(num uint32) {
	if s == nil {
		return
	}

	atomic.AddUint32(&s.SplitQueries, num)
}

func (s *SafeStats) LoadSplitQueries() uint32 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint32(&s.SplitQueries)
}

func (s *SafeStats) AddEstimatedSeriesCount(c uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.EstimatedSeriesCount, c)
}

func (s *SafeStats) LoadEstimatedSeriesCount() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.EstimatedSeriesCount)
}

func (s *SafeStats) AddQueueTime(t time.Duration) {
	if s == nil {
		return
	}

	atomic.AddInt64((*int64)(&s.QueueTime), int64(t))
}

func (s *SafeStats) LoadQueueTime() time.Duration {
	if s == nil {
		return 0
	}

	return time.Duration(atomic.LoadInt64((*int64)(&s.QueueTime)))
}

func (s *SafeStats) AddEncodeTime(t time.Duration) {
	if s == nil {
		return
	}

	atomic.AddInt64((*int64)(&s.EncodeTime), int64(t))
}

func (s *SafeStats) LoadEncodeTime() time.Duration {
	if s == nil {
		return 0
	}

	return time.Duration(atomic.LoadInt64((*int64)(&s.EncodeTime)))
}

func (s *SafeStats) AddSamplesProcessed(c uint64) {
	if s == nil {
		return
	}

	atomic.AddUint64(&s.SamplesProcessed, c)
}

func (s *SafeStats) LoadSamplesProcessed() uint64 {
	if s == nil {
		return 0
	}

	return atomic.LoadUint64(&s.SamplesProcessed)
}

func (s *SafeStats) AddSpunOffSubqueries(num uint32) {
	if s == nil {
		return
	}
	atomic.AddUint32(&s.SpunOffSubqueries, num)
}

func (s *SafeStats) LoadSpunOffSubqueries() uint32 {
	if s == nil {
		return 0
	}
	return atomic.LoadUint32(&s.SpunOffSubqueries)
}

func (s *SafeStats) AddSamplesProcessedPerStep(points []StepStat) {
	// merge is concurrent safe
	s.mergeSamplesProcessedPerStep(points)
}

func (s *SafeStats) LoadSamplesProcessedPerStep() []StepStat {
	if s == nil {
		return nil
	}
	s.mx.Lock()
	defer s.mx.Unlock()
	return s.SamplesProcessedPerStep
}

func (s *SafeStats) AddRemoteExecutionRequests(count uint32) {
	if s == nil {
		return
	}
	atomic.AddUint32(&s.RemoteExecutionRequestCount, count)
}

func (s *SafeStats) LoadRemoteExecutionRequestCount() uint32 {
	if s == nil {
		return 0
	}
	return atomic.LoadUint32(&s.RemoteExecutionRequestCount)
}

func (s *SafeStats) AddSplitRangeVectors(count uint32) {
	if s == nil {
		return
	}
	atomic.AddUint32(&s.SplitRangeVectors, count)
}

func (s *SafeStats) LoadSplitRangeVectors() uint32 {
	if s == nil {
		return 0
	}
	return atomic.LoadUint32(&s.SplitRangeVectors)
}

// Merge the provided Stats into this one.
func (s *SafeStats) Merge(other *SafeStats) {
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
	s.AddRemoteExecutionRequests(other.LoadRemoteExecutionRequestCount())
	s.AddSplitRangeVectors(other.LoadSplitRangeVectors())
}

func (s *SafeStats) mergeSamplesProcessedPerStep(other []StepStat) {
	if s == nil {
		return
	}
	// Hold the lock for the entire merge operation to make it atomic
	s.mx.Lock()
	defer s.mx.Unlock()

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
func (s *SafeStats) Copy() *SafeStats {
	if s == nil {
		return nil
	}
	c := &SafeStats{}
	c.Merge(s)
	return c
}

func ShouldTrackHTTPGRPCResponse(r *httpgrpc.HTTPResponse) bool {
	// Do no track statistics for requests failed because of a server error.
	return r.Code < 500
}
