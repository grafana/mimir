package ingester

import (
	"context"

	"github.com/grafana/dskit/instrument"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/ingester/lookupplan"
)

// statsTrackingChunkQuerier wraps a ChunkQuerier to track query statistics.
type statsTrackingChunkQuerier struct {
	delegate   storage.ChunkQuerier
	queryStats *lookupplan.QueryStats
	metrics    *ingesterMetrics
}

// newStatsTrackingChunkQuerier creates a new stats-tracking chunk querier.
func newStatsTrackingChunkQuerier(delegate storage.ChunkQuerier, queryStats *lookupplan.QueryStats, metrics *ingesterMetrics) *statsTrackingChunkQuerier {
	return &statsTrackingChunkQuerier{
		delegate:   delegate,
		queryStats: queryStats,
		metrics:    metrics,
	}
}

func (q *statsTrackingChunkQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	ctx = lookupplan.ContextWithQueryStats(ctx, q.queryStats)
	return q.delegate.LabelValues(ctx, name, hints, matchers...)
}

func (q *statsTrackingChunkQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	ctx = lookupplan.ContextWithQueryStats(ctx, q.queryStats)
	return q.delegate.LabelNames(ctx, hints, matchers...)
}

func (q *statsTrackingChunkQuerier) Close() error {
	return q.delegate.Close()
}

func (q *statsTrackingChunkQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	ctx = lookupplan.ContextWithQueryStats(ctx, q.queryStats)
	seriesSet := q.delegate.Select(ctx, sortSeries, hints, matchers...)

	trackingSet := &statsTrackingChunkSeriesSet{
		ctx:        ctx,
		delegate:   seriesSet,
		queryStats: q.queryStats,
		metrics:    q.metrics,
	}

	return trackingSet
}

// statsTrackingChunkSeriesSet wraps a ChunkSeriesSet to track series count and record stats.
type statsTrackingChunkSeriesSet struct {
	ctx         context.Context
	delegate    storage.ChunkSeriesSet
	queryStats  *lookupplan.QueryStats
	metrics     *ingesterMetrics
	seriesCount uint64
	exhausted   bool
}

func (s *statsTrackingChunkSeriesSet) Next() bool {
	hasNext := s.delegate.Next()

	if hasNext {
		s.seriesCount++
	} else if !s.exhausted {
		s.exhausted = true
		s.queryStats.SetActualFinalCardinality(s.seriesCount)
		s.recordStatsToMetrics()
		s.metrics.queriedSeries.WithLabelValues("block_filter").Observe(float64(s.seriesCount))
	}
	return hasNext
}

func (s *statsTrackingChunkSeriesSet) At() storage.ChunkSeries {
	return s.delegate.At()
}

func (s *statsTrackingChunkSeriesSet) Err() error {
	return s.delegate.Err()
}

func (s *statsTrackingChunkSeriesSet) Warnings() annotations.Annotations {
	return s.delegate.Warnings()
}

// recordStatsToMetrics records the collected stats to the ingester metrics.
func (s *statsTrackingChunkSeriesSet) recordStatsToMetrics() {
	actualPostings := s.queryStats.LoadActualSelectedPostings()
	actualFinal := s.queryStats.LoadActualFinalCardinality()
	estimatedPostings := s.queryStats.LoadEstimatedSelectedPostings()
	estimatedFinal := s.queryStats.LoadEstimatedFinalCardinality()

	s.metrics.queriedSeries.WithLabelValues("block_index").Observe(float64(actualPostings))
	if actualFinal > 0 {
		instrument.ObserveWithExemplar(s.ctx, s.metrics.actualPostingsToFinalCardinalityRatio, float64(actualPostings)/float64(actualFinal))
	}
	if actualPostings > 0 {
		instrument.ObserveWithExemplar(s.ctx, s.metrics.estimatedToActualPostingsCardinalityRatio, float64(estimatedPostings)/float64(actualPostings))
	}
	if actualFinal > 0 {
		instrument.ObserveWithExemplar(s.ctx, s.metrics.estimatedToActualFinalCardinalityRatio, float64(estimatedFinal)/float64(actualFinal))
	}
}
