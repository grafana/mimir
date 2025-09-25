// SPDX-License-Identifier: AGPL-3.0-only

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
	delegate          storage.ChunkQuerier
	ingesterMetrics   *ingesterMetrics
	lookupPlanMetrics lookupplan.Metrics
}

// newStatsTrackingChunkQuerier creates a new stats-tracking chunk querier.
func newStatsTrackingChunkQuerier(delegate storage.ChunkQuerier, metrics *ingesterMetrics, planMetrics lookupplan.Metrics) *statsTrackingChunkQuerier {
	return &statsTrackingChunkQuerier{
		delegate:          delegate,
		ingesterMetrics:   metrics,
		lookupPlanMetrics: planMetrics,
	}
}

func (q *statsTrackingChunkQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.delegate.LabelValues(ctx, name, hints, matchers...)
}

func (q *statsTrackingChunkQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.delegate.LabelNames(ctx, hints, matchers...)
}

func (q *statsTrackingChunkQuerier) Close() error {
	return q.delegate.Close()
}

func (q *statsTrackingChunkQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	stats := &lookupplan.QueryStats{}
	ctx = lookupplan.ContextWithQueryStats(ctx, stats)
	seriesSet := q.delegate.Select(ctx, sortSeries, hints, matchers...)

	trackingSet := &statsTrackingChunkSeriesSet{
		ctx:               ctx,
		delegate:          seriesSet,
		queryStats:        stats,
		ingesterMetrics:   q.ingesterMetrics,
		lookupPlanMetrics: q.lookupPlanMetrics,
	}

	return trackingSet
}

// statsTrackingChunkSeriesSet wraps a ChunkSeriesSet to track series count and record stats.
type statsTrackingChunkSeriesSet struct {
	ctx               context.Context
	delegate          storage.ChunkSeriesSet
	queryStats        *lookupplan.QueryStats
	ingesterMetrics   *ingesterMetrics
	lookupPlanMetrics lookupplan.Metrics

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

	s.ingesterMetrics.queriedSeries.WithLabelValues("single_block_index").Observe(float64(actualPostings))
	s.ingesterMetrics.queriedSeries.WithLabelValues("single_block_filter").Observe(float64(actualFinal))

	if actualFinal > 0 {
		instrument.ObserveWithExemplar(s.ctx, s.lookupPlanMetrics.FilteredRatio.WithLabelValues(), float64(actualPostings)/float64(actualFinal))
		instrument.ObserveWithExemplar(s.ctx, s.lookupPlanMetrics.FinalCardinalityRatio.WithLabelValues(), float64(estimatedFinal)/float64(actualFinal))
	}
	if actualPostings > 0 {
		instrument.ObserveWithExemplar(s.ctx, s.lookupPlanMetrics.IntersectionSizeRatio.WithLabelValues(), float64(estimatedPostings)/float64(actualPostings))
	}
}
