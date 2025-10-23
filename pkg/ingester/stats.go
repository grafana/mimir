// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"

	"github.com/grafana/dskit/instrument"
	"github.com/grafana/dskit/tracing"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/grafana/mimir/pkg/ingester/lookupplan"
)

// statsTrackingChunkQuerier wraps a ChunkQuerier to track query statistics.
type statsTrackingChunkQuerier struct {
	blockID           ulid.ULID
	delegate          storage.ChunkQuerier
	ingesterMetrics   *ingesterMetrics
	lookupPlanMetrics lookupplan.Metrics
}

// newStatsTrackingChunkQuerier creates a new stats-tracking chunk querier.
func newStatsTrackingChunkQuerier(blockID ulid.ULID, delegate storage.ChunkQuerier, metrics *ingesterMetrics, planMetrics lookupplan.Metrics) *statsTrackingChunkQuerier {
	return &statsTrackingChunkQuerier{
		delegate:          delegate,
		blockID:           blockID,
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
		blockID:           q.blockID,
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
	blockID           ulid.ULID
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

	var (
		filteredRatio         = -1.0
		finalCardinalityRatio = -1.0
		intersectionSizeRatio = -1.0
	)
	if actualFinal > 0 {
		filteredRatio = float64(actualPostings) / float64(actualFinal)
		instrument.ObserveWithExemplar(s.ctx, s.lookupPlanMetrics.FilteredRatio.WithLabelValues(), filteredRatio)

		finalCardinalityRatio = float64(estimatedFinal) / float64(actualFinal)
		instrument.ObserveWithExemplar(s.ctx, s.lookupPlanMetrics.FinalCardinalityRatio.WithLabelValues(), finalCardinalityRatio)
	}
	if actualPostings > 0 {
		intersectionSizeRatio = float64(estimatedPostings) / float64(actualPostings)
		instrument.ObserveWithExemplar(s.ctx, s.lookupPlanMetrics.IntersectionSizeRatio.WithLabelValues(), intersectionSizeRatio)
	}

	if span, _, traceSampled := tracing.SpanFromContext(s.ctx); traceSampled {
		span.AddEvent("block query stats",
			trace.WithAttributes(
				attribute.Stringer("block_id", s.blockID),
				attribute.Int64("estimated_selected_postings", int64(estimatedPostings)),
				attribute.Int64("actual_selected_postings", int64(actualPostings)),
				attribute.Int64("estimated_final_cardinality", int64(estimatedFinal)),
				attribute.Int64("actual_final_cardinality", int64(actualFinal)),
				attribute.Float64("filtered_ratio", filteredRatio),
				attribute.Float64("intersection_size_ratio", intersectionSizeRatio),
				attribute.Float64("final_cardinality_ratio", finalCardinalityRatio),
			),
		)
	}
}
