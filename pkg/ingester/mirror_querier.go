// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/ingester/lookupplan"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type mirroredChunkQuerier struct {
	logger             log.Logger
	comparisonOutcomes *prometheus.CounterVec
	delegate           storage.ChunkQuerier
	blockMeta          tsdb.BlockMeta

	recordedRequest struct {
		minT, maxT int64
		finishedAt time.Time

		ctx        context.Context
		matchers   []*labels.Matcher
		hints      *storage.SelectHints
		sortSeries bool
	}
	returnedSeries *retainingChunkSeriesSet
}

func newMirroredChunkQuerierWithMeta(userID string, comparisonOutcomes *prometheus.CounterVec, minT, maxT int64, blockMeta tsdb.BlockMeta, logger log.Logger, q storage.ChunkQuerier) *mirroredChunkQuerier {
	mq := &mirroredChunkQuerier{
		logger:             log.With(logger, "component", "mirroredChunkQuerier"),
		comparisonOutcomes: comparisonOutcomes.MustCurryWith(prometheus.Labels{"user": userID}),
		delegate:           q,
		blockMeta:          blockMeta,
	}
	mq.recordedRequest.minT = minT
	mq.recordedRequest.maxT = maxT
	return mq
}

func (q *mirroredChunkQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	level.Warn(q.logger).Log("msg", "LabelValues is not implemented in mirroredChunkQuerier, not comparing results")
	return q.delegate.LabelValues(ctx, name, hints, matchers...)
}

func (q *mirroredChunkQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	level.Warn(q.logger).Log("msg", "LabelNames is not implemented in mirroredChunkQuerier, not comparing results")
	return q.delegate.LabelNames(ctx, hints, matchers...)
}

func (q *mirroredChunkQuerier) Close() error {
	if q.returnedSeries.isUnset() {
		level.Debug(q.logger).Log("msg", "Select wasn't invoked, skipping comparison")
		q.comparisonOutcomes.WithLabelValues("no_select").Inc()
		return q.delegate.Close()
	} else if returnedErr := q.returnedSeries.Err(); errors.Is(returnedErr, context.Canceled) {
		level.Debug(q.logger).Log("msg", "Select was canceled, skipping comparison")
		q.comparisonOutcomes.WithLabelValues("context_cancelled").Inc()
		return q.delegate.Close()
	} else if returnedErr != nil {
		level.Error(q.logger).Log("msg", "error reading returned series", "err", returnedErr)
		q.comparisonOutcomes.WithLabelValues("returned_series_error").Inc()
		return q.delegate.Close()
	}
	ctxWithoutPlanning := lookupplan.ContextWithDisabledPlanning(q.recordedRequest.ctx)
	ssWithoutPlanning := q.delegate.Select(ctxWithoutPlanning, q.recordedRequest.sortSeries, q.recordedRequest.hints, q.recordedRequest.matchers...)
	q.compareResults(ssWithoutPlanning)

	return q.delegate.Close()
}

func (q *mirroredChunkQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	q.recordedRequest.ctx = ctx
	q.recordedRequest.sortSeries = sortSeries
	q.recordedRequest.hints = copyParams(hints)
	q.recordedRequest.matchers = slices.Clone(matchers)

	ss := q.delegate.Select(ctx, sortSeries, hints, matchers...)
	q.recordedRequest.finishedAt = time.Now()
	q.returnedSeries = &retainingChunkSeriesSet{delegate: ss}
	return q.returnedSeries
}

// compareResults checks the series in returnedSeries against the secondary storage.ChunkSeriesSet.
// It treats secondary as the "correct" one and prints any extra or missing series.
// It ignores series whose first chunk is beyond maxT.
func (q *mirroredChunkQuerier) compareResults(secondary storage.ChunkSeriesSet) {
	var (
		primaryLabels        = q.returnedSeries.labels
		secondaryLabels      labels.Labels
		secondaryHasCurrent  bool
		secondarySeriesCount int

		// It's possible that a series appears between us finishing the primary query
		// and running the secondary query. To avoid false positives, we ignore
		// any series whose first chunk contains samples from after we finished the first query.
		// This doesn't protect against out-of-order samples, but it's a reasonable compromise.
		ignoreSeriesWithFirstChunkYoungerThan = q.recordedRequest.finishedAt.UnixMilli()
	)

	advanceSecondary := func() bool {
		for secondary.Next() {
			series := secondary.At()
			seriesLabels := series.Labels()

			// Check if this series was probably created after we started the primary query.
			chunkIter := series.Iterator(nil)
			if chunkIter.Next() {
				// We only check the first chunk since that should contain the earliest sample.
				chunk := chunkIter.At()
				if chunk.MinTime > ignoreSeriesWithFirstChunkYoungerThan {
					continue
				}
			}

			secondaryLabels = seriesLabels
			secondarySeriesCount++
			return true
		}
		return false
	}

	// Initialize secondary iterator
	secondaryHasCurrent = advanceSecondary()

	i := 0
	var extraSeries, missingSeries []string
	for i < len(primaryLabels) && secondaryHasCurrent {
		cmp := labels.Compare(primaryLabels[i], secondaryLabels)
		if cmp == 0 {
			// Series match, advance both pointers
			i++
			secondaryHasCurrent = advanceSecondary()
		} else if cmp < 0 {
			// Primary series is "smaller", so it's extra
			extraSeries = append(extraSeries, primaryLabels[i].String())
			i++
		} else {
			// Secondary series is "smaller", so it's missing from primary
			missingSeries = append(missingSeries, secondaryLabels.String())
			secondaryHasCurrent = advanceSecondary()
		}
	}

	// Handle remaining primary series
	for i < len(primaryLabels) {
		extraSeries = append(extraSeries, primaryLabels[i].String())
		i++
	}

	// Handle remaining secondary series
	for secondaryHasCurrent {
		missingSeries = append(missingSeries, secondaryLabels.String())
		secondaryHasCurrent = advanceSecondary()
	}

	if err := secondary.Err(); err != nil {
		if errors.Is(err, context.Canceled) {
			q.comparisonOutcomes.WithLabelValues("context_cancelled").Inc()
			return
		}
		level.Error(q.logger).Log("msg", "error reading secondary series", "err", err)
		q.comparisonOutcomes.WithLabelValues("secondary_error").Inc()
		return
	}

	// Create a fake secondaryLabels slice for the logging function
	q.recordComparisonOutcome(extraSeries, missingSeries, primaryLabels, secondarySeriesCount)
}

func (q *mirroredChunkQuerier) recordComparisonOutcome(extraSeries []string, missingSeries []string, primaryLabels []labels.Labels, secondaryLabels int) {
	switch {
	case len(extraSeries) > 0 && len(missingSeries) > 0:
		q.comparisonOutcomes.WithLabelValues("extra_and_missing_series").Inc()
	case len(extraSeries) > 0:
		q.comparisonOutcomes.WithLabelValues("extra_series").Inc()
	case len(missingSeries) > 0:
		q.comparisonOutcomes.WithLabelValues("missing_series").Inc()
	default:
		q.comparisonOutcomes.WithLabelValues("success").Inc()
	}
	if len(extraSeries) > 0 || len(missingSeries) > 0 {
		tenantID, _ := tenant.TenantID(q.recordedRequest.ctx)
		traceID, sampled := tracing.ExtractSampledTraceID(q.recordedRequest.ctx)
		logger := spanlogger.FromContext(q.recordedRequest.ctx, q.logger)
		level.Warn(logger).Log(
			"msg", "series comparison found differences",
			"user", tenantID,
			"trace_id", traceID,
			"trace_sampled", sampled,
			"request_min_time", q.recordedRequest.minT,
			"request_max_time", q.recordedRequest.maxT,
			"request_sort_series", q.recordedRequest.sortSeries,
			"request_matchers", util.MatchersStringer(q.recordedRequest.matchers).String(),

			"block_ulid", q.blockMeta.ULID.String(),
			"block_min_time", q.blockMeta.MinTime,
			"block_max_time", q.blockMeta.MaxTime,

			"extra_series_count", len(extraSeries),
			"missing_series_count", len(missingSeries),
			"primary_series_count", len(primaryLabels),
			"secondary_series_count", secondaryLabels,
			"extra_series", strings.Join(extraSeries, ";"),
			"missing_series", strings.Join(missingSeries, ";"),
		)
	}
}

func copyParams(params *storage.SelectHints) *storage.SelectHints {
	if params == nil {
		return nil
	}
	copiedParams := *params
	copiedParams.Grouping = slices.Clone(params.Grouping)

	return &copiedParams
}

type retainingChunkSeriesSet struct {
	labels []labels.Labels

	delegate storage.ChunkSeriesSet
	// labelsAppended tracks if current series labels were already appended.
	// This helps not call delegate.At() multiple times for the same series.
	// delegate.At() may allocate extra memory each time it is invoked.
	labelsAppended bool
}

func (r *retainingChunkSeriesSet) Next() bool {
	if !r.delegate.Next() {
		return false
	}

	r.labelsAppended = false
	return true
}

func (r *retainingChunkSeriesSet) At() storage.ChunkSeries {
	series := r.delegate.At()

	// Retain the labels of the current series if not already done
	if !r.labelsAppended {
		r.labels = append(r.labels, series.Labels())
		r.labelsAppended = true
	}

	return series
}

func (r *retainingChunkSeriesSet) Err() error {
	return r.delegate.Err()
}

func (r *retainingChunkSeriesSet) Warnings() annotations.Annotations {
	return r.delegate.Warnings()
}

func (r *retainingChunkSeriesSet) isUnset() bool {
	return r == nil
}
