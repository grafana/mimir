// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"net/http"
	"time"

	"github.com/go-kit/log/level"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	util_log "github.com/grafana/mimir/pkg/util/log"
)

// observeEstimate records what one estimate request cost: one log line, the
// same numbers as attributes of the request's span, and the estimate
// metrics. A failure's line also has what the error returned to the caller
// leaves out: the store-gateway's address and the full error text. The
// line is a warning for a failure that is not the caller's or a limit's.
// The cost is for operators and measurements, not the caller, so it is not
// in the response, which the query-frontend caches: a cache hit would
// return the cost of the request that filled the cache. out is nil
// when the estimate failed; res is then empty, so the line and the span say
// cost=unknown instead of zeros, and the metrics count only answers.
func (q *BlocksStoreQueryable) observeEstimate(ctx context.Context, req CardinalityEstimateRequest, res CardinalityEstimateResult, out *estimateResponse, elapsed time.Duration, err error) {
	attrs := []attribute.KeyValue{
		attribute.Int64("window_start_ms", req.MinT),
		attribute.Int64("window_end_ms", req.MaxT),
		attribute.String("group_by", req.GroupBy),
		attribute.Int64("step_ms", req.Step),
	}
	if out == nil {
		attrs = append(attrs, attribute.String("cost", "unknown"))
	} else {
		attrs = append(attrs, estimateResultAttributes(res)...)
		// A name table that failed while it was merged counted only part
		// of its groups.
		if err == nil {
			attrs = append(attrs, attribute.Int("group_count", out.GroupCount), attribute.Int64("total", out.Total), attribute.Int("estimated_groups", out.EstimatedGroups))
		}
	}
	trace.SpanFromContext(ctx).SetAttributes(attrs...)

	limit := estimateRefusalLimit(err)
	// The memory tracker counts the memory limit's refusals as it makes
	// them, which a caller of Estimate other than the handler sees too.
	if limit != "" && limit != refusedByMemory && q.metrics != nil {
		q.metrics.estimateRefusals.WithLabelValues(limit).Inc()
	}
	if err == nil && q.metrics != nil {
		m := q.metrics
		m.estimateBlocks.WithLabelValues(res.Read).Add(float64(len(res.Blocks)))
		m.estimateSeriesCounted.WithLabelValues(res.Read).Add(float64(res.SeriesCounted))
		for kind, b := range map[string]int64{
			"index":                  res.IndexBytes,
			"postings_offsets_table": res.PostingsOffsetsTableBytes,
			"postings_fetched":       res.PostingsFetchedBytes,
			"series_fetched":         res.SeriesFetchedBytes,
			"hashes":                 res.HashBytes,
			"sketches":               res.SketchBytes,
		} {
			m.estimateBytes.WithLabelValues(res.Read, kind).Add(float64(b))
		}
	}

	if q.logger == nil {
		return
	}
	fields := make([]any, 0, 2*len(attrs)+10)
	fields = append(fields, "msg", "cardinality estimate", "elapsed", elapsed)
	for _, a := range attrs {
		fields = append(fields, string(a.Key), a.Value.AsInterface())
	}
	logger := level.Info(util_log.WithContext(ctx, q.logger))
	if err != nil {
		status := estimateHTTPStatus(err)
		fields = append(fields, "status", status, "err", estimateFailureDetail(err))
		if limit != "" {
			fields = append(fields, "refused_by", limit)
		}
		if addr := estimateSourceAddr(err); addr != "" {
			fields = append(fields, "store_gateway", addr)
		}
		if status == http.StatusInternalServerError {
			logger = level.Warn(util_log.WithContext(ctx, q.logger))
		}
	}
	logger.Log(fields...)
}

// estimateResultAttributes are what an answered estimate found and cost.
func estimateResultAttributes(res CardinalityEstimateResult) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("read", res.Read),
		attribute.String("source", res.Source),
		attribute.Bool("lower_bound", res.LowerBound),
		attribute.Bool("coverage_gap", res.CoverageGap),
		attribute.Bool("head_whole", res.HeadWhole),
		attribute.String("head_fallback", res.HeadFallback),
		attribute.Bool("dedup", res.Dedup),
		attribute.Int("blocks", len(res.Blocks)),
		attribute.Int("missing_blocks", len(res.MissingBlocks)),
		attribute.Int("missing_shards", len(res.MissingShards)),
		attribute.Int("store_gateways", res.StoreGateways),
		attribute.Int("retries", res.Retries),
		attribute.Int64("series_counted", res.SeriesCounted),
		attribute.Int64("index_bytes", res.IndexBytes),
		attribute.Int64("postings_offsets_table_bytes", res.PostingsOffsetsTableBytes),
		attribute.Int64("postings_fetched_bytes", res.PostingsFetchedBytes),
		attribute.Int64("series_fetched_bytes", res.SeriesFetchedBytes),
		attribute.Int64("blocks_from_postings", res.BlocksFromPostings),
		attribute.Int64("hash_bytes", res.HashBytes),
		attribute.Int64("sketch_bytes", res.SketchBytes),
	}
}
