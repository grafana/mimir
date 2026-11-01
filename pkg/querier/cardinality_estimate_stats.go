// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"net/http"
	"time"

	"github.com/go-kit/log/level"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/grafana/mimir/pkg/cardinality"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

// observeEstimate records what one estimate request cost: a log line, span attributes and metrics. A failure's line also has what the returned error leaves out. The cost is not in the response since the query-frontend caches it. out is nil when the estimate failed.
func (q *BlocksStoreQueryable) observeEstimate(ctx context.Context, req CardinalityEstimateRequest, res CardinalityEstimateResult, out *estimateResponse, elapsed time.Duration, err error) {
	attrs := []attribute.KeyValue{
		attribute.Int64("window_start_ms", req.MinT),
		attribute.Int64("window_end_ms", req.MaxT),
		attribute.String("group_by", req.GroupBy),
		attribute.Int64("step_ms", req.Step),
	}
	if n, ok := estimateHashesNeeded(res, err); ok {
		attrs = append(attrs, attribute.Int64("hashes_needed", n))
	}
	if out == nil {
		attrs = append(attrs, attribute.String("cost", "unknown"))
	} else {
		attrs = append(attrs, estimateResultAttributes(res)...)
		// A name table that failed while merged counted only part of its groups.
		if err == nil {
			attrs = append(attrs, attribute.Int("group_count", out.GroupCount), attribute.Int64("total", out.Total), attribute.Int("estimated_groups", out.EstimatedGroups))
		}
	}
	trace.SpanFromContext(ctx).SetAttributes(attrs...)

	limit := estimateRefusalLimit(err)
	// The memory tracker counts the memory limit's refusals itself.
	if limit != "" && limit != refusedByMemory && q.metrics != nil {
		q.metrics.estimateRefusals.WithLabelValues(limit).Inc()
	}
	if q.metrics != nil {
		path, source, status := estimateLabel(res.Read), estimateLabel(res.Source), estimateRequestStatus(err)
		q.metrics.estimateRequests.WithLabelValues(path, source, status).Inc()
		q.metrics.estimateDuration.WithLabelValues(path, source, status).Observe(elapsed.Seconds())
		q.metrics.estimateRetries.Add(float64(res.Retries))
		if err == nil {
			for flag, set := range map[string]bool{
				"lower_bound":    res.LowerBound,
				"coverage_gap":   res.CoverageGap,
				"head_fallback":  res.HeadFallback != "",
				"missing_blocks": len(res.MissingBlocks) > 0,
			} {
				if set {
					q.metrics.estimateFlagged.WithLabelValues(path, flag).Inc()
				}
			}
		}
	}
	if err == nil && q.metrics != nil {
		m := q.metrics
		if res.Dedup {
			m.estimateDedupHashes.Observe(float64(res.DedupHashes))
		}
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
	if res.missingReason != nil {
		fields = append(fields, "missing_reason", estimateFailureDetail(res.missingReason))
		if addr := estimateSourceAddr(res.missingReason); addr != "" {
			fields = append(fields, "missing_store_gateway", addr)
		}
	}
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

// estimateHashesNeeded is how many series a request that deduplicates asks its sources to hash, to size the hashes limit. Refused, it is a lower bound.
func estimateHashesNeeded(res CardinalityEstimateResult, err error) (int64, bool) {
	switch {
	case err != nil:
		return cardinality.HashesNeeded(err)
	case res.HeadFallback != "":
		return cardinality.HashesNeeded(res.headFallbackErr)
	case res.Dedup:
		return res.SeriesCounted, true
	}
	return 0, false
}

// estimateRequestStatus is the status label of a request that ended with err.
func estimateRequestStatus(err error) string {
	switch {
	case err == nil:
		return "ok"
	case estimateHTTPStatus(err) == statusClientClosedRequest:
		return "canceled"
	case estimateHTTPStatus(err) < http.StatusInternalServerError:
		return "refused"
	}
	return "failed"
}

// estimateLabel is v, or unknown for a request that failed before reporting it.
func estimateLabel(v string) string {
	if v == "" {
		return "unknown"
	}
	return v
}

// observeRefusedRequest counts a request refused, with err, before an estimate started.
func (q *BlocksStoreQueryable) observeRefusedRequest(err error) {
	if q.metrics != nil {
		q.metrics.estimateRequests.WithLabelValues("none", "none", estimateRequestStatus(err)).Inc()
	}
}
