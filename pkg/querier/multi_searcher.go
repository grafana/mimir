// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

// metricMetadataFetcher fetches metric metadata for a set of metric names,
// returning it keyed by metric name.
type metricMetadataFetcher interface {
	fetchMetricMetadata(ctx context.Context, names []string) (map[string]metadata.Metadata, error)
}

// findMetadataFetcher returns the first querier that can fetch metric metadata,
// or nil if none can (e.g. ingesters not queried for this time range).
func findMetadataFetcher(queriers []storage.Querier) metricMetadataFetcher {
	for _, q := range queriers {
		if f, ok := q.(metricMetadataFetcher); ok {
			return f
		}
	}
	return nil
}

// mimirSearcher is the cross-source Searcher interface used at the querier
// fan-out layer. It differs from Prometheus's storage.Searcher
// (vendor/github.com/prometheus/prometheus/storage/interface.go) by taking an
// extra *streaminglabelvalues.Params: each leaf (ingester / store-gateway)
// builds its own concurrency-unsafe storage.Filter from these params, so the
// params travel separately from the opaque hints.Filter.
//
// distributorQuerier and blocksStoreQuerier both implement this shape.
type mimirSearcher interface {
	SearchLabelNames(ctx context.Context, params *streaminglabelvalues.Params, hints *storage.SearchHints, matchers ...*labels.Matcher) storage.SearchResultSet
	SearchLabelValues(ctx context.Context, name string, params *streaminglabelvalues.Params, hints *storage.SearchHints, matchers ...*labels.Matcher) storage.SearchResultSet
}

// SearchLabelNames fans out across the per-source queriers, merging streamed
// results via storage.MergeSearchResultSets. The merge preserves the requested
// ordering, deduplicates across sources, and stops after the per-tenant-clamped
// limit.
//
// Children that don't implement mimirSearcher are surfaced as
// storage.ErrSearchResultSet so the merge still composes cleanly.
func (mq *multiQuerier) SearchLabelNames(ctx context.Context, params *streaminglabelvalues.Params, hints *storage.SearchHints, matchers ...*labels.Matcher) storage.SearchResultSet {
	spanLog, ctx := spanlogger.New(ctx, mq.logger, tracer, "multiQuerier.SearchLabelNames")
	defer spanLog.Finish()

	ctx, queriers, _, _, err := mq.getQueriers(ctx, mq.minT, mq.maxT)
	if errors.Is(err, errEmptyTimeRange) {
		return storage.EmptySearchResultSet()
	}
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}

	hints, clampWarn := clampSearchHintsLimit(spanLog, hints, mq.limits.MaxLabelNamesLimit(userID), validation.MaxLabelNamesLimitFlag)
	sets := fanOutSearch(queriers, clampWarn, func(s mimirSearcher) storage.SearchResultSet {
		return s.SearchLabelNames(ctx, params, hints, matchers...)
	})
	return storage.MergeSearchResultSets(sets, hints)
}

// SearchLabelValues mirrors SearchLabelNames; it forwards the label name to
// the children.
func (mq *multiQuerier) SearchLabelValues(ctx context.Context, name string, params *streaminglabelvalues.Params, hints *storage.SearchHints, matchers ...*labels.Matcher) storage.SearchResultSet {
	spanLog, ctx := spanlogger.New(ctx, mq.logger, tracer, "multiQuerier.SearchLabelValues")
	defer spanLog.Finish()

	ctx, queriers, _, _, err := mq.getQueriers(ctx, mq.minT, mq.maxT)
	if errors.Is(err, errEmptyTimeRange) {
		return storage.EmptySearchResultSet()
	}
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}

	hints, clampWarn := clampSearchHintsLimit(spanLog, hints, mq.limits.MaxLabelValuesLimit(userID), validation.MaxLabelValuesLimitFlag)
	sets := fanOutSearch(queriers, clampWarn, func(s mimirSearcher) storage.SearchResultSet {
		return s.SearchLabelValues(ctx, name, params, hints, matchers...)
	})
	merged := storage.MergeSearchResultSets(sets, hints)

	// Metadata enrichment (include_metadata) only applies to metric names, and
	// must run above the source merge: metric metadata is sharded by metric
	// name independently of series, so it can't be attached reliably at the
	// per-source leaves.
	if params != nil && params.IncludeMetadata && name == model.MetricNameLabel {
		if fetcher := findMetadataFetcher(queriers); fetcher != nil {
			// Each buffer fill triggers one all-ingester metadata fan-out, so
			// floor the buffer: a client-chosen batch_size of 1 must not turn
			// into one fan-out per result. batch_size only controls the wire
			// flush granularity, not how many names we fetch metadata for at a
			// time.
			batchSize := max(params.BatchSize, searchDefaultBatchSize)
			return newMetadataEnrichingSearchResultSet(ctx, merged, fetcher.fetchMetricMetadata, batchSize)
		}
	}

	return merged
}

// clampSearchHintsLimit returns a defensively-copied hints with Limit clamped
// to maxLimit per the tenant's per-source ceiling. When the clamp fires it
// also returns a single-element annotations.Annotations carrying the
// MaxLimitError for the merge layer to surface via Warnings(). Mirrors the
// existing LabelNames/LabelValues clamp warning behaviour.
//
// A nil hints becomes a zero hints — no implicit limit is invented.
func clampSearchHintsLimit(spanLog *spanlogger.SpanLogger, hints *storage.SearchHints, maxLimit int, settingName string) (*storage.SearchHints, annotations.Annotations) {
	if hints == nil {
		hints = &storage.SearchHints{}
	} else {
		hintsCopy := *hints
		hints = &hintsCopy
	}
	originalLimit := hints.Limit
	hints.Limit = clampToMaxLimit(spanLog, originalLimit, maxLimit, settingName)
	if hints.Limit > 0 && originalLimit != hints.Limit {
		var warn annotations.Annotations
		warn.Add(NewMaxLimitError(originalLimit, hints.Limit, settingName))
		return hints, warn
	}
	return hints, nil
}

// fanOutSearch collects a SearchResultSet from each child querier by
// type-asserting to mimirSearcher and invoking the caller-supplied closure.
// Children that fail the assertion yield storage.ErrSearchResultSet so the
// merge composes around the error. If clampWarn is non-empty an extra
// warning-only set is appended; the merge primitive merges its Warnings()
// into the final set.
func fanOutSearch(queriers []storage.Querier, clampWarn annotations.Annotations, call func(mimirSearcher) storage.SearchResultSet) []storage.SearchResultSet {
	sets := make([]storage.SearchResultSet, 0, len(queriers)+1)
	for _, q := range queriers {
		s, ok := q.(mimirSearcher)
		if !ok {
			sets = append(sets, storage.ErrSearchResultSet(fmt.Errorf("querier %T does not implement search", q)))
			continue
		}
		sets = append(sets, call(s))
	}
	if len(clampWarn) > 0 {
		sets = append(sets, storage.NewSearchResultSetFromSlice(nil, clampWarn))
	}
	return sets
}

// metadataEnrichingSearchResultSet wraps a metric-name SearchResultSet and
// inlines metric metadata (Type/Help/Unit) on each result. It buffers one
// response batch worth of results, fetches their metadata in one batched call,
// sets it, then streams the enriched batch out — so metadata is fetched exactly
// one response batch at a time, preserving the streaming contract.
//
// Metadata is best-effort: a fetch error does not fail the search. It is
// surfaced as a warning and the batch is emitted un-enriched.
type metadataEnrichingSearchResultSet struct {
	ctx       context.Context
	inner     storage.SearchResultSet
	fetch     func(ctx context.Context, names []string) (map[string]metadata.Metadata, error)
	batchSize int

	buf       []storage.SearchResult
	pos       int
	innerDone bool
	warnings  annotations.Annotations
}

func newMetadataEnrichingSearchResultSet(ctx context.Context, inner storage.SearchResultSet, fetch func(context.Context, []string) (map[string]metadata.Metadata, error), batchSize int) *metadataEnrichingSearchResultSet {
	return &metadataEnrichingSearchResultSet{ctx: ctx, inner: inner, fetch: fetch, batchSize: batchSize}
}

func (s *metadataEnrichingSearchResultSet) Next() bool {
	if s.pos+1 < len(s.buf) {
		s.pos++
		return true
	}
	if s.innerDone {
		return false
	}
	s.buf = s.buf[:0]
	s.pos = 0
	for len(s.buf) < s.batchSize {
		if !s.inner.Next() {
			s.innerDone = true
			break
		}
		s.buf = append(s.buf, s.inner.At())
	}
	if len(s.buf) == 0 {
		return false
	}
	s.enrich()
	return true
}

func (s *metadataEnrichingSearchResultSet) enrich() {
	names := make([]string, len(s.buf))
	for i := range s.buf {
		names[i] = s.buf[i].Value
	}
	md, err := s.fetch(s.ctx, names)
	if err != nil {
		s.warnings.Add(fmt.Errorf("failed to fetch metric metadata: %w", err))
		return
	}
	for i := range s.buf {
		if m, ok := md[s.buf[i].Value]; ok {
			mm := m
			s.buf[i].Metadata = &mm
		}
	}
}

func (s *metadataEnrichingSearchResultSet) At() storage.SearchResult {
	if s.pos < len(s.buf) {
		return s.buf[s.pos]
	}
	return storage.SearchResult{}
}

func (s *metadataEnrichingSearchResultSet) Warnings() annotations.Annotations {
	var ws annotations.Annotations
	ws.Merge(s.inner.Warnings())
	ws.Merge(s.warnings)
	return ws
}

func (s *metadataEnrichingSearchResultSet) Err() error   { return s.inner.Err() }
func (s *metadataEnrichingSearchResultSet) Close() error { return s.inner.Close() }
