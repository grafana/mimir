// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/storage/lazyquery"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

// The production wrappers around multiQuerier must forward the fetch, else
// enrichment silently regresses to skipped.
var (
	_ querierapi.MetricMetadataFetcher = (*memoryTrackingQuerier)(nil)
	_ querierapi.MetricMetadataFetcher = lazyquery.LazyQuerier{}
)

// findMetadataFetcher returns the first querier that can fetch metric metadata,
// or nil if none can (e.g. ingesters not queried for this time range).
func findMetadataFetcher(queriers []storage.Querier) querierapi.MetricMetadataFetcher {
	for _, q := range queriers {
		if f, ok := q.(querierapi.MetricMetadataFetcher); ok {
			return f
		}
	}
	return nil
}

// FetchMetricMetadata delegates to the per-source querier that can supply
// metadata. It returns nil when no such source is available.
func (mq *multiQuerier) FetchMetricMetadata(ctx context.Context, names []string, matcherSets [][]*labels.Matcher) (map[string]metadata.Metadata, error) {
	// Metadata lives in the ingesters, keyed by metric name and independent of
	// the query time range, so fetch it from the distributor querier directly
	// rather than via getQueriers. getQueriers gates the ingester leaf on the
	// search time range (so an older-than-query-ingesters-within search would
	// enrich nothing) and also opens and counts the store-gateway, which holds no
	// metadata.
	if mq.distributor == nil {
		return nil, nil
	}

	// Reuse the distributor querier the in-flight search already opened, if any.
	mq.queriersMtx.Lock()
	fetcher := findMetadataFetcher(mq.queriers)
	mq.queriersMtx.Unlock()

	if fetcher == nil {
		q, err := mq.distributor.Querier(mq.minT, mq.maxT)
		if err != nil {
			return nil, err
		}
		mq.addQueriersToCleanup([]storage.Querier{q})
		var ok bool
		if fetcher, ok = q.(querierapi.MetricMetadataFetcher); !ok {
			return nil, nil
		}
	}

	return fetcher.FetchMetricMetadata(ctx, names, matcherSets)
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
	return storage.MergeSearchResultSets(sets, hints)
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
// Metadata is best-effort: a fetch error does not fail the search, it just
// leaves that batch un-enriched (metadata is optional per result), so no
// warning is surfaced.
type metadataEnrichingSearchResultSet struct {
	ctx       context.Context
	inner     storage.SearchResultSet
	fetch     func(ctx context.Context, names []string) (map[string]metadata.Metadata, error)
	batchSize int
	logger    log.Logger

	buf            []storage.SearchResult
	bufNextReadIdx int
	innerDone      bool
	warnedFetchErr bool
}

func newMetadataEnrichingSearchResultSet(ctx context.Context, inner storage.SearchResultSet, fetch func(context.Context, []string) (map[string]metadata.Metadata, error), batchSize int, logger log.Logger) *metadataEnrichingSearchResultSet {
	return &metadataEnrichingSearchResultSet{ctx: ctx, inner: inner, fetch: fetch, batchSize: batchSize, logger: logger}
}

func (s *metadataEnrichingSearchResultSet) Next() bool {
	// Advance reading from the already-enriched result (if available).
	if s.bufNextReadIdx+1 < len(s.buf) {
		s.bufNextReadIdx++
		return true
	}

	if s.innerDone {
		return false
	}

	// Read the next batch of results, and then enrich it.
	s.buf = s.buf[:0]
	s.bufNextReadIdx = 0
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
		// Best-effort: leave the batch un-enriched on a fetch error. Metadata is
		// optional per result, so we don't fail the search or warn the client,
		// but log it once per request (fetch runs per batch) for observability.
		if !s.warnedFetchErr {
			s.warnedFetchErr = true
			level.Warn(spanlogger.FromContext(s.ctx, s.logger)).Log("msg", "failed to fetch metric metadata for search results enrichment", "err", err)
		}
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
	if s.bufNextReadIdx < len(s.buf) {
		return s.buf[s.bufNextReadIdx]
	}
	return storage.SearchResult{}
}

func (s *metadataEnrichingSearchResultSet) Warnings() annotations.Annotations {
	return s.inner.Warnings()
}

func (s *metadataEnrichingSearchResultSet) Err() error   { return s.inner.Err() }
func (s *metadataEnrichingSearchResultSet) Close() error { return s.inner.Close() }
