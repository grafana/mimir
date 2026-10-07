// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/web/api/v1/search.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package querier

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/common/model"
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
// The fetch also includes the metric family name of each suffixed result (x for
// x_bucket), because metadata is stored by family name. Results past the
// handler's limit are not enriched.
//
// Metadata is best-effort: a fetch error does not fail the search, it just
// leaves that batch un-enriched (metadata is optional per result), so no
// warning is surfaced.
type metadataEnrichingSearchResultSet struct {
	ctx       context.Context
	inner     storage.SearchResultSet
	fetch     func(ctx context.Context, names []string) (map[string]metadata.Metadata, error)
	batchSize int
	// limit is the number of results the handler emits, 0 means no limit.
	limit  int
	logger log.Logger

	buf            []storage.SearchResult
	bufNextReadIdx int
	innerDone      bool
	warnedFetchErr bool
	// readFromCurrentBatch is the number of results readFromCurrentBatch from inner before the current batch.
	readFromCurrentBatch int

	// requested is reused across batches to dedupe the names to fetch.
	requested map[string]struct{}
}

func newMetadataEnrichingSearchResultSet(ctx context.Context, inner storage.SearchResultSet, fetch func(context.Context, []string) (map[string]metadata.Metadata, error), batchSize, limit int, logger log.Logger) *metadataEnrichingSearchResultSet {
	return &metadataEnrichingSearchResultSet{ctx: ctx, inner: inner, fetch: fetch, batchSize: batchSize, limit: limit, logger: logger}
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
	// The handler reads one result past the limit only to detect has_more and
	// does not emit it, so do not enrich it. When that result is alone in its
	// batch, this saves a metadata fetch from all ingesters.
	n := len(s.buf)
	if s.limit > 0 {
		n = min(n, max(s.limit-s.readFromCurrentBatch, 0))
	}
	s.readFromCurrentBatch += len(s.buf)
	if n > 0 {
		s.enrich(s.buf[:n])
	}

	return true
}

func (s *metadataEnrichingSearchResultSet) enrich(batch []storage.SearchResult) {
	// Metadata is stored by metric family name, so a suffixed name such as
	// x_bucket also needs its family name x in the fetch. Result values are
	// unique, but family names can repeat or equal another result value.
	//
	// names is not reused across batches: the fetch request holds it, and
	// ingester calls that lost the quorum race can still be sending it after
	// fetch returns. Capacity is increased to accommodate family names being
	// added to this slice.
	names := make([]string, 0, 2*len(batch))
	if s.requested == nil {
		s.requested = make(map[string]struct{}, 2*len(batch))
	}
	defer clear(s.requested)

	for i := range batch {
		val := batch[i].Value
		names = append(names, val)
		s.requested[val] = struct{}{}
	}
	for i := range batch {
		family, _, ok := metricFamilyName(batch[i].Value)
		if !ok {
			continue
		}
		if _, ok := s.requested[family]; ok {
			continue
		}
		names = append(names, family)
		s.requested[family] = struct{}{}
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

	// Store the metadata of the batch in one slice instead of one allocation
	// per result. It is allocated at the first match, with room for every
	// remaining result, so it never grows and the pointers into it stay valid.
	var mds []metadata.Metadata
	for i := range batch {
		m, ok := metadataForMetric(md, batch[i].Value)
		if !ok {
			continue
		}
		if mds == nil {
			mds = make([]metadata.Metadata, 0, len(batch)-i)
		}
		mds = append(mds, m)
		batch[i].Metadata = &mds[len(mds)-1]
	}
}

// metadataForMetric returns the metadata of the metric family a metric name
// belongs to. An exact match on the family name wins. Otherwise the last
// suffix is stripped and the metadata is returned if the family type allows
// that suffix.
func metadataForMetric(md map[string]metadata.Metadata, name string) (metadata.Metadata, bool) {
	if m, ok := md[name]; ok {
		return m, true
	}
	family, suffix, ok := metricFamilyName(name)
	if !ok {
		return metadata.Metadata{}, false
	}
	if m, ok := md[family]; ok && typeAllowsSuffix(m.Type, suffix) {
		return m, true
	}
	return metadata.Metadata{}, false
}

// metricFamilyName splits a metric name into the family name and the suffix,
// when the suffix is one that typeAllowsSuffix can accept.
//
// A family name that already ends with _total or _info is not returned for the
// same suffix: Prometheus allows a counter family named x_total and an info
// family named x_info, whose series have the same name as the family and
// match exactly. So x_total_total is not a series of the x_total family, as in
// isSeriesPartOfFamily in Prometheus scrape/scrape.go.
func metricFamilyName(name string) (family, suffix string, ok bool) {
	i := strings.LastIndexByte(name, '_')
	if i <= 0 {
		return "", "", false
	}
	family, suffix = name[:i], name[i:]
	switch suffix {
	case "_total", "_info":
		if strings.HasSuffix(family, suffix) {
			return "", "", false
		}
		return family, suffix, true
	case "_bucket", "_sum", "_count", "_gsum", "_gcount":
		return family, suffix, true
	default:
		return "", "", false
	}
}

// typeAllowsSuffix reports whether series of a metric family with the given
// type can be named with the given suffix. It follows isSeriesPartOfFamily in
// Prometheus scrape/scrape.go and additionally accepts _sum and _count for
// gauge histograms, which is how the protobuf parser names them. The _created
// suffix is not accepted because that series holds a timestamp, not a value of
// the family type.
func typeAllowsSuffix(typ model.MetricType, suffix string) bool {
	switch suffix {
	case "_total":
		return typ == model.MetricTypeCounter
	case "_bucket":
		return typ == model.MetricTypeHistogram || typ == model.MetricTypeGaugeHistogram
	case "_sum", "_count":
		return typ == model.MetricTypeHistogram || typ == model.MetricTypeGaugeHistogram || typ == model.MetricTypeSummary
	case "_gsum", "_gcount":
		return typ == model.MetricTypeGaugeHistogram
	case "_info":
		return typ == model.MetricTypeInfo
	default:
		return false
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
