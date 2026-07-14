// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/e8e25eb09e41bf295e0c9e847cd27cf9016a553a/web/api/v1/search.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package querier

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/querier/worker"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/promqlext"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	// searchAPIContentType is the wire content type for NDJSON streaming
	// responses (one JSON object per line). Matches Prometheus PR #18573.
	searchAPIContentType = "application/x-ndjson; charset=utf-8"

	// Default values per Prometheus PR #18573 search API.
	searchDefaultLimit     = 100
	searchDefaultBatchSize = 100

	// maxSearchBatchSize caps user-supplied batch_size to keep the
	// pre-allocation of one batch worth of records bounded. Sized to match
	// the upstream --web.search.max-limit default (10000) so a batch can't
	// usefully exceed the largest reasonable in-flight result count.
	maxSearchBatchSize = 10000

	// maxSearchTermsPerRequest caps the number of search[] query parameters
	// a single request may carry; matches the 32-term upstream cap.
	maxSearchTermsPerRequest = 32
)

// defaultSuccessTrailer is the byte-for-byte JSON output for the common
// "no warnings, has_more=false" trailer, written verbatim to skip the
// json.Encoder reflection round-trip for the most frequent request shape.
var defaultSuccessTrailer = []byte(`{"status":"success","has_more":false}` + "\n")

// Per-(endpoint × score) pools for the per-request batch envelope. The
// pool stores the envelope wrapper (not just the slice) so the
// per-request wrapper alloc is also amortised, and so the encoded
// `*envelope` interface boxing is reused across flushes within a
// request. Each pool's slice is sized at construction to the default
// batch size; requests with a non-default batchSize get a fresh
// allocation (the pool would otherwise return undersized slices that
// the append loop would grow, defeating the point of pooling and
// re-poisoning the pool with ever-larger backing arrays).
var (
	searchLabelNamePool = sync.Pool{
		New: func() any {
			return &searchBatchEnvelope[searchLabelNameRecord]{Results: make([]searchLabelNameRecord, 0, searchDefaultBatchSize)}
		},
	}
	searchLabelNameWithScorePool = sync.Pool{
		New: func() any {
			return &searchBatchEnvelope[searchLabelNameRecordWithScore]{Results: make([]searchLabelNameRecordWithScore, 0, searchDefaultBatchSize)}
		},
	}
	searchLabelValuePool = sync.Pool{
		New: func() any {
			return &searchBatchEnvelope[searchLabelValueRecord]{Results: make([]searchLabelValueRecord, 0, searchDefaultBatchSize)}
		},
	}
	searchLabelValueWithScorePool = sync.Pool{
		New: func() any {
			return &searchBatchEnvelope[searchLabelValueRecordWithScore]{Results: make([]searchLabelValueRecordWithScore, 0, searchDefaultBatchSize)}
		},
	}
	searchMetricNamePool = sync.Pool{
		New: func() any {
			return &searchBatchEnvelope[searchMetricNameRecord]{Results: make([]searchMetricNameRecord, 0, searchDefaultBatchSize)}
		},
	}
	searchMetricNameWithScorePool = sync.Pool{
		New: func() any {
			return &searchBatchEnvelope[searchMetricNameRecordWithScore]{Results: make([]searchMetricNameRecordWithScore, 0, searchDefaultBatchSize)}
		},
	}
)

// Per-endpoint result records. The label-values endpoint uses "value" as
// its JSON key; label-names and metric-names use "name". Matches the
// upstream Prometheus result shapes (searchLabelNameResult,
// searchLabelValueResult, searchMetricNameResult).
//
// Each endpoint has two record variants: a no-score variant used when
// include_score=false (Score field absent from the wire), and a
// *WithScore variant used when include_score=true (Score is a non-pointer
// float64, unconditionally emitted). Two struct types let the encoder
// serialise the score inline without paying the per-record `*float64`
// heap allocation that `omitempty` on a pointer would require.

type searchLabelNameRecord struct {
	Name string `json:"name"`
}

type searchLabelNameRecordWithScore struct {
	Name  string  `json:"name"`
	Score float64 `json:"score"`
}

type searchLabelValueRecord struct {
	Value string `json:"value"`
}

type searchLabelValueRecordWithScore struct {
	Value string  `json:"value"`
	Score float64 `json:"score"`
}

// searchMetricNameRecord carries optional Type/Help/Unit fields for the
// metric-names endpoint.
type searchMetricNameRecord struct {
	Name string `json:"name"`
	Type string `json:"type,omitempty"`
	Help string `json:"help,omitempty"`
	Unit string `json:"unit,omitempty"`
}

type searchMetricNameRecordWithScore struct {
	Name  string  `json:"name"`
	Score float64 `json:"score"`
	Type  string  `json:"type,omitempty"`
	Help  string  `json:"help,omitempty"`
	Unit  string  `json:"unit,omitempty"`
}

// searchBatchEnvelope is the per-line JSON object for streaming result
// batches. Warnings (when any) ride on the trailer, not on the batches.
type searchBatchEnvelope[T any] struct {
	Results []T `json:"results"`
}

// searchTrailerEnvelope is the final NDJSON line on a successful stream.
type searchTrailerEnvelope struct {
	Status   string   `json:"status"`
	HasMore  bool     `json:"has_more"`
	Warnings []string `json:"warnings,omitempty"`
}

// searchErrorEnvelope is the final NDJSON line when iteration fails after at
// least one batch has been flushed. Mirrors Prometheus's error trailer shape.
type searchErrorEnvelope struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
}

// searchRequest holds the parsed, validated query parameters for one search
// RPC. Use parseSearchRequest to construct one from an *http.Request.
type searchRequest struct {
	params *streaminglabelvalues.Params
	// matchers is one selector per match[] URL entry. Multiple entries are
	// unioned by the dispatcher (OR across selectors) — matching the upstream
	// /api/v1/labels semantics and Prometheus PR #18573.
	matchers [][]*labels.Matcher
	hints    *storage.SearchHints
	// limit is the user-facing result cap from the query param.
	// hints.Limit is set to limit+1 so the iterator returns one extra record
	// which allows for an easy determination of has_more.
	// limit==0 means "no limit" (Prometheus convention) and hints.Limit
	// is left at 0 to pass that through.
	limit        int
	startMs      int64
	endMs        int64
	batchSize    int
	includeScore bool
	// includeMetadata specifies whether the response should include per-metric
	// metadata. Always parsed, but only the metric-names handler acts on it.
	includeMetadata bool
	// labelName is only set for the label-values endpoint; required there.
	labelName string
}

// parseSearchRequest reads the HTTP request and builds a searchRequest.
// requireLabelName is true for the label-values endpoint where the `label`
// parameter is mandatory. Returns a wrapped error suitable for surfacing as
// HTTP 400.
func parseSearchRequest(r *http.Request, requireLabelName bool) (*searchRequest, error) {
	if err := r.ParseForm(); err != nil {
		return nil, fmt.Errorf("parse form: %w", err)
	}

	q := r.Form

	// Search terms (search[]). Capped at maxSearchTermsPerRequest to bound
	// per-request filter construction cost; matches Prometheus PR #18573.
	terms := q["search[]"]
	if len(terms) > maxSearchTermsPerRequest {
		return nil, fmt.Errorf("too many search[] terms: got %d, maximum is %d", len(terms), maxSearchTermsPerRequest)
	}

	// Case sensitivity defaults to true per Prometheus URL polarity.
	caseSensitive, err := parseBoolParam(q, "case_sensitive", true)
	if err != nil {
		return nil, err
	}

	// Fuzz algorithm (default subsequence).
	alg := streaminglabelvalues.FuzzAlgSubsequence
	switch q.Get("fuzz_alg") {
	case "", "subsequence":
		// keep default
	case "jarowinkler":
		alg = streaminglabelvalues.FuzzAlgJaroWinkler
	default:
		return nil, fmt.Errorf("invalid fuzz_alg %q (allowed: subsequence, jarowinkler)", q.Get("fuzz_alg"))
	}

	// Fuzz threshold (int 0-100, default 0).
	threshold := 0
	if v := q.Get("fuzz_threshold"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid fuzz_threshold: %w", err)
		}
		if parsed < 0 || parsed > 100 {
			return nil, fmt.Errorf("invalid fuzz_threshold: got %v but must be between 0 and 100", parsed)
		}
		threshold = parsed
	}

	// Ordering: sort_by + sort_dir
	sortBy := q.Get("sort_by")
	if sortBy == "" {
		sortBy = "alpha"
	}
	// sort_by=score sorts by relevance score, which is only meaningful when
	// at least one search[] term has been supplied to produce the scores.
	// Matches Prometheus PR #18573.
	if sortBy == "score" && len(terms) == 0 {
		return nil, errors.New("sort_by=score requires search[] to be set")
	}
	sortDir := q.Get("sort_dir")
	if sortDir == "" {
		sortDir = "asc"
	}
	order, err := parseSortOrder(sortBy, sortDir, q.Get("sort_dir") != "")
	if err != nil {
		return nil, err
	}

	limit := searchDefaultLimit
	if v := q.Get("limit"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid limit: %w", err)
		}
		if parsed < 0 {
			return nil, fmt.Errorf("invalid limit: must be >= 0")
		}
		limit = parsed
	}

	batchSize := searchDefaultBatchSize
	if v := q.Get("batch_size"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil || parsed < 0 {
			return nil, fmt.Errorf("invalid batch_size %q: must be non-negative integer", v)
		}
		if parsed > maxSearchBatchSize {
			return nil, fmt.Errorf("batch_size %d exceeds maximum %d", parsed, maxSearchBatchSize)
		}
		if parsed > 0 {
			batchSize = parsed
		}
	}

	includeScore, err := parseBoolParam(q, "include_score", false)
	if err != nil {
		return nil, err
	}

	// Time range. Defaults match Prometheus PR #18573: start defaults to one
	// hour before now, end defaults to now. Keeps the default window narrow
	// enough that searches over an unspecified range stay cheap.
	now := model.Now()
	startMs := int64(now.Add(-time.Hour))
	if v := q.Get("start"); v != "" {
		startMs, err = util.ParseTime(v)
		if err != nil {
			return nil, fmt.Errorf("invalid start: %w", err)
		}
	}
	endMs := int64(now)
	if v := q.Get("end"); v != "" {
		endMs, err = util.ParseTime(v)
		if err != nil {
			return nil, fmt.Errorf("invalid end: %w", err)
		}
	}
	// end == start is permitted (zero-duration snapshot); only strictly
	// inverted ranges are rejected, mirroring Prometheus PR #18573.
	if endMs < startMs {
		return nil, errors.New("end timestamp must not be before start timestamp")
	}

	// Matchers (match[]). Each entry is a PromQL series selector.
	matchers, err := parseSearchMatchers(q["match[]"])
	if err != nil {
		return nil, err
	}

	params, err := streaminglabelvalues.NewParams(terms, caseSensitive, alg, threshold)
	if err != nil {
		return nil, fmt.Errorf("invalid search params: %w", err)
	}

	// URL param is "label"; required by the label-values endpoint.
	labelName := q.Get("label")
	if requireLabelName && labelName == "" {
		return nil, errors.New(`missing required parameter "label"`)
	}

	// include_metadata is always parsed (so a malformed value is a 400 on every
	// endpoint); only the metric-names handler acts on it.
	includeMetadata, err := parseBoolParam(q, "include_metadata", false)
	if err != nil {
		return nil, err
	}

	// hintsLimit asks downstream for one extra result so the handler can
	// determine if there is more data available past the given limit.
	// 0 = no limit.
	hintsLimit := limit
	if limit > 0 && limit < math.MaxInt {
		hintsLimit = limit + 1
	}

	return &searchRequest{
		params:          params,
		matchers:        matchers,
		hints:           &storage.SearchHints{OrderBy: order, Limit: hintsLimit},
		limit:           limit,
		startMs:         startMs,
		endMs:           endMs,
		batchSize:       batchSize,
		includeScore:    includeScore,
		includeMetadata: includeMetadata,
		labelName:       labelName,
	}, nil
}

// parseBoolParam reads key from q and parses it with strconv.ParseBool. If
// the value is absent, def is returned. Parse errors are wrapped so the
// caller can surface them as HTTP 400.
func parseBoolParam(q url.Values, key string, def bool) (bool, error) {
	v := q.Get(key)
	if v == "" {
		return def, nil
	}
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("invalid %s: %w", key, err)
	}
	return parsed, nil
}

func parseSortOrder(sortBy, sortDir string, sortDirExplicit bool) (storage.Ordering, error) {
	switch sortBy {
	case "alpha":
		switch sortDir {
		case "asc":
			return storage.OrderByValueAsc, nil
		case "dsc", "desc":
			return storage.OrderByValueDesc, nil
		default:
			return 0, fmt.Errorf("invalid sort_dir %q (allowed: asc, dsc)", sortDir)
		}
	case "score":
		if sortDirExplicit && sortDir != "" {
			return 0, errors.New("sort_dir cannot be combined with sort_by=score")
		}
		return storage.OrderByScoreDesc, nil
	default:
		return 0, fmt.Errorf("invalid sort_by %q (allowed: alpha, score)", sortBy)
	}
}

// parseSearchMatchers parses each entry of match[] as a PromQL series
// selector. Returns one matcher slice per input selector — repeated match[]
// entries stay separate so the dispatcher can union them (OR), not AND them.
// An empty input yields a nil slice.
func parseSearchMatchers(raw []string) ([][]*labels.Matcher, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	p := promqlext.NewPromQLParser()
	out := make([][]*labels.Matcher, 0, len(raw))
	for i, sel := range raw {
		ms, err := p.ParseMetricSelector(sel)
		if err != nil {
			return nil, fmt.Errorf("invalid match[%d]=%q: %w", i, sel, err)
		}
		out = append(out, ms)
	}
	return out, nil
}

// dispatchSearchOverMatcherSets runs the per-selector fan-out for repeated
// match[] entries. 0 sets → one call with no matchers; 1 set → one call with
// that set; N sets → N calls whose SearchResultSets are merged via the
// pairwise k-way merger (OR semantics, dedup across selectors).
func dispatchSearchOverMatcherSets(matcherSets [][]*labels.Matcher, hints *storage.SearchHints, run func(matchers []*labels.Matcher) storage.SearchResultSet) storage.SearchResultSet {
	if len(matcherSets) <= 1 {
		var matchers []*labels.Matcher
		if len(matcherSets) == 1 {
			matchers = matcherSets[0]
		}
		return run(matchers)
	}
	sets := make([]storage.SearchResultSet, 0, len(matcherSets))
	for _, matchers := range matcherSets {
		sets = append(sets, run(matchers))
	}
	return storage.MergeSearchResultSets(sets, hints)
}

// SearchLabelNamesHandler returns the handler for GET/POST /api/v1/search/label_names.
func SearchLabelNamesHandler(queryable storage.Queryable, querierCfg Config, _ *validation.Overrides) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !querierCfg.ExperimentalSearchAPIEnabled {
			writeSearchFeatureDisabled(w)
			return
		}
		req, err := parseSearchRequest(r, false)
		if err != nil {
			writeSearchBadRequest(w, err)
			return
		}
		searcher, querier, err := searcherForRequest(r.Context(), queryable, req.startMs, req.endMs)
		if err != nil {
			writeSearcherForRequestError(w, err)
			return
		}
		defer querier.Close()
		rs := dispatchSearchOverMatcherSets(req.matchers, req.hints, func(m []*labels.Matcher) storage.SearchResultSet {
			return searcher.SearchLabelNames(r.Context(), req.params, req.hints, m...)
		})
		defer rs.Close()
		if req.includeScore {
			env := getSearchEnvelope[searchLabelNameRecordWithScore](req, &searchLabelNameWithScorePool)
			defer putSearchEnvelope(env, &searchLabelNameWithScorePool, req)
			streamSearchNDJSON(w, rs, req, env, func(r storage.SearchResult) searchLabelNameRecordWithScore {
				return searchLabelNameRecordWithScore{Name: r.Value, Score: r.Score}
			})
			return
		}
		env := getSearchEnvelope[searchLabelNameRecord](req, &searchLabelNamePool)
		defer putSearchEnvelope(env, &searchLabelNamePool, req)
		streamSearchNDJSON(w, rs, req, env, func(r storage.SearchResult) searchLabelNameRecord {
			return searchLabelNameRecord{Name: r.Value}
		})
	})
}

// SearchLabelValuesHandler returns the handler for GET/POST /api/v1/search/label_values.
func SearchLabelValuesHandler(queryable storage.Queryable, querierCfg Config, _ *validation.Overrides) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !querierCfg.ExperimentalSearchAPIEnabled {
			writeSearchFeatureDisabled(w)
			return
		}
		req, err := parseSearchRequest(r, true)
		if err != nil {
			writeSearchBadRequest(w, err)
			return
		}
		searcher, querier, err := searcherForRequest(r.Context(), queryable, req.startMs, req.endMs)
		if err != nil {
			writeSearcherForRequestError(w, err)
			return
		}
		defer querier.Close()
		rs := dispatchSearchOverMatcherSets(req.matchers, req.hints, func(m []*labels.Matcher) storage.SearchResultSet {
			return searcher.SearchLabelValues(r.Context(), req.labelName, req.params, req.hints, m...)
		})
		defer rs.Close()
		if req.includeScore {
			env := getSearchEnvelope[searchLabelValueRecordWithScore](req, &searchLabelValueWithScorePool)
			defer putSearchEnvelope(env, &searchLabelValueWithScorePool, req)
			streamSearchNDJSON(w, rs, req, env, func(r storage.SearchResult) searchLabelValueRecordWithScore {
				return searchLabelValueRecordWithScore{Value: r.Value, Score: r.Score}
			})
			return
		}
		env := getSearchEnvelope[searchLabelValueRecord](req, &searchLabelValuePool)
		defer putSearchEnvelope(env, &searchLabelValuePool, req)
		streamSearchNDJSON(w, rs, req, env, func(r storage.SearchResult) searchLabelValueRecord {
			return searchLabelValueRecord{Value: r.Value}
		})
	})
}

// SearchMetricNamesHandler returns the handler for /api/v1/search/metric_names.
func SearchMetricNamesHandler(queryable storage.Queryable, querierCfg Config, _ *validation.Overrides, logger log.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !querierCfg.ExperimentalSearchAPIEnabled {
			writeSearchFeatureDisabled(w)
			return
		}

		req, err := parseSearchRequest(r, false)
		if err != nil {
			writeSearchBadRequest(w, err)
			return
		}
		ctx := r.Context()
		searcher, querier, err := searcherForRequest(ctx, queryable, req.startMs, req.endMs)
		if err != nil {
			writeSearcherForRequestError(w, err)
			return
		}
		defer querier.Close()

		rs := dispatchSearchOverMatcherSets(req.matchers, req.hints, func(m []*labels.Matcher) storage.SearchResultSet {
			return searcher.SearchLabelValues(ctx, model.MetricNameLabel, req.params, req.hints, m...)
		})

		// Metric metadata (include_metadata) is enriched here, above every merge,
		// so a single batched fetch covers the fully-deduped result.
		//
		// This handler stays tenant federation agnostic, but if the queryable is
		// tenant federation aware then the metadata will be fetched across tenants.
		//
		// Metadata enrichment is best-effort: a missing fetcher or a fetch error
		// just leaves results un-enriched.
		if req.includeMetadata {
			if fetcher, ok := querier.(querierapi.MetricMetadataFetcher); ok {
				// Floor the fetch batch so a batch_size=1 (or, generally speaking a
				// small batch_size requested via the API) can't turn into one all-ingester
				// metadata fan-out per single/few results.
				fetchBatchSize := max(req.batchSize, searchDefaultBatchSize)
				rs = newMetadataEnrichingSearchResultSet(ctx, rs, fetcher.FetchMetricMetadata, fetchBatchSize, logger)
			}
		}

		// Ensure the result set gets closed (cascading when result sets are wrapped).
		defer rs.Close()

		if req.includeScore {
			env := getSearchEnvelope[searchMetricNameRecordWithScore](req, &searchMetricNameWithScorePool)
			defer putSearchEnvelope(env, &searchMetricNameWithScorePool, req)
			streamSearchNDJSON(w, rs, req, env, func(r storage.SearchResult) searchMetricNameRecordWithScore {
				rec := searchMetricNameRecordWithScore{Name: r.Value, Score: r.Score}
				if md := r.Metadata; md != nil {
					rec.Type = string(md.Type)
					rec.Help = md.Help
					rec.Unit = md.Unit
				}
				return rec
			})
			return
		}
		env := getSearchEnvelope[searchMetricNameRecord](req, &searchMetricNamePool)
		defer putSearchEnvelope(env, &searchMetricNamePool, req)
		streamSearchNDJSON(w, rs, req, env, func(r storage.SearchResult) searchMetricNameRecord {
			rec := searchMetricNameRecord{Name: r.Value}
			if md := r.Metadata; md != nil {
				rec.Type = string(md.Type)
				rec.Help = md.Help
				rec.Unit = md.Unit
			}
			return rec
		})
	})
}

// searcherClientError marks an error from searcherForRequest as a
// client-side failure (e.g. missing or invalid tenant on the request
// context). Handlers map it to HTTP 400; everything else from
// searcherForRequest is a server-side failure and maps to HTTP 500/503/499
// via writePreFlushSearchError.
type searcherClientError struct{ err error }

func (e *searcherClientError) Error() string { return e.err.Error() }
func (e *searcherClientError) Unwrap() error { return e.err }

// searcherForRequest opens a querier for the time range and type-asserts it
// to mimirSearcher. The caller is responsible for calling querier.Close().
// Uses tenant.TenantIDs (plural) rather than TenantID so multi-tenant
// requests routed through tenantfederation.NewQueryable are not rejected
// here — the federation layer resolves per-tenant IDs internally.
//
// Tenant-resolution failures are wrapped in *searcherClientError so callers
// can route them to HTTP 400; other failures (Querier open, type-assertion)
// are server-side and returned bare.
func searcherForRequest(ctx context.Context, queryable storage.Queryable, startMs, endMs int64) (mimirSearcher, storage.Querier, error) {
	if _, err := tenant.TenantIDs(ctx); err != nil {
		return nil, nil, &searcherClientError{err: err}
	}
	q, err := queryable.Querier(startMs, endMs)
	if err != nil {
		return nil, nil, fmt.Errorf("open querier: %w", err)
	}
	s, ok := q.(mimirSearcher)
	if !ok {
		_ = q.Close()
		return nil, nil, fmt.Errorf("queryable does not support search (type %T)", q)
	}
	return s, q, nil
}

// writeSearcherForRequestError classifies err from searcherForRequest and
// routes it to the bad-request envelope (client error) or the pre-flush
// server-error envelope.
func writeSearcherForRequestError(w http.ResponseWriter, err error) {
	var ce *searcherClientError
	if errors.As(err, &ce) {
		writeSearchBadRequest(w, err)
		return
	}
	writePreFlushSearchError(w, err)
}

// getSearchEnvelope returns the per-batch envelope for the request. Uses
// the pool only when the batch size matches the pool's pre-allocated
// capacity; otherwise allocates a fresh envelope so an outsized user
// batch size does not grow the pool's backing slices indefinitely.
func getSearchEnvelope[T any](req *searchRequest, pool *sync.Pool) *searchBatchEnvelope[T] {
	if req.batchSize == searchDefaultBatchSize {
		return pool.Get().(*searchBatchEnvelope[T])
	}
	return &searchBatchEnvelope[T]{Results: make([]T, 0, req.batchSize)}
}

// putSearchEnvelope returns env to pool when the batch size matches the
// default; for non-default sizes the envelope is dropped on the floor
// (matches getSearchEnvelope's allocation rule).
func putSearchEnvelope[T any](env *searchBatchEnvelope[T], pool *sync.Pool, req *searchRequest) {
	if req.batchSize != searchDefaultBatchSize {
		return
	}
	env.Results = env.Results[:0]
	pool.Put(env)
}

// streamSearchNDJSON drains rs and writes NDJSON to w. One JSON object per
// line; results batched per req.batchSize; flusher.Flush() called after each
// batch line. NDJSON Content-Type is set lazily on the first batch flush so
// pre-flush errors can fall back to the standard application/json envelope
// per Prometheus PR #18573 (web/api/v1/search.go: respondPreStreamSearchError
// vs writeStreamSearchError). Once any batch has been flushed, headers are
// on the wire and a later iterator error rides on a status="error" NDJSON
// trailer instead of an HTTP error code.
//
// build maps each storage.SearchResult to the endpoint-specific record
// shape (label-names, label-values, metric-names). Score handling and
// any per-record enrichment (e.g. metadata Type/Help/Unit for
// metric-names) live in the per-endpoint builder so the wire-shape
// contract is enforced at the call site.
//
// env is the pre-built per-batch envelope. The caller pools it (or
// fresh-allocates it for non-default batch sizes) and is responsible for
// returning it to the pool on exit; this function only resets
// env.Results back to length zero between flushes.
func streamSearchNDJSON[T any](w http.ResponseWriter, rs storage.SearchResultSet, req *searchRequest, env *searchBatchEnvelope[T], build func(storage.SearchResult) T) {
	flusher, _ := w.(http.Flusher)
	enc := json.NewEncoder(w)
	// Don't HTML-escape — search values may legitimately contain <, >, & and
	// we're not emitting into an HTML context.
	enc.SetEscapeHTML(false)

	env.Results = env.Results[:0]

	flushedAny := false
	emitted := 0
	flushBatch := func() error {
		if len(env.Results) == 0 {
			return nil
		}
		if !flushedAny {
			w.Header().Set("Content-Type", searchAPIContentType)
			w.Header().Set(worker.ResponseStreamingEnabledHeader, "true")
		}
		if err := enc.Encode(env); err != nil {
			return err
		}
		if flusher != nil {
			flusher.Flush()
		}
		env.Results = env.Results[:0]
		flushedAny = true
		return nil
	}

	for rs.Next() {
		emitted++
		// req.hints.Limit was set to req.limit+1 to probe for has_more.
		// When the iterator returns that (limit+1)-th record we now know
		// data extends past the user's limit — record the signal and drop
		// the probe record so we never emit more than req.limit to the
		// wire.
		if req.limit > 0 && emitted > req.limit {
			break
		}
		env.Results = append(env.Results, build(rs.At()))
		if len(env.Results) >= req.batchSize {
			if err := flushBatch(); err != nil {
				return
			}
		}
	}
	// Flush any tail.
	if err := flushBatch(); err != nil {
		return
	}

	// Iterator-level error. If anything was flushed, surface as an NDJSON
	// error trailer — HTTP 200 headers are already on the wire. Otherwise
	// fall through to writePreFlushSearchError, which writes a standard
	// application/json envelope with a classified status code, mirroring
	// Prometheus' respondPreStreamSearchError.
	if err := rs.Err(); err != nil {
		if flushedAny {
			_ = enc.Encode(searchErrorEnvelope{
				Status:    "error",
				ErrorType: searchErrorType(err),
				Error:     err.Error(),
			})
			if flusher != nil {
				flusher.Flush()
			}
			return
		}
		writePreFlushSearchError(w, err)
		return
	}

	// has_more uses two signals OR'd together:
	//   1. emitted > req.limit. We asked the iterator for req.limit+1
	//      records as a probe; if it produced the extra one we know the
	//      data extends past the user's limit. This avoids the false
	//      positive that "emitted >= limit" would have when the data
	//      exactly fills req.limit.
	//   2. A per-source clamp warning (*MaxLimitError on the search
	//      label-{names,values} flags) AND the iterator actually
	//      saturated the post-clamp cap. The clamp warning fires when the
	//      effective limit was cut (e.g. limit=0 raised to the tenant
	//      ceiling), but iteration may still finish below the cap with
	//      no truncation. Requiring emitted >= enforced avoids the false
	//      positive that "clamp fired" alone would have for limit=0
	//      requests against tenants with a positive ceiling.
	trailer := searchTrailerEnvelope{Status: "success"}
	// If no batches were flushed (e.g. the result set is empty), ensure the
	// NDJSON content type and the internal streaming header are still set
	// before writing the trailer so clients see the expected Content-Type.
	if !flushedAny {
		w.Header().Set("Content-Type", searchAPIContentType)
		w.Header().Set(worker.ResponseStreamingEnabledHeader, "true")
	}
	// clampEnforcedMin tracks the smallest post-clamp cap observed across
	// per-source warnings. -1 means "no clamp fired".
	clampEnforcedMin := -1
	for _, warn := range rs.Warnings() {
		trailer.Warnings = append(trailer.Warnings, warn.Error())
		if enforced, ok := searchClampEnforced(warn); ok {
			if clampEnforcedMin < 0 || enforced < clampEnforcedMin {
				clampEnforcedMin = enforced
			}
		}
	}
	switch {
	case req.limit > 0 && emitted > req.limit:
		trailer.HasMore = true
	case clampEnforcedMin >= 0 && emitted >= clampEnforcedMin:
		trailer.HasMore = true
	}
	// Fast path for the common case: success trailer with no warnings and
	// no has_more flag. Bypassing json.Encoder skips one bytes allocation
	// per request and is the only trailer shape the encoder would have
	// emitted byte-for-byte identical to defaultSuccessTrailer anyway.
	if !trailer.HasMore && len(trailer.Warnings) == 0 {
		_, _ = w.Write(defaultSuccessTrailer)
	} else {
		_ = enc.Encode(trailer)
	}
	if flusher != nil {
		flusher.Flush()
	}
}

// searchClampEnforced reports whether warn is a per-source label-names /
// label-values clamp warning and, if so, returns the post-clamp cap
// (MaxLimitError.Enforced). Any other LimitError (e.g. series-query
// length) is ignored here — only clamps on the search result count
// participate in has_more.
func searchClampEnforced(warn error) (int, bool) {
	var mle *MaxLimitError
	if !errors.As(warn, &mle) {
		return 0, false
	}
	if mle.Flag != validation.MaxLabelNamesLimitFlag && mle.Flag != validation.MaxLabelValuesLimitFlag {
		return 0, false
	}
	return mle.Enforced, true
}

// statusClientClosedConnection mirrors Prometheus' 499 used for
// client-cancelled requests (web/api/v1/api.go).
const statusClientClosedConnection = 499

// searchErrorType classifies err into the Prometheus-style errorType
// string that rides on the JSON envelope. Mimir doesn't carry the full
// Prometheus apiError taxonomy; we map the cases that drive distinct
// HTTP status codes and bucket everything else as "internal".
func searchErrorType(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "internal"
	}
}

// searchErrorStatus picks the HTTP status code for a pre-flush search
// error. Mirrors Prometheus' getDefaultErrorCode mapping for the error
// types we currently emit: canceled → 499, timeout → 503, otherwise
// 500.
func searchErrorStatus(err error) int {
	switch {
	case errors.Is(err, context.Canceled):
		return statusClientClosedConnection
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// writePreFlushSearchError writes the standard JSON error envelope when
// an iterator fails before any NDJSON batch has been flushed. Mirrors
// Prometheus PR #18573's respondPreStreamSearchError: Content-Type is
// application/json (not the streaming NDJSON type), the status code is
// classified, and the body is the same searchErrorEnvelope shape used
// for post-flush errors.
func writePreFlushSearchError(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(searchErrorStatus(err))
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(searchErrorEnvelope{
		Status:    "error",
		ErrorType: searchErrorType(err),
		Error:     err.Error(),
	})
}

// writeSearchFeatureDisabled emits the 404 + feature_not_enabled body per
// Prometheus PR #18573.
func writeSearchFeatureDisabled(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(searchErrorEnvelope{
		Status:    "error",
		ErrorType: "feature_not_enabled",
		Error:     "the experimental search API is not enabled",
	})
}

// writeSearchBadRequest emits a JSON 400 with the parser error.
func writeSearchBadRequest(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(searchErrorEnvelope{
		Status:    "error",
		ErrorType: "bad_data",
		Error:     err.Error(),
	})
}
