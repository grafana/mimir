// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/e8e25eb09e41bf295e0c9e847cd27cf9016a553a/web/api/v1/search.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package querier

import (
	"context"
	"encoding/base64"
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
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

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
	Status     string   `json:"status"`
	HasMore    bool     `json:"has_more"`
	NextCursor string   `json:"next_cursor,omitempty"`
	Warnings   []string `json:"warnings,omitempty"`
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

// searchCursorVersion is the current cursor payload version. Bump this and
// reject unrecognized versions on decode if the payload shape ever changes,
// so an old in-flight cursor fails clearly instead of being misinterpreted.
const searchCursorVersion = 1

// searchCursor is the JSON payload encoded into the opaque `cursor` query
// parameter. It is the sole source of every search-defining parameter for a
// cursor-driven request. See
// docs/superpowers/specs/2026-09-22-search-cursor-pagination-design.md.
//
// SortBy is only ever "" or "alpha" for a server-generated cursor (v1 never
// emits a cursor for sort_by=score) but is still decoded and validated
// defensively against a hand-crafted cursor that tries to smuggle in
// sort_by=score.
type searchCursor struct {
	Version         int               `json:"v"`
	Terms           []string          `json:"terms,omitempty"`
	Expression      string            `json:"expression,omitempty"`
	CaseSensitive   bool              `json:"case_sensitive"`
	FuzzAlg         string            `json:"fuzz_alg"`
	FuzzThreshold   int               `json:"fuzz_threshold"`
	SortBy          string            `json:"sort_by,omitempty"`
	SortDir         string            `json:"sort_dir"`
	Limit           int               `json:"limit"`
	BatchSize       int               `json:"batch_size"`
	IncludeScore    bool              `json:"include_score"`
	IncludeMetadata bool              `json:"include_metadata"`
	Label           string            `json:"label,omitempty"`
	StartMs         int64             `json:"start_ms"`
	EndMs           int64             `json:"end_ms"`
	Matchers        [][]cursorMatcher `json:"matchers,omitempty"`
	ResumeAfter     string            `json:"resume_after"`
}

// cursorMatcher mirrors the wire LabelMatcher encoding (pkg/ingester/client's
// MatchType enum: EQUAL=0, NOT_EQUAL=1, REGEX_MATCH=2, REGEX_NO_MATCH=3,
// which is numerically identical to labels.MatchType) so a matcher set
// round-trips through JSON without a PromQL-text detour.
type cursorMatcher struct {
	Type  int    `json:"type"`
	Name  string `json:"name"`
	Value string `json:"value"`
}

// fuzzAlgToCursorString and cursorStringToFuzzAlg convert between
// streaminglabelvalues.FuzzAlg and the same string vocabulary
// parseSearchRequest already accepts on fuzz_alg, so a cursor's encoding is
// exactly what a client could have typed.
func fuzzAlgToCursorString(alg streaminglabelvalues.FuzzAlg) string {
	switch alg {
	case streaminglabelvalues.FuzzAlgJaroWinkler:
		return "jarowinkler"
	case streaminglabelvalues.FuzzAlgSubstringLeft:
		return "substring_left"
	case streaminglabelvalues.FuzzAlgSubstring:
		return "substring"
	default:
		return "subsequence"
	}
}

func cursorStringToFuzzAlg(s string) (streaminglabelvalues.FuzzAlg, error) {
	switch s {
	case "", "subsequence":
		return streaminglabelvalues.FuzzAlgSubsequence, nil
	case "jarowinkler":
		return streaminglabelvalues.FuzzAlgJaroWinkler, nil
	case "substring_left":
		return streaminglabelvalues.FuzzAlgSubstringLeft, nil
	case "substring":
		return streaminglabelvalues.FuzzAlgSubstring, nil
	default:
		return 0, fmt.Errorf("invalid fuzz_alg %q in cursor", s)
	}
}

// orderingToSortDir converts a resolved storage.Ordering back into the
// sort_dir vocabulary parseSortOrder accepts. Only ever called for
// OrderByValueAsc/OrderByValueDesc — encodeSearchCursor is never reached for
// sort_by=score (see SearchLabelNamesHandler/streamSearchNDJSON in Task 9).
func orderingToSortDir(order storage.Ordering) string {
	if order == storage.OrderByValueDesc {
		return "dsc"
	}
	return "asc"
}

// matchersToCursor and cursorToMatchers convert between the searchRequest's
// [][]*labels.Matcher (one slice per match[] entry) and the cursor's JSON
// encoding.
func matchersToCursor(matcherSets [][]*labels.Matcher) [][]cursorMatcher {
	if len(matcherSets) == 0 {
		return nil
	}
	out := make([][]cursorMatcher, len(matcherSets))
	for i, set := range matcherSets {
		row := make([]cursorMatcher, len(set))
		for j, m := range set {
			row[j] = cursorMatcher{Type: int(m.Type), Name: m.Name, Value: m.Value}
		}
		out[i] = row
	}
	return out
}

func cursorToMatchers(rows [][]cursorMatcher) ([][]*labels.Matcher, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	out := make([][]*labels.Matcher, len(rows))
	for i, row := range rows {
		set := make([]*labels.Matcher, len(row))
		for j, cm := range row {
			m, err := labels.NewMatcher(labels.MatchType(cm.Type), cm.Name, cm.Value)
			if err != nil {
				return nil, fmt.Errorf("invalid cursor matcher %d/%d: %w", i, j, err)
			}
			set[j] = m
		}
		out[i] = set
	}
	return out, nil
}

// encodeSearchCursor builds the opaque cursor value for the next page of
// req, given resumeAfter (the last value emitted on the current page).
// Only ever called when the effective ordering is alpha (see Task 9); it
// panics if handed a score-ordered request, since that would indicate a
// caller bug rather than bad user input.
func encodeSearchCursor(req *searchRequest, resumeAfter string) (string, error) {
	if req.hints.OrderBy == storage.OrderByScoreDesc {
		panic("encodeSearchCursor called for a score-ordered request")
	}
	c := searchCursor{
		Version:         searchCursorVersion,
		Terms:           req.params.Terms,
		Expression:      req.params.Expression(),
		CaseSensitive:   req.params.CaseSensitive,
		FuzzAlg:         fuzzAlgToCursorString(req.params.FuzzAlg),
		FuzzThreshold:   req.params.FuzzThreshold,
		SortDir:         orderingToSortDir(req.hints.OrderBy),
		Limit:           req.limit,
		BatchSize:       req.batchSize,
		IncludeScore:    req.includeScore,
		IncludeMetadata: req.includeMetadata,
		Label:           req.labelName,
		StartMs:         req.startMs,
		EndMs:           req.endMs,
		Matchers:        matchersToCursor(req.matchers),
		ResumeAfter:     resumeAfter,
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// decodeSearchCursor parses and structurally validates the opaque cursor
// value. It does not yet build a searchRequest — see toSearchRequest.
func decodeSearchCursor(raw string) (*searchCursor, error) {
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor: %w", err)
	}
	var c searchCursor
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("invalid cursor: %w", err)
	}
	if c.Version != searchCursorVersion {
		return nil, fmt.Errorf("invalid cursor: unsupported version %d", c.Version)
	}
	if c.SortBy != "" && c.SortBy != "alpha" {
		return nil, fmt.Errorf("invalid cursor: sort_by=%q is not supported with a cursor", c.SortBy)
	}
	return &c, nil
}

// toSearchRequest rebuilds a *searchRequest from a decoded cursor, routing
// every field through the same validation a fresh request would use
// (NewParams/NewExpressionParams, parseSortOrder, limit/batch_size bounds),
// so a malformed or hand-crafted cursor fails the same way a bad request
// would. Cursors are unsigned, so any client can hand-craft one; every
// field must be validated here rather than trusted.
func (c *searchCursor) toSearchRequest(requireLabelName bool) (*searchRequest, error) {
	alg, err := cursorStringToFuzzAlg(c.FuzzAlg)
	if err != nil {
		return nil, err
	}

	if len(c.Terms) > maxSearchTermsPerRequest {
		return nil, fmt.Errorf("invalid cursor: too many search terms: got %d, maximum is %d", len(c.Terms), maxSearchTermsPerRequest)
	}
	if c.Limit < 0 {
		return nil, errors.New("invalid cursor: limit must be >= 0")
	}
	if c.BatchSize < 0 || c.BatchSize > maxSearchBatchSize {
		return nil, fmt.Errorf("invalid cursor: batch_size %d must be between 0 and %d", c.BatchSize, maxSearchBatchSize)
	}
	batchSize := c.BatchSize
	if batchSize == 0 {
		batchSize = searchDefaultBatchSize
	}

	var params *streaminglabelvalues.Params
	if c.Expression != "" {
		if len(c.Terms) > 0 {
			return nil, errors.New("invalid cursor: terms and expression are mutually exclusive")
		}
		params, err = streaminglabelvalues.NewExpressionParams(c.Expression, c.CaseSensitive, alg, c.FuzzThreshold)
		if err != nil {
			return nil, fmt.Errorf("invalid cursor: %w", err)
		}
	} else {
		params, err = streaminglabelvalues.NewParams(c.Terms, c.CaseSensitive, alg, c.FuzzThreshold)
		if err != nil {
			return nil, fmt.Errorf("invalid cursor: %w", err)
		}
	}
	params.SearchAfter = c.ResumeAfter

	order, err := parseSortOrder("alpha", c.SortDir, true)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor: %w", err)
	}

	if requireLabelName && c.Label == "" {
		return nil, errors.New(`invalid cursor: missing required parameter "label"`)
	}
	if c.EndMs < c.StartMs {
		return nil, errors.New("invalid cursor: end timestamp must not be before start timestamp")
	}

	matchers, err := cursorToMatchers(c.Matchers)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor: %w", err)
	}

	hintsLimit := c.Limit
	if c.Limit > 0 && c.Limit < math.MaxInt {
		hintsLimit = c.Limit + 1
	}

	return &searchRequest{
		params:          params,
		matchers:        matchers,
		hints:           &storage.SearchHints{OrderBy: order, Limit: hintsLimit},
		limit:           c.Limit,
		startMs:         c.StartMs,
		endMs:           c.EndMs,
		batchSize:       batchSize,
		includeScore:    c.IncludeScore,
		includeMetadata: c.IncludeMetadata,
		labelName:       c.Label,
	}, nil
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

	// Presence, not non-emptiness: a present-but-empty cursor= must still be
	// treated as "a cursor was supplied" so it cannot silently fall through
	// to ordinary parsing. A lone empty value then fails decode validation.
	if _, present := q["cursor"]; present {
		cursor := q.Get("cursor")
		if len(q) != 1 {
			return nil, errors.New("cursor and other search parameters are mutually exclusive")
		}
		decoded, err := decodeSearchCursor(cursor)
		if err != nil {
			return nil, err
		}
		return decoded.toSearchRequest(requireLabelName)
	}

	// Search terms (search[]). Capped at maxSearchTermsPerRequest to bound
	// per-request filter construction cost; matches Prometheus PR #18573.
	terms := q["search[]"]
	if len(terms) > maxSearchTermsPerRequest {
		return nil, fmt.Errorf("too many search[] terms: got %d, maximum is %d", len(terms), maxSearchTermsPerRequest)
	}

	// search_expr is a boolean expression alternative to search[] (AND/OR/NOT,
	// quoted terms, parentheses); the two are mutually exclusive.
	expr := q.Get("search_expr")
	if expr != "" && len(terms) > 0 {
		return nil, errors.New("search[] and search_expr are mutually exclusive")
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
	case "substring_left":
		alg = streaminglabelvalues.FuzzAlgSubstringLeft
	case "substring":
		alg = streaminglabelvalues.FuzzAlgSubstring
	default:
		return nil, fmt.Errorf("invalid fuzz_alg %q (allowed: subsequence, jarowinkler, substring_left, substring)", q.Get("fuzz_alg"))
	}

	// Fuzz threshold (int 0-100, default 0). fuzz_alg=substring ignores the
	// fuzzy threshold entirely (FilterContains with anyPosition=true always
	// scores 1.0 on a match), so 100 is the only value that makes sense;
	// default to it and reject any other explicit value rather than
	// silently ignoring it.
	threshold := 0
	if alg == streaminglabelvalues.FuzzAlgSubstring {
		threshold = 100
	}
	if v := q.Get("fuzz_threshold"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid fuzz_threshold: %w", err)
		}
		if parsed < 0 || parsed > 100 {
			return nil, fmt.Errorf("invalid fuzz_threshold: got %v but must be between 0 and 100", parsed)
		}
		if alg == streaminglabelvalues.FuzzAlgSubstring && parsed != 100 {
			return nil, fmt.Errorf("invalid fuzz_threshold: fuzz_alg=substring only supports fuzz_threshold=100, got %d", parsed)
		}
		threshold = parsed
	}

	// Ordering: sort_by + sort_dir
	sortBy := q.Get("sort_by")
	if sortBy == "" {
		sortBy = "alpha"
	}
	// sort_by=score sorts by relevance score, which is only meaningful when
	// at least one search[] term or a search_expr has been supplied to
	// produce the scores. Matches Prometheus PR #18573.
	if sortBy == "score" && len(terms) == 0 && expr == "" {
		return nil, errors.New("sort_by=score requires search[] or search_expr to be set")
	}
	// fuzz_alg=substring scores every match 1.0 (FilterContains with
	// anyPosition=true), so sorting by score carries no information; only
	// alpha ordering makes sense.
	if sortBy == "score" && alg == streaminglabelvalues.FuzzAlgSubstring {
		return nil, errors.New("sort_by=score is not supported with fuzz_alg=substring; every match scores 1.0, use sort_by=alpha")
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

	var params *streaminglabelvalues.Params
	if expr != "" {
		params, err = streaminglabelvalues.NewExpressionParams(expr, caseSensitive, alg, threshold)
		if err != nil {
			return nil, fmt.Errorf("invalid search_expr: %w", err)
		}
	} else {
		params, err = streaminglabelvalues.NewParams(terms, caseSensitive, alg, threshold)
		if err != nil {
			return nil, fmt.Errorf("invalid search params: %w", err)
		}
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

// cursorResumingSearchResultSet wraps inner so it discards every record up
// to and including resumeAfter before passing records through unchanged.
// This is the querier-side backstop described in
// docs/superpowers/specs/2026-09-22-search-cursor-pagination-design.md: even
// once every ingester/store-gateway honors Params.SearchAfter, an
// old-binary source during a rolling upgrade will silently ignore it and
// return its ordinary first-N results, so the querier must always be able
// to re-apply the same exclusion itself. resumeAfter == "" makes this a
// no-op pass-through (used when there is no cursor in effect).
//
// The backstop guarantees no already-seen record is ever re-emitted, and
// that the walk terminates. It does NOT recover a fully correct walk
// against a source that ignores search_after: such a source still honors
// the older Limit field positionally, so it returns the same first-Limit
// window of the ordering on every page, and everything past that window is
// unreachable until the source is upgraded. When that window is entirely
// discarded, streamSearchNDJSON discloses the possibly-incomplete page via
// a response warning (see innerRecordsDiscarded) rather than reporting a
// silently complete walk.
type cursorResumingSearchResultSet struct {
	inner       storage.SearchResultSet
	resumeAfter string
	descending  bool
	pastCursor  bool
	innerSeen   int
}

func newCursorResumingSearchResultSet(inner storage.SearchResultSet, resumeAfter string, order storage.Ordering) storage.SearchResultSet {
	if resumeAfter == "" {
		return inner
	}
	return &cursorResumingSearchResultSet{inner: inner, resumeAfter: resumeAfter, descending: order == storage.OrderByValueDesc}
}

func (s *cursorResumingSearchResultSet) Next() bool {
	if s.pastCursor {
		return s.inner.Next()
	}
	for s.inner.Next() {
		s.innerSeen++
		v := s.inner.At().Value
		if s.descending {
			if v < s.resumeAfter {
				s.pastCursor = true
				return true
			}
		} else if v > s.resumeAfter {
			s.pastCursor = true
			return true
		}
	}
	return false
}

// innerRecordsDiscarded reports how many records the wrapped source
// returned up to and including the first one found to be strictly past
// resumeAfter, or the whole response if none was. If that count reached the
// caller's probed limit, the source may have positionally truncated its
// response at or before the cursor's resume point — for example, a source
// mid-rolling-upgrade that doesn't yet understand search_after but still
// honors the older Limit field, which then returns the same fixed window on
// every page. streamSearchNDJSON uses this to disclose a possibly-incomplete
// page via a response warning rather than silently reporting a complete,
// successful walk. It covers both the fully-discarded page and the partially
// discarded one, since both hide the same unreachable tail.
//
// The count stops growing once the wrapper is past the cursor, so a healthy
// search_after-aware source — which returns its first record already past
// resumeAfter — leaves this at 1 and never trips the caller's check.
func (s *cursorResumingSearchResultSet) innerRecordsDiscarded() int {
	return s.innerSeen
}

func (s *cursorResumingSearchResultSet) At() storage.SearchResult          { return s.inner.At() }
func (s *cursorResumingSearchResultSet) Warnings() annotations.Annotations { return s.inner.Warnings() }
func (s *cursorResumingSearchResultSet) Err() error                        { return s.inner.Err() }
func (s *cursorResumingSearchResultSet) Close() error                      { return s.inner.Close() }

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
		rs = newCursorResumingSearchResultSet(rs, req.params.SearchAfter, req.hints.OrderBy)
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
		rs = newCursorResumingSearchResultSet(rs, req.params.SearchAfter, req.hints.OrderBy)
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
		rs = newCursorResumingSearchResultSet(rs, req.params.SearchAfter, req.hints.OrderBy)

		// Metric metadata (include_metadata) is enriched here, above every merge,
		// so a single batched fetch covers the fully-deduped result.
		//
		// This handler stays tenant federation agnostic, but if the queryable is
		// tenant federation aware then the metadata will be fetched across the
		// tenants the request's selectors scope to.
		//
		// Metadata enrichment is best-effort: a missing fetcher or a fetch error
		// just leaves results un-enriched.
		if req.includeMetadata {
			if fetcher, ok := querier.(querierapi.MetricMetadataFetcher); ok {
				// Floor the fetch batch so a batch_size=1 (or, generally speaking a
				// small batch_size requested via the API) can't turn into one all-ingester
				// metadata fan-out per single/few results.
				fetchBatchSize := max(req.batchSize, searchDefaultBatchSize)
				// Pass down the request's selectors so a tenant-federation aware
				// fetcher scopes metadata to the union of tenants the selectors
				// touched.
				matcherSets := req.matchers
				fetch := func(ctx context.Context, names []string) (map[string]metadata.Metadata, error) {
					return fetcher.FetchMetricMetadata(ctx, names, matcherSets)
				}
				rs = newMetadataEnrichingSearchResultSet(ctx, rs, fetch, fetchBatchSize, logger)
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
	var lastValue string
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
		lastValue = rs.At().Value
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
	// Known scope limitation: the assertion is on the concrete rs handed to
	// this function, so metric-names with include_metadata=true never gets
	// this warning — rs is re-wrapped by then.
	if crs, ok := rs.(*cursorResumingSearchResultSet); ok && req.hints.Limit > 0 && crs.innerRecordsDiscarded() >= req.hints.Limit {
		trailer.Warnings = append(trailer.Warnings, "a search source may not yet support cursor-based resume (a rolling upgrade may be in progress); this page's results may be incomplete")
	}
	// lastValue == "" means nothing was emitted, so there is no resume
	// point to encode; a cursor carrying an empty resume_after would
	// restart the walk from the beginning.
	if trailer.HasMore && req.hints.OrderBy != storage.OrderByScoreDesc && lastValue != "" {
		if cursor, err := encodeSearchCursor(req, lastValue); err == nil {
			trailer.NextCursor = cursor
		}
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
