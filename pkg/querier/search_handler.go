// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	v1 "github.com/prometheus/prometheus/web/api/v1"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/querier/worker"
	mimirstorage "github.com/grafana/mimir/pkg/storage"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/promqlext"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

type searchExecutor func(ctx context.Context, searcher mimirstorage.Searcher, hints *mimirstorage.SearchHints, params *searchParams) (mimirstorage.SearcherValueSet, error)

// searchParams holds parsed parameters common to all search endpoints.
type searchParams struct {
	start         int64
	end           int64
	matchers      []*labels.Matcher
	search        []string
	fuzzThreshold int
	fuzzAlg       string
	caseSensitive bool
	includeScore  bool
	sortBy        streaminglabelvalues.SortBy
	sortDir       streaminglabelvalues.SortDirection
	operation     streaminglabelvalues.Operator
	batchSize     int
	limit         int
	labelName     string
}

// buildSearchHints constructs a SearchHints from the parsed search params.
// When search terms are present, a FilterChains is built and attached as hints.Filter:
//   - A string-matching chain (FilterContains) accepts values containing any/all terms.
//   - An optional fuzzy-matching chain (FilterJaro) accepts near-matches above the threshold.
//
// The two chains are OR'd together so that a value accepted by either chain is included.
func buildSearchHints(params *searchParams) *mimirstorage.SearchHints {
	hints := &mimirstorage.SearchHints{Limit: params.limit}

	chain := streaminglabelvalues.NewFilterChains(params.caseSensitive)
	hints.Filter = chain

	if len(params.search) > 0 {

		// String-matching chain: each search term becomes a FilterContains.
		stringChain := streaminglabelvalues.NewFilterChain(params.operation, len(params.search))
		for _, s := range params.search {
			term := s
			if !params.caseSensitive {
				term = strings.ToLower(s)
			}
			stringChain.AddFilter(streaminglabelvalues.NewFilterContains(term))
		}
		chain.AddFilterChain(stringChain)

		// Optional fuzzy-matching chain: each search term becomes a FilterJaro.
		if params.fuzzThreshold > 0 {
			fuzzChain := streaminglabelvalues.NewFilterChain(params.operation, len(params.search))
			threshold := float64(params.fuzzThreshold) / 100.0
			for _, s := range params.search {
				term := s
				if !params.caseSensitive {
					term = strings.ToLower(s)
				}
				fuzzChain.AddFilter(streaminglabelvalues.NewFilterJaro(term, threshold))
			}
			chain.AddFilterChain(fuzzChain)
		}
	}

	if params.sortBy == streaminglabelvalues.Alpha && params.sortDir == streaminglabelvalues.Asc {
		hints.Compare = &streaminglabelvalues.ComparerAlpha{}
	} else if params.sortBy == streaminglabelvalues.Alpha {
		hints.Compare = &streaminglabelvalues.ComparerAlphaDesc{}
	} else if params.sortBy == streaminglabelvalues.Score && params.sortDir == streaminglabelvalues.Desc {
		hints.Compare = streaminglabelvalues.NewCompareScore(params.search)
	} else {
		// TODO
	}

	return hints
}

func doSearchHandler(queryable storage.SampleAndChunkQueryable, _ *validation.Overrides, logger log.Logger, handler string, searchExec searchExecutor, requireLabelNameParam bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		log, ctx := spanlogger.New(ctx, logger, tracer, handler)
		defer log.Finish()

		tenant, err := tenant.TenantID(ctx)
		if err != nil {
			level.Error(log).Log("msg", "error obtaining tenant from context", "err", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		params, err := parseSearchParams(r, requireLabelNameParam)
		if err != nil {
			level.Error(log).Log("msg", "error parsing search metric names request", "tenant", tenant, "err", err)
			writeSearchError(w, err)
			return
		}

		q, err := queryable.Querier(params.start, params.end)
		if err != nil {
			level.Error(log).Log("msg", "error obtaining querier", "tenant", tenant, "err", err, "start", params.start, "end", params.end)
			respondFromError(err, w)
			return
		}
		defer q.Close()

		searcher := q.(mimirstorage.Searcher)
		hints := buildSearchHints(params)

		vs, err := searchExec(ctx, searcher, hints, params)
		if err != nil {
			// we have not started the stream, so we can return with a 4xx error
			level.Error(log).Log("msg", "unable to invoke search", "tenant", tenant, "err", err)
			respondFromError(err, w)
			return
		}
		defer vs.Close()

		writer := newStreamingWriter(w, streaminglabelvalues.NewSearchResultFactory(params.includeScore))
		writer.init()

		err = streamResults(vs, params.batchSize, params.limit, writer)
		if err != nil {
			// we have an error after we have started streaming .... try to write the error to the stream
			level.Error(log).Log("msg", "error writing streaming search results", "tenant", tenant, "err", err)
			writeError := writer.writeError(err)
			if writeError != nil {
				level.Error(log).Log("msg", "error writing error to output stream", "tenant", tenant, "err", err)
			}
		}
	})
}

// SearchMetricNamesHandler returns an HTTP handler for GET/POST /api/v1/search/metric_names.
func SearchMetricNamesHandler(queryable storage.SampleAndChunkQueryable, overrides *validation.Overrides, logger log.Logger) http.Handler {
	searchExec := func(ctx context.Context, searcher mimirstorage.Searcher, hints *mimirstorage.SearchHints, params *searchParams) (mimirstorage.SearcherValueSet, error) {
		return searcher.SearchLabelValues(ctx, model.MetricNameLabel, hints, params.matchers...)
	}
	return doSearchHandler(queryable, overrides, logger, "SearchMetricNamesHandler", searchExec, false)
}

// SearchLabelNamesHandler returns an HTTP handler for GET/POST /api/v1/search/label_names.
func SearchLabelNamesHandler(queryable storage.SampleAndChunkQueryable, overrides *validation.Overrides, logger log.Logger) http.Handler {
	searchExec := func(ctx context.Context, searcher mimirstorage.Searcher, hints *mimirstorage.SearchHints, params *searchParams) (mimirstorage.SearcherValueSet, error) {
		return searcher.SearchLabelNames(ctx, hints, params.matchers...)
	}
	return doSearchHandler(queryable, overrides, logger, "SearchLabelNamesHandler", searchExec, false)
}

// SearchLabelValuesHandler returns an HTTP handler for GET/POST /api/v1/search/label_values.
func SearchLabelValuesHandler(queryable storage.SampleAndChunkQueryable, overrides *validation.Overrides, logger log.Logger) http.Handler {
	searchExec := func(ctx context.Context, searcher mimirstorage.Searcher, hints *mimirstorage.SearchHints, params *searchParams) (mimirstorage.SearcherValueSet, error) {
		return searcher.SearchLabelValues(ctx, params.labelName, hints, params.matchers...)
	}
	return doSearchHandler(queryable, overrides, logger, "SearchLabelNamesHandler", searchExec, true)
}

// streamResults writes results from vs to writer in batches of batchSize.
// When limit > 0, iteration stops after limit values have been consumed.
// Sorting is the Searcher's responsibility (via labelSearchStream.drainAndSort);
// hasMore is read from the stream's limitReached flag, or detected by peeking.
func streamResults(vs mimirstorage.SearcherValueSet, batchSize, limit int, writer *streamingWriter) error {
	results := make([]streaminglabelvalues.SearchResult, 0, batchSize)
	total := 0
	for vs.Next() && (limit == 0 || total < limit) {
		results = append(results, writer.factory(vs.At()))
		total++
		if len(results) >= batchSize {
			if err := writer.writeBatch(results); err != nil {
				return err
			}
			results = results[:0]
		}
	}
	if err := vs.Err(); err != nil {
		if len(results) > 0 {
			_ = writer.writeBatch(results)
		}
		_ = writer.writeError(err)
		return nil
	}
	if len(results) > 0 {
		if err := writer.writeBatch(results); err != nil {
			return err
		}
	}
	// hasMore is true if the Searcher set limitReached (sorted or unsorted eager-limit),
	// or if there is a next item in the stream that we haven't consumed.
	hasMore := false
	if stream, ok := vs.(*labelSearchStream); ok && stream.limitReached {
		hasMore = true
	} else {
		hasMore = vs.Next()
	}
	return writer.writeEnd(hasMore)
}

// parseSearchParams parses common search query parameters from an HTTP request.
func parseSearchParams(r *http.Request, expectLabelName bool) (*searchParams, error) {
	if err := r.ParseForm(); err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	var (
		start int64
		end   int64
		err   error
	)

	if s := r.Form.Get("start"); s != "" {
		start, err = util.ParseTime(s)
		if err != nil {
			return nil, apierror.New(apierror.TypeBadData, "invalid parameter \"start\": "+err.Error())
		}
	} else {
		start = v1.MinTime.UnixMilli()
	}

	if s := r.Form.Get("end"); s != "" {
		end, err = util.ParseTime(s)
		if err != nil {
			return nil, apierror.New(apierror.TypeBadData, "invalid parameter \"end\": "+err.Error())
		}
	} else {
		end = v1.MaxTime.UnixMilli()
	}

	var matcherSets []*labels.Matcher
	for _, s := range r.Form[streaminglabelvalues.MatcherParam] {
		matcherSets, err = promqlext.NewPromQLParser().ParseMetricSelector(s)
		if err != nil {
			return nil, apierror.New(apierror.TypeBadData, "invalid parameter \"match[]\": "+err.Error())
		}
	}

	p := streaminglabelvalues.NewRequestParser(r.Form)

	params := &searchParams{
		start:    start,
		end:      end,
		matchers: matcherSets,
		search:   p.Search(),
	}

	if params.fuzzThreshold, err = p.FuzzThreshold(); err != nil {
		return nil, err
	}
	if params.fuzzAlg, err = p.FuzzAlgorithm(); err != nil {
		return nil, err
	}
	if params.caseSensitive, err = p.CaseSensitive(); err != nil {
		return nil, err
	}
	if params.includeScore, err = p.IncludeScore(); err != nil {
		return nil, err
	}
	if params.batchSize, err = p.BatchSize(); err != nil {
		return nil, err
	}
	if params.sortBy, err = p.SortBy(); err != nil {
		return nil, err
	}
	if params.sortDir, err = p.SortDir(); err != nil {
		return nil, err
	}
	if params.limit, err = p.Limit(); err != nil {
		return nil, err
	}
	if params.operation, err = p.Operator(); err != nil {
		return nil, err
	}
	if params.labelName, err = p.LabelName(expectLabelName); err != nil {
		return nil, err
	}

	return params, nil
}

// writeSearchError writes an API error response with the appropriate HTTP status code.
func writeSearchError(w http.ResponseWriter, err error) {
	var apiErr *apierror.APIError
	if errors.As(err, &apiErr) && apiErr.Type == apierror.TypeBadData {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

// batchMessage is the JSON envelope written by writeBatch.
type batchMessage struct {
	Results []streaminglabelvalues.SearchResult `json:"results"`
}

type streamingWriter struct {
	writer   http.ResponseWriter
	flusher  http.Flusher
	canFlush bool
	encoder  *json.Encoder
	factory  streaminglabelvalues.SearchResultFactory
}

func newStreamingWriter(w http.ResponseWriter, factory streaminglabelvalues.SearchResultFactory) *streamingWriter {
	writer := &streamingWriter{writer: w, encoder: json.NewEncoder(w), factory: factory}
	writer.flusher, writer.canFlush = w.(http.Flusher)
	return writer
}

func (w *streamingWriter) init() {
	w.writer.Header().Set("Content-Type", "application/x-ndjson")
	w.writer.Header().Set(worker.ResponseStreamingEnabledHeader, "true")
}

func (w *streamingWriter) writeBatch(results []streaminglabelvalues.SearchResult) error {
	if err := w.encoder.Encode(batchMessage{Results: results}); err != nil {
		return err
	}
	if w.canFlush {
		w.flusher.Flush()
	}
	return nil
}

func (w *streamingWriter) writeEnd(hasMore bool) error {
	if err := w.encoder.Encode(map[string]any{"status": "success", "has_more": hasMore}); err != nil {
		return err
	}
	if w.canFlush {
		w.flusher.Flush()
	}
	return nil
}

func (w *streamingWriter) writeError(err error) error {
	if err = w.encoder.Encode(map[string]any{"status": "error", "error": err.Error()}); err != nil {
		return err
	}
	if w.canFlush {
		w.flusher.Flush()
	}
	return nil
}
