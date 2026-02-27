// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"

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
	"github.com/grafana/mimir/pkg/util/validation"
)

// searchParams holds parsed parameters common to all search endpoints.
type searchParams struct {
	start         int64
	end           int64
	matchers      []*labels.Matcher
	search        []string
	fuzzThreshold int
	fuzzAlg       string
	caseSensitive bool
	sortBy        streaminglabelvalues.SortBy
	sortDir       streaminglabelvalues.SortDirection
	operation     streaminglabelvalues.Operator
	batchSize     int
	limit         int
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

// searcherFor returns the querier's native Searcher implementation if available,
// otherwise wraps it in a StreamingSearch that provides the same interface.
func searcherFor(q storage.Querier) mimirstorage.Searcher {
	if s, ok := q.(mimirstorage.Searcher); ok {
		return s
	}
	return streaminglabelvalues.NewStreamingSearch(q)
}

// SearchMetricNamesHandler returns an HTTP handler for GET/POST /api/v1/search/metric_names.
func SearchMetricNamesHandler(queryable storage.SampleAndChunkQueryable, _ *validation.Overrides) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		_, err := tenant.TenantID(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		params, err := parseSearchParams(r)
		if err != nil {
			writeSearchError(w, err)
			return
		}

		q, err := queryable.Querier(params.start, params.end)
		if err != nil {
			respondFromError(err, w)
			return
		}
		defer q.Close()

		searcher := searcherFor(q)
		hints := buildSearchHints(params)

		vs, err := searcher.SearchLabelValues(ctx, model.MetricNameLabel, hints, params.matchers...)
		if err != nil {
			// we have not started the stream, so we can return with a 4xx error
			respondFromError(err, w)
			return
		}
		defer vs.Close()

		writer := newStreamingWriter(w)
		writer.init()

		streamSearchResults(vs, hints, params.batchSize, params.limit, writer)
	})
}

// SearchLabelNamesHandler returns an HTTP handler for GET/POST /api/v1/search/label_names.
func SearchLabelNamesHandler(queryable storage.SampleAndChunkQueryable, _ *validation.Overrides) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		_, err := tenant.TenantID(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		params, err := parseSearchParams(r)
		if err != nil {
			writeSearchError(w, err)
			return
		}

		q, err := queryable.Querier(params.start, params.end)
		if err != nil {
			respondFromError(err, w)
			return
		}
		defer q.Close()

		searcher := searcherFor(q)
		hints := buildSearchHints(params)

		vs, err := searcher.SearchLabelNames(ctx, hints, params.matchers...)
		if err != nil {
			// we have not started the stream, so we can return with a 4xx error
			respondFromError(err, w)
			return
		}
		defer vs.Close()

		writer := newStreamingWriter(w)
		writer.init()

		streamSearchResults(vs, hints, params.batchSize, params.limit, writer)
	})
}

// SearchLabelValuesHandler returns an HTTP handler for GET/POST /api/v1/search/label_values.
func SearchLabelValuesHandler(queryable storage.SampleAndChunkQueryable, _ *validation.Overrides) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		_, err := tenant.TenantID(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		labelName := r.Form.Get("label_name")
		if labelName == "" {
			http.Error(w, `missing required parameter "label_name"`, http.StatusBadRequest)
			return
		}

		params, err := parseSearchParams(r)
		if err != nil {
			writeSearchError(w, err)
			return
		}

		q, err := queryable.Querier(params.start, params.end)
		if err != nil {
			respondFromError(err, w)
			return
		}
		defer q.Close()

		searcher := searcherFor(q)
		hints := buildSearchHints(params)

		vs, err := searcher.SearchLabelValues(ctx, labelName, hints, params.matchers...)
		if err != nil {
			// we have not started the stream, so we can return with a 4xx error
			respondFromError(err, w)
			return
		}
		defer vs.Close()

		writer := newStreamingWriter(w)
		writer.init()

		streamSearchResults(vs, hints, params.batchSize, params.limit, writer)
	})
}

// streamSearchResults writes results from vs to writer.
// If hints.Compare is set, all values are accumulated and sorted before applying the
// limit and writing in batches. Otherwise the limit is enforced during streaming.
func streamSearchResults(vs mimirstorage.SearcherValueSet, hints *mimirstorage.SearchHints, batchSize, limit int, writer *streamingWriter) {
	if hints.Compare != nil {
		streamSorted(vs, hints, batchSize, limit, writer)
	} else {
		streamUnsorted(vs, batchSize, limit, writer)
	}
}

// streamSorted accumulates all values, sorts them with hints.Compare, applies the limit,
// then writes in batches.
func streamSorted(vs mimirstorage.SearcherValueSet, hints *mimirstorage.SearchHints, batchSize, limit int, writer *streamingWriter) {
	all := make([]mimirstorage.FilteredResult, 0, 1000) // TODO proper memory allocation
	for vs.Next() {
		v := vs.At()
		all = append(all, v)
		// TODO perodically check for error / cancel
	}
	if err := vs.Err(); err != nil {
		_ = writer.writeError(err)
		return
	}

	slices.SortFunc(all, hints.Compare.Compare)

	hasMore := limit > 0 && len(all) > limit
	if hasMore {
		all = all[:limit]
	}

	for i := 0; i < len(all); i += batchSize {
		end := i + batchSize
		if end > len(all) {
			end = len(all)
		}
		batch := make([]mimirstorage.FilteredResult, end-i)
		for j, r := range all[i:end] {
			batch[j] = r
		}
		if err := writer.writeBatch(batch); err != nil {
			return
		}
	}
	writer.writeEnd(hasMore)
}

// streamUnsorted streams values from vs directly, enforcing the limit during iteration.
func streamUnsorted(vs mimirstorage.SearcherValueSet, batchSize, limit int, writer *streamingWriter) {
	batch := make([]mimirstorage.FilteredResult, 0, batchSize)
	total := 0
	for vs.Next() && (limit == 0 || total < limit) {
		v := vs.At()
		batch = append(batch, v)
		if len(batch) >= batchSize {
			if err := writer.writeBatch(batch); err != nil {
				return
			}
			batch = batch[:0]
		}
		total++
	}
	if err := vs.Err(); err != nil {
		if len(batch) > 0 {
			_ = writer.writeBatch(batch)
		}
		_ = writer.writeError(err)
		return
	}
	if len(batch) > 0 {
		if err := writer.writeBatch(batch); err != nil {
			return
		}
	}
	writer.writeEnd(vs.Next())
}

// parseSearchParams parses common search query parameters from an HTTP request.
func parseSearchParams(r *http.Request) (*searchParams, error) {
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

type streamingWriter struct {
	writer   http.ResponseWriter
	flusher  http.Flusher
	canFlush bool
	encoder  *json.Encoder
}

func newStreamingWriter(w http.ResponseWriter) *streamingWriter {
	writer := &streamingWriter{writer: w, encoder: json.NewEncoder(w)}
	writer.flusher, writer.canFlush = w.(http.Flusher)
	return writer
}

func (w *streamingWriter) init() {
	w.writer.Header().Set("Content-Type", "application/x-ndjson")
	w.writer.Header().Set(worker.ResponseStreamingEnabledHeader, "true")
}

func (w *streamingWriter) writeBatch(batch []mimirstorage.FilteredResult) error {

	xformed := make([]streaminglabelvalues.SearchResult, 0, len(batch))
	for _, r := range batch {
		xformed = append(xformed, streaminglabelvalues.SearchResult{Name: r.Value})
	}

	if err := w.encoder.Encode(map[string]any{"results": xformed}); err != nil {
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
