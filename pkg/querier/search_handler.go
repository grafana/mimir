// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	v1 "github.com/prometheus/prometheus/web/api/v1"

	apierror "github.com/grafana/mimir/pkg/api/error"
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
	batchSize     int
	limit         int
}

// substringFilter is a mimirstorage.Filter that accepts values containing any of
// the provided search terms (OR logic, with optional case folding).
type substringFilter struct {
	terms         []string
	caseSensitive bool
}

func (f *substringFilter) Accept(value string) (bool, float64) {
	cmp := value
	if !f.caseSensitive {
		cmp = strings.ToLower(cmp)
	}
	for _, term := range f.terms {
		t := term
		if !f.caseSensitive {
			t = strings.ToLower(t)
		}
		if strings.Contains(cmp, t) {
			return true, 1.0
		}
	}
	return false, 0
}

// buildSearchHints constructs a SearchHints from the parsed search params,
// including a substring filter when search terms are provided.
func buildSearchHints(params *searchParams) *mimirstorage.SearchHints {
	hints := &mimirstorage.SearchHints{Limit: params.limit}
	if len(params.search) > 0 {
		hints.Filter = &substringFilter{
			terms:         params.search,
			caseSensitive: params.caseSensitive,
		}
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

		// TODO proper memory allocation
		batch := make([]string, 0, params.batchSize)
		total := 0
		for vs.Next() && (params.limit == 0 || total < params.limit) {
			v := vs.At()
			batch = append(batch, v)
			if len(batch) >= params.batchSize {
				err := writer.writeBatch(batch)
				if err != nil {
					respondFromError(err, w)
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
		if len(batch) >= 0 {
			err := writer.writeBatch(batch)
			if err != nil {
				respondFromError(err, w)
				return
			}
		}
		writer.writeEnd(vs.Next())
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

		// TODO proper memory allocation
		batch := make([]string, 0, params.batchSize)
		total := 0
		for vs.Next() && (params.limit == 0 || total < params.limit) {
			v := vs.At()
			batch = append(batch, v)
			if len(batch) >= params.batchSize {
				err := writer.writeBatch(batch)
				if err != nil {
					respondFromError(err, w)
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
		if len(batch) >= 0 {
			err := writer.writeBatch(batch)
			if err != nil {
				respondFromError(err, w)
				return
			}
		}
		writer.writeEnd(vs.Next())
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

		// TODO proper memory allocation
		batch := make([]string, 0, params.batchSize)
		total := 0
		for vs.Next() && (params.limit == 0 || total < params.limit) {
			v := vs.At()
			batch = append(batch, v)
			if len(batch) >= params.batchSize {
				err := writer.writeBatch(batch)
				if err != nil {
					respondFromError(err, w)
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
		if len(batch) >= 0 {
			err := writer.writeBatch(batch)
			if err != nil {
				respondFromError(err, w)
				return
			}
		}
		writer.writeEnd(vs.Next())
	})
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
}

func (w *streamingWriter) writeBatch(batch []string) error {
	if err := w.encoder.Encode(map[string]any{"results": batch}); err != nil {
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
