// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
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

type searchExecutor func(ctx context.Context, searcher mimirstorage.MimirSearcher, hints *mimirstorage.MimirSearchHints, params *searchParams) (mimirstorage.SearchResultSet, annotations.Annotations)

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
	sortBy        mimirstorage.SortBy
	sortDir       mimirstorage.SortDirection
	batchSize     int
	limit         int
	labelName     string
}

// buildMimirSearchHints constructs a MimirSearchHints from the parsed search params.
// The raw parameters are stored directly so they can be serialised and sent to remote
// nodes (store-gateway, ingester) without needing a Go closure.
func buildMimirSearchHints(params *searchParams) *mimirstorage.MimirSearchHints {
	return &mimirstorage.MimirSearchHints{
		Search:          params.search,
		CaseInsensitive: !params.caseSensitive,
		FuzzThreshold:   float64(params.fuzzThreshold) / 100.0,
		FuzzAlg:         params.fuzzAlg,
		SortBy:          params.sortBy,
		SortOrder:       params.sortDir,
		Limit:           params.limit,
	}
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

		searcher := q.(mimirstorage.MimirSearcher)
		hints := buildMimirSearchHints(params)

		vs, callWarns := searchExec(ctx, searcher, hints, params)
		defer vs.Close()

		writer := newStreamingWriter(w, streaminglabelvalues.NewSearchResultFactory(params.includeScore))
		writer.init()

		err = streamResults(vs, params.batchSize, params.limit, writer, callWarns)
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
	searchExec := func(ctx context.Context, searcher mimirstorage.MimirSearcher, hints *mimirstorage.MimirSearchHints, params *searchParams) (mimirstorage.SearchResultSet, annotations.Annotations) {
		return searcher.SearchLabelValues(ctx, model.MetricNameLabel, hints, params.matchers...)
	}
	return doSearchHandler(queryable, overrides, logger, "SearchMetricNamesHandler", searchExec, false)
}

// SearchLabelNamesHandler returns an HTTP handler for GET/POST /api/v1/search/label_names.
func SearchLabelNamesHandler(queryable storage.SampleAndChunkQueryable, overrides *validation.Overrides, logger log.Logger) http.Handler {
	searchExec := func(ctx context.Context, searcher mimirstorage.MimirSearcher, hints *mimirstorage.MimirSearchHints, params *searchParams) (mimirstorage.SearchResultSet, annotations.Annotations) {
		return searcher.SearchLabelNames(ctx, hints, params.matchers...)
	}
	return doSearchHandler(queryable, overrides, logger, "SearchLabelNamesHandler", searchExec, false)
}

// SearchLabelValuesHandler returns an HTTP handler for GET/POST /api/v1/search/label_values.
func SearchLabelValuesHandler(queryable storage.SampleAndChunkQueryable, overrides *validation.Overrides, logger log.Logger) http.Handler {
	searchExec := func(ctx context.Context, searcher mimirstorage.MimirSearcher, hints *mimirstorage.MimirSearchHints, params *searchParams) (mimirstorage.SearchResultSet, annotations.Annotations) {
		return searcher.SearchLabelValues(ctx, params.labelName, hints, params.matchers...)
	}
	return doSearchHandler(queryable, overrides, logger, "SearchLabelValuesHandler", searchExec, true)
}

// streamResults writes results from vs to writer in batches of batchSize.
// When limit > 0, iteration stops after limit values have been consumed.
// The loop enforces the limit as a secondary guard; the primary enforcement is in
// KWayMergeValueSets / UnsortedDedupValueSets (which set LimitReached and stop the
// producer goroutine before this loop sees the channel close).
// Sorting is delegated to the downstream Searcher: KWayMergeValueSets performs an
// O(k)-per-step heap merge across pre-sorted sub-Searcher streams.
// hasMore is read from the stream's LimitReached flag, or detected by peeking.
// callWarns contains annotations returned at search-invocation time (e.g. limit clamped).
//
// TODO(warnings): also wire in per-result-stream annotations from intermediate nodes
// (store-gateway, ingester) once those are plumbed through the streaming protocol.
func streamResults(vs mimirstorage.SearchResultSet, batchSize, limit int, writer *streamingWriter, callWarns annotations.Annotations) error {
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
		// Best-effort: flush any buffered results before writing the error frame.
		// Write errors are ignored here because we are already in the error path
		// and the response has been partially written; the error frame takes priority.
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
	type limitReacher interface{ LimitReached() bool }
	if lr, ok := vs.(limitReacher); ok && lr.LimitReached() {
		hasMore = true
	} else {
		hasMore = vs.Next()
	}
	// Merge iteration-time warnings (e.g. limit hit during sorted drain) with call-time warnings.
	allWarns := callWarns.Merge(vs.Warnings())
	return writer.writeEnd(hasMore, allWarns)
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

func (w *streamingWriter) writeEnd(hasMore bool, warns annotations.Annotations) error {
	msg := map[string]any{"status": "success", "has_more": hasMore}
	if len(warns) > 0 {
		warnStrs := make([]string, 0, len(warns))
		for s := range warns {
			warnStrs = append(warnStrs, s)
		}
		sort.Strings(warnStrs)
		msg["warnings"] = warnStrs
	}
	if err := w.encoder.Encode(msg); err != nil {
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
