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
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/promqlext"
	"github.com/grafana/mimir/pkg/util/validation"
)


// searchParams holds parsed parameters common to all search endpoints.
type searchParams struct {
	start         int64
	end           int64
	matchers      [][]*labels.Matcher
	search        []string
	fuzzThreshold int
	fuzzAlg       string
	caseSensitive bool
	sortBy        streaminglabelvalues.SortBy
	sortDir       streaminglabelvalues.SortDirection
	batchSize     int
	limit         int
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

		var combined []string
		if len(params.matchers) == 0 {
			values, _, err := q.LabelValues(ctx, model.MetricNameLabel, &storage.LabelHints{}, nil...)
			if err != nil {
				respondFromError(err, w)
				return
			}
			combined = values
		} else {
			seen := map[string]struct{}{}
			for _, ms := range params.matchers {
				values, _, err := q.LabelValues(ctx, model.MetricNameLabel, &storage.LabelHints{}, ms...)
				if err != nil {
					respondFromError(err, w)
					return
				}
				for _, v := range values {
					if _, ok := seen[v]; !ok {
						seen[v] = struct{}{}
						combined = append(combined, v)
					}
				}
			}
		}

		filtered := filterSearchValues(combined, params)
		writeSearchNDJSON(w, filtered, params.batchSize, func(v string) any {
			return map[string]string{"metric_name": v}
		})
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

		var combined []string
		if len(params.matchers) == 0 {
			values, _, err := q.LabelNames(ctx, &storage.LabelHints{})
			if err != nil {
				respondFromError(err, w)
				return
			}
			combined = values
		} else {
			seen := map[string]struct{}{}
			for _, ms := range params.matchers {
				values, _, err := q.LabelNames(ctx, &storage.LabelHints{}, ms...)
				if err != nil {
					respondFromError(err, w)
					return
				}
				for _, v := range values {
					if _, ok := seen[v]; !ok {
						seen[v] = struct{}{}
						combined = append(combined, v)
					}
				}
			}
		}

		filtered := filterSearchValues(combined, params)
		writeSearchNDJSON(w, filtered, params.batchSize, func(v string) any {
			return map[string]string{"label_name": v}
		})
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

		var combined []string
		if len(params.matchers) == 0 {
			values, _, err := q.LabelValues(ctx, labelName, &storage.LabelHints{}, nil...)
			if err != nil {
				respondFromError(err, w)
				return
			}
			combined = values
		} else {
			seen := map[string]struct{}{}
			for _, ms := range params.matchers {
				values, _, err := q.LabelValues(ctx, labelName, &storage.LabelHints{}, ms...)
				if err != nil {
					respondFromError(err, w)
					return
				}
				for _, v := range values {
					if _, ok := seen[v]; !ok {
						seen[v] = struct{}{}
						combined = append(combined, v)
					}
				}
			}
		}

		filtered := filterSearchValues(combined, params)
		writeSearchNDJSON(w, filtered, params.batchSize, func(v string) any {
			return map[string]string{"label_value": v}
		})
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

	var matcherSets [][]*labels.Matcher
	for _, s := range r.Form[streaminglabelvalues.MatcherParam] {
		ms, err := promqlext.NewPromQLParser().ParseMetricSelector(s)
		if err != nil {
			return nil, apierror.New(apierror.TypeBadData, "invalid parameter \"match[]\": "+err.Error())
		}
		matcherSets = append(matcherSets, ms)
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

// filterSearchValues filters a list of values by the search[] terms (case-insensitive substring match by default).
func filterSearchValues(values []string, params *searchParams) []string {
	if len(params.search) == 0 {
		return values
	}
	out := values[:0:0]
	for _, v := range values {
		cmp := v
		if !params.caseSensitive {
			cmp = strings.ToLower(cmp)
		}
		for _, s := range params.search {
			term := s
			if !params.caseSensitive {
				term = strings.ToLower(term)
			}
			if strings.Contains(cmp, term) {
				out = append(out, v)
				break
			}
		}
	}
	return out
}

// writeSearchNDJSON streams results as NDJSON. Each chunk contains up to batchSize results,
// followed by a final status chunk: {"status":"success","has_more":false}.
func writeSearchNDJSON(w http.ResponseWriter, values []string, batchSize int, toResult func(string) any) {
	f, canFlush := w.(http.Flusher)
	w.Header().Set("Content-Type", "application/x-ndjson")

	enc := json.NewEncoder(w)

	for i := 0; i < len(values); i += batchSize {
		end := i + batchSize
		if end > len(values) {
			end = len(values)
		}
		batch := values[i:end]
		results := make([]any, len(batch))
		for j, v := range batch {
			results[j] = toResult(v)
		}
		_ = enc.Encode(map[string]any{"results": results})
		if canFlush {
			f.Flush()
		}
	}

	hasMore := false
	_ = enc.Encode(map[string]any{"status": "success", "has_more": hasMore})
	if canFlush {
		f.Flush()
	}
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
