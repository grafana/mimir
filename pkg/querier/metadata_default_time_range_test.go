// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestMetadataDefaultTimeRange(t *testing.T) {
	limits := validation.MockOverrides(func(_ *validation.Limits, tenantLimits map[string]*validation.Limits) {
		tenantLimits["one-hour"] = validation.MockDefaultLimits()
		tenantLimits["one-hour"].MetadataDefaultTimeRange = model.Duration(time.Hour)
		tenantLimits["two-hours"] = validation.MockDefaultLimits()
		tenantLimits["two-hours"].MetadataDefaultTimeRange = model.Duration(2 * time.Hour)
	})

	for name, tc := range map[string]struct {
		ctx      context.Context
		expected time.Duration
	}{
		"no tenant": {
			ctx:      t.Context(),
			expected: 0,
		},
		"tenant without override": {
			ctx:      user.InjectOrgID(t.Context(), "other"),
			expected: 0,
		},
		"tenant with override": {
			ctx:      user.InjectOrgID(t.Context(), "two-hours"),
			expected: 2 * time.Hour,
		},
		"many tenants use the smallest non-zero value": {
			ctx:      user.InjectOrgID(t.Context(), "other|two-hours|one-hour"),
			expected: time.Hour,
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, MetadataDefaultTimeRange(tc.ctx, limits))
		})
	}
}

func TestMetadataDefaultTimeRange_ServerWideDefaultWithTenantOverrides(t *testing.T) {
	limits := validation.MockOverrides(func(defaults *validation.Limits, tenantLimits map[string]*validation.Limits) {
		defaults.MetadataDefaultTimeRange = model.Duration(time.Hour)
		tenantLimits["two-hours"] = validation.MockDefaultLimits()
		tenantLimits["two-hours"].MetadataDefaultTimeRange = model.Duration(2 * time.Hour)
		tenantLimits["no-default-time-range"] = validation.MockDefaultLimits()
		tenantLimits["no-default-time-range"].MetadataDefaultTimeRange = 0
	})

	assert.Equal(t, time.Hour, MetadataDefaultTimeRange(user.InjectOrgID(t.Context(), "no-override"), limits))
	assert.Equal(t, 2*time.Hour, MetadataDefaultTimeRange(user.InjectOrgID(t.Context(), "two-hours"), limits))
	assert.Equal(t, time.Duration(0), MetadataDefaultTimeRange(user.InjectOrgID(t.Context(), "no-default-time-range"), limits))
}

func TestMetadataDefaultTimeRangeMiddleware_PrometheusAPI(t *testing.T) {
	const defaultTimeRange = 2 * time.Hour
	limits := validation.MockOverrides(func(_ *validation.Limits, tenantLimits map[string]*validation.Limits) {
		tenantLimits["enabled"] = validation.MockDefaultLimits()
		tenantLimits["enabled"].MetadataDefaultTimeRange = model.Duration(defaultTimeRange)
	})

	for name, tc := range map[string]struct {
		tenant string
		method string
		target string
		body   url.Values

		// endIsNow means that the querier must receive an end between the times before and after the request.
		endIsNow bool
		endMs    int64
		// startIsDefault means that the querier must receive a start of end minus the default time range.
		startIsDefault bool
		startMs        int64
		// startDeltaMs is the error that the conversion of a very large start to Unix seconds can add.
		startDeltaMs    float64
		expectedWarning bool
	}{
		"label names, no start and no end": {
			tenant:          "enabled",
			method:          http.MethodGet,
			target:          "/api/v1/labels",
			endIsNow:        true,
			startIsDefault:  true,
			expectedWarning: true,
		},
		"label names, only end": {
			tenant:          "enabled",
			method:          http.MethodGet,
			target:          "/api/v1/labels?end=10800",
			endMs:           10800_000,
			startMs:         3600_000,
			expectedWarning: true,
		},
		"label names, only start": {
			tenant:          "enabled",
			method:          http.MethodGet,
			target:          "/api/v1/labels?start=3600",
			endIsNow:        true,
			startMs:         3600_000,
			expectedWarning: true,
		},
		"label names, only end at the formatted Prometheus MaxTime": {
			tenant:          "enabled",
			method:          http.MethodGet,
			target:          "/api/v1/labels?end=" + url.QueryEscape(v1.MaxTime.Format(time.RFC3339Nano)),
			endMs:           v1.MaxTime.UnixMilli(),
			startIsDefault:  true,
			startDeltaMs:    2000,
			expectedWarning: true,
		},
		"label names, only start at the formatted Prometheus MinTime": {
			tenant:          "enabled",
			method:          http.MethodGet,
			target:          "/api/v1/labels?start=" + url.QueryEscape(v1.MinTime.Format(time.RFC3339Nano)),
			endIsNow:        true,
			startMs:         v1.MinTime.UnixMilli(),
			expectedWarning: true,
		},
		"label names, only end in the year 10000": {
			tenant:          "enabled",
			method:          http.MethodGet,
			target:          "/api/v1/labels?end=253402387200",
			endMs:           253402387200_000,
			startIsDefault:  true,
			expectedWarning: true,
		},
		"label names, start and end": {
			tenant:  "enabled",
			method:  http.MethodGet,
			target:  "/api/v1/labels?start=3600&end=7200",
			endMs:   7200_000,
			startMs: 3600_000,
		},
		"label names, POST form with only end": {
			tenant:          "enabled",
			method:          http.MethodPost,
			target:          "/api/v1/labels",
			body:            url.Values{"end": []string{"10800"}},
			endMs:           10800_000,
			startMs:         3600_000,
			expectedWarning: true,
		},
		"label values, only end": {
			tenant:          "enabled",
			method:          http.MethodGet,
			target:          "/api/v1/label/job/values?end=10800",
			endMs:           10800_000,
			startMs:         3600_000,
			expectedWarning: true,
		},
		"series, no start and no end": {
			tenant:          "enabled",
			method:          http.MethodGet,
			target:          "/api/v1/series?match[]=up",
			endIsNow:        true,
			startIsDefault:  true,
			expectedWarning: true,
		},
		"series, POST form with only end": {
			tenant:          "enabled",
			method:          http.MethodPost,
			target:          "/api/v1/series",
			body:            url.Values{"match[]": []string{"up"}, "end": []string{"10800"}},
			endMs:           10800_000,
			startMs:         3600_000,
			expectedWarning: true,
		},
		"tenant without override queries all data": {
			tenant:  "no-default-time-range",
			method:  http.MethodGet,
			target:  "/api/v1/labels",
			endMs:   v1.MaxTime.UnixMilli(),
			startMs: v1.MinTime.UnixMilli(),
		},
	} {
		t.Run(name, func(t *testing.T) {
			queryable := &recordingMetadataQueryable{}
			handler := NewMetadataDefaultTimeRangeMiddleware(limits).Wrap(createPrometheusAPI(NewMetadataDefaultTimeRangeQueryable(queryable)))

			req := newMetadataRequest(t, tc.method, tc.target, tc.body, tc.tenant)
			rec := httptest.NewRecorder()
			before := time.Now().UnixMilli()
			handler.ServeHTTP(rec, req)
			after := time.Now().UnixMilli()

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			resp := decodeMetadataResponse(t, rec.Body.Bytes())
			require.Equal(t, "success", resp.Status)

			startMs, endMs, calls := queryable.lastRange()
			require.Equal(t, 1, calls)
			if tc.endIsNow {
				assert.GreaterOrEqual(t, endMs, before)
				assert.LessOrEqual(t, endMs, after)
			} else {
				assert.Equal(t, tc.endMs, endMs)
			}
			expectedStartMs := tc.startMs
			if tc.startIsDefault {
				expectedStartMs = endMs - defaultTimeRange.Milliseconds()
			}
			if tc.startDeltaMs > 0 {
				assert.InDelta(t, expectedStartMs, startMs, tc.startDeltaMs)
			} else {
				assert.Equal(t, expectedStartMs, startMs)
			}

			if tc.expectedWarning {
				assert.Equal(t, []string{newMetadataDefaultTimeRangeWarning(expectedStartMs, endMs).Error()}, resp.Warnings)
			} else {
				assert.Empty(t, resp.Warnings)
			}
		})
	}
}

func TestMetadataDefaultTimeRangeMiddleware_Errors(t *testing.T) {
	limits := validation.MockOverrides(func(_ *validation.Limits, tenantLimits map[string]*validation.Limits) {
		tenantLimits["enabled"] = validation.MockDefaultLimits()
		tenantLimits["enabled"].MetadataDefaultTimeRange = model.Duration(time.Hour)
	})

	for name, tc := range map[string]struct {
		target        string
		expectedError string
	}{
		"Prometheus reports an invalid start": {
			target:        "/api/v1/labels?start=invalid",
			expectedError: `invalid parameter "start": invalid time value for 'start': cannot parse "invalid" to a valid timestamp`,
		},
		"the middleware reports a form that does not parse": {
			target:        "/api/v1/labels?start=%zz",
			expectedError: `error parsing form values: invalid URL escape "%zz"`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			queryable := &recordingMetadataQueryable{}
			handler := NewMetadataDefaultTimeRangeMiddleware(limits).Wrap(createPrometheusAPI(NewMetadataDefaultTimeRangeQueryable(queryable)))

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, newMetadataRequest(t, http.MethodGet, tc.target, nil, "enabled"))

			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			resp := decodeMetadataResponse(t, rec.Body.Bytes())
			assert.Equal(t, "error", resp.Status)
			assert.Equal(t, "bad_data", resp.ErrorType)
			assert.Equal(t, tc.expectedError, resp.Error)
			_, _, calls := queryable.lastRange()
			assert.Zero(t, calls)
		})
	}
}

func TestMetadataDefaultTimeRangeQuerier(t *testing.T) {
	warning := errors.New("default range warning")
	innerWarning := errors.New("inner warning")
	ctxWithWarning := context.WithValue(t.Context(), metadataDefaultTimeRangeWarningKey{}, warning)

	queryable := NewMetadataDefaultTimeRangeQueryable(&recordingMetadataQueryable{innerWarning: innerWarning})
	q, err := queryable.Querier(0, 1)
	require.NoError(t, err)

	t.Run("label names", func(t *testing.T) {
		_, warnings, err := q.LabelNames(ctxWithWarning, nil)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{warning.Error(), innerWarning.Error()}, warningStrings(warnings))

		_, warnings, err = q.LabelNames(t.Context(), nil)
		require.NoError(t, err)
		assert.Equal(t, []string{innerWarning.Error()}, warningStrings(warnings))
	})

	t.Run("label values", func(t *testing.T) {
		_, warnings, err := q.LabelValues(ctxWithWarning, "job", nil)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{warning.Error(), innerWarning.Error()}, warningStrings(warnings))
	})

	t.Run("series", func(t *testing.T) {
		set := q.Select(ctxWithWarning, false, &storage.SelectHints{Func: "series"})
		assert.ElementsMatch(t, []string{warning.Error(), innerWarning.Error()}, warningStrings(set.Warnings()))
	})

	t.Run("select that is not for the series endpoint", func(t *testing.T) {
		set := q.Select(ctxWithWarning, false, &storage.SelectHints{Func: "rate"})
		assert.Equal(t, []string{innerWarning.Error()}, warningStrings(set.Warnings()))

		set = q.Select(ctxWithWarning, false, nil)
		assert.Equal(t, []string{innerWarning.Error()}, warningStrings(set.Warnings()))
	})
}

func newMetadataRequest(t *testing.T, method, target string, body url.Values, tenantID string) *http.Request {
	t.Helper()
	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, target, strings.NewReader(body.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	} else {
		req = httptest.NewRequest(method, target, nil)
	}
	return req.WithContext(user.InjectOrgID(t.Context(), tenantID))
}

type metadataResponse struct {
	Status    string   `json:"status"`
	ErrorType string   `json:"errorType"`
	Error     string   `json:"error"`
	Warnings  []string `json:"warnings"`
}

func decodeMetadataResponse(t *testing.T, body []byte) metadataResponse {
	t.Helper()
	var resp metadataResponse
	require.NoError(t, json.Unmarshal(body, &resp), string(body))
	return resp
}

func warningStrings(warnings annotations.Annotations) []string {
	out := make([]string, 0, len(warnings))
	for _, w := range warnings {
		out = append(out, w.Error())
	}
	return out
}

// recordingMetadataQueryable records the time range of the last querier that it creates.
type recordingMetadataQueryable struct {
	innerWarning error

	mtx          sync.Mutex
	mint, maxt   int64
	querierCalls int
}

func (q *recordingMetadataQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	q.mint, q.maxt = mint, maxt
	q.querierCalls++
	return recordingMetadataQuerier{innerWarning: q.innerWarning}, nil
}

func (q *recordingMetadataQueryable) ChunkQuerier(int64, int64) (storage.ChunkQuerier, error) {
	return nil, errors.New("not implemented")
}

func (q *recordingMetadataQueryable) lastRange() (int64, int64, int) {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	return q.mint, q.maxt, q.querierCalls
}

type recordingMetadataQuerier struct {
	innerWarning error
}

func (q recordingMetadataQuerier) warnings() annotations.Annotations {
	if q.innerWarning == nil {
		return nil
	}
	var warnings annotations.Annotations
	warnings.Add(q.innerWarning)
	return warnings
}

func (q recordingMetadataQuerier) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return []string{"value"}, q.warnings(), nil
}

func (q recordingMetadataQuerier) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return []string{"job"}, q.warnings(), nil
}

func (q recordingMetadataQuerier) Select(context.Context, bool, *storage.SelectHints, ...*labels.Matcher) storage.SeriesSet {
	return series.NewSeriesSetWithWarnings(storage.EmptySeriesSet(), q.warnings())
}

func (q recordingMetadataQuerier) Close() error {
	return nil
}
