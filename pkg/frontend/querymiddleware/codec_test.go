// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/marshaling_test.go
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/query_range_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	jsoniter "github.com/json-iterator/go"
	v1Client "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql/parser"
	v1API "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/util/chunkinfologger"
	testutil "github.com/grafana/mimir/pkg/util/test"
)

var (
	matrix = model.ValMatrix.String()
)

func parseQuery(t require.TestingT, query string) parser.Expr {
	queryExpr, err := promqlext.NewPromQLParser().ParseExpr(query)
	require.NoError(t, err)
	return queryExpr
}

func withQuery(t require.TestingT, req MetricsQueryRequest, query string) MetricsQueryRequest {
	req, err := req.WithQuery(query)
	require.NoError(t, err)
	return req
}

// requireEqualMetricsQueryRequest solves for the limitation that testify asserts do not always
// recognize the Prometheus parser.Parsed representations for two equivalent queries as equal;
// the string-formatted representation of the parsed query is used instead as this is stable.
func requireEqualMetricsQueryRequest(t *testing.T, expected, actual MetricsQueryRequest) {
	require.Equal(t, expected.GetPath(), actual.GetPath())
	require.Equal(t, expected.GetStart(), actual.GetStart())
	require.Equal(t, expected.GetEnd(), actual.GetEnd())
	require.Equal(t, expected.GetStep(), actual.GetStep())
	require.Equal(t, expected.GetQuery(), actual.GetQuery())
	require.Equal(t, expected.GetMinT(), actual.GetMinT())
	require.Equal(t, expected.GetMaxT(), actual.GetMaxT())
	require.Equal(t, expected.GetOptions(), actual.GetOptions())
	require.Equal(t, expected.GetHints(), actual.GetHints())
}

func TestCodec_EncodeMetricsQueryRequest(t *testing.T) {
	codec := newTestCodec()

	for i, tc := range []struct {
		url         string
		expected    MetricsQueryRequest
		expectedErr error
	}{
		{
			url: "/api/v1/query_range?end=1536716880&query=sum+by+%28namespace%29+%28container_memory_rss%29&start=1536673680&step=120",
			expected: NewPrometheusRangeQueryRequest(
				"/api/v1/query_range",
				nil,
				1536673680*1e3,
				1536716880*1e3,
				(2 * time.Minute).Milliseconds(),
				0,
				parseQuery(t, "sum(container_memory_rss) by (namespace)"),
				Options{},
				nil,
				"",
			),
		},
		// Same as above, but with stats=all.
		{
			url: "/api/v1/query_range?end=1536716880&query=sum+by+%28namespace%29+%28container_memory_rss%29&start=1536673680&stats=all&step=120",
			expected: NewPrometheusRangeQueryRequest(
				"/api/v1/query_range",
				nil,
				1536673680*1e3,
				1536716880*1e3,
				(2 * time.Minute).Milliseconds(),
				0,
				parseQuery(t, "sum(container_memory_rss) by (namespace)"),
				Options{},
				nil,
				"all",
			),
		},
		{
			url: "/api/v1/query?query=sum+by+%28namespace%29+%28container_memory_rss%29&time=1536716880",
			expected: NewPrometheusInstantQueryRequest(
				"/api/v1/query",
				nil,
				1536716880*1e3,
				0*time.Minute,
				parseQuery(t, "sum(container_memory_rss) by (namespace)"),
				Options{},
				nil,
				"",
			),
		},
		{
			url:         "/api/v1/query_range?start=foo",
			expectedErr: apierror.New(apierror.TypeBadData, "invalid parameter \"start\": cannot parse \"foo\" to a valid timestamp"),
		},
		{
			url:         "/api/v1/query_range?start=123&end=bar",
			expectedErr: apierror.New(apierror.TypeBadData, "invalid parameter \"end\": cannot parse \"bar\" to a valid timestamp"),
		},
		{
			url:         "/api/v1/query_range?start=123&end=0",
			expectedErr: errEndBeforeStart,
		},
		{
			url:         "/api/v1/query_range?start=123&end=456&step=baz",
			expectedErr: apierror.New(apierror.TypeBadData, "invalid parameter \"step\": cannot parse \"baz\" to a valid duration"),
		},
		{
			url:         "/api/v1/query_range?start=123&end=456&step=-1",
			expectedErr: errNegativeStep,
		},
		{
			url:         "/api/v1/query_range?start=0&end=11001&step=1",
			expectedErr: errStepTooSmall,
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			const userID = "user-1"

			r, err := http.NewRequest("GET", tc.url, nil)
			require.NoError(t, err)

			ctx := user.InjectOrgID(context.Background(), userID)
			r = r.WithContext(ctx)

			req, err := codec.DecodeMetricsQueryRequest(ctx, r)
			if err != nil || tc.expectedErr != nil {
				require.Equal(t, tc.expectedErr, err)
				return
			}
			requireEqualMetricsQueryRequest(t, tc.expected, req)

			encodedReq, err := codec.EncodeMetricsQueryRequest(ctx, req)
			require.NoError(t, err)
			require.Equal(t, tc.url, encodedReq.RequestURI)

			actualUserID, _, err := user.ExtractOrgIDFromHTTPRequest(encodedReq)
			require.NoError(t, err)
			require.Equal(t, userID, actualUserID)
		})
	}
}

func TestMetricsQuery_MinMaxTime(t *testing.T) {

	startTime, err := time.Parse(time.RFC3339, "2024-02-21T00:00:00-08:00")
	require.NoError(t, err)
	endTime, err := time.Parse(time.RFC3339, "2024-02-22T00:00:00-08:00")
	require.NoError(t, err)

	atModifierDuration := 10 * time.Minute

	stepDurationStr := "60s"
	stepDuration, _ := time.ParseDuration(stepDurationStr)

	rangeVectorDurationStr := "10m"
	rangeVectorDuration, _ := time.ParseDuration(rangeVectorDurationStr)
	rangeVectorDurationMS := rangeVectorDuration.Milliseconds()

	offsetDurationStr := "1h"
	offsetDuration, _ := time.ParseDuration(offsetDurationStr)
	offsetDurationMS := offsetDuration.Milliseconds()

	loobackDurationStr := "5m"
	lookbackDuration, _ := time.ParseDuration(loobackDurationStr)
	lookbackDurationMS := lookbackDuration.Milliseconds()

	rangeRequest := NewPrometheusRangeQueryRequest(
		"/api/v1/query_range",
		nil,
		startTime.UnixMilli(),
		endTime.UnixMilli(),
		stepDuration.Milliseconds(),
		lookbackDuration,
		parseQuery(t, "go_goroutines{}"),
		Options{},
		nil,
		"",
	)
	instantRequest := NewPrometheusInstantQueryRequest(
		"/api/v1/query",
		nil,
		endTime.UnixMilli(),
		lookbackDuration,
		parseQuery(t, "go_goroutines{}"),
		Options{},
		nil,
		"",
	)

	for _, testCase := range []struct {
		name         string
		metricsQuery MetricsQueryRequest
		expectedMinT int64
		expectedMaxT int64
	}{
		// permutations with and without range vectors and offsets
		{
			name:         "range query: without range vector, without offset",
			metricsQuery: rangeRequest,
			expectedMinT: startTime.UnixMilli() - lookbackDurationMS + 1,
			expectedMaxT: endTime.UnixMilli(),
		},
		{
			name:         "instant query: without range vector, without offset",
			metricsQuery: instantRequest,
			expectedMinT: endTime.UnixMilli() - lookbackDurationMS + 1,
			expectedMaxT: endTime.UnixMilli(),
		},
		{
			name:         "range query: with range vector, without offset",
			metricsQuery: withQuery(t, rangeRequest, fmt.Sprintf("rate(go_goroutines{}[%s])", rangeVectorDurationStr)),
			expectedMinT: startTime.UnixMilli() - rangeVectorDurationMS + 1, // lookback duration not used with range vectors
			expectedMaxT: endTime.UnixMilli(),
		},
		{
			name:         "instant query: with range vector, without offset",
			metricsQuery: withQuery(t, instantRequest, fmt.Sprintf("rate(go_goroutines{}[%s])", rangeVectorDurationStr)),
			expectedMinT: endTime.UnixMilli() - rangeVectorDurationMS + 1, // lookback duration not used with range vectors
			expectedMaxT: endTime.UnixMilli(),
		},
		{
			name:         "range query: without range vector, with offset",
			metricsQuery: withQuery(t, rangeRequest, fmt.Sprintf("go_goroutines{} offset %s", offsetDurationStr)),
			expectedMinT: startTime.UnixMilli() - offsetDurationMS - lookbackDurationMS + 1,
			expectedMaxT: endTime.UnixMilli() - offsetDurationMS,
		},
		{
			name:         "instant query: without range vector, with offset",
			metricsQuery: withQuery(t, instantRequest, fmt.Sprintf("go_goroutines{} offset %s", offsetDurationStr)),
			expectedMinT: endTime.UnixMilli() - offsetDurationMS - lookbackDurationMS + 1,
			expectedMaxT: endTime.UnixMilli() - offsetDurationMS,
		},
		{
			name:         "range query: with range vector, with offset",
			metricsQuery: withQuery(t, rangeRequest, fmt.Sprintf("rate(go_goroutines{}[%s] offset %s)", rangeVectorDurationStr, offsetDurationStr)),
			expectedMinT: startTime.UnixMilli() - rangeVectorDurationMS - offsetDurationMS + 1, // lookback duration not used with range vectors
			expectedMaxT: endTime.UnixMilli() - offsetDurationMS,
		},
		{
			name:         "instant query: with range vector, with offset",
			metricsQuery: withQuery(t, instantRequest, fmt.Sprintf("rate(go_goroutines{}[%s] offset %s)", rangeVectorDurationStr, offsetDurationStr)),
			expectedMinT: endTime.UnixMilli() - rangeVectorDurationMS - offsetDurationMS + 1, // lookback duration not used with range vectors
			expectedMaxT: endTime.UnixMilli() - offsetDurationMS,
		},
		// permutations with and without range vectors and @ modifiers
		{
			name:         "range query: with @ modifer",
			metricsQuery: withQuery(t, rangeRequest, fmt.Sprintf("go_goroutines{} @ %d", endTime.Add(-atModifierDuration).Unix())),
			expectedMinT: endTime.Add(-atModifierDuration).UnixMilli() - lookbackDurationMS + 1,
			expectedMaxT: endTime.Add(-atModifierDuration).UnixMilli(),
		},
		{
			name:         "instant query: with @ modifer",
			metricsQuery: withQuery(t, instantRequest, fmt.Sprintf("go_goroutines{} @ %d", endTime.Add(-atModifierDuration).Unix())),
			expectedMinT: endTime.Add(-atModifierDuration).UnixMilli() - lookbackDurationMS + 1,
			expectedMaxT: endTime.Add(-atModifierDuration).UnixMilli(),
		},
		{
			name:         "range query: with range vector, with @ modifer",
			metricsQuery: withQuery(t, rangeRequest, fmt.Sprintf("go_goroutines{}[%s] @ %d", rangeVectorDurationStr, endTime.Add(-atModifierDuration).Unix())),
			expectedMinT: endTime.Add(-(atModifierDuration + rangeVectorDuration)).UnixMilli() + 1, // lookback duration not used with range vectors
			expectedMaxT: endTime.Add(-atModifierDuration).UnixMilli(),
		},
		{
			name:         "instant query: with range vector, with @ modifer",
			metricsQuery: withQuery(t, instantRequest, fmt.Sprintf("go_goroutines{}[%s] @ %d", rangeVectorDurationStr, endTime.Add(-atModifierDuration).Unix())),
			expectedMinT: endTime.Add(-(atModifierDuration + rangeVectorDuration)).UnixMilli() + 1, // lookback duration not used with range vectors
			expectedMaxT: endTime.Add(-atModifierDuration).UnixMilli(),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			minT := testCase.metricsQuery.GetMinT()

			maxT := testCase.metricsQuery.GetMaxT()

			require.EqualValues(t, testCase.expectedMinT, minT)
			require.EqualValues(t, testCase.expectedMaxT, maxT)
		})
	}
}

func TestMetricsQuery_WithStartEnd_TransformConsistency(t *testing.T) {
	startTime, err := time.Parse(time.RFC3339, "2024-02-21T00:00:00-08:00")
	require.NoError(t, err)
	endTime, err := time.Parse(time.RFC3339, "2024-02-22T00:00:00-08:00")
	require.NoError(t, err)

	updatedStartTime, err := time.Parse(time.RFC3339, "2024-02-21T00:00:00Z")
	require.NoError(t, err)
	updatedEndTime, err := time.Parse(time.RFC3339, "2024-02-22T00:00:00Z")
	require.NoError(t, err)

	stepDurationStr := "60s"
	stepDuration, _ := time.ParseDuration(stepDurationStr)

	rangeRequest := NewPrometheusRangeQueryRequest(
		"/api/v1/query_range",
		nil,
		startTime.UnixMilli(),
		endTime.UnixMilli(),
		stepDuration.Milliseconds(),
		time.Duration(0),
		parseQuery(t, "go_goroutines{}"),
		Options{},
		nil,
		"",
	)
	instantRequest := NewPrometheusInstantQueryRequest(
		"/api/v1/query",
		nil,
		endTime.UnixMilli(),
		time.Duration(0),
		parseQuery(t, "go_goroutines{}"),
		Options{},
		nil,
		"",
	)

	for _, testCase := range []struct {
		name                string
		initialMetricsQuery MetricsQueryRequest

		updatedStartTime time.Time
		updatedEndTime   time.Time

		expectedUpdatedMinT int64
		expectedUpdatedMaxT int64
	}{
		{
			name:                "range query: transform with start and end changes minT and maxT",
			initialMetricsQuery: rangeRequest,
			updatedStartTime:    updatedStartTime,
			updatedEndTime:      updatedEndTime,

			expectedUpdatedMinT: updatedStartTime.UnixMilli() + 1, // query range is left-open, but minT is inclusive
			expectedUpdatedMaxT: updatedEndTime.UnixMilli(),
		},
		{
			name:                "instant query: transform with start and end changes minT and maxT",
			initialMetricsQuery: instantRequest,
			updatedStartTime:    updatedEndTime,
			updatedEndTime:      updatedEndTime,

			expectedUpdatedMinT: updatedEndTime.UnixMilli() + 1, // query range is left-open, but minT is inclusive
			expectedUpdatedMaxT: updatedEndTime.UnixMilli(),
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// apply WithStartEnd
			newStart := testCase.updatedStartTime.UnixMilli()
			newEnd := testCase.updatedEndTime.UnixMilli()
			updatedMetricsQuery, err := testCase.initialMetricsQuery.WithStartEnd(newStart, newEnd)
			require.NoError(t, err)

			require.Equal(t, testCase.expectedUpdatedMinT, updatedMetricsQuery.GetMinT())
			require.Equal(t, testCase.expectedUpdatedMaxT, updatedMetricsQuery.GetMaxT())
		})
	}
}

func TestMetricsQuery_WithQuery_WithExpr_TransformConsistency(t *testing.T) {

	startTime, err := time.Parse(time.RFC3339, "2024-02-21T00:00:00-08:00")
	require.NoError(t, err)
	endTime, err := time.Parse(time.RFC3339, "2024-02-22T00:00:00-08:00")
	require.NoError(t, err)

	stepDurationStr := "60s"
	stepDuration, _ := time.ParseDuration(stepDurationStr)

	rangeVectorDurationStr := "5m"
	rangeVectorDuration, _ := time.ParseDuration(rangeVectorDurationStr)
	rangeVectorDurationMS := rangeVectorDuration.Milliseconds()

	offsetDurationStr := "1h"
	offsetDuration, _ := time.ParseDuration(offsetDurationStr)
	offsetDurationMS := offsetDuration.Milliseconds()

	rangeRequest := NewPrometheusRangeQueryRequest(
		"/api/v1/query_range",
		nil,
		startTime.UnixMilli(),
		endTime.UnixMilli(),
		stepDuration.Milliseconds(),
		time.Duration(0),
		parseQuery(t, "go_goroutines{}"),
		Options{},
		nil,
		"",
	)
	instantRequest := NewPrometheusInstantQueryRequest(
		"/api/v1/query",
		nil,
		endTime.UnixMilli(),
		time.Duration(0),
		parseQuery(t, "go_goroutines{}"),
		Options{},
		nil,
		"",
	)

	for _, testCase := range []struct {
		name                string
		initialMetricsQuery MetricsQueryRequest

		updatedQuery string

		expectedUpdatedMinT int64
		expectedUpdatedMaxT int64
		expectedErr         parser.ParseErrors
	}{
		{
			name:                "range query: transform with query changes minT and maxT",
			initialMetricsQuery: rangeRequest,
			updatedQuery:        fmt.Sprintf("rate(go_goroutines{}[%s] offset %s)", rangeVectorDurationStr, offsetDurationStr),

			expectedUpdatedMinT: startTime.UnixMilli() - rangeVectorDurationMS - offsetDurationMS + 1,
			expectedUpdatedMaxT: endTime.UnixMilli() - offsetDurationMS,
			expectedErr:         nil,
		},
		{
			name:                "instant query: transform with query changes minT and maxT",
			initialMetricsQuery: instantRequest,
			updatedQuery:        fmt.Sprintf("rate(go_goroutines{}[%s] offset %s)", rangeVectorDurationStr, offsetDurationStr),

			expectedUpdatedMinT: endTime.UnixMilli() - rangeVectorDurationMS - offsetDurationMS + 1,
			expectedUpdatedMaxT: endTime.UnixMilli() - offsetDurationMS,
			expectedErr:         nil,
		},

		// error cases
		{
			name:                "range query: transform with malformed query returns error",
			initialMetricsQuery: rangeRequest,
			updatedQuery:        "go_goroutines{}[",

			expectedErr: parser.ParseErrors{},
		},
		{
			name:                "instant query: transform with malformed query returns error",
			initialMetricsQuery: instantRequest,
			updatedQuery:        "go_goroutines{} offset",

			expectedErr: parser.ParseErrors{},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {

			// test WithQuery
			updatedMetricsQuery, err := testCase.initialMetricsQuery.WithQuery(testCase.updatedQuery)

			if err != nil || testCase.expectedErr != nil {
				require.IsType(t, testCase.expectedErr, err)
			} else {
				require.Equal(t, testCase.expectedUpdatedMinT, updatedMetricsQuery.GetMinT())
				require.Equal(t, testCase.expectedUpdatedMaxT, updatedMetricsQuery.GetMaxT())
			}

			// test WithExpr on the same query as WithQuery
			queryExpr, err := promqlext.NewPromQLParser().ParseExpr(testCase.updatedQuery)
			updatedMetricsQuery = mustSucceed(testCase.initialMetricsQuery.WithExpr(queryExpr))

			if err != nil || testCase.expectedErr != nil {
				require.IsType(t, testCase.expectedErr, err)
			} else {
				require.Equal(t, testCase.expectedUpdatedMinT, updatedMetricsQuery.GetMinT())
				require.Equal(t, testCase.expectedUpdatedMaxT, updatedMetricsQuery.GetMaxT())
			}
		})
	}
}

func TestCodec_DecodeEncodeLabelsQueryRequest(t *testing.T) {
	for _, testCase := range []struct {
		name                      string
		propagateHeaders          []string
		url                       string
		headers                   http.Header
		expectedURL               string
		expectedStruct            LabelsSeriesQueryRequest
		expectedGetLabelName      string
		expectedGetStartOrDefault int64
		expectedGetEndOrDefault   int64
		expectedErr               string
		expectedLimit             uint64
	}{
		{
			name:        "label names with start and end timestamps, no matcher sets",
			url:         "/api/v1/labels?end=1708588800&start=1708502400",
			expectedURL: "/api/v1/labels?end=1708588800&start=1708502400",
			expectedStruct: &PrometheusLabelNamesQueryRequest{
				Path:             "/api/v1/labels",
				Start:            1708502400 * 1e3,
				End:              1708588800 * 1e3,
				LabelMatcherSets: nil,
			},
			expectedGetLabelName:      "",
			expectedGetStartOrDefault: 1708502400 * 1e3,
			expectedGetEndOrDefault:   1708588800 * 1e3,
		},
		{
			name:        "label values with start and end timestamps, no matcher sets",
			url:         "/api/v1/label/job/values?end=1708588800&start=1708502400",
			expectedURL: "/api/v1/label/job/values?end=1708588800&start=1708502400",
			expectedStruct: &PrometheusLabelValuesQueryRequest{
				Path:             "/api/v1/label/job/values",
				LabelName:        "job",
				Start:            1708502400 * 1e3,
				End:              1708588800 * 1e3,
				LabelMatcherSets: nil,
			},
			expectedGetLabelName:      "",
			expectedGetStartOrDefault: 1708502400 * 1e3,
			expectedGetEndOrDefault:   1708588800 * 1e3,
		},
		{
			name:        "label names with start timestamp, no end timestamp, no matcher sets",
			url:         "/api/v1/labels?start=1708502400",
			expectedURL: "/api/v1/labels?start=1708502400",
			expectedStruct: &PrometheusLabelNamesQueryRequest{
				Path:             "/api/v1/labels",
				Start:            1708502400 * 1e3,
				End:              0,
				LabelMatcherSets: nil,
			},
			expectedGetLabelName:      "",
			expectedGetStartOrDefault: 1708502400 * 1e3,
			expectedGetEndOrDefault:   v1API.MaxTime.UnixMilli(),
		},
		{
			name:        "label values with start timestamp, no end timestamp, no matcher sets",
			url:         "/api/v1/label/job/values?start=1708502400",
			expectedURL: "/api/v1/label/job/values?start=1708502400",
			expectedStruct: &PrometheusLabelValuesQueryRequest{
				Path:             "/api/v1/label/job/values",
				LabelName:        "job",
				Start:            1708502400 * 1e3,
				End:              0,
				LabelMatcherSets: nil,
			},
			expectedGetLabelName:      "job",
			expectedGetStartOrDefault: 1708502400 * 1e3,
			expectedGetEndOrDefault:   v1API.MaxTime.UnixMilli(),
		},
		{
			name:        "label names with end timestamp, no start timestamp, no matcher sets",
			url:         "/api/v1/labels?end=1708588800",
			expectedURL: "/api/v1/labels?end=1708588800",
			expectedStruct: &PrometheusLabelNamesQueryRequest{
				Path:             "/api/v1/labels",
				Start:            0,
				End:              1708588800 * 1e3,
				LabelMatcherSets: nil,
			},
			expectedGetLabelName:      "",
			expectedGetStartOrDefault: v1API.MinTime.UnixMilli(),
			expectedGetEndOrDefault:   1708588800 * 1e3,
		},
		{
			name:        "label values with end timestamp, no start timestamp, no matcher sets",
			url:         "/api/v1/label/job/values?end=1708588800",
			expectedURL: "/api/v1/label/job/values?end=1708588800",
			expectedStruct: &PrometheusLabelValuesQueryRequest{
				Path:             "/api/v1/label/job/values",
				LabelName:        "job",
				Start:            0,
				End:              1708588800 * 1e3,
				LabelMatcherSets: nil,
			},
			expectedGetLabelName:      "job",
			expectedGetStartOrDefault: v1API.MinTime.UnixMilli(),
			expectedGetEndOrDefault:   1708588800 * 1e3,
		},
		{
			name:        "label names with start and end timestamp, multiple matcher sets",
			url:         "/api/v1/labels?end=1708588800&match%5B%5D=go_goroutines%7Bcontainer%3D~%22quer.%2A%22%7D&match%5B%5D=go_goroutines%7Bcontainer%21%3D%22query-scheduler%22%7D&start=1708502400",
			expectedURL: "/api/v1/labels?end=1708588800&match%5B%5D=go_goroutines%7Bcontainer%3D~%22quer.%2A%22%7D&match%5B%5D=go_goroutines%7Bcontainer%21%3D%22query-scheduler%22%7D&start=1708502400",
			expectedStruct: &PrometheusLabelNamesQueryRequest{
				Path:  "/api/v1/labels",
				Start: 1708502400 * 1e3,
				End:   1708588800 * 1e3,
				LabelMatcherSets: []string{
					"go_goroutines{container=~\"quer.*\"}",
					"go_goroutines{container!=\"query-scheduler\"}",
				},
			},
			expectedGetLabelName:      "",
			expectedGetStartOrDefault: 1708502400 * 1e3,
			expectedGetEndOrDefault:   1708588800 * 1e3,
		},
		{
			name:        "label values with start and end timestamp, multiple matcher sets",
			url:         "/api/v1/label/job/values?end=1708588800&match%5B%5D=go_goroutines%7Bcontainer%3D~%22quer.%2A%22%7D&match%5B%5D=go_goroutines%7Bcontainer%21%3D%22query-scheduler%22%7D&start=1708502400",
			expectedURL: "/api/v1/label/job/values?end=1708588800&match%5B%5D=go_goroutines%7Bcontainer%3D~%22quer.%2A%22%7D&match%5B%5D=go_goroutines%7Bcontainer%21%3D%22query-scheduler%22%7D&start=1708502400",
			expectedStruct: &PrometheusLabelValuesQueryRequest{
				Path:      "/api/v1/label/job/values",
				LabelName: "job",
				Start:     1708502400 * 1e3,
				End:       1708588800 * 1e3,
				LabelMatcherSets: []string{
					"go_goroutines{container=~\"quer.*\"}",
					"go_goroutines{container!=\"query-scheduler\"}",
				},
			},
			expectedGetLabelName:      "job",
			expectedGetStartOrDefault: 1708502400 * 1e3,
			expectedGetEndOrDefault:   1708588800 * 1e3,
		},
		{
			name:        "label names with start and end timestamp, multiple matcher sets, limit",
			url:         "/api/v1/labels?end=1708588800&limit=10&match%5B%5D=go_goroutines%7Bcontainer%3D~%22quer.%2A%22%7D&match%5B%5D=go_goroutines%7Bcontainer%21%3D%22query-scheduler%22%7D&start=1708502400",
			expectedURL: "/api/v1/labels?end=1708588800&limit=10&match%5B%5D=go_goroutines%7Bcontainer%3D~%22quer.%2A%22%7D&match%5B%5D=go_goroutines%7Bcontainer%21%3D%22query-scheduler%22%7D&start=1708502400",
			expectedStruct: &PrometheusLabelNamesQueryRequest{
				Path:  "/api/v1/labels",
				Start: 1708502400 * 1e3,
				End:   1708588800 * 1e3,
				Limit: 10,
				LabelMatcherSets: []string{
					"go_goroutines{container=~\"quer.*\"}",
					"go_goroutines{container!=\"query-scheduler\"}",
				},
			},
			expectedGetLabelName:      "",
			expectedLimit:             10,
			expectedGetStartOrDefault: 1708502400 * 1e3,
			expectedGetEndOrDefault:   1708588800 * 1e3,
		},
		{
			name:        "label values with start and end timestamp, multiple matcher sets, limit",
			url:         "/api/v1/label/job/values?end=1708588800&limit=10&match%5B%5D=go_goroutines%7Bcontainer%3D~%22quer.%2A%22%7D&match%5B%5D=go_goroutines%7Bcontainer%21%3D%22query-scheduler%22%7D&start=1708502400",
			expectedURL: "/api/v1/label/job/values?end=1708588800&limit=10&match%5B%5D=go_goroutines%7Bcontainer%3D~%22quer.%2A%22%7D&match%5B%5D=go_goroutines%7Bcontainer%21%3D%22query-scheduler%22%7D&start=1708502400",
			expectedStruct: &PrometheusLabelValuesQueryRequest{
				Path:      "/api/v1/label/job/values",
				LabelName: "job",
				Start:     1708502400 * 1e3,
				End:       1708588800 * 1e3,
				Limit:     10,
				LabelMatcherSets: []string{
					"go_goroutines{container=~\"quer.*\"}",
					"go_goroutines{container!=\"query-scheduler\"}",
				},
			},
			expectedGetLabelName:      "job",
			expectedLimit:             10,
			expectedGetStartOrDefault: 1708502400 * 1e3,
			expectedGetEndOrDefault:   1708588800 * 1e3,
		},
		{
			name:        "zero limit is allowed",
			url:         "/api/v1/label/job/values?limit=0&start=1708502400&end=1708588800",
			expectedURL: "/api/v1/label/job/values?end=1708588800&start=1708502400", // Zero limit is omitted (it's the default).
			expectedStruct: &PrometheusLabelValuesQueryRequest{
				Path:      "/api/v1/label/job/values",
				LabelName: "job",
				Start:     1708502400 * 1e3,
				End:       1708588800 * 1e3,
				Limit:     0,
			},
			expectedGetLabelName:      "job",
			expectedLimit:             0,
			expectedGetStartOrDefault: 1708502400 * 1e3,
			expectedGetEndOrDefault:   1708588800 * 1e3,
		},
		{
			name:        "negative limit is not allowed",
			url:         "/api/v1/label/job/values?limit=-1",
			expectedURL: "/api/v1/label/job/values?limit=-1",
			expectedErr: "limit parameter must be greater than or equal to 0, got -1",
		},
		{
			name: "propagates headers",
			headers: http.Header{
				"X-Special-Header": []string{"some-value"},
			},
			url:              "/api/v1/labels?end=1708588800&start=1708502400",
			expectedURL:      "/api/v1/labels?end=1708588800&start=1708502400",
			propagateHeaders: []string{"X-Special-Header"},
			expectedStruct: &PrometheusLabelNamesQueryRequest{
				Path:  "/api/v1/labels",
				Start: 1708502400 * 1e3,
				End:   1708588800 * 1e3,
				Headers: httpHeadersToProm(
					http.Header{"X-Special-Header": []string{"some-value"}},
				),
				LabelMatcherSets: nil,
			},
			expectedGetLabelName:      "",
			expectedGetStartOrDefault: 1708502400 * 1e3,
			expectedGetEndOrDefault:   1708588800 * 1e3,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			for _, reqMethod := range []string{http.MethodGet, http.MethodPost} {
				t.Run(reqMethod, func(t *testing.T) {
					const userID = "user-1"

					var r *http.Request
					var err error

					expectedStruct := testCase.expectedStruct

					switch reqMethod {
					case http.MethodGet:
						r, err = http.NewRequest(reqMethod, testCase.url, nil)
						require.NoError(t, err)
					case http.MethodPost:
						parsedURL, _ := url.Parse(testCase.url)
						r, err = http.NewRequest(reqMethod, parsedURL.Path, strings.NewReader(parsedURL.RawQuery))
						require.NoError(t, err)
						r.Header.Set("Content-Type", "application/x-www-form-urlencoded")

						if expectedStruct != nil {
							headers := append(expectedStruct.GetHeaders(), &PrometheusHeader{"Content-Type", []string{"application/x-www-form-urlencoded"}})

							// Decoding headers also sorts them. We sort here to be able to make assertions on the slice of headers.
							slices.SortFunc(headers, func(a, b *PrometheusHeader) int {
								return strings.Compare(a.Name, b.Name)
							})
							expectedStruct, err = expectedStruct.WithHeaders(headers)
							require.NoError(t, err)
						}
					default:
						t.Fatalf("unsupported HTTP method %q", reqMethod)
					}

					ctx := user.InjectOrgID(context.Background(), userID)
					r = r.WithContext(ctx)
					for k, v := range testCase.headers {
						for _, v := range v {
							r.Header.Add(k, v)
						}
					}

					codec := newTestCodecWithHeaders(testCase.propagateHeaders)
					reqDecoded, err := codec.DecodeLabelsSeriesQueryRequest(ctx, r)
					if err != nil || testCase.expectedErr != "" {
						require.EqualError(t, err, testCase.expectedErr)
						return
					}

					require.EqualValues(t, expectedStruct, reqDecoded)
					require.EqualValues(t, testCase.expectedGetStartOrDefault, reqDecoded.GetStartOrDefault())
					require.EqualValues(t, testCase.expectedGetEndOrDefault, reqDecoded.GetEndOrDefault())
					require.EqualValues(t, testCase.expectedLimit, reqDecoded.GetLimit())

					reqEncoded, err := codec.EncodeLabelsSeriesQueryRequest(ctx, reqDecoded)
					require.NoError(t, err)
					require.EqualValues(t, testCase.expectedURL, reqEncoded.RequestURI)

					actualUserID, _, err := user.ExtractOrgIDFromHTTPRequest(reqEncoded)
					require.NoError(t, err)
					require.Equal(t, userID, actualUserID)
				})
			}
		})
	}
}

func TestCodec_EncodeMetricsQueryRequest_AcceptHeader(t *testing.T) {
	for _, queryResultPayloadFormat := range allFormats {
		t.Run(queryResultPayloadFormat, func(t *testing.T) {
			codec := newTestCodecWithFormat(queryResultPayloadFormat)
			req := PrometheusInstantQueryRequest{}
			ctx := user.InjectOrgID(context.Background(), "user-1")
			encodedRequest, err := codec.EncodeMetricsQueryRequest(ctx, &req)
			require.NoError(t, err)

			switch queryResultPayloadFormat {
			case formatJSON:
				require.Equal(t, "application/json", encodedRequest.Header.Get("Accept"))
			case formatProtobuf:
				require.Equal(t, "application/vnd.mimir.queryresponse+protobuf,application/json", encodedRequest.Header.Get("Accept"))
			default:
				t.Fatalf("unknown query result payload format: %v", queryResultPayloadFormat)
			}
		})
	}
}

func TestCodec_EncodeMetricsQueryRequest_ReadConsistency(t *testing.T) {
	for _, consistencyLevel := range api.ReadConsistencies {
		for _, maxDelay := range []time.Duration{0, time.Minute} {
			t.Run(fmt.Sprintf("level: %s max delay: %s", consistencyLevel, maxDelay.String()), func(t *testing.T) {
				codec := newTestCodecWithFormat(formatProtobuf)
				ctx := api.ContextWithReadConsistencyLevel(user.InjectOrgID(context.Background(), "user-1"), consistencyLevel)
				if maxDelay > 0 {
					ctx = api.ContextWithReadConsistencyMaxDelay(ctx, maxDelay)
				}

				encodedRequest, err := codec.EncodeMetricsQueryRequest(ctx, &PrometheusInstantQueryRequest{})
				require.NoError(t, err)
				require.Equal(t, consistencyLevel, encodedRequest.Header.Get(api.ReadConsistencyHeader))

				if maxDelay > 0 {
					require.Equal(t, maxDelay.String(), encodedRequest.Header.Get(api.ReadConsistencyMaxDelayHeader))
				} else {
					require.Empty(t, encodedRequest.Header.Get(api.ReadConsistencyMaxDelayHeader))
				}
			})
		}
	}
}

func TestCodec_EncodeMetricsQueryRequest_ShouldPropagateHeadersInAllowList(t *testing.T) {
	const notAllowedHeader = "X-Some-Name"

	codec := newTestCodecWithFormat(formatProtobuf)
	expectedOffsets := map[int32]int64{0: 1, 1: 2}

	ctx := user.InjectOrgID(context.Background(), "user-1")
	req, err := codec.EncodeMetricsQueryRequest(ctx, &PrometheusInstantQueryRequest{
		headers: []*PrometheusHeader{
			// Allowed.
			{Name: compat.ForceFallbackHeaderName, Values: []string{"true"}},
			{Name: chunkinfologger.ChunkInfoLoggingHeader, Values: []string{"label"}},
			{Name: api.ReadConsistencyOffsetsHeader, Values: []string{string(api.EncodeOffsets(expectedOffsets))}},

			// Not allowed.
			{Name: notAllowedHeader, Values: []string{"some-value"}},
		},
	})

	require.NoError(t, err)
	require.Equal(t, []string{"true"}, req.Header.Values(compat.ForceFallbackHeaderName))
	require.Equal(t, []string{"label"}, req.Header.Values(chunkinfologger.ChunkInfoLoggingHeader))
	require.Empty(t, req.Header.Values(notAllowedHeader))

	// Ensure strong read consistency offsets are propagated.
	require.Len(t, req.Header.Values(api.ReadConsistencyOffsetsHeader), 1)
	actualOffsets := api.EncodedOffsets(req.Header.Values(api.ReadConsistencyOffsetsHeader)[0])
	for partitionID, expectedOffset := range expectedOffsets {
		actualOffset, ok := actualOffsets.Lookup(partitionID)
		require.True(t, ok)
		require.Equal(t, expectedOffset, actualOffset)
	}
}

func TestCodec_EncodeResponse_ContentNegotiation(t *testing.T) {
	testResponse := &PrometheusResponse{
		Status:    statusError,
		ErrorType: string(v1Client.ErrExec),
		Error:     "something went wrong",
	}

	jsonBody, err := jsonFormatter{}.EncodeQueryResponse(testResponse)
	require.NoError(t, err)

	protobufBody, err := ProtobufFormatter{}.EncodeQueryResponse(testResponse)
	require.NoError(t, err)

	scenarios := map[string]struct {
		acceptHeader                string
		expectedResponseContentType string
		expectedResponseBody        []byte
		expectedError               error
	}{
		"no content type in Accept header": {
			acceptHeader:                "",
			expectedResponseContentType: jsonMimeType,
			expectedResponseBody:        jsonBody,
		},
		"unsupported content type in Accept header": {
			acceptHeader:  "testing/not-a-supported-content-type",
			expectedError: apierror.New(apierror.TypeNotAcceptable, "none of the content types in the Accept header are supported"),
		},
		"multiple unsupported content types in Accept header": {
			acceptHeader:  "testing/not-a-supported-content-type,testing/also-not-a-supported-content-type",
			expectedError: apierror.New(apierror.TypeNotAcceptable, "none of the content types in the Accept header are supported"),
		},
		"single supported content type in Accept header": {
			acceptHeader:                "application/json",
			expectedResponseContentType: jsonMimeType,
			expectedResponseBody:        jsonBody,
		},
		"wildcard subtype in Accept header": {
			acceptHeader:                "application/*",
			expectedResponseContentType: jsonMimeType,
			expectedResponseBody:        jsonBody,
		},
		"wildcard in Accept header": {
			acceptHeader:                "*/*",
			expectedResponseContentType: jsonMimeType,
			expectedResponseBody:        jsonBody,
		},
		"multiple supported content types in Accept header": {
			acceptHeader:                "application/vnd.mimir.queryresponse+protobuf,application/json",
			expectedResponseContentType: mimirpb.QueryResponseMimeType,
			expectedResponseBody:        protobufBody,
		},
	}

	codec := newTestCodec()

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, "/something", nil)
			require.NoError(t, err)
			req.Header.Set("Accept", scenario.acceptHeader)

			encodedResponse, err := codec.EncodeMetricsQueryResponse(context.Background(), req, testResponse)
			require.Equal(t, scenario.expectedError, err)

			if scenario.expectedError == nil {
				actualResponseContentType := encodedResponse.Header.Get("Content-Type")
				require.Equal(t, scenario.expectedResponseContentType, actualResponseContentType)

				actualResponseBody, err := io.ReadAll(encodedResponse.Body)
				require.NoError(t, err)
				require.Equal(t, scenario.expectedResponseBody, actualResponseBody)
			}
		})
	}
}

type prometheusAPIResponse struct {
	Status    string             `json:"status"`
	Data      interface{}        `json:"data,omitempty"`
	ErrorType v1Client.ErrorType `json:"errorType,omitempty"`
	Error     string             `json:"error,omitempty"`
	Warnings  []string           `json:"warnings,omitempty"`
}

type prometheusResponseData struct {
	Type   model.ValueType `json:"resultType"`
	Result model.Value     `json:"result"`
}

func stringErrorResponse(statusCode int, message string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Body:       io.NopCloser(strings.NewReader(message)),
	}
}

func jsonErrorResponse(t *testing.T, errType apierror.Type, message string) *http.Response {
	apiErr := apierror.New(errType, message)
	b, err := apiErr.EncodeJSON()
	if err != nil {
		t.Fatalf("unexpected serialization error: %s", err)
	}

	return &http.Response{
		StatusCode: apiErr.StatusCode(),
		Header: http.Header{
			http.CanonicalHeaderKey("Content-Type"): []string{jsonMimeType},
		},
		Body: io.NopCloser(bytes.NewReader(b)),
	}
}

func TestCodec_DecodeResponse_Errors(t *testing.T) {
	scenarios := map[string]struct {
		response                    *http.Response
		expectedResponseContentType string
		expectedResponseStatusCode  int
	}{
		"internal error - no content type": {
			response:                    stringErrorResponse(http.StatusInternalServerError, "something failed"),
			expectedResponseContentType: jsonMimeType,
			expectedResponseStatusCode:  http.StatusInternalServerError,
		},
		"too many requests - no content type": {
			response:                    stringErrorResponse(http.StatusTooManyRequests, "something failed"),
			expectedResponseContentType: jsonMimeType,
			expectedResponseStatusCode:  http.StatusTooManyRequests,
		},
		"too larger entity - no content type": {
			response:                    stringErrorResponse(http.StatusRequestEntityTooLarge, "something failed"),
			expectedResponseContentType: jsonMimeType,
			expectedResponseStatusCode:  http.StatusRequestEntityTooLarge,
		},
		"service unavailable - no content type": {
			response:                    stringErrorResponse(http.StatusServiceUnavailable, "something failed"),
			expectedResponseContentType: jsonMimeType,
			expectedResponseStatusCode:  http.StatusServiceUnavailable,
		},
		"internal error - JSON content type": {
			response:                    jsonErrorResponse(t, apierror.TypeInternal, "something failed"),
			expectedResponseContentType: jsonMimeType,
			expectedResponseStatusCode:  http.StatusInternalServerError,
		},
		"too many requests - JSON content type": {
			response:                    jsonErrorResponse(t, apierror.TypeTooManyRequests, "something failed"),
			expectedResponseContentType: jsonMimeType,
			expectedResponseStatusCode:  http.StatusTooManyRequests,
		},
		"too larger entity - JSON content type": {
			response:                    jsonErrorResponse(t, apierror.TypeTooLargeEntry, "something failed"),
			expectedResponseContentType: jsonMimeType,
			expectedResponseStatusCode:  http.StatusRequestEntityTooLarge,
		},
		"service unavailable - JSON content type": {
			response:                    jsonErrorResponse(t, apierror.TypeUnavailable, "something failed"),
			expectedResponseContentType: jsonMimeType,
			expectedResponseStatusCode:  http.StatusServiceUnavailable,
		},
	}

	for name, testCase := range scenarios {
		t.Run(name, func(t *testing.T) {
			codec := newTestCodec()

			_, err := codec.DecodeMetricsQueryResponse(context.Background(), testCase.response, nil, testutil.NewTestingLogger(t))
			require.Error(t, err)
			require.True(t, apierror.IsAPIError(err))
			resp, ok := apierror.HTTPResponseFromError(err)
			require.True(t, ok, "Error should be able to represent HTTPResponse")
			require.Equal(t, int32(testCase.expectedResponseStatusCode), resp.Code)
		})
	}
}

func TestCodec_DecodeResponse_ContentTypeHandling(t *testing.T) {
	for _, tc := range []struct {
		name            string
		responseHeaders http.Header
		expectedErr     error
	}{
		{
			name:            "unknown content type in response",
			responseHeaders: http.Header{"Content-Type": []string{"something/else"}},
			expectedErr:     apierror.New(apierror.TypeInternal, "unknown response content type 'something/else'"),
		},
		{
			name:            "no content type in response",
			responseHeaders: http.Header{},
			expectedErr:     apierror.New(apierror.TypeInternal, "unknown response content type ''"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			codec := newTestCodecWithFormat(formatJSON)

			resp := prometheusAPIResponse{}
			body, err := json.Marshal(resp)
			require.NoError(t, err)
			httpResponse := &http.Response{
				StatusCode:    200,
				Header:        tc.responseHeaders,
				Body:          io.NopCloser(bytes.NewBuffer(body)),
				ContentLength: int64(len(body)),
			}

			_, err = codec.DecodeMetricsQueryResponse(context.Background(), httpResponse, nil, log.NewNopLogger())
			require.Equal(t, tc.expectedErr, err)
		})
	}
}

func TestMergeAPIResponses(t *testing.T) {
	codec := newTestCodec()

	histogram1 := mimirpb.FloatHistogram{
		CounterResetHint: histogram.GaugeType,
		Schema:           3,
		ZeroThreshold:    1.23,
		ZeroCount:        456,
		Count:            9001,
		Sum:              789.1,
		PositiveSpans: []mimirpb.BucketSpan{
			{Offset: 4, Length: 1},
			{Offset: 3, Length: 2},
		},
		NegativeSpans: []mimirpb.BucketSpan{
			{Offset: 7, Length: 3},
			{Offset: 9, Length: 1},
		},
		PositiveBuckets: []float64{100, 200, 300},
		NegativeBuckets: []float64{400, 500, 600, 700},
	}

	histogram2 := mimirpb.FloatHistogram{
		CounterResetHint: histogram.GaugeType,
		Schema:           3,
		ZeroThreshold:    1.23,
		ZeroCount:        456,
		Count:            9001,
		Sum:              100789.1,
		PositiveSpans: []mimirpb.BucketSpan{
			{Offset: 4, Length: 1},
			{Offset: 3, Length: 2},
		},
		NegativeSpans: []mimirpb.BucketSpan{
			{Offset: 7, Length: 3},
			{Offset: 9, Length: 1},
		},
		PositiveBuckets: []float64{100, 200, 300},
		NegativeBuckets: []float64{400, 500, 600, 700},
	}

	for _, tc := range []struct {
		name     string
		input    []Response
		expected Response
	}{
		{
			name:  "No responses shouldn't panic and return a non-null result and result type.",
			input: []Response{},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: matrix,
					Result:     []SampleStream{},
				},
			},
		},

		{
			name: "A single empty response shouldn't panic.",
			input: []Response{
				&PrometheusResponse{
					Status: statusSuccess,
					Data: &PrometheusData{
						ResultType: matrix,
						Result:     []SampleStream{},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: matrix,
					Result:     []SampleStream{},
				},
			},
		},

		{
			name: "Multiple empty responses shouldn't panic.",
			input: []Response{
				&PrometheusResponse{
					Status: statusSuccess,
					Data: &PrometheusData{
						ResultType: matrix,
						Result:     []SampleStream{},
					},
				},
				&PrometheusResponse{
					Status: statusSuccess,
					Data: &PrometheusData{
						ResultType: matrix,
						Result:     []SampleStream{},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: matrix,
					Result:     []SampleStream{},
				},
			},
		},

		{
			name: "Basic merging of two responses.",
			input: []Response{
				&PrometheusResponse{
					Status: statusSuccess,
					Data: &PrometheusData{
						ResultType: matrix,
						Result: []SampleStream{
							{
								Labels: []mimirpb.LabelAdapter{},
								Samples: []mimirpb.Sample{
									{Value: 0, TimestampMs: 0},
									{Value: 1, TimestampMs: 1},
								},
							},
						},
					},
				},
				&PrometheusResponse{
					Status: statusSuccess,
					Data: &PrometheusData{
						ResultType: matrix,
						Result: []SampleStream{
							{
								Labels: []mimirpb.LabelAdapter{},
								Samples: []mimirpb.Sample{
									{Value: 2, TimestampMs: 2},
									{Value: 3, TimestampMs: 3},
								},
							},
						},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: matrix,
					Result: []SampleStream{
						{
							Labels: []mimirpb.LabelAdapter{},
							Samples: []mimirpb.Sample{
								{Value: 0, TimestampMs: 0},
								{Value: 1, TimestampMs: 1},
								{Value: 2, TimestampMs: 2},
								{Value: 3, TimestampMs: 3},
							},
						},
					},
				},
			},
		},

		{
			name: "Merging of responses when labels are in different order.",
			input: []Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[0,"0"],[1,"1"]]}]}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"c":"d","a":"b"},"values":[[2,"2"],[3,"3"]]}]}}`),
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: matrix,
					Result: []SampleStream{
						{
							Labels: []mimirpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
							Samples: []mimirpb.Sample{
								{Value: 0, TimestampMs: 0},
								{Value: 1, TimestampMs: 1000},
								{Value: 2, TimestampMs: 2000},
								{Value: 3, TimestampMs: 3000},
							},
						},
					},
				},
			},
		},

		{
			name: "Merging of samples where there is single overlap.",
			input: []Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[1,"1"],[2,"2"]]}]}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"c":"d","a":"b"},"values":[[2,"2"],[3,"3"]]}]}}`),
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: matrix,
					Result: []SampleStream{
						{
							Labels: []mimirpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
							Samples: []mimirpb.Sample{
								{Value: 1, TimestampMs: 1000},
								{Value: 2, TimestampMs: 2000},
								{Value: 3, TimestampMs: 3000},
							},
						},
					},
				},
			},
		},
		{
			name: "Merging of samples where there is multiple partial overlaps.",
			input: []Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[1,"1"],[2,"2"],[3,"3"]]}]}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"c":"d","a":"b"},"values":[[2,"2"],[3,"3"],[4,"4"],[5,"5"]]}]}}`),
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: matrix,
					Result: []SampleStream{
						{
							Labels: []mimirpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
							Samples: []mimirpb.Sample{
								{Value: 1, TimestampMs: 1000},
								{Value: 2, TimestampMs: 2000},
								{Value: 3, TimestampMs: 3000},
								{Value: 4, TimestampMs: 4000},
								{Value: 5, TimestampMs: 5000},
							},
						},
					},
				},
			},
		},
		{
			name: "Merging of samples where there is complete overlap.",
			input: []Response{
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"a":"b","c":"d"},"values":[[2,"2"],[3,"3"]]}]}}`),
				mustParse(t, `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"c":"d","a":"b"},"values":[[2,"2"],[3,"3"],[4,"4"],[5,"5"]]}]}}`),
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: matrix,
					Result: []SampleStream{
						{
							Labels: []mimirpb.LabelAdapter{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
							Samples: []mimirpb.Sample{
								{Value: 2, TimestampMs: 2000},
								{Value: 3, TimestampMs: 3000},
								{Value: 4, TimestampMs: 4000},
								{Value: 5, TimestampMs: 5000},
							},
						},
					},
				},
			},
		},

		{
			name: "Handling single histogram result",
			input: []Response{
				&PrometheusResponse{
					Status: statusSuccess,
					Data: &PrometheusData{
						ResultType: matrix,
						Result: []SampleStream{
							{
								Labels: []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
								Histograms: []mimirpb.FloatHistogramPair{
									{TimestampMs: 1000, Histogram: &histogram1},
								},
							},
						},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: matrix,
					Result: []SampleStream{
						{
							Labels: []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
							Histograms: []mimirpb.FloatHistogramPair{
								{
									TimestampMs: 1000,
									Histogram:   &histogram1,
								},
							},
						},
					},
				},
			},
		},

		{
			name: "Handling non overlapping histogram result",
			input: []Response{
				&PrometheusResponse{
					Status: statusSuccess,
					Data: &PrometheusData{
						ResultType: matrix,
						Result: []SampleStream{
							{
								Labels: []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
								Histograms: []mimirpb.FloatHistogramPair{
									{TimestampMs: 1000, Histogram: &histogram1},
								},
							},
						},
					},
				},
				&PrometheusResponse{
					Status: statusSuccess,
					Data: &PrometheusData{
						ResultType: matrix,
						Result: []SampleStream{
							{
								Labels: []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
								Histograms: []mimirpb.FloatHistogramPair{
									{TimestampMs: 2000, Histogram: &histogram2},
								},
							},
						},
					},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: matrix,
					Result: []SampleStream{
						{
							Labels: []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
							Histograms: []mimirpb.FloatHistogramPair{
								{TimestampMs: 1000, Histogram: &histogram1},
								{TimestampMs: 2000, Histogram: &histogram2},
							},
						},
					},
				},
			},
		},

		{
			name: "Merging annotations",
			input: []Response{
				&PrometheusResponse{
					Status: statusSuccess,
					Data: &PrometheusData{
						ResultType: matrix,
						Result: []SampleStream{
							{
								Labels: []mimirpb.LabelAdapter{},
								Samples: []mimirpb.Sample{
									{Value: 0, TimestampMs: 0},
									{Value: 1, TimestampMs: 1},
								},
							},
						},
					},
					Warnings: []string{"dummy warning"},
				},
				&PrometheusResponse{
					Status: statusSuccess,
					Data: &PrometheusData{
						ResultType: matrix,
						Result: []SampleStream{
							{
								Labels: []mimirpb.LabelAdapter{},
								Samples: []mimirpb.Sample{
									{Value: 2, TimestampMs: 2},
									{Value: 3, TimestampMs: 3},
								},
							},
						},
					},
					Infos: []string{"dummy info"},
				},
			},
			expected: &PrometheusResponse{
				Status: statusSuccess,
				Data: &PrometheusData{
					ResultType: matrix,
					Result: []SampleStream{
						{
							Labels: []mimirpb.LabelAdapter{},
							Samples: []mimirpb.Sample{
								{Value: 0, TimestampMs: 0},
								{Value: 1, TimestampMs: 1},
								{Value: 2, TimestampMs: 2},
								{Value: 3, TimestampMs: 3},
							},
						},
					},
				},
				Warnings: []string{"dummy warning"},
				Infos:    []string{"dummy info"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := codec.MergeResponse(tc.input...)
			require.NoError(t, err)
			requireEqualPrometheusResponse(t, tc.expected, output)
		})
	}

	t.Run("shouldn't merge unsuccessful responses", func(t *testing.T) {
		successful := &PrometheusResponse{
			Status: statusSuccess,
			Data:   &PrometheusData{ResultType: matrix},
		}
		unsuccessful := &PrometheusResponse{
			Status: statusError,
			Data:   &PrometheusData{ResultType: matrix},
		}

		_, err := codec.MergeResponse(successful, unsuccessful)
		require.Error(t, err)
	})

	t.Run("shouldn't merge nil data", func(t *testing.T) {
		// nil data has no type, so we can't merge it, it's basically an unsuccessful response,
		// and we should never reach the point where we're merging an unsuccessful response.
		successful := &PrometheusResponse{
			Status: statusSuccess,
			Data:   &PrometheusData{ResultType: matrix},
		}
		nilData := &PrometheusResponse{
			Status: statusSuccess, // shouldn't have nil data with a successful response, but we want to test everything.
			Data:   nil,
		}
		_, err := codec.MergeResponse(successful, nilData)
		require.Error(t, err)
	})

	t.Run("shouldn't merge non-matrix data", func(t *testing.T) {
		matrixResponse := &PrometheusResponse{
			Status: statusSuccess,
			Data:   &PrometheusData{ResultType: matrix},
		}
		vectorResponse := &PrometheusResponse{
			Status: statusSuccess,
			Data:   &PrometheusData{ResultType: model.ValVector.String()},
		}
		_, err := codec.MergeResponse(matrixResponse, vectorResponse)
		require.Error(t, err)
	})
}

func requireEqualPrometheusResponse(t *testing.T, expected, actual Response) {
	prometheusResponse, ok := expected.GetPrometheusResponse()
	require.True(t, ok)
	prometheusResponseActual, ok := actual.GetPrometheusResponse()
	require.True(t, ok)
	require.Equal(t, prometheusResponse, prometheusResponseActual)
}

func mustParse(t *testing.T, response string) Response {
	var resp PrometheusResponse
	// Needed as goimports automatically add a json import otherwise.
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	require.NoError(t, json.Unmarshal([]byte(response), &resp))
	return &resp
}

func BenchmarkCodec_DecodeResponse(b *testing.B) {
	const (
		numSeries           = 1000
		numSamplesPerSeries = 1000
	)

	codec := newTestCodec()

	// Generate a mocked response and marshal it.
	res := mockPrometheusResponse(numSeries, numSamplesPerSeries, "matrix")
	encodedRes, err := json.Marshal(res)
	require.NoError(b, err)
	b.Log("test prometheus response size:", len(encodedRes))

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		_, err := codec.DecodeMetricsQueryResponse(context.Background(), &http.Response{
			StatusCode:    200,
			Body:          io.NopCloser(bytes.NewReader(encodedRes)),
			ContentLength: int64(len(encodedRes)),
			Header: map[string][]string{
				"Content-Type": {"application/json"},
			},
		}, nil, log.NewNopLogger())
		require.NoError(b, err)
	}
}

func BenchmarkCodec_EncodeResponse(b *testing.B) {
	const (
		numSeries           = 1000
		numSamplesPerSeries = 1000
	)

	codec := newTestCodec()
	req, err := http.NewRequest(http.MethodGet, "/something", nil)
	require.NoError(b, err)

	// Generate a mocked response and marshal it.

	b.Run("matrix", func(b *testing.B) {
		res := mockPrometheusResponse(numSeries, numSamplesPerSeries, "matrix")
		b.ReportAllocs()
		for b.Loop() {
			_, err := codec.EncodeMetricsQueryResponse(context.Background(), req, res)
			require.NoError(b, err)
		}
	})

	b.Run("vector", func(b *testing.B) {
		res := mockPrometheusResponse(numSeries, 1, "vector")
		b.ReportAllocs()
		for b.Loop() {
			_, err := codec.EncodeMetricsQueryResponse(context.Background(), req, res)
			require.NoError(b, err)
		}
	})
}

func mockPrometheusResponse(numSeries, numSamplesPerSeries int, ty string) *PrometheusResponse {
	stream := make([]SampleStream, numSeries)
	for s := 0; s < numSeries; s++ {
		// Generate random samples.
		samples := make([]mimirpb.Sample, numSamplesPerSeries)
		for i := 0; i < numSamplesPerSeries; i++ {
			samples[i] = mimirpb.Sample{
				Value:       rand.Float64(),
				TimestampMs: int64(i),
			}
		}

		// Generate random labels.
		lbls := make([]mimirpb.LabelAdapter, 10)
		for i := range lbls {
			lbls[i].Name = "a_medium_size_label_name"
			lbls[i].Value = "a_medium_size_label_value_that_is_used_to_benchmark_marshalling"
		}

		stream[s] = SampleStream{
			Labels:  lbls,
			Samples: samples,
		}
	}

	return &PrometheusResponse{
		Status: "success",
		Data: &PrometheusData{
			ResultType: ty,
			Result:     stream,
		},
	}
}

func mockPrometheusResponseSingleSeries(series []mimirpb.LabelAdapter, samples ...mimirpb.Sample) *PrometheusResponse {
	return &PrometheusResponse{
		Status: "success",
		Data: &PrometheusData{
			ResultType: "matrix",
			Result: []SampleStream{
				{
					Labels:  series,
					Samples: samples,
				},
			},
		},
	}
}

func mockPrometheusResponseWithSamplesAndHistograms(labels []mimirpb.LabelAdapter, samples []mimirpb.Sample, histograms []mimirpb.FloatHistogramPair) *PrometheusResponse {
	return &PrometheusResponse{
		Status: "success",
		Data: &PrometheusData{
			ResultType: "matrix",
			Result: []SampleStream{
				{
					Labels:     labels,
					Samples:    samples,
					Histograms: histograms,
				},
			},
		},
	}
}

func TestDecodeRangeQueryTimeParams(t *testing.T) {
	for _, tt := range []struct {
		name          string
		input         *url.Values
		expectedStart int64
		expectedEnd   int64
		expectedStep  int64
		expectedErr   error
	}{
		{
			name: "success",
			input: &url.Values{
				"start": []string{"1997-08-29T12:00:00Z"},
				"end":   []string{"1997-08-29T18:00:00Z"},
				"step":  []string{"5m"},
			},
			expectedStart: 872856000000,
			expectedEnd:   872877600000,
			expectedStep:  300000,
			expectedErr:   nil,
		},
		{
			name: "missing start",
			input: &url.Values{
				"end":  []string{"1997-08-29T18:00:00Z"},
				"step": []string{"5m"},
			},
			expectedStart: 0,
			expectedEnd:   0,
			expectedStep:  0,
			expectedErr:   apierror.New(apierror.TypeBadData, "missing required parameter \"start\""),
		},
		{
			name: "missing end",
			input: &url.Values{
				"start": []string{"1997-08-29T12:00:00Z"},
				"step":  []string{"5m"},
			},
			expectedStart: 0,
			expectedEnd:   0,
			expectedStep:  0,
			expectedErr:   apierror.New(apierror.TypeBadData, "missing required parameter \"end\""),
		},
		{
			name: "missing step",
			input: &url.Values{
				"start": []string{"1997-08-29T12:00:00Z"},
				"end":   []string{"1997-08-29T18:00:00Z"},
			},
			expectedStart: 0,
			expectedEnd:   0,
			expectedStep:  0,
			expectedErr:   apierror.New(apierror.TypeBadData, "missing required parameter \"step\""),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			actualStart, actualEnd, actualStep, err := DecodeRangeQueryTimeParams(tt.input)
			assert.Equal(t, tt.expectedStart, actualStart)
			assert.Equal(t, tt.expectedEnd, actualEnd)
			assert.Equal(t, tt.expectedStep, actualStep)
			assert.Equal(t, tt.expectedErr, err)
		})
	}
}

func Test_DecodeOptions(t *testing.T) {
	for _, tt := range []struct {
		name     string
		input    *http.Request
		expected *Options
	}{
		{
			name: "default",
			input: &http.Request{
				Header: http.Header{},
			},
			expected: &Options{},
		},
		{
			name: "disable cache",
			input: &http.Request{
				Header: http.Header{
					cacheControlHeader: []string{noStoreValue},
				},
			},
			expected: &Options{
				CacheDisabled: true,
			},
		},
		{
			name: "custom sharding",
			input: &http.Request{
				Header: http.Header{
					totalShardsControlHeader: []string{"64"},
				},
			},
			expected: &Options{
				TotalShards: 64,
			},
		},
		{
			name: "disable sharding",
			input: &http.Request{
				Header: http.Header{
					totalShardsControlHeader: []string{"0"},
				},
			},
			expected: &Options{
				ShardingDisabled: true,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			actual := &Options{}
			DecodeOptions(tt.input, actual)
			require.Equal(t, tt.expected, actual)
		})
	}
}

// TestCodec_DecodeEncode_Metrics tests that decoding and re-encoding a
// metrics query request does not lose relevant information about the original request.
func TestCodec_DecodeEncode_Metrics(t *testing.T) {
	codec := newTestCodec()
	for _, tt := range []struct {
		name    string
		headers http.Header
	}{
		{
			name: "no custom headers",
		},
		{
			name:    "shard count header",
			headers: http.Header{totalShardsControlHeader: []string{"128"}},
		},
		{
			name:    "shard count disabled via header",
			headers: http.Header{totalShardsControlHeader: []string{"0"}},
		},
		{
			name:    "cache disabled via header",
			headers: http.Header{cacheControlHeader: []string{noStoreValue}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			const userID = "user-1"

			queryURL := "/api/v1/query?query=sum+by+%28namespace%29+%28container_memory_rss%29&time=1704270202.066"
			expected, err := http.NewRequest("GET", queryURL, nil)
			require.NoError(t, err)
			expected.Body = http.NoBody
			expected.Header = tt.headers
			if expected.Header == nil {
				expected.Header = make(http.Header)
			}

			// This header is set by EncodeMetricsQueryRequest according to the codec's config, so we
			// should always expect it to be present on the re-encoded request.
			expected.Header.Set("Accept", "application/json")

			// This header is set by EncodeMetricsQueryRequest and based on the provided context.
			expected.Header.Set(user.OrgIDHeaderName, userID)

			ctx := user.InjectOrgID(context.Background(), userID)
			decoded, err := codec.DecodeMetricsQueryRequest(ctx, expected)
			require.NoError(t, err)
			encoded, err := codec.EncodeMetricsQueryRequest(ctx, decoded)
			require.NoError(t, err)

			assert.Equal(t, expected.URL, encoded.URL)
			assert.Equal(t, expected.Header, encoded.Header)
		})
	}
}

// TestCodec_DecodeEncodeMultipleTimes_Labels tests that decoding and re-encoding a
// labels query request multiple times does not lose relevant information about the original request.
func TestCodec_DecodeEncodeMultipleTimes_Labels(t *testing.T) {

	defaultHeaders := httpHeadersToProm(http.Header{
		"Accept": {"application/json"},
	})
	codec := newTestCodec()
	for _, tc := range []struct {
		name     string
		queryURL string
		request  LabelsSeriesQueryRequest
	}{
		{
			name:     "label names - minimal",
			queryURL: "/api/v1/labels?end=1708588800&start=1708502400",
			request: &PrometheusLabelNamesQueryRequest{
				Path:    "/api/v1/labels",
				Headers: defaultHeaders,
				Start:   1708502400000,
				End:     1708588800000,
			},
		},
		{
			name:     "label names - all",
			queryURL: "/api/v1/labels?end=1708588800&limit=10&match%5B%5D=go_goroutines%7Bcontainer%3D~%22quer.%2A%22%7D&match%5B%5D=go_goroutines%7Bcontainer%21%3D%22query-scheduler%22%7D&start=1708502400",
			request: &PrometheusLabelNamesQueryRequest{
				Path:    "/api/v1/labels",
				Headers: defaultHeaders,
				Start:   1708502400000,
				End:     1708588800000,
				LabelMatcherSets: []string{
					"go_goroutines{container=~\"quer.*\"}",
					"go_goroutines{container!=\"query-scheduler\"}",
				},
				Limit: 10,
			},
		},
		{
			name:     "label values - minimal",
			queryURL: "/api/v1/label/job/values?end=1708588800&start=1708502400",
			request: &PrometheusLabelValuesQueryRequest{
				Path:      "/api/v1/label/job/values",
				Headers:   defaultHeaders,
				LabelName: "job",
				Start:     1708502400000,
				End:       1708588800000,
			},
		},
		{
			name:     "label values - all",
			queryURL: "/api/v1/label/job/values?end=1708588800&limit=10&match%5B%5D=go_goroutines%7Bcontainer%3D~%22quer.%2A%22%7D&match%5B%5D=go_goroutines%7Bcontainer%21%3D%22query-scheduler%22%7D&start=1708502400",
			request: &PrometheusLabelValuesQueryRequest{
				Path:      "/api/v1/label/job/values",
				Headers:   defaultHeaders,
				LabelName: "job",
				Start:     1708502400000,
				End:       1708588800000,
				LabelMatcherSets: []string{
					"go_goroutines{container=~\"quer.*\"}",
					"go_goroutines{container!=\"query-scheduler\"}",
				},
				Limit: 10,
			},
		},
		{
			name:     "series - minimal",
			queryURL: "/api/v1/series?end=1708588800&match%5B%5D=go_goroutines%7Bcontainer%21%3D%22query-scheduler%22%7D&start=1708502400",
			request: &PrometheusSeriesQueryRequest{
				Path:    "/api/v1/series",
				Headers: defaultHeaders,
				Start:   1708502400000,
				End:     1708588800000,
				LabelMatcherSets: []string{
					"go_goroutines{container!=\"query-scheduler\"}",
				},
			},
		},
		{
			name:     "series - all",
			queryURL: "/api/v1/series?end=1708588800&limit=10&match%5B%5D=go_goroutines%7Bcontainer%3D~%22quer.%2A%22%7D&match%5B%5D=go_goroutines%7Bcontainer%21%3D%22query-scheduler%22%7D&start=1708502400",
			request: &PrometheusSeriesQueryRequest{
				Path:    "/api/v1/series",
				Headers: defaultHeaders,
				Start:   1708502400000,
				End:     1708588800000,
				LabelMatcherSets: []string{
					"go_goroutines{container=~\"quer.*\"}",
					"go_goroutines{container!=\"query-scheduler\"}",
				},
				Limit: 10,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			const userID = "user-1"

			// Inject the auth in the original request.
			var err error
			tc.request, err = tc.request.WithHeaders(append(
				tc.request.GetHeaders(),
				&PrometheusHeader{Name: http.CanonicalHeaderKey(user.OrgIDHeaderName), Values: []string{userID}}))
			require.NoError(t, err)

			expected, err := http.NewRequest("GET", tc.queryURL, nil)
			require.NoError(t, err)
			expected.Body = http.NoBody
			expected.Header = make(http.Header)

			// This header is set by EncodeLabelsSeriesQueryRequest according to the codec's config, so we
			// should always expect it to be present on the re-encoded request.
			expected.Header.Set("Accept", "application/json")

			// This header is set by EncodeMetricsQueryRequest and based on the provided context.
			expected.Header.Set(user.OrgIDHeaderName, userID)

			ctx := user.InjectOrgID(context.Background(), userID)

			decoded, err := codec.DecodeLabelsSeriesQueryRequest(ctx, expected)
			require.NoError(t, err)
			assert.Equal(t, tc.request, decoded)

			encoded, err := codec.EncodeLabelsSeriesQueryRequest(ctx, decoded)
			require.NoError(t, err)
			assert.Equal(t, expected.URL, encoded.URL)
			assert.Equal(t, expected.Header, encoded.Header)

			decoded, err = codec.DecodeLabelsSeriesQueryRequest(ctx, encoded)
			require.NoError(t, err)
			assert.Equal(t, tc.request, decoded)

			encoded, err = codec.EncodeLabelsSeriesQueryRequest(ctx, decoded)
			require.NoError(t, err)
			assert.Equal(t, expected.URL, encoded.URL)
			assert.Equal(t, expected.Header, encoded.Header)
		})
	}
}

func TestCodec_DecodeMultipleTimes(t *testing.T) {
	const query = "sum by (namespace) (container_memory_rss)"
	t.Run("instant query", func(t *testing.T) {
		params := url.Values{
			"query": []string{query},
			"time":  []string{"1000000000.011"},
		}
		req, err := http.NewRequest("POST", "/api/v1/query?", strings.NewReader(params.Encode()))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Accept", "application/json")

		ctx := context.Background()
		codec := newTestCodec()
		decoded, err := codec.DecodeMetricsQueryRequest(ctx, req)
		require.NoError(t, err)
		require.Equal(t, query, decoded.GetQuery())

		// Decode the same request again.
		decoded2, err := codec.DecodeMetricsQueryRequest(ctx, req)
		require.NoError(t, err)
		require.Equal(t, query, decoded2.GetQuery())

		require.Equal(t, decoded, decoded2)
	})
	t.Run("range query", func(t *testing.T) {
		params := url.Values{
			"query": []string{query},
			"start": []string{"1000000000.011"},
			"end":   []string{"1000000010.022"},
			"step":  []string{"1s"},
		}
		req, err := http.NewRequest("POST", "/api/v1/query_range?", strings.NewReader(params.Encode()))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Accept", "application/json")

		ctx := context.Background()
		codec := newTestCodec()
		decoded, err := codec.DecodeMetricsQueryRequest(ctx, req)
		require.NoError(t, err)
		require.Equal(t, query, decoded.GetQuery())

		// Decode the same request again.
		decoded2, err := codec.DecodeMetricsQueryRequest(ctx, req)
		require.NoError(t, err)
		require.Equal(t, query, decoded2.GetQuery())

		require.Equal(t, decoded, decoded2)
	})
}

func TestCodec_DecodeEncode_Stats(t *testing.T) {
	const query = "sum by (namespace) (container_memory_rss)"
	const allStats = "all"
	t.Run("instant query", func(t *testing.T) {
		params := url.Values{
			"query": []string{query},
			"time":  []string{"1000000000.011"},
			"stats": []string{allStats},
		}
		req, err := http.NewRequest("POST", "/api/v1/query?", strings.NewReader(params.Encode()))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Accept", "application/json")

		ctx := context.Background()
		codec := newTestCodec()
		decoded, err := codec.DecodeMetricsQueryRequest(ctx, req)
		require.NoError(t, err)
		require.Equal(t, allStats, decoded.GetStats())
	})
	t.Run("range query", func(t *testing.T) {
		params := url.Values{
			"query": []string{query},
			"start": []string{"1000000000.011"},
			"end":   []string{"1000000010.022"},
			"step":  []string{"1s"},
			"stats": []string{allStats},
		}
		req, err := http.NewRequest("POST", "/api/v1/query_range?", strings.NewReader(params.Encode()))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Accept", "application/json")

		ctx := context.Background()
		codec := newTestCodec()
		decoded, err := codec.DecodeMetricsQueryRequest(ctx, req)
		require.NoError(t, err)
		require.Equal(t, allStats, decoded.GetStats())
	})
	t.Run("instant query with no stats", func(t *testing.T) {
		params := url.Values{
			"query": []string{query},
			"time":  []string{"1000000000.011"},
		}
		req, err := http.NewRequest("POST", "/api/v1/query?", strings.NewReader(params.Encode()))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Accept", "application/json")

		ctx := context.Background()
		codec := newTestCodec()
		decoded, err := codec.DecodeMetricsQueryRequest(ctx, req)
		require.NoError(t, err)
		require.Equal(t, "", decoded.GetStats())
	})
	t.Run("range query with no stats", func(t *testing.T) {
		params := url.Values{
			"query": []string{query},
			"start": []string{"1000000000.011"},
			"end":   []string{"1000000010.022"},
			"step":  []string{"1s"},
		}
		req, err := http.NewRequest("POST", "/api/v1/query_range?", strings.NewReader(params.Encode()))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Accept", "application/json")

		ctx := context.Background()
		codec := newTestCodec()
		decoded, err := codec.DecodeMetricsQueryRequest(ctx, req)
		require.NoError(t, err)
		require.Equal(t, "", decoded.GetStats())
	})
}

func newTestCodec() Codec {
	return newTestCodecWithHeaders(nil)
}

func newTestCodecWithFormat(format string) Codec {
	return newTestCodecWithFormatAndHeaders(format, nil)
}

func newTestCodecWithHeaders(propagateHeaders []string) Codec {
	return newTestCodecWithFormatAndHeaders(formatJSON, propagateHeaders)
}

func newTestCodecWithFormatAndHeaders(format string, propagateHeaders []string) Codec {
	return NewCodec(prometheus.NewPedanticRegistry(), 0*time.Minute, format, propagateHeaders, &api.ConsistencyInjector{})
}

func mustSucceed[T any](value T, err error) T {
	if err != nil {
		panic(err)
	}

	return value
}

func TestContextHeaderPropagation(t *testing.T) {
	headers := HeadersToPropagateFromContext(context.Background())
	require.Empty(t, headers)

	ctx := ContextWithHeadersToPropagate(context.Background(), map[string][]string{"Some-Header": {"Some-Value"}})
	headers = HeadersToPropagateFromContext(ctx)
	require.Equal(t, map[string][]string{"Some-Header": {"Some-Value"}}, headers)
}
