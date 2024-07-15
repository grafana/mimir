// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/cardinality"
	pkg_distributor "github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestLabelNamesCardinalityHandler(t *testing.T) {
	items := []*client.LabelValues{
		{LabelName: "label-c", Values: []string{"0c"}},
		{LabelName: "label-b", Values: []string{"0b", "1b"}},
		{LabelName: "label-a", Values: []string{"0a", "1a"}},
		{LabelName: "label-z", Values: []string{"0z", "1z", "2z"}},
	}
	distributor := mockDistributorLabelNamesAndValues(items, nil)
	handler := createEnabledHandler(t, LabelNamesCardinalityHandler, distributor)
	ctx := user.InjectOrgID(context.Background(), "team-a")
	request, err := http.NewRequestWithContext(ctx, "GET", "/ignored-url?limit=4", http.NoBody)
	require.NoError(t, err)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Result().StatusCode)
	body := recorder.Result().Body
	defer body.Close()
	responseBody := api.LabelNamesCardinalityResponse{}
	bodyContent, err := io.ReadAll(body)
	require.NoError(t, err)
	err = json.Unmarshal(bodyContent, &responseBody)
	require.NoError(t, err)
	require.Equal(t, 4, responseBody.LabelNamesCount)
	require.Equal(t, 8, responseBody.LabelValuesCountTotal)
	require.Len(t, responseBody.Cardinality, 4)
	require.Equal(t, responseBody.Cardinality[0], &api.LabelNamesCardinalityItem{LabelName: "label-z", LabelValuesCount: 3},
		"items must be sorted by LabelValuesCount in DESC order and by LabelName in ASC order")
	require.Equal(t, responseBody.Cardinality[1], &api.LabelNamesCardinalityItem{LabelName: "label-a", LabelValuesCount: 2},
		"items must be sorted by LabelValuesCount in DESC order and by LabelName in ASC order")
	require.Equal(t, responseBody.Cardinality[2], &api.LabelNamesCardinalityItem{LabelName: "label-b", LabelValuesCount: 2},
		"items must be sorted by LabelValuesCount in DESC order and by LabelName in ASC order")
	require.Equal(t, responseBody.Cardinality[3], &api.LabelNamesCardinalityItem{LabelName: "label-c", LabelValuesCount: 1},
		"items must be sorted by LabelValuesCount in DESC order and by LabelName in ASC order")
}

func TestLabelNamesCardinalityHandler_MatchersTest(t *testing.T) {
	td := []struct {
		name             string
		selector         string
		expectedMatchers []*labels.Matcher
	}{
		{
			name:             "expected selector to be parsed",
			selector:         "{__name__='metric'}",
			expectedMatchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "metric")},
		},
		{
			name:             "expected no error if selector is missed",
			selector:         "",
			expectedMatchers: nil,
		},
		{
			name:     "selector with metric name to be parse",
			selector: "metric{env!='prod'}",
			expectedMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "metric"),
				labels.MustNewMatcher(labels.MatchNotEqual, "env", "prod"),
			},
		},
		{
			name:     "selector with two matchers to be parse",
			selector: "{__name__='metric',env!='prod'}",
			expectedMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "metric"),
				labels.MustNewMatcher(labels.MatchNotEqual, "env", "prod"),
			},
		},
	}
	for _, data := range td {
		t.Run(data.name, func(t *testing.T) {
			distributor := mockDistributorLabelNamesAndValues([]*client.LabelValues{}, nil)
			handler := createEnabledHandler(t, LabelNamesCardinalityHandler, distributor)
			ctx := user.InjectOrgID(context.Background(), "team-a")
			recorder := httptest.NewRecorder()
			path := "/ignored-url"
			if len(data.selector) > 0 {
				path += "?selector=" + data.selector
			}
			request, err := http.NewRequestWithContext(ctx, "GET", path, http.NoBody)
			require.NoError(t, err)
			handler.ServeHTTP(recorder, request)
			body := recorder.Result().Body
			defer body.Close()
			bodyContent, err := io.ReadAll(body)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, recorder.Result().StatusCode, "unexpected error %v", string(bodyContent))
			distributor.AssertCalled(t, "LabelNamesAndValues", mock.Anything, data.expectedMatchers, cardinality.InMemoryMethod)
		})
	}
}

func TestLabelNamesCardinalityHandler_LimitTest(t *testing.T) {
	td := []struct {
		name                string
		limit               int
		expectedValuesCount int
	}{
		{
			name:                "expected 10 labels in response if limit param is 10",
			limit:               10,
			expectedValuesCount: 10,
		},
		{
			name:                "expected default limit 20 to be applied if limit param is not defined",
			limit:               -1,
			expectedValuesCount: 20,
		},
		{
			name:                "expected all items in response if limit param is greater than count of items",
			limit:               40,
			expectedValuesCount: 30,
		},
		{
			name:                "expected empty items list in response if limit param is 0",
			limit:               0,
			expectedValuesCount: 0,
		},
	}
	for _, data := range td {
		t.Run(data.name, func(t *testing.T) {
			labelCountTotal := 30
			items, valuesCountTotal := generateLabelValues(labelCountTotal)
			distributor := mockDistributorLabelNamesAndValues(items, nil)
			handler := createEnabledHandler(t, LabelNamesCardinalityHandler, distributor)

			ctx := user.InjectOrgID(context.Background(), "team-a")
			path := "/ignored-url"
			if data.limit >= 0 {
				path += fmt.Sprintf("?limit=%v", data.limit)
			}
			request, err := http.NewRequestWithContext(ctx, "GET", path, http.NoBody)
			require.NoError(t, err)
			recorder := httptest.NewRecorder()

			handler.ServeHTTP(recorder, request)

			require.Equal(t, http.StatusOK, recorder.Result().StatusCode)
			body := recorder.Result().Body
			defer body.Close()
			responseBody := api.LabelNamesCardinalityResponse{}
			bodyContent, err := io.ReadAll(body)
			require.NoError(t, err)
			err = json.Unmarshal(bodyContent, &responseBody)
			require.NoError(t, err)
			require.Equal(t, labelCountTotal, responseBody.LabelNamesCount)
			require.Equal(t, valuesCountTotal, responseBody.LabelValuesCountTotal)
			require.Len(t, responseBody.Cardinality, data.expectedValuesCount)
		})
	}
}

func TestLabelNamesCardinalityHandler_DistributorError(t *testing.T) {
	const labelNamesURL = "/label_names"

	tests := map[string]struct {
		distributorError       error
		expectedHTTPStatusCode int
		expectedHTTPBody       string
	}{
		"should return an HTTP response with status code and response body of the httpgrpc error returned by the distributor": {
			distributorError: httpgrpc.ErrorFromHTTPResponse(&httpgrpc.HTTPResponse{
				Code: int32(400),
				Body: []byte("httpgrpc error"),
			}),
			expectedHTTPStatusCode: 400,
			expectedHTTPBody:       "httpgrpc error",
		},
		"should return internal server error if the distributor returns a non httpgrpc error": {
			distributorError:       fmt.Errorf("non httpgrpc error"),
			expectedHTTPStatusCode: 500,
			expectedHTTPBody:       "non httpgrpc error\n",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			distributor := mockDistributorLabelNamesAndValues([]*client.LabelValues{}, testData.distributorError)
			limits := validation.Limits{}
			flagext.DefaultValues(&limits)
			limits.CardinalityAnalysisEnabled = true
			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)
			handler := LabelNamesCardinalityHandler(distributor, overrides)
			ctx := user.InjectOrgID(context.Background(), "test")

			request, err := http.NewRequestWithContext(ctx, "GET", labelNamesURL, http.NoBody)
			require.NoError(t, err)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)

			require.Equal(t, testData.expectedHTTPStatusCode, recorder.Result().StatusCode)

			body := recorder.Result().Body
			defer func() { _ = body.Close() }()

			bodyContent, err := io.ReadAll(body)
			require.NoError(t, err)
			bodyErrMessage := string(bodyContent)

			require.Equal(t, testData.expectedHTTPBody, bodyErrMessage)
		})
	}
}

func TestLabelNamesCardinalityHandler_NegativeTests(t *testing.T) {
	td := []struct {
		name                        string
		request                     *http.Request
		expectedErrorMessage        string
		cardinalityAnalysisDisabled bool
	}{
		{
			name:                 "expected error if `limit` param is negative",
			request:              createRequest("/ignored-url?limit=-1", "team-a"),
			expectedErrorMessage: "'limit' param cannot be less than '0'",
		},
		{
			name:                 "expected error if `limit` param is negative",
			request:              createRequest("/ignored-url?limit=5000", "team-a"),
			expectedErrorMessage: "'limit' param cannot be greater than '500'",
		},
		{
			name:                 "expected error if tenantId is not defined",
			request:              createRequest("/ignored-url", ""),
			expectedErrorMessage: "no org id",
		},
		{
			name:                 "expected error if multiple limits are sent",
			request:              createRequest("/ignored-url?limit=10&limit=20", "team-a"),
			expectedErrorMessage: "multiple 'limit' params are not allowed",
		},
		{
			name:                        "expected error that cardinality analysis feature is disabled",
			request:                     createRequest("/ignored-url", "team-a"),
			expectedErrorMessage:        "cardinality analysis is disabled for the tenant: team-a",
			cardinalityAnalysisDisabled: true,
		},
	}
	for _, data := range td {
		t.Run(data.name, func(t *testing.T) {
			limits := validation.Limits{}
			flagext.DefaultValues(&limits)
			if !data.cardinalityAnalysisDisabled {
				limits.CardinalityAnalysisEnabled = true
			}
			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)
			handler := LabelNamesCardinalityHandler(mockDistributorLabelNamesAndValues([]*client.LabelValues{}, nil), overrides)

			recorder := httptest.NewRecorder()

			handler.ServeHTTP(recorder, data.request)

			require.Equal(t, http.StatusBadRequest, recorder.Result().StatusCode)
			body := recorder.Result().Body
			defer body.Close()
			bytes, err := io.ReadAll(body)
			require.NoError(t, err)
			require.Contains(t, string(bytes), data.expectedErrorMessage)
		})
	}
}

func TestLabelValuesCardinalityHandler_Success(t *testing.T) {
	const labelValuesURL = "/label_values"
	seriesCountTotal := uint64(100)
	nameMatcher, _ := labels.NewMatcher(labels.MatchEqual, "__name__", "test_1")

	tests := map[string]struct {
		getRequestParams       string
		postRequestForm        url.Values
		labelNames             []model.LabelName
		matcher                []*labels.Matcher
		scope                  cardinality.CountMethod
		labelValuesCardinality *client.LabelValuesCardinalityResponse
		expectedResponse       api.LabelValuesCardinalityResponse
	}{
		"should return the label values cardinality for the specified label name": {
			getRequestParams: "?label_names[]=__name__",
			postRequestForm: url.Values{
				"label_names[]": []string{"__name__"},
			},
			labelNames: []model.LabelName{"__name__"},
			matcher:    []*labels.Matcher(nil),
			scope:      cardinality.InMemoryMethod,
			labelValuesCardinality: &client.LabelValuesCardinalityResponse{
				Items: []*client.LabelValueSeriesCount{{
					LabelName:        labels.MetricName,
					LabelValueSeries: map[string]uint64{"test_1": 10},
				}},
			},
			expectedResponse: api.LabelValuesCardinalityResponse{
				SeriesCountTotal: seriesCountTotal,
				Labels: []api.LabelNamesCardinality{{
					LabelName:        "__name__",
					LabelValuesCount: 1,
					SeriesCount:      10,
					Cardinality: []api.LabelValuesCardinality{
						{LabelValue: "test_1", SeriesCount: 10},
					},
				}},
			},
		},
		"should return the label values cardinality for the specified label name with matching selector": {
			getRequestParams: "?label_names[]=__name__&selector={__name__='test_1'}",
			postRequestForm: url.Values{
				"label_names[]": []string{"__name__"},
				"selector":      []string{"{__name__='test_1'}"},
			},
			labelNames: []model.LabelName{"__name__"},
			matcher:    []*labels.Matcher{nameMatcher},
			scope:      cardinality.InMemoryMethod,
			labelValuesCardinality: &client.LabelValuesCardinalityResponse{
				Items: []*client.LabelValueSeriesCount{{
					LabelName:        labels.MetricName,
					LabelValueSeries: map[string]uint64{"test_1": 10},
				}},
			},
			expectedResponse: api.LabelValuesCardinalityResponse{
				SeriesCountTotal: seriesCountTotal,
				Labels: []api.LabelNamesCardinality{{
					LabelName:        "__name__",
					LabelValuesCount: 1,
					SeriesCount:      10,
					Cardinality: []api.LabelValuesCardinality{
						{LabelValue: "test_1", SeriesCount: 10},
					},
				}},
			},
		},
		"should return the label values cardinality for the specified label names in descending order": {
			getRequestParams: "?label_names[]=foo&label_names[]=bar",
			postRequestForm: url.Values{
				"label_names[]": []string{"foo", "bar"},
			},
			labelNames: []model.LabelName{"bar", "foo"},
			matcher:    []*labels.Matcher(nil),
			scope:      cardinality.InMemoryMethod,
			labelValuesCardinality: &client.LabelValuesCardinalityResponse{
				Items: []*client.LabelValueSeriesCount{
					{
						LabelName:        "bar",
						LabelValueSeries: map[string]uint64{"test_1": 20},
					},
					{
						LabelName:        "foo",
						LabelValueSeries: map[string]uint64{"test_1": 10},
					},
				},
			},
			expectedResponse: api.LabelValuesCardinalityResponse{
				SeriesCountTotal: seriesCountTotal,
				Labels: []api.LabelNamesCardinality{
					{
						LabelName:        "bar",
						LabelValuesCount: 1,
						SeriesCount:      20,
						Cardinality: []api.LabelValuesCardinality{
							{LabelValue: "test_1", SeriesCount: 20},
						},
					},
					{
						LabelName:        "foo",
						LabelValuesCount: 1,
						SeriesCount:      10,
						Cardinality: []api.LabelValuesCardinality{
							{LabelValue: "test_1", SeriesCount: 10},
						},
					},
				},
			},
		},
		"should return the label values cardinality for the specified label names in ascending order for label names with the same series count": {
			getRequestParams: "?label_names[]=foo&label_names[]=bar",
			postRequestForm: url.Values{
				"label_names[]": []string{"foo", "bar"},
			},
			labelNames: []model.LabelName{"bar", "foo"},
			matcher:    []*labels.Matcher(nil),
			scope:      cardinality.InMemoryMethod,
			labelValuesCardinality: &client.LabelValuesCardinalityResponse{
				Items: []*client.LabelValueSeriesCount{
					{
						LabelName:        "bar",
						LabelValueSeries: map[string]uint64{"test_1": 10},
					},
					{
						LabelName:        "foo",
						LabelValueSeries: map[string]uint64{"test_1": 10},
					},
				},
			},
			expectedResponse: api.LabelValuesCardinalityResponse{
				SeriesCountTotal: seriesCountTotal,
				Labels: []api.LabelNamesCardinality{
					{
						LabelName:        "bar",
						LabelValuesCount: 1,
						SeriesCount:      10,
						Cardinality: []api.LabelValuesCardinality{
							{LabelValue: "test_1", SeriesCount: 10},
						},
					},
					{
						LabelName:        "foo",
						LabelValuesCount: 1,
						SeriesCount:      10,
						Cardinality: []api.LabelValuesCardinality{
							{LabelValue: "test_1", SeriesCount: 10},
						},
					},
				},
			},
		},
		"should return the label values cardinality sorted by series count in descending order": {
			getRequestParams: "?label_names[]=__name__",
			postRequestForm: url.Values{
				"label_names[]": []string{"__name__"},
			},
			labelNames: []model.LabelName{"__name__"},
			matcher:    []*labels.Matcher(nil),
			scope:      cardinality.InMemoryMethod,
			labelValuesCardinality: &client.LabelValuesCardinalityResponse{
				Items: []*client.LabelValueSeriesCount{{
					LabelName:        labels.MetricName,
					LabelValueSeries: map[string]uint64{"test_1": 10, "test_2": 20},
				}},
			},
			expectedResponse: api.LabelValuesCardinalityResponse{
				SeriesCountTotal: seriesCountTotal,
				Labels: []api.LabelNamesCardinality{{
					LabelName:        "__name__",
					LabelValuesCount: 2,
					SeriesCount:      30,
					Cardinality: []api.LabelValuesCardinality{
						{LabelValue: "test_2", SeriesCount: 20},
						{LabelValue: "test_1", SeriesCount: 10},
					},
				}},
			},
		},
		"should return the label values cardinality sorted by label name in ascending order for label values with the same series count": {
			getRequestParams: "?label_names[]=__name__",
			postRequestForm: url.Values{
				"label_names[]": []string{"__name__"},
			},
			labelNames: []model.LabelName{"__name__"},
			matcher:    []*labels.Matcher(nil),
			scope:      cardinality.InMemoryMethod,
			labelValuesCardinality: &client.LabelValuesCardinalityResponse{
				Items: []*client.LabelValueSeriesCount{{
					LabelName:        labels.MetricName,
					LabelValueSeries: map[string]uint64{"test_1": 10, "test_2": 10},
				}},
			},
			expectedResponse: api.LabelValuesCardinalityResponse{
				SeriesCountTotal: seriesCountTotal,
				Labels: []api.LabelNamesCardinality{{
					LabelName:        "__name__",
					LabelValuesCount: 2,
					SeriesCount:      20,
					Cardinality: []api.LabelValuesCardinality{
						{LabelValue: "test_1", SeriesCount: 10},
						{LabelValue: "test_2", SeriesCount: 10},
					},
				}},
			},
		},
		"should return all the label values cardinality array if the number of label values is equal to the specified limit": {
			getRequestParams: "?label_names[]=__name__&limit=3",
			postRequestForm: url.Values{
				"label_names[]": []string{"__name__"},
				"limit":         []string{"3"},
			},
			labelNames: []model.LabelName{"__name__"},
			matcher:    []*labels.Matcher(nil),
			scope:      cardinality.InMemoryMethod,
			labelValuesCardinality: &client.LabelValuesCardinalityResponse{
				Items: []*client.LabelValueSeriesCount{{
					LabelName:        labels.MetricName,
					LabelValueSeries: map[string]uint64{"test_1": 100, "test_2": 20, "test_3": 30},
				}},
			},
			expectedResponse: api.LabelValuesCardinalityResponse{
				SeriesCountTotal: seriesCountTotal,
				Labels: []api.LabelNamesCardinality{{
					LabelName:        "__name__",
					LabelValuesCount: 3,
					SeriesCount:      150,
					Cardinality: []api.LabelValuesCardinality{
						{LabelValue: "test_1", SeriesCount: 100},
						{LabelValue: "test_3", SeriesCount: 30},
						{LabelValue: "test_2", SeriesCount: 20},
					},
				}},
			},
		},
		"should return the label values cardinality array limited by the limit param": {
			getRequestParams: "?label_names[]=__name__&limit=2",
			postRequestForm: url.Values{
				"label_names[]": []string{"__name__"},
				"limit":         []string{"2"},
			},
			labelNames: []model.LabelName{"__name__"},
			matcher:    []*labels.Matcher(nil),
			scope:      cardinality.InMemoryMethod,
			labelValuesCardinality: &client.LabelValuesCardinalityResponse{
				Items: []*client.LabelValueSeriesCount{{
					LabelName:        labels.MetricName,
					LabelValueSeries: map[string]uint64{"test_1": 100, "test_2": 20, "test_3": 30},
				}},
			},
			expectedResponse: api.LabelValuesCardinalityResponse{
				SeriesCountTotal: seriesCountTotal,
				Labels: []api.LabelNamesCardinality{{
					LabelName:        "__name__",
					LabelValuesCount: 3,
					SeriesCount:      150,
					Cardinality: []api.LabelValuesCardinality{
						{LabelValue: "test_1", SeriesCount: 100},
						{LabelValue: "test_3", SeriesCount: 30},
					},
				}},
			},
		},
	}

	for testName, testData := range tests {
		distributor := mockDistributorLabelValuesCardinality(
			testData.labelNames,
			testData.matcher,
			testData.scope,
			seriesCountTotal,
			testData.labelValuesCardinality,
			nil)
		handler := createEnabledHandler(t, LabelValuesCardinalityHandler, distributor)
		ctx := user.InjectOrgID(context.Background(), "test")

		t.Run("GET request "+testName, func(t *testing.T) {
			request, err := http.NewRequestWithContext(ctx, "GET", labelValuesURL+testData.getRequestParams, http.NoBody)
			require.NoError(t, err)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)

			require.Equal(t, http.StatusOK, recorder.Result().StatusCode)

			body := recorder.Result().Body
			defer func() { _ = body.Close() }()

			responseBody := api.LabelValuesCardinalityResponse{}
			bodyContent, err := io.ReadAll(body)
			require.NoError(t, err)
			err = json.Unmarshal(bodyContent, &responseBody)
			require.NoError(t, err)

			require.Equal(t, testData.expectedResponse, responseBody)
		})
		t.Run("POST request "+testName, func(t *testing.T) {
			request, err := http.NewRequestWithContext(ctx, "POST", labelValuesURL, strings.NewReader(testData.postRequestForm.Encode()))
			request.Header.Add("Content-Type", "application/x-www-form-urlencoded")
			require.NoError(t, err)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)

			require.Equal(t, http.StatusOK, recorder.Result().StatusCode)

			body := recorder.Result().Body
			defer func() { _ = body.Close() }()

			responseBody := api.LabelValuesCardinalityResponse{}
			bodyContent, err := io.ReadAll(body)
			require.NoError(t, err)
			err = json.Unmarshal(bodyContent, &responseBody)
			require.NoError(t, err)

			require.Equal(t, testData.expectedResponse, responseBody)
		})
	}
}

func TestLabelValuesCardinalityHandler_FeatureFlag(t *testing.T) {
	const labelValuesURL = "/label_values?label_names[]=foo"

	tests := map[string]struct {
		request                    *http.Request
		cardinalityAnalysisEnabled bool
		expectedStatusCode         int
		expectedErrorMessage       string
	}{
		"should return an error if the cardinality analysis feature is disabled by default": {
			request:                    createRequest(labelValuesURL, "team-a"),
			cardinalityAnalysisEnabled: false,
			expectedStatusCode:         http.StatusBadRequest,
			expectedErrorMessage:       "cardinality analysis is disabled for the tenant: team-a\n",
		},
		"should succeed if the cardinality analysis feature is enabled by default": {
			request:                    createRequest(labelValuesURL, "team-a"),
			cardinalityAnalysisEnabled: true,
			expectedStatusCode:         http.StatusOK,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			distributor := mockDistributorLabelValuesCardinality(
				[]model.LabelName{"foo"},
				[]*labels.Matcher(nil),
				cardinality.InMemoryMethod,
				uint64(0),
				&client.LabelValuesCardinalityResponse{Items: []*client.LabelValueSeriesCount{}},
				nil)

			limits := validation.Limits{CardinalityAnalysisEnabled: testData.cardinalityAnalysisEnabled}
			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)
			handler := LabelValuesCardinalityHandler(distributor, overrides)

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, testData.request)

			require.Equal(t, testData.expectedStatusCode, recorder.Result().StatusCode)

			if len(testData.expectedErrorMessage) > 0 {
				body := recorder.Result().Body
				defer func() { _ = body.Close() }()

				bodyContent, err := io.ReadAll(body)
				require.NoError(t, err)
				require.Equal(t, string(bodyContent), testData.expectedErrorMessage)
			}
		})
	}
}

func TestLabelValuesCardinalityHandler_ParseError(t *testing.T) {
	distributor := mockDistributorLabelValuesCardinality(
		[]model.LabelName{},
		[]*labels.Matcher(nil),
		cardinality.InMemoryMethod,
		uint64(0),
		&client.LabelValuesCardinalityResponse{Items: []*client.LabelValueSeriesCount{}},
		nil)
	handler := createEnabledHandler(t, LabelValuesCardinalityHandler, distributor)
	ctx := user.InjectOrgID(context.Background(), "test")

	t.Run("should return bad request if no tenant id is provided", func(t *testing.T) {
		request, err := http.NewRequestWithContext(context.Background(), "GET", "/label_values", http.NoBody)
		require.NoError(t, err)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)

		require.Equal(t, http.StatusBadRequest, recorder.Result().StatusCode)
	})
	t.Run("should return bad request if ", func(t *testing.T) {
		tests := map[string]struct {
			url                  string
			expectedErrorMessage string
		}{
			"label_names param is empty": {
				url:                  "/label_values",
				expectedErrorMessage: "'label_names[]' param is required",
			},
			"label_names param is invalid": {
				url:                  "/label_values?label_names[]=olá",
				expectedErrorMessage: "invalid 'label_names' param 'olá'",
			},
			"multiple selector params are provided": {
				url:                  "/label_values?label_names[]=hello&selector=foo&selector=bar",
				expectedErrorMessage: "multiple 'selector' params are not allowed",
			},
			"limit param is not a number": {
				url:                  "/label_values?label_names[]=hello&limit=foo",
				expectedErrorMessage: "strconv.Atoi: parsing \"foo\": invalid syntax",
			},
			"limit param is a negative number": {
				url:                  "/label_values?label_names[]=hello&limit=-20",
				expectedErrorMessage: "'limit' param cannot be less than '0'",
			},
			"limit param exceeds the maximum limit parameter": {
				url:                  "/label_values?label_names[]=hello&limit=501",
				expectedErrorMessage: "'limit' param cannot be greater than '500'",
			},
		}
		for testName, testData := range tests {
			t.Run(testName, func(t *testing.T) {
				request, err := http.NewRequestWithContext(ctx, "GET", testData.url, http.NoBody)
				require.NoError(t, err)
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, request)

				require.Equal(t, http.StatusBadRequest, recorder.Result().StatusCode)

				body := recorder.Result().Body
				defer func() { _ = body.Close() }()

				bytes, err := io.ReadAll(body)
				require.NoError(t, err)
				require.Contains(t, string(bytes), testData.expectedErrorMessage)
			})

		}
	})
}

func TestLabelValuesCardinalityHandler_DistributorError(t *testing.T) {
	const labelValuesURL = "/label_values?label_names[]=foo&label_names[]=bar"

	tests := map[string]struct {
		distributorError       error
		expectedHTTPStatusCode int
		expectedHTTPBody       string
	}{
		"should return an HTTP response with status code and response body of the httpgrpc error returned by the distributor": {
			distributorError: httpgrpc.ErrorFromHTTPResponse(&httpgrpc.HTTPResponse{
				Code: int32(400),
				Body: []byte("httpgrpc error"),
			}),
			expectedHTTPStatusCode: 400,
			expectedHTTPBody:       "httpgrpc error",
		},
		"should return internal server error if the distributor returns a non httpgrpc error": {
			distributorError:       fmt.Errorf("non httpgrpc error"),
			expectedHTTPStatusCode: 500,
			expectedHTTPBody:       "non httpgrpc error\n",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			distributor := mockDistributorLabelValuesCardinality(
				[]model.LabelName{"bar", "foo"},
				[]*labels.Matcher(nil),
				cardinality.InMemoryMethod,
				uint64(0),
				&client.LabelValuesCardinalityResponse{Items: []*client.LabelValueSeriesCount{}},
				testData.distributorError)
			handler := createEnabledHandler(t, LabelValuesCardinalityHandler, distributor)
			ctx := user.InjectOrgID(context.Background(), "test")

			request, err := http.NewRequestWithContext(ctx, "GET", labelValuesURL, http.NoBody)
			require.NoError(t, err)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)

			require.Equal(t, testData.expectedHTTPStatusCode, recorder.Result().StatusCode)

			body := recorder.Result().Body
			defer func() { _ = body.Close() }()

			bodyContent, err := io.ReadAll(body)
			require.NoError(t, err)
			bodyErrMessage := string(bodyContent)

			require.Equal(t, testData.expectedHTTPBody, bodyErrMessage)
		})
	}
}

func TestActiveSeriesCardinalityHandler(t *testing.T) {
	tests := []struct {
		name                 string
		requestParams        map[string][]string
		expectMatcherSetSize int
		returnedError        error
		expectStatusCode     int
	}{
		{
			name:             "should error on missing selector param",
			expectStatusCode: http.StatusBadRequest,
		},
		{
			name:             "should error on invalid selector",
			requestParams:    map[string][]string{"selector": {"-not-valid-"}},
			expectStatusCode: http.StatusBadRequest,
		},
		{
			name:             "should error on multiple selectors",
			requestParams:    map[string][]string{"selector": {"a", "b"}},
			expectStatusCode: http.StatusBadRequest,
		},
		{
			name:             "valid selector",
			requestParams:    map[string][]string{"selector": {`{job="prometheus"}`}},
			expectStatusCode: http.StatusOK,
		},
		{
			name:             "upstream error: response too large",
			requestParams:    map[string][]string{"selector": {`{job="prometheus"}`}},
			returnedError:    pkg_distributor.ErrResponseTooLarge,
			expectStatusCode: http.StatusRequestEntityTooLarge,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := &mockDistributor{}

			series := []labels.Labels{
				labels.FromStrings("__name__", "up", "job", "prometheus"),
				labels.FromStrings("__name__", "process_start_time_seconds", "job", "prometheus"),
			}
			d.On("ActiveSeries", mock.Anything, mock.Anything).Return(series, test.returnedError)

			handler := createEnabledHandler(t, ActiveSeriesCardinalityHandler, d)
			ctx := user.InjectOrgID(context.Background(), "test")

			data := url.Values{}
			for key, values := range test.requestParams {
				for _, value := range values {
					data.Add(key, value)
				}
			}
			request, err := http.NewRequestWithContext(ctx, "POST", "/active_series", strings.NewReader(data.Encode()))
			require.NoError(t, err)
			request.Header.Add("Content-Type", "application/x-www-form-urlencoded")

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)

			assert.Equal(t, test.expectStatusCode, recorder.Result().StatusCode)

			if test.expectStatusCode != http.StatusOK {
				return
			}

			require.Equal(t, http.StatusOK, recorder.Result().StatusCode)

			body := recorder.Result().Body
			defer func(body io.ReadCloser) {
				err := body.Close()
				require.NoError(t, err)
			}(body)
			bodyContent, err := io.ReadAll(body)
			require.NoError(t, err)

			resp := api.ActiveSeriesResponse{}
			err = json.Unmarshal(bodyContent, &resp)
			require.NoError(t, err)
			assert.Len(t, resp.Data, len(series))
		})
	}
}

func BenchmarkActiveSeriesHandler_ServeHTTP(b *testing.B) {
	const numResponseSeries = 1000

	d := &mockDistributor{}

	var s []labels.Labels
	for i := 0; i < numResponseSeries; i++ {
		s = append(s, labels.FromStrings("__name__", "up", "job", "prometheus", "instance", "instance"+fmt.Sprint(i)))
	}
	d.On("ActiveSeries", mock.Anything, mock.Anything).Return(s, nil)

	handler := createEnabledHandler(b, ActiveSeriesCardinalityHandler, d)
	ctx := user.InjectOrgID(context.Background(), "test")

	for i := 0; i < b.N; i++ {
		// Prepare a request.
		r := httptest.NewRequest("POST", "/active_series", strings.NewReader("selector={job=\"prometheus\"}"))
		r.Header.Add("Content-Type", "application/x-www-form-urlencoded")

		// Run the benchmark.
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, r.WithContext(ctx))

		// Make sure we're not benchmarking error responses.
		require.Equal(b, http.StatusOK, recorder.Result().StatusCode)
	}
}

func TestActiveNativeHistogramMetricsCardinalityHandler(t *testing.T) {
	tests := []struct {
		name                 string
		requestParams        map[string][]string
		expectMatcherSetSize int
		returnedError        error
		expectStatusCode     int
	}{
		{
			name:             "should error on missing selector param",
			expectStatusCode: http.StatusBadRequest,
		},
		{
			name:             "should error on invalid selector",
			requestParams:    map[string][]string{"selector": {"-not-valid-"}},
			expectStatusCode: http.StatusBadRequest,
		},
		{
			name:             "should error on multiple selectors",
			requestParams:    map[string][]string{"selector": {"a", "b"}},
			expectStatusCode: http.StatusBadRequest,
		},
		{
			name:             "valid selector",
			requestParams:    map[string][]string{"selector": {`{job="prometheus"}`}},
			expectStatusCode: http.StatusOK,
		},
		{
			name:             "upstream error: response too large",
			requestParams:    map[string][]string{"selector": {`{job="prometheus"}`}},
			returnedError:    pkg_distributor.ErrResponseTooLarge,
			expectStatusCode: http.StatusRequestEntityTooLarge,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := &mockDistributor{}

			dResp := cardinality.ActiveNativeHistogramMetricsResponse{
				Data: []cardinality.ActiveMetricWithBucketCount{
					{
						Metric:         "request_duration_seconds",
						SeriesCount:    10,
						BucketCount:    200,
						AvgBucketCount: 20.0,
						MinBucketCount: 1,
						MaxBucketCount: 40,
					},
					{
						Metric:         "store_duration_seconds",
						SeriesCount:    10,
						BucketCount:    100,
						AvgBucketCount: 10.0,
						MinBucketCount: 10,
						MaxBucketCount: 10,
					},
				},
			}
			d.On("ActiveNativeHistogramMetrics", mock.Anything, mock.Anything).Return(&dResp, test.returnedError)

			handler := createEnabledHandler(t, ActiveNativeHistogramMetricsHandler, d)
			ctx := user.InjectOrgID(context.Background(), "test")

			data := url.Values{}
			for key, values := range test.requestParams {
				for _, value := range values {
					data.Add(key, value)
				}
			}
			request, err := http.NewRequestWithContext(ctx, "POST", "/active_native_histogram_metrics", strings.NewReader(data.Encode()))
			require.NoError(t, err)
			request.Header.Add("Content-Type", "application/x-www-form-urlencoded")

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)

			assert.Equal(t, test.expectStatusCode, recorder.Result().StatusCode)

			if test.expectStatusCode != http.StatusOK {
				return
			}

			require.Equal(t, http.StatusOK, recorder.Result().StatusCode)

			body := recorder.Result().Body
			defer func(body io.ReadCloser) {
				err := body.Close()
				require.NoError(t, err)
			}(body)
			bodyContent, err := io.ReadAll(body)
			require.NoError(t, err)

			resp := cardinality.ActiveNativeHistogramMetricsResponse{}
			err = json.Unmarshal(bodyContent, &resp)
			require.NoError(t, err)
			assert.Len(t, resp.Data, len(dResp.Data))
		})
	}
}

func BenchmarkActiveNativeHistogramMetricsHandler_ServeHTTP(b *testing.B) {
	const numResponseMetrics = 1000

	d := &mockDistributor{}

	dResp := cardinality.ActiveNativeHistogramMetricsResponse{}
	for i := 0; i < numResponseMetrics; i++ {
		dResp.Data = append(dResp.Data, cardinality.ActiveMetricWithBucketCount{
			Metric:         "request_duration_" + fmt.Sprint(i) + "_seconds",
			SeriesCount:    10,
			BucketCount:    200,
			AvgBucketCount: 20.0,
			MinBucketCount: 1,
			MaxBucketCount: 40,
		})
	}
	d.On("ActiveNativeHistogramMetrics", mock.Anything, mock.Anything).Return(&dResp, nil)

	handler := createEnabledHandler(b, ActiveNativeHistogramMetricsHandler, d)
	ctx := user.InjectOrgID(context.Background(), "test")

	for i := 0; i < b.N; i++ {
		// Prepare a request.
		r := httptest.NewRequest("POST", "/active_native_histogram_metrics", strings.NewReader("selector={job=\"prometheus\"}"))
		r.Header.Add("Content-Type", "application/x-www-form-urlencoded")

		// Run the benchmark.
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, r.WithContext(ctx))

		// Make sure we're not benchmarking error responses.
		require.Equal(b, http.StatusOK, recorder.Result().StatusCode)
	}
}

// createEnabledHandler creates a cardinalityHandler that can be either a LabelNamesCardinalityHandler or a LabelValuesCardinalityHandler
func createEnabledHandler(t testing.TB, cardinalityHandler func(Distributor, *validation.Overrides) http.Handler, distributor *mockDistributor) http.Handler {
	limits := validation.Limits{CardinalityAnalysisEnabled: true}
	overrides, err := validation.NewOverrides(limits, nil)
	require.NoError(t, err)

	handler := cardinalityHandler(distributor, overrides)
	return handler
}

func createRequest(path string, tenantID string) *http.Request {
	ctx := context.Background()
	if len(tenantID) > 0 {
		ctx = user.InjectOrgID(ctx, tenantID)
	}
	request, _ := http.NewRequestWithContext(ctx, "GET", path, http.NoBody)
	return request
}

func generateLabelValues(count int) ([]*client.LabelValues, int) {
	valuesCount := 0
	items := make([]*client.LabelValues, count)
	for i := 0; i < count; i++ {
		values := make([]string, i+1)
		for j := 0; j < i+1; j++ {
			valuesCount++
			values[i] = fmt.Sprintf("value-%v", j)
		}
		items[i] = &client.LabelValues{LabelName: fmt.Sprintf("label-%v", i), Values: values}
	}
	return items, valuesCount
}

func mockDistributorLabelNamesAndValues(items []*client.LabelValues, err error) *mockDistributor {
	distributor := &mockDistributor{}
	distributor.On("LabelNamesAndValues", mock.Anything, mock.Anything, mock.Anything).Return(&client.LabelNamesAndValuesResponse{Items: items}, err)
	return distributor
}

func mockDistributorLabelValuesCardinality(labelNames []model.LabelName, matchers []*labels.Matcher, countMethod cardinality.CountMethod, seriesCount uint64, cardinalityResponse *client.LabelValuesCardinalityResponse, err error) *mockDistributor {
	distributor := &mockDistributor{}
	distributor.On("LabelValuesCardinality", mock.Anything, labelNames, matchers, countMethod).Return(seriesCount, cardinalityResponse, err)
	return distributor
}
