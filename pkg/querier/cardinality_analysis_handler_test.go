// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
)

func TestLabelValuesCardinalityHandler_Success(t *testing.T) {
	seriesCountTotal := uint64(100)
	nameMatcher, _ := labels.NewMatcher(labels.MatchEqual, "__name__", "test1")

	tests := map[string]struct {
		url                       string
		labelNames                []model.LabelName
		matcher                   []*labels.Matcher
		labelValuesCardinalityMap map[string]map[string]uint64
		expectedResponse          labelValuesCardinalityResponse
	}{
		"should return the label values cardinality for the specified label name": {
			url:                       "/label_values?label_names[]=__name__",
			labelNames:                []model.LabelName{"__name__"},
			matcher:                   []*labels.Matcher(nil),
			labelValuesCardinalityMap: map[string]map[string]uint64{"__name__": {"test1": 10}},
			expectedResponse: labelValuesCardinalityResponse{
				SeriesCountTotal: seriesCountTotal,
				Labels: []labelNamesCardinality{{
					LabelName:        "__name__",
					LabelValuesCount: 1,
					SeriesCount:      10,
					Cardinality: []labelValuesCardinality{
						{LabelValue: "test1", SeriesCount: 10},
					},
				}},
			},
		},
		"should return the label values cardinality for the specified label name with matching selector": {
			url:                       "/label_values?label_names[]=__name__&selector[]={__name__='test1'}",
			labelNames:                []model.LabelName{"__name__"},
			matcher:                   []*labels.Matcher{nameMatcher},
			labelValuesCardinalityMap: map[string]map[string]uint64{"__name__": {"test1": 10}},
			expectedResponse: labelValuesCardinalityResponse{
				SeriesCountTotal: seriesCountTotal,
				Labels: []labelNamesCardinality{{
					LabelName:        "__name__",
					LabelValuesCount: 1,
					SeriesCount:      10,
					Cardinality: []labelValuesCardinality{
						{LabelValue: "test1", SeriesCount: 10},
					},
				}},
			},
		},
		"should return the label values cardinality for the specified label names": {
			url:                       "/label_values?label_names[]=foo&label_names[]=bar",
			labelNames:                []model.LabelName{"foo", "bar"},
			matcher:                   []*labels.Matcher(nil),
			labelValuesCardinalityMap: map[string]map[string]uint64{"foo": {"test1": 10}, "bar": {"test1": 10}},
			expectedResponse: labelValuesCardinalityResponse{
				SeriesCountTotal: seriesCountTotal,
				Labels: []labelNamesCardinality{
					{
						LabelName:        "foo",
						LabelValuesCount: 1,
						SeriesCount:      10,
						Cardinality: []labelValuesCardinality{
							{LabelValue: "test1", SeriesCount: 10},
						},
					},
					{
						LabelName:        "bar",
						LabelValuesCount: 1,
						SeriesCount:      10,
						Cardinality: []labelValuesCardinality{
							{LabelValue: "test1", SeriesCount: 10},
						},
					},
				},
			},
		},
		"should return the label values cardinality sorted by series count in descending order": {
			url:                       "/label_values?label_names[]=__name__",
			labelNames:                []model.LabelName{"__name__"},
			matcher:                   []*labels.Matcher(nil),
			labelValuesCardinalityMap: map[string]map[string]uint64{"__name__": {"test1": 10, "test2": 20}},
			expectedResponse: labelValuesCardinalityResponse{
				SeriesCountTotal: seriesCountTotal,
				Labels: []labelNamesCardinality{{
					LabelName:        "__name__",
					LabelValuesCount: 2,
					SeriesCount:      30,
					Cardinality: []labelValuesCardinality{
						{LabelValue: "test2", SeriesCount: 20},
						{LabelValue: "test1", SeriesCount: 10},
					},
				}},
			},
		},
		"should return the label values cardinality sorted by label name in ascending order for label values with the same series count": {
			url:                       "/label_values?label_names[]=__name__",
			labelNames:                []model.LabelName{"__name__"},
			matcher:                   []*labels.Matcher(nil),
			labelValuesCardinalityMap: map[string]map[string]uint64{"__name__": {"test1": 10, "test2": 10}},
			expectedResponse: labelValuesCardinalityResponse{
				SeriesCountTotal: seriesCountTotal,
				Labels: []labelNamesCardinality{{
					LabelName:        "__name__",
					LabelValuesCount: 2,
					SeriesCount:      20,
					Cardinality: []labelValuesCardinality{
						{LabelValue: "test1", SeriesCount: 10},
						{LabelValue: "test2", SeriesCount: 10},
					},
				}},
			},
		},
		"should return the label values cardinality array with the defined limit": {
			url:                       "/label_values?label_names[]=__name__&limit3",
			labelNames:                []model.LabelName{"__name__"},
			matcher:                   []*labels.Matcher(nil),
			labelValuesCardinalityMap: map[string]map[string]uint64{"__name__": {"test1": 100, "test2": 20, "test3": 30}},
			expectedResponse: labelValuesCardinalityResponse{
				SeriesCountTotal: seriesCountTotal,
				Labels: []labelNamesCardinality{{
					LabelName:        "__name__",
					LabelValuesCount: 3,
					SeriesCount:      150,
					Cardinality: []labelValuesCardinality{
						{LabelValue: "test1", SeriesCount: 100},
						{LabelValue: "test3", SeriesCount: 30},
						{LabelValue: "test2", SeriesCount: 20},
					},
				}},
			},
		},
		"should return the label values cardinality array limited by the limit param": {
			url:                       "/label_values?label_names[]=__name__&limit=2",
			labelNames:                []model.LabelName{"__name__"},
			matcher:                   []*labels.Matcher(nil),
			labelValuesCardinalityMap: map[string]map[string]uint64{"__name__": {"test1": 100, "test2": 20, "test3": 30}},
			expectedResponse: labelValuesCardinalityResponse{
				SeriesCountTotal: seriesCountTotal,
				Labels: []labelNamesCardinality{{
					LabelName:        "__name__",
					LabelValuesCount: 3,
					SeriesCount:      150,
					Cardinality: []labelValuesCardinality{
						{LabelValue: "test1", SeriesCount: 100},
						{LabelValue: "test3", SeriesCount: 30},
					},
				}},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			distributor := mockDistributorLabelValuesCardinality(testData.labelNames, testData.matcher)(seriesCountTotal, testData.labelValuesCardinalityMap)
			handler := LabelValuesCardinalityHandler(distributor)
			ctx := user.InjectOrgID(context.Background(), "test")

			request, err := http.NewRequestWithContext(ctx, "GET", testData.url, http.NoBody)
			require.NoError(t, err)
			recorder := httptest.NewRecorder()

			handler.ServeHTTP(recorder, request)

			require.Equal(t, http.StatusOK, recorder.Result().StatusCode)

			body := recorder.Result().Body
			defer func() { _ = body.Close() }()

			responseBody := labelValuesCardinalityResponse{}
			bodyContent, err := ioutil.ReadAll(body)
			require.NoError(t, err)
			err = json.Unmarshal(bodyContent, &responseBody)
			require.NoError(t, err)

			require.Equal(t, testData.expectedResponse, responseBody)
		})
	}
}

func TestLabelValuesCardinalityHandler_ParseError(t *testing.T) {
	distributor := mockDistributorLabelValuesCardinality([]model.LabelName{}, []*labels.Matcher(nil))(uint64(0), map[string]map[string]uint64{})
	handler := LabelValuesCardinalityHandler(distributor)
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
				expectedErrorMessage: "label_names param is required",
			},
			"label_names param is invalid": {
				url:                  "/label_values?label_names[]=olá",
				expectedErrorMessage: "invalid label_names param 'olá'",
			},
			"multiple selector params are provided": {
				url:                  "/label_values?label_names[]=hello&selector[]=foo&selector[]=bar",
				expectedErrorMessage: "multiple `selector` params are not allowed",
			},
			"limit param is not a number": {
				url:                  "/label_values?label_names[]=hello&limit=foo",
				expectedErrorMessage: "strconv.Atoi: parsing \"foo\": invalid syntax",
			},
			"limit param is a negative number": {
				url:                  "/label_values?label_names[]=hello&limit=-20",
				expectedErrorMessage: "limit param can not be negative",
			},
			"limit param exceeds the maximum limit parameter": {
				url:                  "/label_values?label_names[]=hello&limit=501",
				expectedErrorMessage: "limit param can not greater than 500",
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

				bytes, err := ioutil.ReadAll(body)
				require.NoError(t, err)
				require.Contains(t, string(bytes), testData.expectedErrorMessage)
			})

		}
	})
}

func mockDistributorLabelValuesCardinality(labelNames []model.LabelName, matchers []*labels.Matcher) func(uint64, map[string]map[string]uint64) *mockDistributor {
	return func(seriesCount uint64, labelValuesCardinalityMap map[string]map[string]uint64) *mockDistributor {
		distributor := &mockDistributor{}
		distributor.On("LabelValuesCardinality", mock.Anything, labelNames, matchers).Return(seriesCount, labelValuesCardinalityMap, nil)
		return distributor
	}
}
