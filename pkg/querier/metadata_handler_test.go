// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/metadata_handler_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/prometheus/prometheus/scrape"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestMetadataHandler_Success(t *testing.T) {
	fullJSON := `
		{
			"status": "success",
			"data": {
				"alertmanager_dispatcher_aggregation_groups": [
					{
						"help": "Number of active aggregation groups",
						"type": "gauge",
						"unit": ""
					}
				],
				"go_gc_duration_seconds": [
					{
						"help": "A summary of the pause duration of garbage collection cycles",
						"type": "summary",
						"unit": ""
					},
					{
						"help": "A summary of the pause duration of garbage collection cycles 2",
						"type": "summary",
						"unit": ""
					}
				]
			}
		}
	`
	testCases := map[string]struct {
		queryParams  url.Values
		expectedJSON string
	}{
		"no params": {
			queryParams:  url.Values{},
			expectedJSON: fullJSON,
		},
		"limit=-1": {
			queryParams: url.Values{
				"limit": {"-1"},
			},
			expectedJSON: fullJSON,
		},
		"limit=0": {
			queryParams: url.Values{
				"limit": {"0"},
			},
			expectedJSON: `
				{
					"status": "success",
					"data": {}
				}
			`,
		},
		"limit=1": {
			queryParams: url.Values{
				"limit": {"1"},
			},
			expectedJSON: `
				{
					"status": "success",
					"data": {
						"alertmanager_dispatcher_aggregation_groups": [
							{
								"help": "Number of active aggregation groups",
								"type": "gauge",
								"unit": ""
							}
						]
					}
				}
			`,
		},
		"limit=2": {
			queryParams: url.Values{
				"limit": {"2"},
			},
			expectedJSON: `
				{
					"status": "success",
					"data": {
						"alertmanager_dispatcher_aggregation_groups": [
							{
								"help": "Number of active aggregation groups",
								"type": "gauge",
								"unit": ""
							}
						],
						"go_gc_duration_seconds": [
							{
								"help": "A summary of the pause duration of garbage collection cycles",
								"type": "summary",
								"unit": ""
							},
							{
								"help": "A summary of the pause duration of garbage collection cycles 2",
								"type": "summary",
								"unit": ""
							}
						]
					}
				}
			`,
		},
		"limit_per_metric=-1": {
			queryParams: url.Values{
				"limit_per_metric": {"-1"},
			},
			expectedJSON: fullJSON,
		},
		"limit_per_metric=0": {
			queryParams: url.Values{
				"limit_per_metric": {"0"},
			},
			expectedJSON: fullJSON,
		},
		"limit_per_metric=1": {
			queryParams: url.Values{
				"limit_per_metric": {"1"},
			},
			expectedJSON: `
				{
					"status": "success",
					"data": {
						"alertmanager_dispatcher_aggregation_groups": [
							{
								"help": "Number of active aggregation groups",
								"type": "gauge",
								"unit": ""
							}
						],
						"go_gc_duration_seconds": [
							{
								"help": "A summary of the pause duration of garbage collection cycles",
								"type": "summary",
								"unit": ""
							}
						]
					}
				}
			`,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			d := &mockDistributor{}
			d.On("MetricsMetadata", mock.Anything, mock.Anything).Return(
				[]scrape.MetricMetadata{
					{Metric: "alertmanager_dispatcher_aggregation_groups", Help: "Number of active aggregation groups", Type: "gauge", Unit: ""},
					{Metric: "go_gc_duration_seconds", Help: "A summary of the pause duration of garbage collection cycles", Type: "summary", Unit: ""},
					{Metric: "go_gc_duration_seconds", Help: "A summary of the pause duration of garbage collection cycles 2", Type: "summary", Unit: ""},
				},
				nil)

			handler := NewMetadataHandler(d)

			request, err := http.NewRequest("GET", "/metadata", nil)
			request.URL.RawQuery = tc.queryParams.Encode()
			require.NoError(t, err)

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)

			require.Equal(t, http.StatusOK, recorder.Result().StatusCode)
			responseBody, err := io.ReadAll(recorder.Result().Body)
			require.NoError(t, err)

			require.JSONEq(t, tc.expectedJSON, string(responseBody))
		})
	}
}

func TestMetadataHandler_Empty(t *testing.T) {
	d := &mockDistributor{}
	d.On("MetricsMetadata", mock.Anything, mock.Anything).Return(
		[]scrape.MetricMetadata{},
		nil)

	handler := NewMetadataHandler(d)

	request, err := http.NewRequest("GET", "/metadata", nil)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Result().StatusCode)
	responseBody, err := io.ReadAll(recorder.Result().Body)
	require.NoError(t, err)

	expectedJSON := `
	{
		"status": "success",
		"data": {}
	}
	`

	require.JSONEq(t, expectedJSON, string(responseBody))
}

func TestMetadataHandler_Error(t *testing.T) {
	d := &mockDistributor{}
	d.On("MetricsMetadata", mock.Anything, mock.Anything).Return([]scrape.MetricMetadata{}, fmt.Errorf("no user id"))

	handler := NewMetadataHandler(d)

	request, err := http.NewRequest("GET", "/metadata", nil)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Result().StatusCode)
	responseBody, err := io.ReadAll(recorder.Result().Body)
	require.NoError(t, err)

	expectedJSON := `
	{
		"status": "error",
		"error": "no user id"
	}
	`

	require.JSONEq(t, expectedJSON, string(responseBody))
}
