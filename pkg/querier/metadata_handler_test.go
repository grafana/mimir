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
	d := &mockDistributor{}
	d.On("MetricsMetadata", mock.Anything).Return(
		[]scrape.MetricMetadata{
			{Metric: "alertmanager_dispatcher_aggregation_groups", Help: "Number of active aggregation groups", Type: "gauge", Unit: ""},
			{Metric: "go_gc_duration_seconds", Help: "A summary of the pause duration of garbage collection cycles", Type: "summary", Unit: ""},
		},
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
	`

	require.JSONEq(t, expectedJSON, string(responseBody))
}

func TestMetadataHandler_Success_Limit(t *testing.T) {
	d := &mockDistributor{}
	d.On("MetricsMetadata", mock.Anything).Return(
		[]scrape.MetricMetadata{
			{Metric: "alertmanager_dispatcher_aggregation_groups", Help: "Number of active aggregation groups", Type: "gauge", Unit: ""},
			{Metric: "go_gc_duration_seconds", Help: "A summary of the pause duration of garbage collection cycles", Type: "summary", Unit: ""},
		},
		nil)

	handler := NewMetadataHandler(d)

	request, err := http.NewRequest("GET", "/metadata", nil)
	request.URL.RawQuery = url.Values{
		"limit": {"1"},
	}.Encode()
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Result().StatusCode)
	responseBody, err := io.ReadAll(recorder.Result().Body)
	require.NoError(t, err)

	expectedJSON := `
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
	`

	require.JSONEq(t, expectedJSON, string(responseBody))
}

func TestMetadataHandler_Success_LimitPerMetric(t *testing.T) {
	d := &mockDistributor{}
	d.On("MetricsMetadata", mock.Anything).Return(
		[]scrape.MetricMetadata{
			{Metric: "alertmanager_dispatcher_aggregation_groups", Help: "Number of active aggregation groups", Type: "gauge", Unit: ""},
			{Metric: "go_gc_duration_seconds", Help: "A summary of the pause duration of garbage collection cycles", Type: "summary", Unit: ""},
			{Metric: "go_gc_duration_seconds", Help: "A summary of the pause duration of garbage collection cycles 2", Type: "summary", Unit: ""},
		},
		nil)

	handler := NewMetadataHandler(d)

	request, err := http.NewRequest("GET", "/metadata", nil)
	request.URL.RawQuery = url.Values{
		"limit_per_metric": {"1"},
	}.Encode()
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Result().StatusCode)
	responseBody, err := io.ReadAll(recorder.Result().Body)
	require.NoError(t, err)

	expectedJSON := `
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
	`

	require.JSONEq(t, expectedJSON, string(responseBody))
}

func TestMetadataHandler_Success_Metric(t *testing.T) {
	d := &mockDistributor{}
	d.On("MetricsMetadata", mock.Anything).Return(
		[]scrape.MetricMetadata{
			{Metric: "alertmanager_dispatcher_aggregation_groups", Help: "Number of active aggregation groups", Type: "gauge", Unit: ""},
			{Metric: "go_gc_duration_seconds", Help: "A summary of the pause duration of garbage collection cycles", Type: "summary", Unit: ""},
		},
		nil)

	handler := NewMetadataHandler(d)

	request, err := http.NewRequest("GET", "/metadata", nil)
	request.URL.RawQuery = url.Values{
		"metric": {"go_gc_duration_seconds"},
	}.Encode()
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Result().StatusCode)
	responseBody, err := io.ReadAll(recorder.Result().Body)
	require.NoError(t, err)

	expectedJSON := `
	{
		"status": "success",
		"data": {
			"go_gc_duration_seconds": [
				{
					"help": "A summary of the pause duration of garbage collection cycles",
					"type": "summary",
					"unit": ""
				}
			]
		}
	}
	`

	require.JSONEq(t, expectedJSON, string(responseBody))
}

func TestMetadataHandler_Empty(t *testing.T) {
	d := &mockDistributor{}
	d.On("MetricsMetadata", mock.Anything).Return(
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
	d.On("MetricsMetadata", mock.Anything).Return([]scrape.MetricMetadata{}, fmt.Errorf("no user id"))

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
