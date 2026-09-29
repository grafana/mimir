// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSampleMetricNamesKeepsMetricsWithinTheSeriesBounds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/prometheus/api/v1/cardinality/label_values", r.URL.Path)
		require.Equal(t, "tenant", r.Header.Get("X-Scope-OrgID"))
		require.Equal(t, "500", r.URL.Query().Get("limit"), "Mimir rejects larger limits")
		_, _ = w.Write([]byte(`{"labels":[{"label_name":"__name__","cardinality":[
			{"label_value":"huge","series_count":5000000},
			{"label_value":"large","series_count":150000},
			{"label_value":"medium","series_count":2000},
			{"label_value":"tiny","series_count":10}]}]}`))
	}))
	defer server.Close()
	cfg := config{address: server.URL + "/prometheus", metricsSample: 10, minSeries: 1000, maxSeries: 200_000}
	names, err := sampleMetricNames(context.Background(), server.Client(), cfg, "tenant")
	require.NoError(t, err)
	slices.Sort(names)
	require.Equal(t, []string{"large", "medium"}, names)
}

func TestAggregatedMetricsAreNoLongerPicked(t *testing.T) {
	names := newMetricNames()
	names.set("tenant", []string{"aggregated", "raw"})
	names.remove("tenant", "aggregated")
	for range 20 {
		name, ok := names.pick("tenant")
		require.True(t, ok)
		require.Equal(t, "raw", name)
	}
}
