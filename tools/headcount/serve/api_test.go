// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/tools/headcount/cardgen"
	"github.com/grafana/mimir/tools/headcount/model"
)

func testServer(t *testing.T) *server {
	start := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	pop, err := model.New(model.Config{
		Seed:        1,
		Start:       start,
		End:         start.Add(3 * time.Hour),
		MetricNames: 15,
		SeriesZipfS: 1.2, SeriesFloor: 1, SeriesCap: 20,
		SpikeMetric: 0, SpikeDay: 0, SpikeBaseValues: 3, SpikePeakValues: 60,
	})
	require.NoError(t, err)
	bucket := t.TempDir()
	require.NoError(t, os.MkdirAll(bucket+"/anonymous", 0o755))
	_, err = cardgen.Generate(pop, bucket+"/anonymous", cardgen.Config{Partitions: 1, Seed: 1})
	require.NoError(t, err)

	s, err := newServer(bucket, pop, &mimir{})
	require.NoError(t, err)
	return s
}

func getJSON(t *testing.T, s *server, path string, out any) int {
	mux := http.NewServeMux()
	s.register(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	if rec.Code == http.StatusOK {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), out))
	}
	return rec.Code
}

func TestHandleGrowth_ExactAgainstTruth(t *testing.T) {
	s := testServer(t)
	require.Len(t, s.days, 3, "one block range per hour")

	var out struct {
		Rows       []growthRow `json:"rows"`
		Names      int         `json:"names"`
		NamesExact int         `json:"names_exact"`
		Cost       headcountCost
	}
	require.Equal(t, http.StatusOK, getJSON(t, s, "/api/growth?day=1&base=0&limit=5", &out))
	require.Len(t, out.Rows, 5)
	require.Equal(t, out.Names, out.NamesExact, "every name must equal the model's truth on both days")
	for _, r := range out.Rows {
		require.Equal(t, r.TruthDay, r.Day, r.Name)
		require.Equal(t, r.Day-r.Base, r.Growth, r.Name)
	}
	require.Zero(t, out.Cost.ObjectStorageBytes)

	require.Equal(t, http.StatusBadRequest, getJSON(t, s, "/api/growth?day=9", &out))
}

func TestHandleBreakdown_BudgetGivesLowerBound(t *testing.T) {
	s := testServer(t)
	var out struct {
		Values     int  `json:"values"`
		Total      int  `json:"total"`
		LowerBound bool `json:"lower_bound"`
		TruthTotal int  `json:"truth_total"`
	}
	path := "/api/breakdown?day=0&label=pod&metric=" + s.spikeName
	require.Equal(t, http.StatusOK, getJSON(t, s, path, &out))
	require.False(t, out.LowerBound)
	require.Equal(t, out.TruthTotal, out.Total)
	require.Equal(t, 60, out.Values)

	require.Equal(t, http.StatusOK, getJSON(t, s, path+"&budget=10", &out))
	require.True(t, out.LowerBound)
	require.Equal(t, 10, out.Total)
}

func TestGrowthRows_OrderedByGrowth(t *testing.T) {
	rows := growthRows(map[string]int{"a": 5, "b": 1}, map[string]int{"a": 6, "b": 9, "c": 2})
	require.Equal(t, []string{"b", "c", "a"}, []string{rows[0].Name, rows[1].Name, rows[2].Name})
	require.Equal(t, 8, rows[0].Growth)
}

func TestLogfmtFields(t *testing.T) {
	f := logfmtFields(`ts=x level=info msg="query stats" fetched_series_count=12 param_query="count(up)" sharded_queries=16`)
	require.Equal(t, "12", f["fetched_series_count"])
	require.Equal(t, "16", f["sharded_queries"])
	require.NotContains(t, f, "msg", "quoted values are skipped")
}
