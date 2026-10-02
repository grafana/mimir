// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
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

// fakeMimir answers Mimir's cardinality/estimate route from the model's
// truth, picking the read the way the querier does: index-headers for every
// name over exactly one block range, the full index otherwise, with dedup
// when the window spans more than one block range.
func fakeMimir(t *testing.T, s *server) *httptest.Server {
	parseT := func(v string) int64 {
		f, err := strconv.ParseFloat(v, 64)
		require.NoError(t, err)
		return int64(f*1000 + 0.5)
	}
	isBlockRange := func(minT, maxT int64) bool {
		for _, r := range s.ranges {
			if r.MinT == minT && r.MaxT == maxT {
				return true
			}
		}
		return false
	}
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/prometheus/api/v1/cardinality/estimate" {
			http.NotFound(w, r)
			return
		}
		q := r.URL.Query()
		minT, maxT := parseT(q.Get("start")), parseT(q.Get("end"))
		var matchers []*labels.Matcher
		if sel := q.Get("match[]"); sel != "" {
			matchers = matchName(strings.TrimSuffix(strings.TrimPrefix(sel, `{__name__="`), `"}`))
		}
		groupBy := q.Get("group_by")
		if groupBy == "" {
			groupBy = "__name__"
		}
		type group struct {
			Value  string `json:"value"`
			Count  int    `json:"count"`
			Counts []int  `json:"counts,omitempty"`
		}
		var out struct {
			Read   string  `json:"read"`
			Dedup  bool    `json:"dedup"`
			Counts []group `json:"counts"`
		}
		out.Read = "full_index"
		if matchers == nil && groupBy == "__name__" && q.Get("step") == "" && isBlockRange(minT, maxT) {
			out.Read = "index_header"
		}
		out.Dedup = out.Read == "full_index" && len(s.pieces(minT, maxT)) > 1
		var stepMs int64
		if v := q.Get("step"); v != "" {
			d, err := time.ParseDuration(v)
			require.NoError(t, err)
			stepMs = d.Milliseconds()
		}
		for v, n := range s.pop.TruthBy(matchers, groupBy, minT, maxT) {
			g := group{Value: v, Count: n}
			for b := minT; stepMs > 0 && b < maxT; b += stepMs {
				g.Counts = append(g.Counts, s.pop.TruthBy(matchers, groupBy, b, b+stepMs)[v])
			}
			out.Counts = append(out.Counts, g)
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(fake.Close)
	return fake
}

func TestWindowParsing(t *testing.T) {
	s := testServer(t)
	start := s.dataStart() / 1000
	var out struct{}
	for _, q := range []string{
		"",
		fmt.Sprintf("start=%d&end=%d", start, start),
		fmt.Sprintf("start=%d&end=%d", start+60, start+3600),
		fmt.Sprintf("start=%d&end=%d", start-3600, start+3600),
		fmt.Sprintf("start=%d&end=%d", start, start+4*3600),
	} {
		assert.Equal(t, http.StatusBadRequest, getJSON(t, s, "/api/names?"+q, &out), q)
	}
}

func TestHandlers_AgainstTruth(t *testing.T) {
	s := testServer(t)
	require.Len(t, s.ranges, 3, "one block range per hour")
	s.mimir.baseURL = fakeMimir(t, s).URL
	at := func(hour int64) int64 { return s.dataStart()/1000 + hour*3600 }
	metric := url.QueryEscape(s.spikeName)

	t.Run("names over one block range read index-headers", func(t *testing.T) {
		var out struct {
			Rows       []growthRow `json:"rows"`
			Names      int         `json:"names"`
			NamesExact int         `json:"names_exact"`
			Previous   windowJSON  `json:"previous"`
			Read       readInfo    `json:"read"`
		}
		require.Equal(t, http.StatusOK, getJSON(t, s, fmt.Sprintf("/api/names?start=%d&end=%d", at(1), at(2)), &out))
		assert.Equal(t, "index_header", out.Read.Route)
		assert.Equal(t, windowJSON{at(0), at(1)}, out.Previous, "the hour before")
		assert.Equal(t, out.Names, out.NamesExact)
		require.NotEmpty(t, out.Rows)
		for _, r := range out.Rows {
			assert.Equal(t, r.TruthDay, r.Day)
		}
	})

	t.Run("names across block ranges use dedup", func(t *testing.T) {
		var out struct {
			Names      int        `json:"names"`
			NamesExact int        `json:"names_exact"`
			Previous   windowJSON `json:"previous"`
			Read       readInfo   `json:"read"`
		}
		require.Equal(t, http.StatusOK, getJSON(t, s, fmt.Sprintf("/api/names?start=%d&end=%d", at(1), at(3)), &out))
		assert.Equal(t, "full_index", out.Read.Route)
		assert.Contains(t, out.Read.Method, "dedup")
		assert.Equal(t, windowJSON{at(0), at(1)}, out.Previous, "clipped to the data")
		assert.Equal(t, out.Names, out.NamesExact)
	})

	t.Run("breakdown", func(t *testing.T) {
		var out struct {
			Values      int        `json:"values"`
			SetValues   int        `json:"set_values"`
			TruthValues int        `json:"truth_values"`
			Total       int        `json:"total"`
			TruthTotal  int        `json:"truth_total"`
			Rows        []valueRow `json:"rows"`
		}
		require.Equal(t, http.StatusOK, getJSON(t, s, fmt.Sprintf("/api/breakdown?start=%d&end=%d&label=pod&limit=100&metric=%s", at(0), at(1), metric), &out))
		assert.Equal(t, out.TruthValues, out.Values)
		assert.Equal(t, out.Values, out.SetValues, "every series of the metric has a pod label")
		assert.Equal(t, out.TruthTotal, out.Total)
		for _, r := range out.Rows {
			assert.Equal(t, r.Truth, r.Count)
		}
	})

	t.Run("window: adding the block ranges overcounts, dedup doesn't", func(t *testing.T) {
		var out struct {
			Pieces      int `json:"pieces"`
			SummedTotal int `json:"summed_total"`
			ExactTotal  int `json:"exact_total"`
			TruthTotal  int `json:"truth_total"`
		}
		require.Equal(t, http.StatusOK, getJSON(t, s, fmt.Sprintf("/api/window?start=%d&end=%d", at(0), at(3)), &out))
		assert.Equal(t, 3, out.Pieces)
		assert.Equal(t, out.TruthTotal, out.ExactTotal)
		assert.Greater(t, out.SummedTotal, out.ExactTotal)
	})

	t.Run("buckets", func(t *testing.T) {
		type buckets struct {
			Counts []int    `json:"counts"`
			Exact  bool     `json:"exact"`
			StepS  int64    `json:"step_s"`
			Read   readInfo `json:"read"`
		}
		var one, many buckets
		// Inside one block range: one call with a step.
		require.Equal(t, http.StatusOK, getJSON(t, s, fmt.Sprintf("/api/buckets?start=%d&end=%d&metric=%s", at(0), at(1), metric), &one))
		assert.Equal(t, int64(3600), one.StepS)
		assert.Equal(t, 1, one.Read.Calls)
		assert.True(t, one.Exact)
		// Across block ranges, with buckets inside them: one call per block range.
		require.Equal(t, http.StatusOK, getJSON(t, s, fmt.Sprintf("/api/buckets?start=%d&end=%d&metric=%s", at(0), at(3), metric), &many))
		assert.Len(t, many.Counts, 3)
		assert.Equal(t, 3, many.Read.Calls)
		assert.Contains(t, many.Read.Method, "one call per block range")
		assert.True(t, many.Exact)
	})

	t.Run("an explicit step", func(t *testing.T) {
		var out struct {
			Counts []int `json:"counts"`
			StepS  int64 `json:"step_s"`
			Exact  bool  `json:"exact"`
		}
		require.Equal(t, http.StatusOK, getJSON(t, s, fmt.Sprintf("/api/buckets?start=%d&end=%d&step=3600&metric=%s", at(0), at(2), metric), &out))
		assert.Equal(t, int64(3600), out.StepS)
		assert.Len(t, out.Counts, 2)
		assert.True(t, out.Exact)
		for _, bad := range []string{"0", "60", "7200x"} {
			assert.Equal(t, http.StatusBadRequest, getJSON(t, s, fmt.Sprintf("/api/buckets?start=%d&end=%d&step=%s&metric=%s", at(0), at(3), bad, metric), &out), bad)
		}
		assert.Equal(t, http.StatusBadRequest, getJSON(t, s, fmt.Sprintf("/api/buckets?start=%d&end=%d&step=7200&metric=%s", at(0), at(3), metric), &out), "2h doesn't divide 3h")
	})

	t.Run("a Mimir without the routes", func(t *testing.T) {
		s.mimir.baseURL += "/missing"
		var out struct{}
		assert.Equal(t, http.StatusBadGateway, getJSON(t, s, fmt.Sprintf("/api/names?start=%d&end=%d", at(1), at(2)), &out))
		assert.Equal(t, http.StatusBadGateway, getJSON(t, s, fmt.Sprintf("/api/buckets?start=%d&end=%d&metric=%s", at(0), at(1), metric), &out))
	})
}

func TestSetValues(t *testing.T) {
	assert.Equal(t, 0, setValues(map[string]int{}))
	assert.Equal(t, 0, setValues(map[string]int{"": 30}), "only series without the label")
	assert.Equal(t, 2, setValues(map[string]int{"a": 1, "b": 2}))
	assert.Equal(t, 2, setValues(map[string]int{"": 5, "a": 1, "b": 2}))
}
