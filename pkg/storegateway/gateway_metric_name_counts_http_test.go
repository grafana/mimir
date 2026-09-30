// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/gorilla/mux"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestBucketStore_MetricNameCounts(t *testing.T) {
	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)

	cfg := defaultPrepareStoreConfig(t)
	// Non-overlapping blocks: the first block range holds only the first
	// block, with series[:4]; every later range holds two blocks.
	cfg.nonOverlappingBlocks = true
	cfg.numBlocks = 4
	cfg.series = []labels.Labels{
		labels.FromStrings("__name__", "up", "job", "a"),
		labels.FromStrings("__name__", "up", "job", "b"),
		labels.FromStrings("__name__", "up", "job", "c"),
		labels.FromStrings("__name__", "requests_total", "job", "a"),
		labels.FromStrings("__name__", "up", "job", "d"),
		labels.FromStrings("__name__", "errors_total", "job", "a"),
		labels.FromStrings("__name__", "errors_total", "job", "b"),
		labels.FromStrings("__name__", "errors_total", "job", "c"),
	}
	s := prepareStoreWithTestBlocks(t, bkt, cfg)
	const twoHours = int64(2 * 60 * 60 * 1000)

	res, err := s.store.metricNameCounts(t.Context(), s.minTime, s.minTime+twoHours)
	require.NoError(t, err)
	assert.Len(t, res.blocks, 1)
	assert.Equal(t, map[string]int64{"up": 3, "requests_total": 1}, res.counts)

	// The next range holds two blocks without shard IDs, which may share series.
	_, err = s.store.metricNameCounts(t.Context(), s.minTime+twoHours, s.minTime+2*twoHours)
	require.ErrorContains(t, err, "no shard ID")

	// A window that cuts through a block.
	_, err = s.store.metricNameCounts(t.Context(), s.minTime, s.minTime+twoHours/2)
	require.ErrorContains(t, err, "cuts through")

	// A window of two block ranges.
	_, err = s.store.metricNameCounts(t.Context(), s.minTime, s.minTime+2*twoHours)
	require.ErrorContains(t, err, "more than one block range")

	// A window with no blocks at all.
	res, err = s.store.metricNameCounts(t.Context(), s.maxTime+twoHours, s.maxTime+2*twoHours)
	require.NoError(t, err)
	assert.Empty(t, res.counts)
}

func TestCheckSeriesDisjointShards(t *testing.T) {
	meta := func(shard string) *block.Meta {
		m := &block.Meta{}
		m.Thanos.Labels = map[string]string{}
		if shard != "" {
			m.Thanos.Labels[block.CompactorShardIDExternalLabel] = shard
		}
		return m
	}
	assert.NoError(t, checkSeriesDisjointShards(nil))
	assert.NoError(t, checkSeriesDisjointShards([]*block.Meta{meta("")}))
	assert.NoError(t, checkSeriesDisjointShards([]*block.Meta{meta("1_of_2"), meta("2_of_2")}))
	assert.ErrorContains(t, checkSeriesDisjointShards([]*block.Meta{meta("1_of_2"), meta("1_of_2")}), "appears twice")
	assert.ErrorContains(t, checkSeriesDisjointShards([]*block.Meta{meta("1_of_2"), meta("2_of_4")}), "shard counts")
	assert.ErrorContains(t, checkSeriesDisjointShards([]*block.Meta{meta("1_of_2"), meta("")}), "no shard ID")
}

func TestStoreGateway_MetricNameCountsHandler_BadRequests(t *testing.T) {
	g := &StoreGateway{stores: &BucketStores{stores: map[string]*BucketStore{}}}
	for name, tc := range map[string]struct {
		tenant, query string
		code          int
	}{
		"no tenant":        {"", "start=0&end=1", http.StatusBadRequest},
		"bad start":        {"t", "start=x&end=1", http.StatusBadRequest},
		"end before start": {"t", "start=10&end=5", http.StatusBadRequest},
		"bad limit":        {"t", "start=0&end=1&limit=-1", http.StatusBadRequest},
		"unknown tenant":   {"t", "start=0&end=1", http.StatusNotFound},
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/store-gateway/tenant/"+tc.tenant+"/metric_name_counts?"+tc.query, nil)
			req = mux.SetURLVars(req, map[string]string{"tenant": tc.tenant})
			rec := httptest.NewRecorder()
			g.MetricNameCountsHandler(rec, req)
			assert.Equal(t, tc.code, rec.Code, rec.Body.String())
		})
	}
}

func TestStoreGateway_MetricNameCountsHandler(t *testing.T) {
	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	cfg := defaultPrepareStoreConfig(t)
	cfg.nonOverlappingBlocks = true
	cfg.numBlocks = 2
	cfg.series = []labels.Labels{
		labels.FromStrings("__name__", "up", "job", "a"),
		labels.FromStrings("__name__", "up", "job", "b"),
		labels.FromStrings("__name__", "requests_total", "job", "a"),
		labels.FromStrings("__name__", "requests_total", "job", "b"),
		labels.FromStrings("__name__", "errors_total", "job", "a"),
		labels.FromStrings("__name__", "errors_total", "job", "b"),
		labels.FromStrings("__name__", "errors_total", "job", "c"),
		labels.FromStrings("__name__", "errors_total", "job", "d"),
	}
	s := prepareStoreWithTestBlocks(t, bkt, cfg)
	g := &StoreGateway{stores: &BucketStores{stores: map[string]*BucketStore{"tenant": s.store}}}

	// Seconds, as the Prometheus HTTP API takes them.
	start := strconv.FormatFloat(float64(s.minTime)/1000, 'f', 3, 64)
	end := strconv.FormatFloat(float64(s.minTime+2*60*60*1000)/1000, 'f', 3, 64)
	req := httptest.NewRequest(http.MethodGet, "/store-gateway/tenant/tenant/metric_name_counts?limit=1&start="+start+"&end="+end, nil)
	req = mux.SetURLVars(req.WithContext(t.Context()), map[string]string{"tenant": "tenant"})
	rec := httptest.NewRecorder()
	g.MetricNameCountsHandler(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out metricNameCountsResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, 2, out.Names)
	assert.Equal(t, int64(4), out.Series, "series[:4]: two up and two requests_total")
	assert.Len(t, out.Blocks, 1)
	assert.Equal(t, []metricNameCount{{Name: "requests_total", Count: 2}}, out.Counts, "ties sort by name, and limit keeps the first")
}
