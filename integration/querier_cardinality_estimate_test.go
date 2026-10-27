// SPDX-License-Identifier: AGPL-3.0-only

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/e2e"
	e2ecache "github.com/grafana/e2e/cache"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/bucket/s3"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/test"
)

// cardinalityEstimate is the part of the /api/v1/cardinality/estimate
// response the tests check.
type cardinalityEstimate struct {
	Read        string  `json:"read"`
	Source      string  `json:"source"`
	Dedup       bool    `json:"dedup"`
	LowerBound  bool    `json:"lower_bound"`
	CoverageGap bool    `json:"coverage_gap"`
	Total       int64   `json:"total"`
	Totals      []int64 `json:"totals"`
	WindowMS    int64   `json:"window_ms"`
	Counts      []struct {
		Value  string  `json:"value"`
		Count  int64   `json:"count"`
		Counts []int64 `json:"counts"`
	} `json:"groups"`
}

// ownedSeriesUpdateInterval is how often the ingesters' owned-series
// service runs in the test. It records the tenant's shard size, which the
// head-only counts need to have been unchanged since before the window.
const ownedSeriesUpdateInterval = time.Second

// TestCardinalityEstimate checks the cardinality estimate route against a
// PromQL count over the same window, on an ingest storage cluster with two
// partitions and a query-frontend with a results cache in front of the
// querier: from blocks only (the name table from the index-headers, and a
// breakdown by another label with per-bucket counts) on both index-header
// versions, from the ingesters' heads only, and from both across the
// store-gateways/ingesters cut.
func TestCardinalityEstimate(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, blocksBucketName)
	kafka := e2edb.NewKafka()
	memcached := e2ecache.NewMemcached()
	require.NoError(t, s.StartAndWaitReady(consul, minio, kafka, memcached))

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		IngestStorageFlags(e2edb.KafkaAuthNone),
		map[string]string{
			"-querier.cardinality-estimate-enabled": "true",
			// The cut between the store-gateways and the ingesters is an
			// hour ago, and the ingesters are asked for the last two hours.
			"-querier.query-store-after":            "1h",
			"-querier.query-ingesters-within":       "2h",
			"-blocks-storage.tsdb.retention-period": "3h",
			// The uploaded blocks are recent; load them anyway, and leave
			// them as they are.
			"-blocks-storage.bucket-store.ignore-blocks-within": "0",
			"-blocks-storage.bucket-store.sync-interval":        "1s",
			"-compactor.first-level-compaction-wait-period":     "24h",
			"-compactor.cleanup-interval":                       "1s",
			// Partitions count as stable a minute after they turn ACTIVE.
			"-validation.create-grace-period": "1s",
			// Counts are added up across partitions only when the
			// ingesters have seen the tenant's shard size unchanged since
			// before the window, which the owned-series service tracks.
			"-ingester.track-ingester-owned-series":  "true",
			"-ingester.owned-series-update-interval": ownedSeriesUpdateInterval.String(),
			// The query-frontend keeps the estimates in a results cache.
			"-query-frontend.cache-results":                     "true",
			"-query-frontend.results-cache.backend":             "memcached",
			"-query-frontend.results-cache.memcached.addresses": "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
		},
	)

	now := time.Now()
	// Two 24h block ranges three and two days ago that share a series, and a
	// block inside the last three hours, older than the cut.
	day := now.Add(-72 * time.Hour).Truncate(24 * time.Hour)
	recentMin, recentMax := now.Add(-150*time.Minute), now.Add(-100*time.Minute)
	uploadBlocks(t, minio, []block.SeriesSpecs{
		{
			series("a_total", "job", "x", day, day.Add(13*time.Hour)),
			series("a_total", "job", "y", day.Add(time.Hour), day.Add(5*time.Hour)),
			series("a_total", "job", "z", day.Add(20*time.Hour), day.Add(24*time.Hour-time.Millisecond)),
			series("b_total", "job", "x", day.Add(2*time.Hour), day.Add(3*time.Hour)),
		},
		{
			// x is in both days' blocks.
			series("a_total", "job", "x", day.Add(24*time.Hour), day.Add(30*time.Hour)),
			// w runs to the end of the day, so the block covers the whole range.
			series("a_total", "job", "w", day.Add(25*time.Hour), day.Add(48*time.Hour-time.Millisecond)),
		},
		{
			series("c_total", "job", "old", recentMin, recentMax),
			series("c_total", "job", "both", recentMin, recentMax),
		},
	})

	// The query-frontend and the querier meet at the query-scheduler.
	queryScheduler := e2emimir.NewQueryScheduler("query-scheduler", flags)
	require.NoError(t, s.StartAndWaitReady(queryScheduler))
	flags["-query-frontend.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
	flags["-querier.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
	queryFrontend := e2emimir.NewQueryFrontend("query-frontend", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.Start(queryFrontend))

	// Two ingesters, one partition each.
	ingester0 := e2emimir.NewIngester("ingester-0", consul.NetworkHTTPEndpoint(), flags)
	ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), flags)
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	// The first store-gateway runs index-header version 1; one on version
	// 2 replaces it after the blocks-only cases.
	storeGateway := newEstimateStoreGateway(consul, flags, 1)
	compactor := e2emimir.NewCompactor("compactor", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(ingester0, ingester1, distributor, storeGateway, compactor))
	partitionsActive := time.Now()
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(querier))
	require.NoError(t, s.WaitReady(queryFrontend))
	for _, svc := range []*e2emimir.MimirService{distributor, querier} {
		require.NoError(t, svc.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_partition_ring_partitions"},
			e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "state", "Active"))), svc.Name())
	}
	// The compactor writes the bucket index, then the store-gateway loads the blocks.
	require.NoError(t, storeGateway.WaitSumMetrics(e2e.Equals(3), "cortex_bucket_store_blocks_loaded"))

	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), queryFrontend.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)

	// estimateRaw sends the request through the query-frontend and returns
	// the status and body. bypassCache asks the frontend not to use its
	// results cache for it.
	estimateRaw := func(t *testing.T, start, end time.Time, params url.Values, bypassCache bool) (int, []byte) {
		t.Helper()
		params.Set("start", formatSeconds(start))
		params.Set("end", formatSeconds(end))
		req, err := http.NewRequest(http.MethodGet, "http://"+queryFrontend.HTTPEndpoint()+"/prometheus/api/v1/cardinality/estimate?"+params.Encode(), nil)
		require.NoError(t, err)
		req.Header.Set("X-Scope-OrgID", userID)
		if bypassCache {
			req.Header.Set("Cache-Control", "no-store")
		}
		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		return res.StatusCode, body
	}
	estimateWith := func(t *testing.T, start, end time.Time, params url.Values, bypassCache bool) cardinalityEstimate {
		t.Helper()
		status, body := estimateRaw(t, start, end, params, bypassCache)
		require.Equal(t, http.StatusOK, status, string(body))
		var out cardinalityEstimate
		require.NoError(t, json.Unmarshal(body, &out), string(body))
		return out
	}
	estimate := func(t *testing.T, start, end time.Time, params url.Values) cardinalityEstimate {
		t.Helper()
		return estimateWith(t, start, end, params, false)
	}
	// cacheCounts is how many estimates the query-frontend looked up in its
	// results cache, and how many it answered from it.
	cacheCounts := func(t *testing.T) (requests, hits float64) {
		t.Helper()
		sums, err := queryFrontend.SumMetrics(
			[]string{"cortex_frontend_query_result_cache_requests_total", "cortex_frontend_query_result_cache_hits_total"},
			e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "request_type", "cardinality_estimate")))
		require.NoError(t, err)
		return sums[0], sums[1]
	}
	// promqlCount is count by (groupBy) of the series matching selector with
	// a sample in [start, end): last_over_time at end minus 1ms over a range
	// of the window's length, since range selectors are left-open.
	promqlCount := func(t *testing.T, selector, groupBy string, start, end time.Time) map[string]int64 {
		t.Helper()
		q := fmt.Sprintf("count by (%s) (last_over_time(%s[%dms]))", groupBy, selector, end.Sub(start).Milliseconds())
		v, _, _, err := client.Query(q, end.Add(-time.Millisecond))
		require.NoError(t, err)
		out := map[string]int64{}
		for _, sample := range v.(model.Vector) {
			out[string(sample.Metric[model.LabelName(groupBy)])] = int64(sample.Value)
		}
		return out
	}
	countsOf := func(e cardinalityEstimate) map[string]int64 {
		out := map[string]int64{}
		for _, g := range e.Counts {
			out[g.Value] = g.Count
		}
		return out
	}

	// blocksOnlyCases are the cases the store-gateway answers alone. They
	// run once per index-header version; the second run bypasses the
	// query-frontend's cache, which holds the first run's answers.
	blocksOnlyCases := func(t *testing.T, bypassCache bool) {
		estimate := func(t *testing.T, start, end time.Time, params url.Values) cardinalityEstimate {
			t.Helper()
			return estimateWith(t, start, end, params, bypassCache)
		}

		t.Run("every metric name of one block range from the index-headers", func(t *testing.T) {
			start, end := day, day.Add(24*time.Hour)
			got := estimate(t, start, end, url.Values{})
			assert.Equal(t, "index_header", got.Read)
			assert.Equal(t, "blocks", got.Source)
			assert.False(t, got.LowerBound)
			assert.Equal(t, map[string]int64{"a_total": 3, "b_total": 1}, countsOf(got))
			assert.Equal(t, promqlCount(t, `{__name__=~".+"}`, "__name__", start, end), countsOf(got))
			assert.Equal(t, int64(4), got.Total)
		})

		t.Run("every job of one block range from the index-headers", func(t *testing.T) {
			start, end := day, day.Add(24*time.Hour)
			got := estimate(t, start, end, url.Values{"group_by": {"job"}})
			assert.Equal(t, "index_header", got.Read)
			assert.Equal(t, "blocks", got.Source)
			assert.False(t, got.LowerBound)
			assert.Equal(t, map[string]int64{"x": 2, "y": 1, "z": 1}, countsOf(got))
			assert.Equal(t, promqlCount(t, `{__name__=~".+"}`, "job", start, end), countsOf(got))
		})

		t.Run("two block ranges that share a series count it once", func(t *testing.T) {
			start, end := day, day.Add(48*time.Hour)
			got := estimate(t, start, end, url.Values{"match[]": {`{__name__="a_total"}`}, "group_by": {"job"}})
			assert.Equal(t, "blocks", got.Source)
			assert.True(t, got.Dedup, "the block ranges may share series")
			assert.False(t, got.LowerBound)
			assert.Equal(t, map[string]int64{"x": 1, "y": 1, "z": 1, "w": 1}, countsOf(got))
			assert.Equal(t, promqlCount(t, `{__name__="a_total"}`, "job", start, end), countsOf(got))
			assert.Equal(t, int64(4), got.Total)
		})

		t.Run("by job with per-bucket counts", func(t *testing.T) {
			start, end := day, day.Add(24*time.Hour)
			got := estimate(t, start, end, url.Values{"match[]": {`{__name__="a_total"}`}, "group_by": {"job"}, "step": {"12h"}})
			assert.Equal(t, "full_index", got.Read)
			buckets := map[string][]int64{}
			for _, g := range got.Counts {
				buckets[g.Value] = g.Counts
			}
			for i, bucketStart := range []time.Time{start, start.Add(12 * time.Hour)} {
				want := promqlCount(t, `{__name__="a_total"}`, "job", bucketStart, bucketStart.Add(12*time.Hour))
				for job := range buckets {
					assert.Equal(t, want[job], buckets[job][i], "job %s, bucket %d", job, i)
				}
				for job, n := range want {
					require.Contains(t, buckets, job)
					assert.Equal(t, n, buckets[job][i], "job %s, bucket %d", job, i)
				}
			}
			assert.Equal(t, []int64{1, 1}, buckets["x"], "x has samples in both halves")
			// x and y in the first half, x and z in the second.
			assert.Equal(t, []int64{2, 2}, got.Totals)
		})

		t.Run("by job, buckets every 6h each looking back 12h", func(t *testing.T) {
			// Buckets end at 12h, 18h and 24h into the day, each counting the 12h before it.
			start, end := day.Add(6*time.Hour), day.Add(24*time.Hour)
			got := estimate(t, start, end, url.Values{"match[]": {`{__name__="a_total"}`}, "group_by": {"job"}, "step": {"6h"}, "window": {"12h"}})
			assert.Equal(t, int64(12*time.Hour/time.Millisecond), got.WindowMS)
			buckets := map[string][]int64{}
			for _, g := range got.Counts {
				buckets[g.Value] = g.Counts
			}
			for i, bucketEnd := range []time.Time{day.Add(12 * time.Hour), day.Add(18 * time.Hour), day.Add(24 * time.Hour)} {
				want := promqlCount(t, `{__name__="a_total"}`, "job", bucketEnd.Add(-12*time.Hour), bucketEnd)
				for job := range buckets {
					assert.Equal(t, want[job], buckets[job][i], "job %s, bucket %d", job, i)
				}
				for job, n := range want {
					require.Contains(t, buckets, job)
					assert.Equal(t, n, buckets[job][i], "job %s, bucket %d", job, i)
				}
			}
			// x runs to 13h, so the overlapping buckets all hold it; y, from 1h to 5h, is only in the first, and
			// z, from 20h, only in the last.
			assert.Equal(t, []int64{1, 1, 1}, buckets["x"])
			assert.Equal(t, []int64{1, 0, 0}, buckets["y"])
			assert.Equal(t, []int64{0, 0, 1}, buckets["z"])
		})

		t.Run("a request over max_index_bytes is refused with 422", func(t *testing.T) {
			start, end := day, day.Add(24*time.Hour)
			status, body := estimateRaw(t, start, end, url.Values{"match[]": {`{__name__="a_total"}`}, "group_by": {"job"}, "step": {"12h"}, "max_index_bytes": {"1"}}, bypassCache)
			assert.Equal(t, http.StatusUnprocessableEntity, status, string(body))
			assert.Contains(t, string(body), "max_index_bytes")
		})
	}

	t.Run("blocks only, index-header version 1", func(t *testing.T) {
		blocksOnlyCases(t, false)
	})

	t.Run("query-frontend cache: a repeated request is answered from the cache", func(t *testing.T) {
		start, end := day, day.Add(24*time.Hour)
		params := url.Values{"match[]": {`{__name__="a_total"}`}, "group_by": {"job"}}
		requestsBefore, hitsBefore := cacheCounts(t)
		first := estimate(t, start, end, params)
		assert.Equal(t, map[string]int64{"x": 1, "y": 1, "z": 1}, countsOf(first))
		second := estimate(t, start, end, params)
		assert.Equal(t, first, second)
		requests, hits := cacheCounts(t)
		assert.Equal(t, requestsBefore+2, requests, "both requests looked the cache up")
		assert.Equal(t, hitsBefore+1, hits, "the second was answered from it")
	})

	t.Run("query-frontend cache: a lower bound is not cached", func(t *testing.T) {
		// No block holds this window and the ingesters no longer answer for
		// it, so the empty table is a lower bound.
		start, end := day.Add(-48*time.Hour), day.Add(-24*time.Hour)
		requestsBefore, hitsBefore := cacheCounts(t)
		for i := 0; i < 2; i++ {
			got := estimate(t, start, end, url.Values{"match[]": {`{__name__="a_total"}`}})
			assert.Equal(t, "blocks", got.Source)
			assert.True(t, got.LowerBound)
			assert.True(t, got.CoverageGap)
			assert.Empty(t, got.Counts)
		}
		requests, hits := cacheCounts(t)
		assert.Equal(t, requestsBefore+2, requests, "both requests looked the cache up")
		assert.Equal(t, hitsBefore, hits, "neither was answered from it")
	})

	// Replace the store-gateway with one on index-header version 2, and
	// wait for the querier to see only it in the ring and for it to load
	// the blocks.
	require.NoError(t, s.Stop(storeGateway))
	storeGateway = newEstimateStoreGateway(consul, flags, 2)
	require.NoError(t, s.StartAndWaitReady(storeGateway))
	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "store-gateway-client"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))
	require.NoError(t, storeGateway.WaitSumMetrics(e2e.Equals(3), "cortex_bucket_store_blocks_loaded"))

	t.Run("blocks only, index-header version 2", func(t *testing.T) {
		blocksOnlyCases(t, true)
	})

	// Push to both partitions: series spread over the partitions by hash.
	pushed := time.Now()
	var pushes []prompb.TimeSeries
	for i := 0; i < 20; i++ {
		pushes = append(pushes, prompb.TimeSeries{
			Labels:  []prompb.Label{{Name: "__name__", Value: "d_total"}, {Name: "i", Value: strconv.Itoa(i)}},
			Samples: []prompb.Sample{{Value: 1, Timestamp: pushed.UnixMilli()}},
		})
	}
	// c_total{job="both"} is in the recent block and in the ingesters.
	pushes = append(pushes, prompb.TimeSeries{
		Labels:  []prompb.Label{{Name: "__name__", Value: "c_total"}, {Name: "job", Value: "both"}},
		Samples: []prompb.Sample{{Value: 1, Timestamp: pushed.UnixMilli()}},
	}, prompb.TimeSeries{
		Labels:  []prompb.Label{{Name: "__name__", Value: "c_total"}, {Name: "job", Value: "new"}},
		Samples: []prompb.Sample{{Value: 1, Timestamp: pushed.UnixMilli()}},
	})
	res, err := client.Push(pushes)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)
	for _, ing := range []*e2emimir.MimirService{ingester0, ingester1} {
		require.NoError(t, ing.WaitSumMetrics(e2e.Greater(0), "cortex_ingester_memory_series"), ing.Name())
	}
	// Both ingesters hold the tenant's series now, so their owned-series
	// service records the tenant's shard size on its next run.
	shardSizeRecorded := time.Now().Add(ownedSeriesUpdateInterval)

	t.Run("head only, partitions changed inside the window: by hash", func(t *testing.T) {
		start, end := partitionsActive.Add(-time.Minute), time.Now().Add(time.Second)
		got := estimate(t, start, end, url.Values{"match[]": {`{__name__="d_total"}`}})
		assert.Equal(t, "head", got.Source)
		assert.True(t, got.Dedup, "the partitions turned ACTIVE inside the window")
		assert.Equal(t, map[string]int64{"d_total": 20}, countsOf(got))
		assert.Equal(t, promqlCount(t, `{__name__="d_total"}`, "__name__", start, end), countsOf(got))
	})

	t.Run("head only, partitions stable since before the window: counts added up", func(t *testing.T) {
		// Stable once the creation grace period and the minute for the
		// change to reach every distributor have passed, since both the
		// partitions turned ACTIVE and the ingesters recorded the tenant's
		// shard size. Whole seconds, so the one-bucket step divides the
		// window exactly.
		start := partitionsActive
		if shardSizeRecorded.After(start) {
			start = shardSizeRecorded
		}
		start = start.Add(65 * time.Second).Truncate(time.Second).Add(time.Second)
		// The pushed samples are before start; push again inside the window.
		// Half a second in, not at start: a sample stamped with the clock
		// as the sleep ends can read a millisecond before start, outside
		// the window for the estimate and PromQL alike.
		inWindow := start.Add(500 * time.Millisecond)
		if wait := time.Until(inWindow); wait > 0 {
			time.Sleep(wait)
		}
		for i := range pushes {
			pushes[i].Samples = []prompb.Sample{{Value: 2, Timestamp: inWindow.UnixMilli()}}
		}
		res, err := client.Push(pushes)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode)

		end := time.Now().Truncate(time.Second).Add(2 * time.Second)
		got := estimate(t, start, end, url.Values{"match[]": {`{__name__="d_total"}`}, "step": {formatDuration(end.Sub(start))}})
		assert.Equal(t, "head", got.Source)
		assert.False(t, got.Dedup, "the partitions' counts add up")
		assert.Equal(t, map[string]int64{"d_total": 20}, countsOf(got))
		assert.Equal(t, promqlCount(t, `{__name__="d_total"}`, "__name__", start, end), countsOf(got))
	})

	t.Run("blocks and head across the cut: deduplicated by hash", func(t *testing.T) {
		start, end := recentMin, time.Now().Add(time.Second)
		got := estimate(t, start, end, url.Values{"match[]": {`{__name__="c_total"}`}, "group_by": {"job"}})
		assert.Equal(t, "blocks+head", got.Source)
		assert.True(t, got.Dedup)
		assert.False(t, got.LowerBound)
		assert.Equal(t, map[string]int64{"old": 1, "both": 1, "new": 1}, countsOf(got), "c_total{job=\"both\"} counts once")
		assert.Equal(t, promqlCount(t, `{__name__="c_total"}`, "job", start, end), countsOf(got))
	})

	t.Run("blocks and head across the cut: per-bucket counts", func(t *testing.T) {
		// Two buckets: the first holds the recent block's samples, the second
		// the pushed ones, and the cut is inside the window. The window starts
		// at the block's first sample, so the blocks cover it up to the
		// ingesters' range.
		start := recentMin
		step := (time.Since(start)/2 + 2*time.Second).Truncate(time.Millisecond)
		end := start.Add(2 * step)
		got := estimate(t, start, end, url.Values{"match[]": {`{__name__="c_total"}`}, "group_by": {"job"}, "step": {formatDuration(step)}})
		assert.Equal(t, "blocks+head", got.Source)
		assert.False(t, got.LowerBound)
		buckets := map[string][]int64{}
		for _, g := range got.Counts {
			buckets[g.Value] = g.Counts
		}
		for i, bucketStart := range []time.Time{start, start.Add(step)} {
			want := promqlCount(t, `{__name__="c_total"}`, "job", bucketStart, bucketStart.Add(step))
			for job := range buckets {
				assert.Equal(t, want[job], buckets[job][i], "job %s, bucket %d", job, i)
			}
			for job, n := range want {
				require.Contains(t, buckets, job)
				assert.Equal(t, n, buckets[job][i], "job %s, bucket %d", job, i)
			}
		}
		assert.Equal(t, []int64{1, 1}, buckets["both"], "in the block and in the ingesters")
		assert.Equal(t, []int64{1, 0}, buckets["old"])
		assert.Equal(t, []int64{0, 1}, buckets["new"])
	})
}

// newEstimateStoreGateway is a store-gateway on the given index-header
// version, named after it.
func newEstimateStoreGateway(consul *e2e.HTTPService, flags map[string]string, indexHeaderVersion int) *e2emimir.MimirService {
	return e2emimir.NewStoreGateway(fmt.Sprintf("store-gateway-v%d", indexHeaderVersion), consul.NetworkHTTPEndpoint(), mergeFlags(flags, map[string]string{
		"-blocks-storage.bucket-store.index-header.version": strconv.Itoa(indexHeaderVersion),
	}))
}

// series is one series with a two-sample chunk at from and to.
func series(name, label, value string, from, to time.Time) *block.SeriesSpec {
	return &block.SeriesSpec{
		Labels: labels.FromStrings("__name__", name, label, value),
		Chunks: []chunks.Meta{must(chunks.ChunkFromSamples([]chunks.Sample{
			test.Sample{TS: from.UnixMilli(), Val: 1},
			test.Sample{TS: to.UnixMilli(), Val: 1},
		}))},
	}
}

// uploadBlocks writes one block per spec list for userID into minio's
// blocks bucket.
func uploadBlocks(t *testing.T, minio *e2e.HTTPService, blocks []block.SeriesSpecs) {
	t.Helper()
	bkt, err := s3.NewBucketClient(s3.Config{
		Endpoint:        minio.HTTPEndpoint(),
		Insecure:        true,
		BucketName:      blocksBucketName,
		AccessKeyID:     e2edb.MinioAccessKey,
		SecretAccessKey: flagext.SecretWithValue(e2edb.MinioSecretKey),
	}, "test", log.NewNopLogger())
	require.NoError(t, err)
	userBkt := bucket.NewPrefixedBucketClient(bkt, userID)
	defer func() { require.NoError(t, userBkt.Close()) }()
	dir := t.TempDir()
	for _, specs := range blocks {
		meta, err := block.GenerateBlockFromSpec(dir, specs)
		require.NoError(t, err)
		_, err = block.Upload(context.Background(), log.NewNopLogger(), userBkt, filepath.Join(dir, meta.ULID.String()), meta)
		require.NoError(t, err)
	}
}

// formatSeconds formats t as the API's seconds with millisecond precision.
func formatSeconds(t time.Time) string {
	return strconv.FormatFloat(float64(t.UnixMilli())/1000, 'f', 3, 64)
}

// formatDuration formats d as a Prometheus duration in milliseconds.
func formatDuration(d time.Duration) string {
	return fmt.Sprintf("%dms", d.Milliseconds())
}
