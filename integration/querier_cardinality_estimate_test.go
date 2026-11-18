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
	Read        string `json:"read"`
	Source      string `json:"source"`
	Dedup       bool   `json:"dedup"`
	LowerBound  bool   `json:"lower_bound"`
	CoverageGap bool   `json:"coverage_gap"`
	UpperBound  bool   `json:"upper_bound"`
	Total       int64  `json:"total"`
	Counts      []struct {
		Value string `json:"value"`
		Count int64  `json:"count"`
	} `json:"groups"`
}

// cardinalityBounds is the part of a bounds answer the tests check.
type cardinalityBounds struct {
	Ranges []struct {
		Start       int64             `json:"start"`
		End         int64             `json:"end"`
		MainSet     bool              `json:"main_set"`
		MainBlocks  int               `json:"main_blocks"`
		OtherBlocks int               `json:"other_blocks"`
		LowerBound  bool              `json:"lower_bound"`
		Groups      []boundsGroupJSON `json:"groups"`
	} `json:"ranges"`
	NotCovered []struct {
		Start int64 `json:"start"`
		End   int64 `json:"end"`
	} `json:"not_covered"`
	Head *struct {
		Approximate bool `json:"approximate"`
		Groups      []struct {
			Value    string `json:"value"`
			InMemory int64  `json:"in_memory"`
		} `json:"groups"`
	} `json:"head"`
}

type boundsGroupJSON struct {
	Value string `json:"value"`
	Lower int64  `json:"lower"`
	Upper int64  `json:"upper"`
}

// largeGroupSeries is how many series of e_total the recent block holds, a
// group deduplicated by hash with every one of its hashes.
const largeGroupSeries = 12_000

// TestCardinalityEstimate checks the cardinality estimate route against a
// PromQL count over the same window, on an ingest storage cluster with two
// partitions and a query-frontend in front of the querier: from blocks only (the name table from the index-headers, and a
// breakdown by another label) on both index-header versions, from the
// ingesters' heads only, and from both across the store-gateways/ingesters
// cut.
func TestCardinalityEstimate(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, blocksBucketName)
	kafka := e2edb.NewKafka()
	require.NoError(t, s.StartAndWaitReady(consul, minio, kafka))

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
		append(block.SeriesSpecs{
			series("c_total", "job", "old", recentMin, recentMax),
			series("c_total", "job", "both", recentMin, recentMax),
		}, manySeries("e_total", "i", largeGroupSeries, recentMin, recentMax)...),
	})

	// A day of two split-compactor shards with an out-of-order block of shard 1 beside them, as the
	// compactor leaves them: it never merges the out-of-order group into the main one. Each series is in
	// the shard the compactor would put it in, which sharded queries rely on. m_total has three series
	// in the shards, and the out-of-order block repeats one of shard 1 and adds a fourth.
	boundsDay := day.Add(-96 * time.Hour)
	uploadLabelledBlocks(t, minio, boundsDayBlocks(boundsDay))

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
	require.NoError(t, storeGateway.WaitSumMetrics(e2e.Equals(6), "cortex_bucket_store_blocks_loaded"))

	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), queryFrontend.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)

	// estimateRaw sends the request through the query-frontend and returns
	// the status and body.
	estimateRaw := func(t *testing.T, start, end time.Time, params url.Values) (int, []byte) {
		t.Helper()
		params.Set("start", formatSeconds(start))
		params.Set("end", formatSeconds(end))
		req, err := http.NewRequest(http.MethodGet, "http://"+queryFrontend.HTTPEndpoint()+"/prometheus/api/v1/cardinality/estimate?"+params.Encode(), nil)
		require.NoError(t, err)
		req.Header.Set("X-Scope-OrgID", userID)
		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer res.Body.Close()
		body, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		return res.StatusCode, body
	}
	estimate := func(t *testing.T, start, end time.Time, params url.Values) cardinalityEstimate {
		t.Helper()
		status, body := estimateRaw(t, start, end, params)
		require.Equal(t, http.StatusOK, status, string(body))
		var out cardinalityEstimate
		require.NoError(t, json.Unmarshal(body, &out), string(body))
		return out
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
	// run once per index-header version.
	blocksOnlyCases := func(t *testing.T) {

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

		t.Run("by job over each half of the day", func(t *testing.T) {
			// x has samples in both halves, y in the first and z in the second.
			for _, half := range []struct {
				start time.Time
				want  map[string]int64
			}{
				{day, map[string]int64{"x": 1, "y": 1}},
				{day.Add(12 * time.Hour), map[string]int64{"x": 1, "z": 1}},
			} {
				start, end := half.start, half.start.Add(12*time.Hour)
				got := estimate(t, start, end, url.Values{"match[]": {`{__name__="a_total"}`}, "group_by": {"job"}})
				assert.Equal(t, "full_index", got.Read)
				assert.Equal(t, half.want, countsOf(got))
				assert.Equal(t, promqlCount(t, `{__name__="a_total"}`, "job", start, end), countsOf(got))
			}
		})
	}

	t.Run("blocks only, index-header version 1", func(t *testing.T) {
		blocksOnlyCases(t)
	})

	// boundsOf sends a bounds request and returns its answer.
	boundsOf := func(t *testing.T, start, end time.Time, params url.Values) cardinalityBounds {
		t.Helper()
		params.Set("mode", "bounds")
		status, body := estimateRaw(t, start, end, params)
		require.Equal(t, http.StatusOK, status, string(body))
		var out cardinalityBounds
		require.NoError(t, json.Unmarshal(body, &out), string(body))
		return out
	}

	t.Run("bounds: a day with out-of-order blocks beside its shards holds the truth", func(t *testing.T) {
		// A window inside the day is widened to it.
		got := boundsOf(t, boundsDay.Add(4*time.Hour), boundsDay.Add(6*time.Hour), url.Values{})
		require.Len(t, got.Ranges, 1)
		r := got.Ranges[0]
		assert.Equal(t, boundsDay.UnixMilli(), r.Start)
		assert.Equal(t, boundsDay.Add(24*time.Hour).UnixMilli(), r.End)
		assert.True(t, r.MainSet)
		assert.Equal(t, 2, r.MainBlocks)
		assert.Equal(t, 1, r.OtherBlocks)
		assert.False(t, r.LowerBound)
		truth := promqlCount(t, `{__name__=~"m_total|n_total"}`, "__name__", boundsDay, boundsDay.Add(24*time.Hour))
		require.Equal(t, map[string]int64{"m_total": 4, "n_total": 1}, truth)
		for _, g := range r.Groups {
			assert.LessOrEqual(t, g.Lower, truth[g.Value], g.Value)
			assert.GreaterOrEqual(t, g.Upper, truth[g.Value], g.Value)
		}
		assert.Equal(t, []boundsGroupJSON{{Value: "m_total", Lower: 3, Upper: 5}, {Value: "n_total", Lower: 1, Upper: 1}}, r.Groups)
		assert.Zero(t, got.Head)
		assert.Empty(t, got.NotCovered)
	})

	t.Run("bounds: a range of shards alone is exact, by another label too", func(t *testing.T) {
		start, end := day, day.Add(24*time.Hour)
		got := boundsOf(t, start, end, url.Values{"group_by": {"job"}})
		require.Len(t, got.Ranges, 1)
		r := got.Ranges[0]
		truth := promqlCount(t, `{__name__=~".+"}`, "job", start, end)
		for _, g := range r.Groups {
			assert.Equal(t, truth[g.Value], g.Lower, g.Value)
			assert.Equal(t, g.Lower, g.Upper, g.Value)
		}
		assert.Len(t, r.Groups, len(truth))
	})

	t.Run("bounds: matchers are refused", func(t *testing.T) {
		status, body := estimateRaw(t, day, day.Add(24*time.Hour), url.Values{"mode": {"bounds"}, "match[]": {`{job="x"}`}})
		assert.Equal(t, http.StatusBadRequest, status, string(body))
	})

	t.Run("a window no block or ingester holds is a lower bound", func(t *testing.T) {
		start, end := day.Add(-48*time.Hour), day.Add(-24*time.Hour)
		got := estimate(t, start, end, url.Values{"match[]": {`{__name__="a_total"}`}})
		assert.Equal(t, "blocks", got.Source)
		assert.True(t, got.LowerBound)
		assert.True(t, got.CoverageGap)
		assert.Empty(t, got.Counts)
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
	require.NoError(t, storeGateway.WaitSumMetrics(e2e.Equals(6), "cortex_bucket_store_blocks_loaded"))

	t.Run("blocks only, index-header version 2", func(t *testing.T) {
		blocksOnlyCases(t)
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

	t.Run("head only, partitions changed inside the window: by hash", func(t *testing.T) {
		start, end := partitionsActive.Add(-time.Minute), time.Now().Add(time.Second)
		got := estimate(t, start, end, url.Values{"match[]": {`{__name__="d_total"}`}})
		assert.Equal(t, "head", got.Source)
		assert.True(t, got.Dedup, "the partitions turned ACTIVE inside the window")
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

	t.Run("blocks and head across the cut: a large group is exact", func(t *testing.T) {
		// The window reaches into the ingesters' range, so the series are
		// deduplicated, and the store-gateway sends every hash of e_total,
		// held by the recent block alone.
		start, end := recentMin, time.Now().Add(time.Second)
		got := estimate(t, start, end, url.Values{"match[]": {`{__name__="e_total"}`}})
		assert.Equal(t, "blocks+head", got.Source)
		assert.True(t, got.Dedup)
		exact := promqlCount(t, `{__name__="e_total"}`, "__name__", start, end)
		require.Equal(t, map[string]int64{"e_total": largeGroupSeries}, exact)
		assert.Equal(t, exact, countsOf(got))
	})

	t.Run("blocks and head across the cut: each half of the window", func(t *testing.T) {
		// The first half holds the recent block's samples, the second the
		// pushed ones, and the cut is inside the window. The window starts at
		// the block's first sample, so the blocks cover it up to the
		// ingesters' range.
		start := recentMin
		half := (time.Since(start)/2 + 2*time.Second).Truncate(time.Millisecond)
		for i, from := range []time.Time{start, start.Add(half)} {
			got := estimate(t, from, from.Add(half), url.Values{"match[]": {`{__name__="c_total"}`}, "group_by": {"job"}})
			assert.False(t, got.LowerBound, "half %d", i)
			assert.Equal(t, promqlCount(t, `{__name__="c_total"}`, "job", from, from.Add(half)), countsOf(got), "half %d", i)
			assert.Equal(t, int64(1), countsOf(got)["both"], "half %d: in the block and in the ingesters", i)
		}
	})

	t.Run("bounds: a window that reaches the ingesters has head rows and the band after the blocks not covered", func(t *testing.T) {
		start, end := recentMin, time.Now().Add(time.Second)
		got := boundsOf(t, start, end, url.Values{})
		require.NotNil(t, got.Head)
		assert.False(t, got.Head.Approximate, "ingest storage")
		assert.NotEmpty(t, got.Head.Groups)
		assert.NotEmpty(t, got.NotCovered)
	})

	// Last, so the cases above run while the partitions become stable.
	t.Run("head only, partitions stable since before the window: counts added up", func(t *testing.T) {
		// Stable once the creation grace period and the minute for the
		// change to reach every distributor have passed since the
		// partitions turned ACTIVE.
		start := partitionsActive.Add(65 * time.Second).Truncate(time.Second).Add(time.Second)
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
		got := estimate(t, start, end, url.Values{"match[]": {`{__name__="d_total"}`}})
		assert.Equal(t, "head", got.Source)
		assert.False(t, got.Dedup, "the partitions' counts add up")
		assert.True(t, got.UpperBound, "two partitions' counts added up are an upper value")
		assert.Equal(t, map[string]int64{"d_total": 20}, countsOf(got))
		assert.Equal(t, promqlCount(t, `{__name__="d_total"}`, "__name__", start, end), countsOf(got))
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

// manySeries is n series of one metric told apart by label, each a
// two-sample chunk at from and to.
func manySeries(name, label string, n int, from, to time.Time) block.SeriesSpecs {
	specs := make(block.SeriesSpecs, 0, n)
	for i := range n {
		specs = append(specs, series(name, label, strconv.Itoa(i), from, to))
	}
	return specs
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

// boundsDayBlocks returns the blocks of the bounds day starting at from: two shard blocks of
// m_total and n_total, and an out-of-order block of shard 1 that repeats one of its m_total series
// and adds one more.
func boundsDayBlocks(from time.Time) []labelledBlock {
	shardOf := func(lset labels.Labels) uint64 { return labels.StableHash(lset) % 2 }
	whole := func(name, value string) *block.SeriesSpec {
		return series(name, "p", value, from, from.Add(24*time.Hour-time.Millisecond))
	}
	// One m_total series in each shard, then a third.
	var shards [2]block.SeriesSpecs
	var values []string
	firstIn := [2]string{}
	for c := 'a'; c <= 'z' && (firstIn[0] == "" || firstIn[1] == ""); c++ {
		if i := shardOf(labels.FromStrings("__name__", "m_total", "p", string(c))); firstIn[i] == "" {
			firstIn[i] = string(c)
		}
	}
	values = append(values, firstIn[0], firstIn[1])
	for c := 'a'; c <= 'z'; c++ {
		if v := string(c); v != firstIn[0] && v != firstIn[1] {
			values = append(values, v)
			break
		}
	}
	for _, v := range values {
		spec := whole("m_total", v)
		shards[shardOf(spec.Labels)] = append(shards[shardOf(spec.Labels)], spec)
	}
	n := whole("n_total", "a")
	shards[shardOf(n.Labels)] = append(shards[shardOf(n.Labels)], n)
	ooo := block.SeriesSpecs{series("m_total", "p", firstIn[0], from.Add(2*time.Hour), from.Add(3*time.Hour))}
	for c := 'A'; c <= 'Z'; c++ {
		if lset := labels.FromStrings("__name__", "m_total", "p", string(c)); shardOf(lset) == 0 {
			ooo = append(ooo, whole("m_total", string(c)))
			break
		}
	}
	out := []labelledBlock{
		{labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_2", block.OutOfOrderExternalLabel: block.OutOfOrderExternalLabelValue}, specs: ooo},
	}
	for i, specs := range shards {
		if len(specs) > 0 {
			out = append(out, labelledBlock{labels: map[string]string{block.CompactorShardIDExternalLabel: fmt.Sprintf("%d_of_2", i+1)}, specs: specs})
		}
	}
	return out
}

// labelledBlock is one block's series and the external labels of its meta.json.
type labelledBlock struct {
	labels map[string]string
	specs  block.SeriesSpecs
}

// uploadLabelledBlocks writes one block per entry for userID into minio's blocks bucket, with its
// external labels.
func uploadLabelledBlocks(t *testing.T, minio *e2e.HTTPService, blocks []labelledBlock) {
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
	for _, b := range blocks {
		meta, err := block.GenerateBlockFromSpec(dir, b.specs)
		require.NoError(t, err)
		meta.Thanos.Labels = b.labels
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
