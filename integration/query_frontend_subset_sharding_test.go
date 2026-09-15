// SPDX-License-Identifier: AGPL-3.0-only

package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
	"github.com/grafana/mimir/tools/querytee"
)

// subsetShardingHeader enables subset-label query sharding for a request. It must be added to
// -query-frontend.extra-propagated-headers so the query-frontend forwards it to the sharding pass.
const subsetShardingHeader = "X-Mimir-Subset-Label-Sharding"

// TestSubsetLabelSharding_ResultConsistency replicates the correctness result from the subset-label
// query sharding experiment (SUBSET_LABEL_SHARDING_EXPERIMENT_RESULTS.md) in a local cluster.
//
// It pushes a classic histogram (traces_spanmetrics_latency_bucket with le and span_name labels) and
// runs the experiment's query shape, histogram_quantile(0.9, sum by (le, span_name) (rate(...))),
// three ways through the same query-frontend: unsharded, classic sharding, and subset-label sharding.
// It asserts, mirroring the experiment:
//   - unsharded, classic, and subset responses are identical (query-tee's SamplesComparator);
//   - only the sharded arms increment the query-frontend's sharding-rewrite counter;
//   - /api/v1/analyze shows a classic 1_of_N selector for classic and a 1_of_N_by_span_name selector
//     for subset.
//
// The frontend-memory and backend-work numbers from the experiment are environment-specific (data
// path, block/cache state) and are not reproduced here; this test replicates the correctness claim,
// which is the experiment's central result.
func TestSubsetLabelSharding_ResultConsistency(t *testing.T) {
	const (
		tenant     = "subset-tenant"
		shardCount = 8
		metricName = "traces_spanmetrics_latency_bucket"
		query      = `histogram_quantile(0.9, sum by (le, span_name) (rate(traces_spanmetrics_latency_bucket[5m])))`
	)

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	queryFrontend, distributor := startSubsetShardingCluster(t, s, tenant, shardCount)

	// Push a classic histogram: for each span_name, a cumulative set of le buckets.
	now := time.Now()
	writeClient, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", tenant)
	require.NoError(t, err)

	res, err := writeClient.Push(buildHistogramBucketSeries(now, metricName, 12, 20))
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Three clients hitting the same query-frontend, differing only in how the request is sharded.
	unshardedClient, err := e2emimir.NewClient("", queryFrontend.HTTPEndpoint(), "", "", tenant, e2emimir.WithAddHeader("Sharding-Control", "0"))
	require.NoError(t, err)
	classicClient, err := e2emimir.NewClient("", queryFrontend.HTTPEndpoint(), "", "", tenant, e2emimir.WithAddHeader("Sharding-Control", strconv.Itoa(shardCount)))
	require.NoError(t, err)
	subsetClient, err := e2emimir.NewClient("", queryFrontend.HTTPEndpoint(), "", "", tenant,
		e2emimir.WithAddHeader("Sharding-Control", strconv.Itoa(shardCount)),
		e2emimir.WithAddHeader(subsetShardingHeader, "true"),
	)
	require.NoError(t, err)

	comparator := querytee.NewSamplesComparator(querytee.SampleComparisonOptions{
		Tolerance:        1e-6,
		UseRelativeError: true,
	})

	queryStart := now.Add(-15 * time.Minute)
	queryEnd := now.Add(-1 * time.Minute)
	queryStep := time.Minute

	// runQuery issues a raw query and asserts its effect on the query-frontend's sharding-rewrite
	// counter: unsharded requests must leave it unchanged, sharded requests must increase it. Only the
	// direction is checked, so this tolerates query splitting.
	runQuery := func(t *testing.T, expectSharded bool, do func() (*http.Response, []byte, error)) []byte {
		t.Helper()
		const rewritesMetric = "cortex_frontend_query_sharding_rewrites_succeeded_total"

		before, err := queryFrontend.SumMetrics([]string{rewritesMetric}, e2e.WaitMissingMetrics)
		require.NoError(t, err)

		res, body, reqErr := do()
		require.NoError(t, reqErr)
		require.Equal(t, http.StatusOK, res.StatusCode, "unexpected status code, body: %s", body)

		if expectSharded {
			require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Greater(before[0]), rewritesMetric))
		} else {
			require.NoError(t, queryFrontend.WaitSumMetricsWithOptions(e2e.Equals(before[0]), []string{rewritesMetric}, e2e.WaitMissingMetrics))
		}
		return body
	}

	t.Run("instant", func(t *testing.T) {
		unshardedResp := runQuery(t, false, func() (*http.Response, []byte, error) { return unshardedClient.QueryRawAt(query, queryEnd) })
		classicResp := runQuery(t, true, func() (*http.Response, []byte, error) { return classicClient.QueryRawAt(query, queryEnd) })
		subsetResp := runQuery(t, true, func() (*http.Response, []byte, error) { return subsetClient.QueryRawAt(query, queryEnd) })

		// Guard against a vacuous comparison: an all-empty result would make Compare pass trivially.
		requireNonEmptyResult(t, unshardedResp)

		_, err := comparator.Compare(unshardedResp, classicResp, queryEnd)
		require.NoError(t, err, "classic sharding result differs from unsharded")
		_, err = comparator.Compare(unshardedResp, subsetResp, queryEnd)
		require.NoError(t, err, "subset sharding result differs from unsharded")
	})

	t.Run("range", func(t *testing.T) {
		unshardedResp := runQuery(t, false, func() (*http.Response, []byte, error) {
			return unshardedClient.QueryRangeRaw(query, queryStart, queryEnd, queryStep)
		})
		classicResp := runQuery(t, true, func() (*http.Response, []byte, error) {
			return classicClient.QueryRangeRaw(query, queryStart, queryEnd, queryStep)
		})
		subsetResp := runQuery(t, true, func() (*http.Response, []byte, error) {
			return subsetClient.QueryRangeRaw(query, queryStart, queryEnd, queryStep)
		})

		requireNonEmptyResult(t, unshardedResp)

		_, err := comparator.Compare(unshardedResp, classicResp, queryEnd)
		require.NoError(t, err, "classic sharding result differs from unsharded")
		_, err = comparator.Compare(unshardedResp, subsetResp, queryEnd)
		require.NoError(t, err, "subset sharding result differs from unsharded")
	})

	// /api/v1/analyze confirms the plan uses classic 1_of_N selectors for classic sharding and
	// 1_of_N_by_span_name subset selectors for subset sharding, as the experiment reported.
	t.Run("analyze", func(t *testing.T) {
		analyze := func(c *e2emimir.Client) string {
			addr := fmt.Sprintf("http://%s/api/v1/analyze?query=%s&time=%s",
				queryFrontend.HTTPEndpoint(), url.QueryEscape(query), e2emimir.FormatTime(queryEnd))
			res, body, err := c.DoPostBody(addr, nil)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, res.StatusCode, "analyze failed, body: %s", body)
			return string(body)
		}

		classicPlan := analyze(classicClient)
		require.Contains(t, classicPlan, fmt.Sprintf("1_of_%d", shardCount))
		require.NotContains(t, classicPlan, "by_span_name")

		subsetPlan := analyze(subsetClient)
		require.Contains(t, subsetPlan, fmt.Sprintf("1_of_%d_by_span_name", shardCount))
		require.True(t, strings.Contains(subsetPlan, "histogram_quantile"), "subset plan should retain histogram_quantile per shard")
	})
}

// requireNonEmptyResult asserts a raw query response carries at least one result series, so a
// result-consistency comparison of two empty responses can't pass vacuously.
func requireNonEmptyResult(t *testing.T, body []byte) {
	t.Helper()
	var resp querytee.SamplesResponse
	require.NoError(t, json.Unmarshal(body, &resp))
	var result []json.RawMessage
	require.NoError(t, json.Unmarshal(resp.Data.Result, &result))
	require.NotEmpty(t, result, "expected a non-empty query result")
}

// TestSubsetLabelSharding_FrontendMemory replicates the query-frontend memory reduction from the
// subset-label sharding experiment (SUBSET_LABEL_SHARDING_EXPERIMENT_RESULTS.md: MQE estimated peak
// memory ~143 MB classic vs ~31 MB subset, a 78.5% reduction).
//
// The reduction is a distributed property, not visible in a single in-process engine: for this query
// shape classic sharding makes the frontend re-aggregate shardCount x (le x span_name) partial sums,
// while subset sharding pushes the whole histogram_quantile into each shard so the frontend only
// concatenates the final per-shard output series. We measure it by scraping the frontend engine's
// estimated-peak-memory histogram (which is not exposed in the per-query response stats) as a per-query
// average across a before/after window around each arm.
//
// Absolute bytes depend on data/scale, so — as the experiment notes — only the within-run classic/subset
// ratio is asserted (subset must be materially lower); both values are logged for inspection.
func TestSubsetLabelSharding_FrontendMemory(t *testing.T) {
	const (
		tenant     = "subset-tenant"
		shardCount = 8
		metricName = "traces_spanmetrics_latency_bucket"
		query      = `histogram_quantile(0.9, sum by (le, span_name) (rate(traces_spanmetrics_latency_bucket[5m])))`
	)

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	queryFrontend, distributor := startSubsetShardingCluster(t, s, tenant, shardCount)

	// Use a higher span_name cardinality than the correctness test so classic sharding's frontend
	// re-aggregation buffer is meaningfully larger than subset's, making the ratio robust to noise.
	now := time.Now()
	writeClient, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", tenant)
	require.NoError(t, err)
	res, err := writeClient.Push(buildHistogramBucketSeries(now, metricName, 40, 20))
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	classicClient, err := e2emimir.NewClient("", queryFrontend.HTTPEndpoint(), "", "", tenant, e2emimir.WithAddHeader("Sharding-Control", strconv.Itoa(shardCount)))
	require.NoError(t, err)
	subsetClient, err := e2emimir.NewClient("", queryFrontend.HTTPEndpoint(), "", "", tenant,
		e2emimir.WithAddHeader("Sharding-Control", strconv.Itoa(shardCount)),
		e2emimir.WithAddHeader(subsetShardingHeader, "true"),
	)
	require.NoError(t, err)

	queryStart := now.Add(-15 * time.Minute)
	queryEnd := now.Add(-1 * time.Minute)
	queryStep := time.Minute

	classicPeak := frontendQueryPeakMemoryBytes(t, queryFrontend, func() {
		res, body, err := classicClient.QueryRangeRaw(query, queryStart, queryEnd, queryStep)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode, "classic query failed, body: %s", body)
		requireNonEmptyResult(t, body)
	})
	subsetPeak := frontendQueryPeakMemoryBytes(t, queryFrontend, func() {
		res, body, err := subsetClient.QueryRangeRaw(query, queryStart, queryEnd, queryStep)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode, "subset query failed, body: %s", body)
		requireNonEmptyResult(t, body)
	})

	t.Logf("query-frontend estimated peak memory per query: classic=%.0f bytes, subset=%.0f bytes (%.1f%% lower)",
		classicPeak, subsetPeak, 100*(classicPeak-subsetPeak)/classicPeak)

	// Subset sharding must materially reduce the frontend's peak memory. A 0.75x ceiling is well within
	// the experiment's ~0.21x while tolerating small-scale noise.
	require.Less(t, subsetPeak, classicPeak*0.75,
		"expected subset frontend peak memory (%.0f) to be well below classic (%.0f)", subsetPeak, classicPeak)
}

// frontendQueryPeakMemoryBytes runs do (a single query) and returns the query-frontend engine's mean
// estimated peak memory per query observed during it, from the estimated-peak-memory histogram's
// sum/count delta around the call.
func frontendQueryPeakMemoryBytes(t *testing.T, queryFrontend *e2emimir.MimirService, do func()) float64 {
	t.Helper()
	const metric = "cortex_mimir_query_engine_estimated_query_peak_memory_consumption"

	sumBefore, err := queryFrontend.SumMetrics([]string{metric}, e2e.WaitMissingMetrics)
	require.NoError(t, err)
	countBefore, err := queryFrontend.SumMetrics([]string{metric}, e2e.WithMetricCount, e2e.WaitMissingMetrics)
	require.NoError(t, err)

	do()

	// Wait until the query's observation is recorded before reading the sum.
	require.NoError(t, queryFrontend.WaitSumMetricsWithOptions(e2e.Greater(countBefore[0]), []string{metric}, e2e.WithMetricCount, e2e.WaitMissingMetrics))

	sumAfter, err := queryFrontend.SumMetrics([]string{metric}, e2e.WaitMissingMetrics)
	require.NoError(t, err)
	countAfter, err := queryFrontend.SumMetrics([]string{metric}, e2e.WithMetricCount, e2e.WaitMissingMetrics)
	require.NoError(t, err)

	deltaCount := countAfter[0] - countBefore[0]
	deltaSum := sumAfter[0] - sumBefore[0]
	require.Greater(t, deltaCount, float64(0), "no peak-memory observations recorded for the query")
	return deltaSum / deltaCount
}

// startSubsetShardingCluster starts a minimal read-write cluster (consul, minio, query-scheduler,
// query-frontend, ingester, 2 queriers, distributor) with query sharding and the subset-label header
// enabled for tenant, and returns the query-frontend and distributor. The caller owns s.Close().
func startSubsetShardingCluster(t *testing.T, s *e2e.Scenario, tenant string, shardCount int) (queryFrontend, distributor *e2emimir.MimirService) {
	t.Helper()

	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul))

	flags := mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
		// Results caching is disabled: arms share tenant, query and time range, so a shared results cache
		// could serve one path's response for another.
		"-query-frontend.cache-results":                       "false",
		"-query-frontend.parallelize-shardable-queries":       "true",
		"-query-frontend.query-sharding-total-shards":         "0", // Enabled per-tenant via runtime config below.
		"-query-frontend.enable-remote-execution":             "true",
		"-query-frontend.use-mimir-query-engine-for-sharding": "true",
		// Allow the subset header through the query path so the sharding optimization pass can read it.
		"-query-frontend.extra-propagated-headers": subsetShardingHeader,
	})

	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(minio))

	// Sharding-Control pins the exact shard count per request, but getShardsForQuery still requires the
	// tenant's total shards to be > 1 for sharding to be enabled.
	runtimeConfig := "runtime-config.yaml"
	require.NoError(t, writeFileToSharedDir(s, runtimeConfig, []byte(fmt.Sprintf(`
overrides:
  %s:
    query_sharding_total_shards: %d
`, tenant, shardCount))))
	flags["-runtime-config.file"] = filepath.Join(e2e.ContainerSharedDir, runtimeConfig)

	queryScheduler := e2emimir.NewQueryScheduler("query-scheduler", flags)
	require.NoError(t, s.StartAndWaitReady(queryScheduler))
	flags["-query-frontend.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
	flags["-querier.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()

	queryFrontend = e2emimir.NewQueryFrontend("query-frontend", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.Start(queryFrontend))

	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
	distributor = e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	querier1 := e2emimir.NewQuerier("querier-1", consul.NetworkHTTPEndpoint(), flags)
	querier2 := e2emimir.NewQuerier("querier-2", consul.NetworkHTTPEndpoint(), flags)

	require.NoError(t, s.StartAndWaitReady(querier1, querier2, ingester, distributor))
	require.NoError(t, s.WaitReady(queryFrontend))

	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))
	require.NoError(t, querier1.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier2.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	require.NoError(t, queryFrontend.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "querier"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Wait until both queriers connect to the scheduler before issuing frontend queries, otherwise a
	// sharded query can race the querier-to-scheduler worker connections and time out.
	require.NoError(t, queryScheduler.WaitSumMetrics(e2e.Equals(8), "cortex_query_scheduler_connected_querier_clients"))

	return queryFrontend, distributor
}

// buildHistogramBucketSeries builds classic-histogram series for numSpanNames span_name values, each
// with a cumulative set of le buckets. Larger le has a >= count (value grows with the bucket index) and
// values grow over time (counter-like), so histogram_quantile and rate produce real values.
func buildHistogramBucketSeries(now time.Time, metricName string, numSpanNames, numSamples int) []prompb.TimeSeries {
	buckets := []string{"0.1", "0.5", "1", "2.5", "5", "10", "+Inf"}

	var allSeries []prompb.TimeSeries
	for spanIdx := 0; spanIdx < numSpanNames; spanIdx++ {
		for bucketIdx, le := range buckets {
			samples := make([]prompb.Sample, numSamples)
			for i := 0; i < numSamples; i++ {
				samples[i] = prompb.Sample{
					Value:     float64((bucketIdx + 1) * (i + 1) * 10),
					Timestamp: now.Add(time.Duration(i-numSamples+1) * time.Minute).UnixMilli(),
				}
			}

			allSeries = append(allSeries, prompb.TimeSeries{
				Labels: []prompb.Label{
					{Name: model.MetricNameLabel, Value: metricName},
					{Name: "le", Value: le},
					{Name: "span_name", Value: fmt.Sprintf("span_%d", spanIdx)},
				},
				Samples: samples,
			})
		}
	}
	return allSeries
}
