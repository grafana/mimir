// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"math"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	promvalue "github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/chunk"
)

func wireChunk(_ *testing.T, encoding chunk.Encoding, c chunkenc.Chunk) client.Chunk {
	return client.Chunk{Encoding: int32(encoding), Data: c.Bytes(), StartTimestampMs: 0, EndTimestampMs: math.MaxInt64}
}

func histogramChunk(t *testing.T, histograms ...*histogram.Histogram) chunkenc.Chunk {
	c := chunkenc.NewHistogramChunk()
	app, err := c.Appender()
	require.NoError(t, err)
	var current chunkenc.Chunk = c
	for i, h := range histograms {
		next, _, appender, err := app.AppendHistogram(nil, 0, int64(i+1)*1000, h, false)
		require.NoError(t, err)
		// A layout change can recode the chunk into a new one with the widened layout.
		if next != nil {
			current = next
		}
		app = appender
	}
	return current
}

func request() *client.QueryRequest {
	return &client.QueryRequest{StartTimestampMs: 0, EndTimestampMs: math.MaxInt64}
}

// Compacting the iterator's shared bucket slices in place used to corrupt the samples after the
// first one, and Go's widened layouts must compare equal to the original ones.
func TestHistogramSamplesCompareBucketCountsAcrossLayouts(t *testing.T) {
	narrow := &histogram.Histogram{
		Count: 2, Sum: 3, Schema: 0,
		PositiveSpans:   []histogram.Span{{Offset: 0, Length: 1}, {Offset: 2, Length: 1}},
		PositiveBuckets: []int64{1, 0},
	}
	widened := &histogram.Histogram{
		Count: 2, Sum: 3, Schema: 0,
		PositiveSpans:   []histogram.Span{{Offset: 0, Length: 4}},
		PositiveBuckets: []int64{1, -1, 0, 1},
	}
	later := &histogram.Histogram{
		Count: 5, Sum: 9, Schema: 0,
		PositiveSpans:   []histogram.Span{{Offset: 0, Length: 1}, {Offset: 2, Length: 1}},
		PositiveBuckets: []int64{2, 1},
	}
	rust, err := samples(wireChunk(t, chunk.PrometheusHistogramChunk, histogramChunk(t, narrow, later)), request(), map[string]int{})
	require.NoError(t, err)
	goSide, err := samples(wireChunk(t, chunk.PrometheusHistogramChunk, histogramChunk(t, widened, later)), request(), map[string]int{})
	require.NoError(t, err)
	require.Len(t, rust, 2)
	require.Equal(t, rust, goSide)
	require.NotEqual(t, rust[0], rust[1], "the second sample must keep its own bucket counts")
}

func TestStaleMarkersCompareEqualAcrossEncodings(t *testing.T) {
	float := chunkenc.NewXORChunk()
	app, err := float.Appender()
	require.NoError(t, err)
	app.Append(0, 1000, math.Float64frombits(promvalue.StaleNaN))

	stale := &histogram.Histogram{Sum: math.Float64frombits(promvalue.StaleNaN)}
	counts := map[string]int{}
	fromFloat, err := samples(wireChunk(t, chunk.PrometheusXorChunk, float), request(), counts)
	require.NoError(t, err)
	fromHistogram, err := samples(wireChunk(t, chunk.PrometheusHistogramChunk, histogramChunk(t, stale)), request(), counts)
	require.NoError(t, err)
	require.Equal(t, []string{"1000:stale"}, fromFloat)
	require.Equal(t, fromFloat, fromHistogram)
	require.Equal(t, 2, counts["stale"])
}

func TestCompareReportsMissingAndDifferingSeries(t *testing.T) {
	reference := map[string][]string{"a": {"1:f:1"}, "b": {"1:f:1"}}
	compared := map[string][]string{"a": {"1:f:1"}, "b": {"1:f:2"}, "c": {"1:f:1"}}
	require.Len(t, compare(reference, compared), 2)
	require.Empty(t, compare(reference, map[string][]string{"a": {"1:f:1"}, "b": {"1:f:1"}}))
}

func testData() tenantData {
	return tenantData{
		nameSeries: map[string]uint64{"http_requests_total": 40, "http_errors_total": 10, "go_goroutines": 5, "huge_metric": 1_000_000},
		labelValues: map[string]map[string][]string{
			"http_requests_total": {"job": {"api", "web"}, "path": {"/v1/query", "/v1/push?x=1"}, "code": {"200", "500"}},
			"go_goroutines":       {"instance": {"a:9090"}},
		},
		rareValues: map[string]map[string]uint64{"job": {"api": 3, "web": 1_000_000}},
	}
}

// Every generated selector, and its shard variants, parses as PromQL; names over the bound aren't picked.
func TestGeneratedSelectorsParse(t *testing.T) {
	cfg := generationConfig{seed: 1, names: 8, maxSeries: 100, maxSelectors: 200}
	data := testData()
	picked := pickNames(data.nameSeries, cfg.names, cfg.maxSeries, rand.New(rand.NewPCG(1, 0)))
	require.NotContains(t, picked, "huge_metric")
	selectors := generate(data, sortedKeys(data.labelValues), cfg, rand.New(rand.NewPCG(1, 0)))
	require.NotEmpty(t, selectors)
	for _, s := range selectors {
		for _, variant := range []string{s, withShard(s, "1_of_16"), withShard(s, "3_of_4")} {
			_, err := parser.NewParser(parser.Options{}).ParseMetricSelector(variant)
			require.NoError(t, err, variant)
		}
	}
	joined := strings.Join(selectors, "\n")
	for _, shape := range []string{`__name__=~"http_requests_total|`, `job!~`, `job=~"(?i)`, `parity_probe_missing=""`, `parity_probe_missing!=""`, `job=~".+"`, `job=~".*"`, `path="/v1/query"`, `path=~"/v1/query|/v1/push\\?x=1"`, `{job="api"}`} {
		require.Contains(t, joined, shape)
	}
	require.NotContains(t, joined, `"web"}`, "a value with too many series isn't picked for a nameless selector")
	// The same seed generates the same selectors.
	require.Equal(t, selectors, generate(data, sortedKeys(data.labelValues), cfg, rand.New(rand.NewPCG(1, 0))))
}

func TestShardsTogetherReturnTheUnshardedAnswer(t *testing.T) {
	unsharded := map[string][]string{"a": {"1:f:1", "2:f:2"}, "b": {"1:f:3"}}
	shards := []result{
		{series: map[string][]string{"a": {"1:f:1", "2:f:2"}}},
		{series: map[string][]string{"b": {"1:f:3"}}},
		{series: map[string][]string{}},
	}
	require.Empty(t, compare(unsharded, union(shards)))
	require.NotEmpty(t, compare(unsharded, union(shards[:1])), "a missing shard loses series")
}

// A querier drops samples a series repeats across chunks, as after compaction.
func TestRepeatedSamplesAcrossChunksCountOnce(t *testing.T) {
	require.Equal(t, []string{"1:f:1", "2:f:2"}, dedupe([]string{"1:f:1", "1:f:1", "2:f:2"}))
}

// Lookups read around a compared one tolerate what entered or left the head in between.
func TestSetAndCountChecksTolerateChangesBetweenReferenceReads(t *testing.T) {
	differences, unstable := setDifferences([]string{"a", "b"}, []string{"a", "b", "c"}, []string{"a", "b", "c"})
	require.Empty(t, differences)
	require.Equal(t, 1, unstable)
	differences, _ = setDifferences([]string{"a", "b"}, []string{"a", "x"}, []string{"a", "b"})
	require.Equal(t, []string{"only in compared: x", "only in reference: b"}, differences)

	require.Empty(t, countDifferences(map[string]uint64{"k": 10}, map[string]uint64{"k": 11}, map[string]uint64{"k": 12}, 0))
	require.Len(t, countDifferences(map[string]uint64{"k": 10}, map[string]uint64{"k": 13}, map[string]uint64{"k": 12}, 0), 1)
	require.Len(t, countDifferences(map[string]uint64{"k": 10}, map[string]uint64{}, map[string]uint64{"k": 10}, 0), 1)
	// Active counts get slack for series that just went idle.
	require.Empty(t, countDifferences(map[string]uint64{"k": 1000}, map[string]uint64{"k": 991}, map[string]uint64{"k": 1000}, 0.01))
	require.Len(t, countDifferences(map[string]uint64{"k": 1000}, map[string]uint64{"k": 980}, map[string]uint64{"k": 1000}, 0.01), 1)
}

func TestTargetsAreNamedAddresses(t *testing.T) {
	name, addr, err := parseTarget("rust=ingester-kafka-rust-5.ingester-kafka-rust:9095")
	require.NoError(t, err)
	require.Equal(t, "rust", name)
	require.Equal(t, "ingester-kafka-rust-5.ingester-kafka-rust:9095", addr)
	_, _, err = parseTarget("no-name")
	require.Error(t, err)
}
