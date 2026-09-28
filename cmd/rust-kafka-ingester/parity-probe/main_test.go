// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	promvalue "github.com/prometheus/prometheus/model/value"
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
	rust := samples(wireChunk(t, chunk.PrometheusHistogramChunk, histogramChunk(t, narrow, later)), request(), map[string]int{})
	goSide := samples(wireChunk(t, chunk.PrometheusHistogramChunk, histogramChunk(t, widened, later)), request(), map[string]int{})
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
	fromFloat := samples(wireChunk(t, chunk.PrometheusXorChunk, float), request(), counts)
	fromHistogram := samples(wireChunk(t, chunk.PrometheusHistogramChunk, histogramChunk(t, stale)), request(), counts)
	require.Equal(t, []string{"1000:stale"}, fromFloat)
	require.Equal(t, fromFloat, fromHistogram)
	require.Equal(t, 2, counts["stale"])
}

func TestCompareReportsMissingAndDifferingSeries(t *testing.T) {
	rust := map[string][]string{"a": {"1:f:1"}, "b": {"1:f:1"}}
	golang := map[string][]string{"a": {"1:f:1"}, "b": {"1:f:2"}, "c": {"1:f:1"}}
	require.Equal(t, 2, compare(rust, golang))
	require.Equal(t, 0, compare(rust, map[string][]string{"a": {"1:f:1"}, "b": {"1:f:1"}}))
}
