// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"strconv"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/chunk"
)

// A chunk's histograms read as they were appended: the iterator shares its spans with the histograms it returns, so
// compacting one in place shifted the buckets of the ones after it, by where each engine's chunks started.
func TestChunkSamplesReadEachHistogramOfAChunk(t *testing.T) {
	c := chunkenc.NewHistogramChunk()
	app, err := c.Appender()
	require.NoError(t, err)
	var want []string
	for step := range 4 {
		// The first buckets are empty, which compacting a histogram drops.
		buckets := []int64{0, 0, int64(step + 1), 1, -1}
		h := &histogram.Histogram{Count: uint64(4*step + 5), Sum: float64(step), ZeroThreshold: 1e-128,
			PositiveSpans: []histogram.Span{{Offset: 0, Length: 5}}, PositiveBuckets: buckets}
		ts := int64(step) * 60_000
		_, _, app, err = app.AppendHistogram(nil, 0, ts, h.Copy(), false)
		require.NoError(t, err)
		expected := h.ToFloat(nil).Compact(0)
		expected.CounterResetHint = histogram.UnknownCounterReset
		want = append(want, strconv.FormatInt(ts, 10)+"\x00h"+expected.String())
	}

	var got []string
	wire := client.Chunk{Encoding: int32(chunk.PrometheusHistogramChunk), Data: c.Bytes()}
	require.NoError(t, chunkSamples(wire, 0, 1<<40, func(item string) error {
		got = append(got, item)
		return nil
	}))
	require.Equal(t, want, got)
}
