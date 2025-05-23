// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus-community/parquet-common/blob/382b6ec8ae40fb5dcdcabd8019f69a4be1cd8869/schema/encoder_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package schema

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecode(t *testing.T) {
	ts := promqltest.LoadedStorage(t, `
load 1m
	float_only{env="prod"} 5 2+3x20
    float_histogram_conversion{env="prod"} 5 2+3x2 _ stale {{schema:1 sum:3 count:22 buckets:[5 10 7]}}
	http_requests_histogram{job="api-server", instance="3", group="canary"} {{schema:2 count:4 sum:10 buckets:[1 0 0 0 1 0 0 1 1]}}
    histogram_with_reset_bucket{le="1"} 1  3  9
    histogram_with_reset_bucket{le="2"} 3  3  9
    histogram_with_reset_bucket{le="4"} 8  5 12
    histogram_with_reset_bucket{le="8"} 10  6 18
    histogram_with_reset_sum{}          36 16 61

load_with_nhcb 1m
    histogram_over_time_bucket{le="0"} 0 1 3 9
    histogram_over_time_bucket{le="1"} 2 3 3 9
    histogram_over_time_bucket{le="2"} 3 8 5 10
    histogram_over_time_bucket{le="4"} 3 10 6 18
`)
	require.NotNil(t, ts)

	// Manually append some histogram as the promqltest only generates float histograms
	app := ts.Head().Appender(context.Background())
	generateHistogram(t, app)
	require.NoError(t, app.Commit())

	mint, maxt := ts.Head().MinTime(), ts.Head().MaxTime()
	sb := NewBuilder(mint, maxt, (time.Minute * 60).Milliseconds())
	s, err := sb.Build()
	enc := NewPrometheusParquetChunksEncoder(s)
	dec := NewPrometheusParquetChunksDecoder(chunkenc.NewPool())

	require.NoError(t, err)

	indexr, err := ts.Head().Index()
	require.NoError(t, err)
	cr, err := ts.Head().Chunks()
	require.NoError(t, err)
	n, v := index.AllPostingsKey()
	p, err := indexr.Postings(context.Background(), n, v)
	require.NoError(t, err)
	chks := []chunks.Meta{}
	builder := labels.ScratchBuilder{}

	for p.Next() {
		require.NoError(t, indexr.Series(p.At(), &builder, &chks))
		totalSamples := 0
		decodedSamples := 0
		for i, chk := range chks {
			c, _, err := cr.ChunkOrIterable(chk)
			require.NoError(t, err)
			chks[i].Chunk = c
			totalSamples += c.NumSamples()
		}
		decodedChunksByTime, err := enc.Encode(storage.NewListChunkSeriesIterator(chks...))
		require.NoError(t, err)
		for _, chunksByTime := range decodedChunksByTime {
			decodedChunkMeta, err := dec.Decode(chunksByTime, mint, maxt)
			require.NoError(t, err)
			require.Len(t, decodedChunkMeta, len(chks))
			for _, decodedChunk := range decodedChunkMeta {
				decodedSamples += decodedChunk.Chunk.NumSamples()
			}
		}
		require.Equal(t, totalSamples, decodedSamples)
	}
}

func generateHistogram(t *testing.T, app storage.Appender) {
	for i := 0; i < 20; i++ {
		_, err := app.AppendHistogram(0, labels.FromStrings(labels.MetricName, "histogram"), int64(i), tsdbutil.GenerateTestCustomBucketsHistogram(int64(i%10)), nil)
		require.NoError(t, err)
	}
}
