// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestWrappedQueryStreamResponseUnmarshal(t *testing.T) {
	var wrappedResp WrappedQueryStreamResponse

	const (
		numSeries          = 10
		numChunksPerSeries = 100
	)
	resp := &QueryStreamResponse{
		Chunkseries: []TimeSeriesChunk{
			{
				Labels: []mimirpb.LabelAdapter{
					{Name: "foo", Value: "bar"},
				},
			},
		},
		Timeseries: []mimirpb.TimeSeries{
			{
				Labels: []mimirpb.LabelAdapter{
					{Name: "foo", Value: "bar"},
				},
			},
		},
	}
	for i := 0; i < numSeries; i++ {
		ss := TimeSeriesChunk{
			Labels: []mimirpb.LabelAdapter{
				{Name: "foo", Value: fmt.Sprintf("bar-%d", i)},
			},
		}
		for j := 0; j < numChunksPerSeries; j++ {
			ss.Chunks = append(ss.Chunks, Chunk{
				StartTimestampMs: int64(j) * 100,
				EndTimestampMs:   int64(j)*100 + 10,
				Data:             []byte(fmt.Sprintf("chunk-%d", j)),
			})
		}
		resp.Chunkseries = append(resp.Chunkseries, ss)
	}

	b, err := proto.Marshal(resp)
	require.NoError(t, err)
	require.True(t, len(b) > 0)

	err = proto.Unmarshal(b, &wrappedResp)
	require.NoError(t, err)

	require.Equal(t, resp.String(), wrappedResp.QueryStreamResponse.String())
}

func BenchmarkQueryStreamResponseUnmarshal(b *testing.B) {
	const benchWrappedResponse = true

	var getTargetFn func() proto.Message
	var doneFn func(proto.Message)

	if benchWrappedResponse {
		getTargetFn = func() proto.Message { return &WrappedQueryStreamResponse{&QueryStreamResponse{}} }
		doneFn = func(m proto.Message) {
			ReuseQueryStreamResponse(m.(*WrappedQueryStreamResponse).QueryStreamResponse)
		}
	} else {
		getTargetFn = func() proto.Message { return &QueryStreamResponse{} }
		doneFn = func(_ proto.Message) {}
	}

	bcs := []struct {
		name                string
		numSeries           int
		numChunksPerSeries  int
		numSamplesPerSeries int
	}{
		{name: "10 series, 10 chunks, 100 samples", numSeries: 10, numChunksPerSeries: 10, numSamplesPerSeries: 100},
		{name: "10 series, 100 chunks, 100 samples", numSeries: 10, numChunksPerSeries: 100, numSamplesPerSeries: 100},
		{name: "100 series, 1k chunks, 1k samples", numSeries: 100, numChunksPerSeries: 1_000, numSamplesPerSeries: 1_000},
		{name: "100 series, 10k chunks, 1k samples", numSeries: 100, numChunksPerSeries: 10_000, numSamplesPerSeries: 1_000},
		{name: "1k series, 10k chunks, 10k samples", numSeries: 1_000, numChunksPerSeries: 10_000, numSamplesPerSeries: 10_000},
	}
	for _, bc := range bcs {
		b.Run(bc.name, func(b *testing.B) {
			// Create a response with the given number of series, chunks and samples.
			sentResp := &QueryStreamResponse{
				Chunkseries: make([]TimeSeriesChunk, bc.numSeries),
				Timeseries:  make([]mimirpb.TimeSeries, bc.numSeries),
			}
			for i := 0; i < bc.numSeries; i++ {
				for j := 0; j < 100; j++ {
					sentResp.Chunkseries[i].Labels = append(sentResp.Chunkseries[i].Labels, mimirpb.LabelAdapter{
						Name: "foo", Value: strconv.Itoa(i),
					})
				}
				for j := 0; j < bc.numChunksPerSeries; j++ {
					sentResp.Chunkseries[i].Chunks = append(sentResp.Chunkseries[i].Chunks, Chunk{
						Data: []byte(fmt.Sprintf("chunk-%d", j)),
					})
				}
				for j := 0; j < bc.numSamplesPerSeries; j++ {
					sentResp.Timeseries[i].Samples = append(sentResp.Timeseries[i].Samples, mimirpb.Sample{
						TimestampMs: int64(j),
						Value:       float64(j),
					})
				}
			}

			// Marshal the response.
			bt, err := proto.Marshal(sentResp)
			require.NoError(b, err)

			// Warm up.
			b.ReportAllocs()
			b.ResetTimer()

			// Run the benchmark.
			for i := 0; i < b.N; i++ {
				resp := getTargetFn()

				err := proto.Unmarshal(bt, resp)
				require.NoError(b, err)
				require.NotNil(b, resp)

				doneFn(resp)
			}
		})
	}
}
