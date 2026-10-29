// SPDX-License-Identifier: AGPL-3.0-only

package cost

import (
	"context"
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/test"
)

func TestSamplingMeasurements(t *testing.T) {
	floatChunk := chunkenc.NewXORChunk()
	a, err := floatChunk.Appender()
	require.NoError(t, err)
	for ts := int64(0); ts <= 60000; ts += 15000 {
		a.Append(0, ts, 1)
	}
	res := &SamplingResponse{}
	ok, err := res.Observe(t.Context(), chunks.Meta{MinTime: 0, MaxTime: 60000, Chunk: floatChunk}, &SamplingRequest{Start: 0, End: 60000})
	require.NoError(t, err)
	require.True(t, ok)
	stats, warnings, err := res.Statistics()
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.Equal(t, storage.CostSamplingStats{IntervalMilliseconds: 15000, AveragePointCost: 1}, stats)
	require.Equal(t, uint64(1), res.ChunksExamined)
	require.Equal(t, uint64(len(floatChunk.Bytes())), res.ChunkBytes)

	h := test.GenerateTestFloatHistogram(20)
	histChunk := chunkenc.NewFloatHistogramChunk()
	ha, err := histChunk.Appender()
	require.NoError(t, err)
	_, _, _, err = ha.AppendFloatHistogram(nil, 0, 0, h, false)
	require.NoError(t, err)
	res = &SamplingResponse{}
	ok, err = res.Observe(t.Context(), chunks.Meta{Chunk: histChunk}, &SamplingRequest{End: 60000})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, float64((h.Size()+8)/16), res.PointCost)
	fullCost := res.PointCost
	res = &SamplingResponse{}
	ok, err = res.Observe(t.Context(), chunks.Meta{Chunk: histChunk}, &SamplingRequest{End: 60000, SkipHistogramBuckets: true})
	require.NoError(t, err)
	require.True(t, ok)
	require.Positive(t, res.PointCost)
	require.Less(t, res.PointCost, fullCost)
}

func TestSamplingBudgetsAndCancellation(t *testing.T) {
	chunk := &samplingUndecodableChunk{data: make([]byte, MaxChunkBytes+1)}
	res := &SamplingResponse{}
	ok, err := res.Observe(t.Context(), chunks.Meta{Chunk: chunk}, &SamplingRequest{})
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, res.Points)
	require.Zero(t, res.ChunkBytes)
	res.ChunksExamined = MaxChunks
	ok, err = res.Observe(t.Context(), chunks.Meta{Chunk: chunk}, &SamplingRequest{})
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, uint64(MaxChunks), res.ChunksExamined)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = res.Observe(ctx, chunks.Meta{Chunk: chunk}, &SamplingRequest{})
	require.ErrorIs(t, err, context.Canceled)
}

type samplingUndecodableChunk struct {
	chunkenc.Chunk
	data []byte
}

func (c *samplingUndecodableChunk) Bytes() []byte { return c.data }

func TestSamplingProtocol(t *testing.T) {
	req := NewRequest(100, 200, true, []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "job", "a|b")})
	data, err := req.Marshal()
	require.NoError(t, err)
	var decoded SamplingRequest
	require.NoError(t, decoded.Unmarshal(data))
	require.Equal(t, req, &decoded)
	matchers, err := decoded.Validate()
	require.NoError(t, err)
	require.True(t, matchers[0].Matches("a"))
	require.False(t, matchers[0].Matches("c"))
	_, err = (&SamplingRequest{Start: 2, End: 1}).Validate()
	require.EqualError(t, err, "invalid sampling time range")
	_, err = (&SamplingRequest{Matchers: []Matcher{{Type: 99}}}).Validate()
	require.EqualError(t, err, "invalid sampling matcher type 99")
	for _, invalid := range []*SamplingResponse{
		nil, {ChunksExamined: MaxChunks + 1}, {SeriesExamined: MaxSeries + 1}, {ChunkBytes: MaxChunkBytes + 1}, {PointCost: math.NaN()}, {IntervalMilliseconds: math.Inf(1)},
	} {
		require.Error(t, invalid.Validate())
	}
	res := &SamplingResponse{Intervals: 2, IntervalMilliseconds: 30000, Points: 2, PointCost: 4}
	res.Merge(&SamplingResponse{Intervals: 1, IntervalMilliseconds: 30000, Points: 1, PointCost: 2})
	stats, _, err := res.Statistics()
	require.NoError(t, err)
	require.Equal(t, storage.CostSamplingStats{IntervalMilliseconds: 20000, AveragePointCost: 2}, stats)
	res.Merge(&SamplingResponse{Unsupported: true})
	_, _, err = res.Statistics()
	require.ErrorIs(t, err, storage.ErrCostSamplingUnsupported)
}
