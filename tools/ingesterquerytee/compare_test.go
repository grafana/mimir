// SPDX-License-Identifier: AGPL-3.0-only

package ingesterquerytee

import (
	"math"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func wire(t *testing.T, msg proto.Message) frame {
	t.Helper()
	b, err := proto.Marshal(msg)
	require.NoError(t, err)
	return b
}

func floatChunk(t *testing.T, times []int64, values []float64) client.Chunk {
	t.Helper()
	c := chunkenc.NewXORChunk()
	a, err := c.Appender()
	require.NoError(t, err)
	for i, ts := range times {
		a.Append(0, ts, values[i])
	}
	return client.Chunk{Encoding: 4, Data: c.Bytes(), StartTimestampMs: times[0], EndTimestampMs: times[len(times)-1]}
}

func streamFrames(t *testing.T, chunks ...client.Chunk) []frame {
	t.Helper()
	return []frame{
		wire(t, &client.QueryStreamResponse{StreamingSeries: []client.QueryStreamSeries{{Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "test"}}, ChunkCount: int64(len(chunks))}}, IsEndOfSeriesStream: true}),
		wire(t, &client.QueryStreamResponse{StreamingSeriesChunks: []client.QueryStreamSeriesChunks{{SeriesIndex: 0, Chunks: chunks}}}),
	}
}

func testConfig() Config {
	return Config{PrimaryAddress: "primary", ShadowAddress: "shadow", ShadowTimeout: time.Second, SampleRate: 1, MaxConcurrent: 2, MaxResponseBytes: 1 << 20, MaxSamples: 1000}
}

func TestQueryComparison(t *testing.T) {
	cfg := testConfig()
	req := wire(t, &client.QueryRequest{StartTimestampMs: 100, EndTimestampMs: 300})
	primary := streamFrames(t, floatChunk(t, []int64{100, 200, 300}, []float64{1, 2, 3}))
	t.Run("different chunk boundaries", func(t *testing.T) {
		shadow := streamFrames(t, floatChunk(t, []int64{100}, []float64{1}), floatChunk(t, []int64{200, 300}, []float64{2, 3}))
		require.NoError(t, compareResponses("QueryStream", req, primary, shadow, cfg, time.Now()))
	})
	t.Run("overlapping chunks", func(t *testing.T) {
		shadow := streamFrames(t, floatChunk(t, []int64{100, 200}, []float64{1, 2}), floatChunk(t, []int64{200, 300}, []float64{2, 3}))
		require.NoError(t, compareResponses("QueryStream", req, primary, shadow, cfg, time.Now()))
	})
	t.Run("conflicting duplicate", func(t *testing.T) {
		shadow := streamFrames(t, floatChunk(t, []int64{100, 200}, []float64{1, 2}), floatChunk(t, []int64{200, 300}, []float64{9, 3}))
		require.EqualError(t, compareResponses("QueryStream", req, primary, shadow, cfg, time.Now()), "conflicting duplicate sample")
	})
	t.Run("changed value", func(t *testing.T) {
		shadow := streamFrames(t, floatChunk(t, []int64{100, 200, 300}, []float64{1, 9, 3}))
		require.ErrorIs(t, compareResponses("QueryStream", req, primary, shadow, cfg, time.Now()), errMismatch)
	})
	t.Run("missing sample", func(t *testing.T) {
		shadow := streamFrames(t, floatChunk(t, []int64{100, 300}, []float64{1, 3}))
		require.ErrorIs(t, compareResponses("QueryStream", req, primary, shadow, cfg, time.Now()), errMismatch)
	})
	t.Run("recent and out of range samples", func(t *testing.T) {
		shadow := streamFrames(t, floatChunk(t, []int64{50, 100, 200, 300, 400}, []float64{9, 1, 2, 9, 9}))
		cfg := cfg
		cfg.SkipRecentSamples = 100 * time.Millisecond
		require.NoError(t, compareResponses("QueryStream", req, primary, shadow, cfg, time.UnixMilli(350)))
	})
	t.Run("decoded sample limit", func(t *testing.T) {
		cfg := cfg
		cfg.MaxSamples = 2
		require.ErrorIs(t, compareResponses("QueryStream", req, primary, primary, cfg, time.Now()), errResponseLimit)
	})
	t.Run("truncated stream", func(t *testing.T) {
		require.EqualError(t, compareResponses("QueryStream", req, primary, primary[:1], cfg, time.Now()), "incomplete query stream")
	})
	t.Run("corrupt chunk", func(t *testing.T) {
		bad := streamFrames(t, client.Chunk{Encoding: 4, Data: []byte{1}})
		require.EqualError(t, compareResponses("QueryStream", req, primary, bad, cfg, time.Now()), "invalid query chunk")
	})
	t.Run("invalid series index", func(t *testing.T) {
		bad := []frame{primary[0], wire(t, &client.QueryStreamResponse{StreamingSeriesChunks: []client.QueryStreamSeriesChunks{{SeriesIndex: 5}}})}
		require.EqualError(t, compareResponses("QueryStream", req, primary, bad, cfg, time.Now()), "invalid series index or missing end marker")
	})
}

func TestHistogramComparison(t *testing.T) {
	makeChunk := func(floating bool, count uint64, hint histogram.CounterResetHint, timestamps ...int64) client.Chunk {
		var c chunkenc.Chunk
		var enc int32
		if floating {
			c = chunkenc.NewFloatHistogramChunk()
			enc = 6
		} else {
			c = chunkenc.NewHistogramChunk()
			enc = 5
		}
		a, err := c.Appender()
		require.NoError(t, err)
		h := &histogram.Histogram{Schema: 0, Count: count, Sum: float64(count), ZeroThreshold: 0.001, ZeroCount: count, CounterResetHint: hint}
		if len(timestamps) == 0 {
			timestamps = []int64{100}
		}
		for _, ts := range timestamps {
			var next chunkenc.Chunk
			if floating {
				next, _, a, err = a.AppendFloatHistogram(nil, 0, ts, h.ToFloat(nil), false)
			} else {
				next, _, a, err = a.AppendHistogram(nil, 0, ts, h, false)
			}
			require.NoError(t, err)
			require.Nil(t, next)
		}
		return client.Chunk{Encoding: enc, Data: c.Bytes(), StartTimestampMs: timestamps[0], EndTimestampMs: timestamps[len(timestamps)-1]}
	}
	req := wire(t, &client.QueryRequest{StartTimestampMs: 100, EndTimestampMs: 100})
	a := streamFrames(t, makeChunk(false, 2, histogram.GaugeType))
	b := streamFrames(t, makeChunk(true, 2, histogram.GaugeType))
	require.NoError(t, compareResponses("QueryStream", req, a, b, testConfig(), time.Now()))
	b = streamFrames(t, makeChunk(true, 3, histogram.GaugeType))
	require.ErrorIs(t, compareResponses("QueryStream", req, a, b, testConfig(), time.Now()), errMismatch)
	b = streamFrames(t, makeChunk(true, 2, histogram.UnknownCounterReset))
	require.ErrorIs(t, compareResponses("QueryStream", req, a, b, testConfig(), time.Now()), errMismatch)
	a = streamFrames(t, makeChunk(false, 2, histogram.CounterReset))
	require.NoError(t, compareResponses("QueryStream", req, a, b, testConfig(), time.Now()))
	b = streamFrames(t, makeChunk(true, 2, histogram.NotCounterReset))
	require.NoError(t, compareResponses("QueryStream", req, a, b, testConfig(), time.Now()))
	req = wire(t, &client.QueryRequest{StartTimestampMs: 100, EndTimestampMs: 200})
	a = streamFrames(t, makeChunk(false, 2, histogram.UnknownCounterReset, 100, 200))
	b = streamFrames(t, makeChunk(true, 2, histogram.UnknownCounterReset, 100), makeChunk(true, 2, histogram.UnknownCounterReset, 200))
	require.NoError(t, compareResponses("QueryStream", req, a, b, testConfig(), time.Now()))
}

func TestSpecialFloatComparison(t *testing.T) {
	req := wire(t, &client.QueryRequest{StartTimestampMs: 100, EndTimestampMs: 300})
	a := streamFrames(t, floatChunk(t, []int64{100, 200, 300}, []float64{math.NaN(), math.Inf(1), math.Float64frombits(0x7ff0000000000002)}))
	require.NoError(t, compareResponses("QueryStream", req, a, a, testConfig(), time.Now()))
	b := streamFrames(t, floatChunk(t, []int64{100, 200, 300}, []float64{math.NaN(), math.Inf(1), math.NaN()}))
	require.ErrorIs(t, compareResponses("QueryStream", req, a, b, testConfig(), time.Now()), errMismatch)
}

func TestReadComparisonOrderAndBatches(t *testing.T) {
	tests := []struct {
		method string
		a, b   []proto.Message
	}{
		{"LabelNames", []proto.Message{&client.LabelNamesResponse{LabelNames: []string{"b", "a"}}}, []proto.Message{&client.LabelNamesResponse{LabelNames: []string{"a", "b"}}}},
		{"LabelValues", []proto.Message{&client.LabelValuesResponse{LabelValues: []string{"b", "a"}}}, []proto.Message{&client.LabelValuesResponse{LabelValues: []string{"a", "b"}}}},
		{"MetricsMetadata", []proto.Message{&client.MetricsMetadataResponse{Metadata: []*mimirpb.MetricMetadata{{MetricFamilyName: "a"}, {MetricFamilyName: "b"}}}}, []proto.Message{&client.MetricsMetadataResponse{Metadata: []*mimirpb.MetricMetadata{{MetricFamilyName: "b"}, {MetricFamilyName: "a"}}}}},
		{"LabelNamesAndValues", []proto.Message{&client.LabelNamesAndValuesResponse{Items: []*client.LabelValues{{LabelName: "a", Values: []string{"1", "2"}}}}}, []proto.Message{&client.LabelNamesAndValuesResponse{Items: []*client.LabelValues{{LabelName: "a", Values: []string{"2"}}}}, &client.LabelNamesAndValuesResponse{Items: []*client.LabelValues{{LabelName: "a", Values: []string{"1"}}}}}},
		{"LabelValuesCardinality", []proto.Message{&client.LabelValuesCardinalityResponse{Items: []*client.LabelValueSeriesCount{{LabelName: "a", LabelValueSeries: map[string]uint64{"1": 2, "2": 3}}}}}, []proto.Message{&client.LabelValuesCardinalityResponse{Items: []*client.LabelValueSeriesCount{{LabelName: "a", LabelValueSeries: map[string]uint64{"2": 3}}}}, &client.LabelValuesCardinalityResponse{Items: []*client.LabelValueSeriesCount{{LabelName: "a", LabelValueSeries: map[string]uint64{"1": 2}}}}}},
		{"SearchLabelNames", []proto.Message{&client.SearchResultBatch{Results: []client.SearchResultBatch_Result{{Value: "a", Score: 1}, {Value: "b", Score: 2}}}}, []proto.Message{&client.SearchResultBatch{Results: []client.SearchResultBatch_Result{{Value: "a", Score: 1}}}, &client.SearchResultBatch{Results: []client.SearchResultBatch_Result{{Value: "b", Score: 2}}}}},
	}
	for _, test := range tests {
		t.Run(test.method, func(t *testing.T) {
			var a, b []frame
			for _, msg := range test.a {
				a = append(a, wire(t, msg))
			}
			for _, msg := range test.b {
				b = append(b, wire(t, msg))
			}
			require.NoError(t, compareResponses(test.method, nil, a, b, testConfig(), time.Now()))
			empty := proto.Clone(test.a[0])
			empty.Reset()
			require.ErrorIs(t, compareResponses(test.method, nil, a, []frame{wire(t, empty)}, testConfig(), time.Now()), errMismatch)
		})
	}
}

func TestCaptureLimit(t *testing.T) {
	var r response
	r.add(frame{1, 2}, 130)
	r.add(frame{}, 130)
	require.False(t, r.limited)
	r.add(frame{}, 130)
	require.True(t, r.limited)
	require.Nil(t, r.frames)
}

func TestQuerySeriesAndLabelOrder(t *testing.T) {
	labelsA := []mimirpb.LabelAdapter{{Name: "__name__", Value: "a"}, {Name: "job", Value: "test"}}
	labelsB := []mimirpb.LabelAdapter{{Name: "__name__", Value: "b"}}
	chunkA := floatChunk(t, []int64{100}, []float64{1})
	chunkB := floatChunk(t, []int64{100}, []float64{2})
	a := []frame{
		wire(t, &client.QueryStreamResponse{StreamingSeries: []client.QueryStreamSeries{{Labels: labelsA, ChunkCount: 1}, {Labels: labelsB, ChunkCount: 1}}, IsEndOfSeriesStream: true}),
		wire(t, &client.QueryStreamResponse{StreamingSeriesChunks: []client.QueryStreamSeriesChunks{{SeriesIndex: 0, Chunks: []client.Chunk{chunkA}}, {SeriesIndex: 1, Chunks: []client.Chunk{chunkB}}}}),
	}
	b := []frame{
		wire(t, &client.QueryStreamResponse{StreamingSeries: []client.QueryStreamSeries{{Labels: labelsB, ChunkCount: 1}}}),
		wire(t, &client.QueryStreamResponse{StreamingSeries: []client.QueryStreamSeries{{Labels: []mimirpb.LabelAdapter{labelsA[1], labelsA[0]}, ChunkCount: 1}}, IsEndOfSeriesStream: true}),
		wire(t, &client.QueryStreamResponse{StreamingSeriesChunks: []client.QueryStreamSeriesChunks{{SeriesIndex: 1, Chunks: []client.Chunk{chunkA}}}}),
		wire(t, &client.QueryStreamResponse{StreamingSeriesChunks: []client.QueryStreamSeriesChunks{{SeriesIndex: 0, Chunks: []client.Chunk{chunkB}}}}),
	}
	req := wire(t, &client.QueryRequest{StartTimestampMs: 100, EndTimestampMs: 100})
	require.NoError(t, compareResponses("QueryStream", req, a, b, testConfig(), time.Now()))
	require.ErrorIs(t, compareResponses("QueryStream", req, a, streamFrames(t, chunkA), testConfig(), time.Now()), errMismatch)
}

func TestRemainingReadComparisons(t *testing.T) {
	metric := &mimirpb.Metric{Labels: []mimirpb.LabelAdapter{{Name: "a", Value: "1"}}}
	tests := []struct {
		method   string
		response proto.Message
	}{
		{"MetricsForLabelMatchers", &client.MetricsForLabelMatchersResponse{Metric: []*mimirpb.Metric{metric}}},
		{"ActiveSeries", &client.ActiveSeriesResponse{Metric: []*mimirpb.Metric{metric}, BucketCount: []uint64{3}}},
		{"QueryExemplars", &client.ExemplarQueryResponse{Timeseries: []mimirpb.TimeSeries{{Labels: metric.Labels, Exemplars: []mimirpb.Exemplar{{Labels: metric.Labels, TimestampMs: 100, Value: math.Inf(1)}}}}}},
		{"SearchLabelValues", &client.SearchResultBatch{Results: []client.SearchResultBatch_Result{{Value: "a", Score: 1}}, Warnings: []string{"warning"}}},
	}
	for _, test := range tests {
		t.Run(test.method, func(t *testing.T) {
			a := []frame{wire(t, test.response)}
			require.NoError(t, compareResponses(test.method, nil, a, a, testConfig(), time.Now()))
			empty := proto.Clone(test.response)
			empty.Reset()
			require.ErrorIs(t, compareResponses(test.method, nil, a, []frame{wire(t, empty)}, testConfig(), time.Now()), errMismatch)
		})
	}
}

func TestConfigValidation(t *testing.T) {
	require.NoError(t, testConfig().Validate())
	for _, rate := range []float64{math.NaN(), math.Inf(1), -1, 2} {
		cfg := testConfig()
		cfg.SampleRate = rate
		require.EqualError(t, cfg.Validate(), "sample-rate must be between 0 and 1")
	}
	cfg := testConfig()
	cfg.ShadowAddress = cfg.PrimaryAddress
	require.EqualError(t, cfg.Validate(), "primary-address and shadow-address must be nonempty and different")
	cfg = testConfig()
	cfg.ShadowTimeout = 0
	require.EqualError(t, cfg.Validate(), "shadow-timeout and comparison limits must be positive")
	cfg = testConfig()
	cfg.SkipRecentSamples = -time.Second
	require.EqualError(t, cfg.Validate(), "skip-recent-samples must be nonnegative")
}
