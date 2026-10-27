// SPDX-License-Identifier: AGPL-3.0-only

package chunks

import (
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/value"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func intHistogram(timestamp, scale int64) mimirpb.Histogram {
	return mimirpb.Histogram{
		Timestamp:      timestamp,
		Count:          &mimirpb.Histogram_CountInt{CountInt: uint64(12 * scale)},
		ZeroCount:      &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: uint64(scale)},
		Sum:            1.5 * float64(scale),
		Schema:         3,
		ZeroThreshold:  1e-128,
		PositiveSpans:  []mimirpb.BucketSpan{{Offset: -2, Length: 2}, {Offset: 5, Length: 1}},
		NegativeSpans:  []mimirpb.BucketSpan{{Offset: 1, Length: 1}},
		PositiveDeltas: []int64{scale, 3 * scale, -scale},
		NegativeDeltas: []int64{2 * scale},
	}
}

func TestHistogramsDecodeWhatTheyEncode(t *testing.T) {
	var ints []mimirpb.Histogram
	for timestamp := int64(1); timestamp < 40; timestamp++ {
		ints = append(ints, intHistogram(timestamp*15_000+timestamp*timestamp, timestamp))
	}
	ints[0].ResetHint = mimirpb.Histogram_YES
	encoded, err := EncodeHistogramSequence(ints)
	require.NoError(t, err)
	require.Equal(t, EncodingHistogram, encoded.Encoding)
	// Like the Prometheus iterator: the first hint is unknown, the others not a reset.
	expected := append([]mimirpb.Histogram(nil), ints...)
	expected[0].ResetHint = mimirpb.Histogram_UNKNOWN
	for index := range expected[1:] {
		expected[index+1].ResetHint = mimirpb.Histogram_NO
	}
	decoded, err := DecodeHistograms(encoded.Encoding, encoded.Data)
	require.NoError(t, err)
	require.Equal(t, expected, decoded)

	var floats []mimirpb.Histogram
	for step := int64(0); step < 30; step++ {
		floats = append(floats, mimirpb.Histogram{
			Timestamp: 1_000 + step*60_000,
			Count:     &mimirpb.Histogram_CountFloat{CountFloat: 3.5 + float64(step)},
			// Custom bucket histograms have no zero bucket, which chunkenc doesn't store.
			ZeroCount:      &mimirpb.Histogram_ZeroCountFloat{},
			Sum:            float64(step) * 1.1,
			Schema:         -53,
			CustomValues:   []float64{0.5, 1.0, 2.5, 1e30},
			PositiveSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 3}},
			PositiveCounts: []float64{1.0, float64(step) / 3.0, 2.0},
			ResetHint:      mimirpb.Histogram_GAUGE,
		})
	}
	encoded, err = EncodeHistogramSequence(floats)
	require.NoError(t, err)
	decoded, err = DecodeHistograms(encoded.Encoding, encoded.Data)
	require.NoError(t, err)
	// Gauge chunks mark every sample as a gauge.
	require.Equal(t, floats, decoded)
	_, err = DecodeHistograms(EncodingHistogram, []byte{0, 1})
	require.Error(t, err)
}

func TestEqualHistogramValuesIgnoresResetHintsAndEmptySpans(t *testing.T) {
	base := mimirpb.Histogram{
		Timestamp:      1,
		Count:          &mimirpb.Histogram_CountInt{CountInt: 3},
		Sum:            2.0,
		PositiveSpans:  []mimirpb.BucketSpan{{Offset: 1, Length: 0}, {Offset: 2, Length: 2}},
		PositiveDeltas: []int64{1, 1},
	}
	other := base
	other.ResetHint = mimirpb.Histogram_NO
	other.ZeroCount = &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: 0}
	other.PositiveSpans = []mimirpb.BucketSpan{{Offset: 3, Length: 2}}
	require.True(t, EqualHistogramValues(&base, &other))
	other.Sum = 2.5
	require.False(t, EqualHistogramValues(&base, &other))
}

// The appender follows Prometheus's rules: a new bucket recodes the chunk, a counter reset or a
// type change starts a new one, which a chunk rebuilt from its samples continues identically.
func TestHistogramAppenderRecodesAndCutsLikePrometheus(t *testing.T) {
	sample := func(timestamp int64, buckets ...int64) *mimirpb.Histogram {
		var total uint64
		var current int64
		for _, delta := range buckets {
			current += delta
			total += uint64(current)
		}
		return &mimirpb.Histogram{
			Timestamp:      timestamp,
			Count:          &mimirpb.Histogram_CountInt{CountInt: total},
			ZeroCount:      &mimirpb.Histogram_ZeroCountInt{},
			Sum:            float64(total),
			PositiveSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: uint32(len(buckets))}},
			PositiveDeltas: buckets,
		}
	}
	appender := NewHistogramAppender(sample(1_000, 1), nil)
	require.Equal(t, HeaderUnknown, appender.Header())
	result, next, err := appender.Append(sample(2_000, 2))
	require.NoError(t, err)
	require.Equal(t, AppendedInChunk, result)
	require.Nil(t, next)
	// A second bucket widens the chunk's layout.
	result, _, err = appender.Append(sample(3_000, 2, 1))
	require.NoError(t, err)
	require.Equal(t, AppendedRecoded, result)
	require.Equal(t, 3, appender.Len())
	require.Equal(t, int64(1_000), appender.FirstTimestamp())
	// A decrease is a counter reset: the new chunk says so.
	result, next, err = appender.Append(sample(4_000, 1, 1))
	require.NoError(t, err)
	require.Equal(t, AppendedNewChunk, result)
	require.Equal(t, 3, appender.Len(), "the full chunk keeps its samples")
	require.Equal(t, int64(3_000), appender.Last().Timestamp)
	require.Equal(t, HeaderCounterReset, next.Header())
	require.Equal(t, int64(4_000), next.FirstTimestamp())
	// A float histogram needs its own chunk.
	float := &mimirpb.Histogram{Timestamp: 5_000, Count: &mimirpb.Histogram_CountFloat{CountFloat: 1}, ZeroCount: &mimirpb.Histogram_ZeroCountFloat{ZeroCountFloat: 1}}
	result, floatChunk, err := next.Append(float)
	require.NoError(t, err)
	require.Equal(t, AppendedNewChunk, result)
	require.Equal(t, EncodingFloatHistogram, floatChunk.Encoded().Encoding)

	samples, err := appender.Samples()
	require.NoError(t, err)
	rebuilt, err := NewHistogramAppenderFromSamples(samples)
	require.NoError(t, err)
	require.Equal(t, appender.Encoded(), rebuilt.Encoded())
	// And both continue the same way.
	_, _, err = appender.Append(sample(3_500, 3, 1))
	require.NoError(t, err)
	_, _, err = rebuilt.Append(sample(3_500, 3, 1))
	require.NoError(t, err)
	require.Equal(t, appender.Encoded(), rebuilt.Encoded())
}

func TestHistogramAppenderKeepsStaleMarkers(t *testing.T) {
	appender := NewHistogramAppender(&mimirpb.Histogram{
		Timestamp: 1_000,
		Count:     &mimirpb.Histogram_CountInt{CountInt: 1},
		ZeroCount: &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: 1},
		Sum:       1,
	}, nil)
	stale := &mimirpb.Histogram{Timestamp: 2_000, Count: &mimirpb.Histogram_CountInt{}, Sum: math.Float64frombits(value.StaleNaN)}
	result, _, err := appender.Append(stale)
	require.NoError(t, err)
	require.Equal(t, AppendedInChunk, result)
	decoded, err := DecodeHistograms(appender.Encoded().Encoding, appender.Encoded().Data)
	require.NoError(t, err)
	require.Len(t, decoded, 2)
	require.True(t, value.IsStaleNaN(decoded[1].Sum))
}

func TestValidHistograms(t *testing.T) {
	valid := intHistogram(1, 1)
	valid.Count = &mimirpb.Histogram_CountInt{CountInt: 11}
	require.True(t, IsValidHistogram(&valid))
	valid.Count = &mimirpb.Histogram_CountInt{CountInt: 12}
	require.False(t, IsValidHistogram(&valid), "the buckets hold 11 observations")
}
