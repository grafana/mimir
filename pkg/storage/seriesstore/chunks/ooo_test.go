// SPDX-License-Identifier: AGPL-3.0-only

package chunks

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func floatChunk(samples ...FloatSample) Chunk {
	return Chunk{
		MinTime:  samples[0].T,
		MaxTime:  samples[len(samples)-1].T,
		Encoding: EncodingXOR,
		Data:     EncodeXOR(samples),
	}
}

func floatValues(t *testing.T, chunks []Chunk) []FloatSample {
	var values []FloatSample
	for _, chunk := range chunks {
		decoded, err := DecodeXOR(chunk.Data)
		require.NoError(t, err)
		values = append(values, decoded...)
	}
	return values
}

// Results observed from the Go ingester in the Go/Rust parity test.
func TestTimestampTiesFollowTheChainIterator(t *testing.T) {
	// The in-order chunk sorts first and the out-of-order one waits in the heap, so at 2000 the
	// out-of-order sample wins.
	merged, err := MergeOverlapping([]Candidate{
		{Chunk: floatChunk(FloatSample{1000, 1}, FloatSample{2000, 2}, FloatSample{3000, 3})},
		{Chunk: floatChunk(FloatSample{2000, 20}), OutOfOrder: true},
	})
	require.NoError(t, err)
	require.Equal(t, []FloatSample{{1000, 1}, {2000, 20}, {3000, 3}}, floatValues(t, merged))
	// After the out-of-order chunk emitted 1500 it is the current iterator, so the in-order
	// sample waiting in the heap wins at 2000.
	merged, err = MergeOverlapping([]Candidate{
		{Chunk: floatChunk(FloatSample{1000, 1}, FloatSample{2000, 2}, FloatSample{3000, 3})},
		{Chunk: floatChunk(FloatSample{1500, 15}, FloatSample{2000, 20}), OutOfOrder: true},
	})
	require.NoError(t, err)
	require.Equal(t, []FloatSample{{1000, 1}, {1500, 15}, {2000, 2}, {3000, 3}}, floatValues(t, merged))
}

func TestNonOverlappingChunksAreReturnedUnchanged(t *testing.T) {
	first := floatChunk(FloatSample{1000, 1}, FloatSample{2000, 2})
	second := floatChunk(FloatSample{3000, 3})
	merged, err := MergeOverlapping([]Candidate{
		{Chunk: second, OutOfOrder: true},
		{Chunk: first},
	})
	require.NoError(t, err)
	require.Equal(t, []Chunk{first, second}, merged)
}

// Chunks with one min time sort in-order first, then by position, whatever order they come in: the
// chain iterator's current iterator is the first one, and the heap takes the others in that order.
func TestCandidatesSortLikeHeadChunkReferences(t *testing.T) {
	candidates := []Candidate{
		{Chunk: floatChunk(FloatSample{1000, 30}), OutOfOrder: true, Index: 1},
		{Chunk: floatChunk(FloatSample{1000, 20}), OutOfOrder: true, Index: 0},
		{Chunk: floatChunk(FloatSample{1000, 10}, FloatSample{2000, 11}), Index: 3},
	}
	sorted, err := MergeOverlapping([]Candidate{candidates[2], candidates[1], candidates[0]})
	require.NoError(t, err)
	for _, order := range [][3]int{{0, 1, 2}, {1, 0, 2}, {2, 0, 1}, {0, 2, 1}} {
		merged, err := MergeOverlapping([]Candidate{candidates[order[0]], candidates[order[1]], candidates[order[2]]})
		require.NoError(t, err)
		require.Equal(t, sorted, merged, "order %v", order)
	}
	// Like the first tie above, the out-of-order sample waiting in the heap wins.
	require.Equal(t, []FloatSample{{1000, 20}, {2000, 11}}, floatValues(t, sorted))
}

func TestMergedFloatChunksAreCutLikePopulateChunksFromIterable(t *testing.T) {
	var inOrder, outOfOrder []FloatSample
	for index := range 1000 {
		inOrder = append(inOrder, FloatSample{int64(index) * 1000, math.Float64frombits(uint64(index) * 0x9e37_79b9_7f4a_7c15)})
		if index%10 == 5 {
			outOfOrder = append(outOfOrder, FloatSample{int64(index)*1000 + 500, float64(index)})
		}
	}
	merged, err := MergeOverlapping([]Candidate{
		{Chunk: floatChunk(inOrder...)},
		{Chunk: floatChunk(outOfOrder...), OutOfOrder: true},
	})
	require.NoError(t, err)
	require.Greater(t, len(merged), 1)
	for _, chunk := range merged {
		require.LessOrEqual(t, len(chunk.Data), 1024)
	}
	require.Len(t, floatValues(t, merged), len(inOrder)+len(outOfOrder))
}

func TestOutOfOrderHeadSplitsByValueType(t *testing.T) {
	histogram := func(timestamp int64) *mimirpb.Histogram {
		return &mimirpb.Histogram{
			Timestamp:      timestamp,
			Count:          &mimirpb.Histogram_CountInt{CountInt: 1},
			ZeroCount:      &mimirpb.Histogram_ZeroCountInt{},
			PositiveSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 1}},
			PositiveDeltas: []int64{1},
		}
	}
	chunks, err := EncodeOutOfOrder([]Sample{
		{T: 1000, H: histogram(1000)},
		{T: 1500, H: histogram(1500)},
		{T: 2000, F: 22},
	})
	require.NoError(t, err)
	var shapes [][3]int64
	for _, chunk := range chunks {
		shapes = append(shapes, [3]int64{chunk.MinTime, chunk.MaxTime, int64(chunk.Encoding)})
	}
	require.Equal(t, [][3]int64{{1000, 1500, 5}, {2000, 2000, int64(EncodingXOR)}}, shapes)
	decoded, err := Decode(EncodingHistogram, chunks[0].Data)
	require.NoError(t, err)
	require.Len(t, decoded, 2)
}

func TestMergesOverlappingHistogramChunks(t *testing.T) {
	sample := func(timestamp int64, count uint64) Sample {
		return Sample{T: timestamp, H: &mimirpb.Histogram{
			Timestamp:      timestamp,
			Count:          &mimirpb.Histogram_CountInt{CountInt: count},
			ZeroCount:      &mimirpb.Histogram_ZeroCountInt{},
			PositiveSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 1}},
			PositiveDeltas: []int64{int64(count)},
		}}
	}
	inOrder, err := EncodeOutOfOrder([]Sample{sample(1000, 1), sample(3000, 3)})
	require.NoError(t, err)
	late, err := EncodeOutOfOrder([]Sample{sample(2000, 2)})
	require.NoError(t, err)
	merged, err := MergeOverlapping([]Candidate{
		{Chunk: inOrder[0]},
		{Chunk: late[0], OutOfOrder: true},
	})
	require.NoError(t, err)
	require.Len(t, merged, 1)
	decoded, err := Decode(merged[0].Encoding, merged[0].Data)
	require.NoError(t, err)
	var timestamps []int64
	for _, sample := range decoded {
		timestamps = append(timestamps, sample.T)
	}
	require.Equal(t, []int64{1000, 2000, 3000}, timestamps)
}

// Merging a series' overlapping in-order and out-of-order float chunks, as queries of series with
// out-of-order samples do.
func BenchmarkMergeOverlapping(b *testing.B) {
	// A 120-sample in-order chunk scraped every 15 s, and an out-of-order chunk of late samples
	// between them, one on a timestamp the in-order chunk has.
	var inOrder, outOfOrder []FloatSample
	for index := range 120 {
		inOrder = append(inOrder, FloatSample{int64(index) * 15_000, math.Sin(float64(index) * 1.7)})
	}
	for index := range 30 {
		outOfOrder = append(outOfOrder, FloatSample{int64(index)*60_000 + 7_500*int64(index%2), float64(index)})
	}
	first, second := floatChunk(inOrder...), floatChunk(outOfOrder...)
	candidates := make([]Candidate, 2)
	for b.Loop() {
		candidates[0] = Candidate{Chunk: first, Index: 1}
		candidates[1] = Candidate{Chunk: second, OutOfOrder: true, Index: 1}
		if _, err := MergeOverlapping(candidates); err != nil {
			b.Fatal(err)
		}
	}
}
