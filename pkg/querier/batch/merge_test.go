// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/batch/merge_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package batch

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

func TestMergeIter(t *testing.T) {
	for _, enc := range []chunk.Encoding{chunk.PrometheusXorChunk, chunk.PrometheusHistogramChunk, chunk.PrometheusFloatHistogramChunk} {
		t.Run(enc.String(), func(t *testing.T) {
			chunk1 := mkGenericChunk(t, 0, 100, enc)
			chunk2 := mkGenericChunk(t, model.TimeFromUnix(25), 100, enc)
			chunk3 := mkGenericChunk(t, model.TimeFromUnix(50), 100, enc)
			chunk4 := mkGenericChunk(t, model.TimeFromUnix(75), 100, enc)
			chunk5 := mkGenericChunk(t, model.TimeFromUnix(100), 100, enc)

			iter := NewGenericChunkMergeIterator(nil, labels.EmptyLabels(), []GenericChunk{chunk1, chunk2, chunk3, chunk4, chunk5})
			testIter(t, 200, iter, enc)
			iter = NewGenericChunkMergeIterator(nil, labels.EmptyLabels(), []GenericChunk{chunk1, chunk2, chunk3, chunk4, chunk5})
			testSeek(t, 200, iter, enc)

			// Re-use iterator.
			iter = NewGenericChunkMergeIterator(iter, labels.EmptyLabels(), []GenericChunk{chunk1, chunk2, chunk3, chunk4, chunk5})
			testIter(t, 200, iter, enc)
			iter = NewGenericChunkMergeIterator(iter, labels.EmptyLabels(), []GenericChunk{chunk1, chunk2, chunk3, chunk4, chunk5})
			testSeek(t, 200, iter, enc)
		})
	}
}

func TestMergeHarder(t *testing.T) {
	var (
		numChunks = 24 * 15
		chunks    = make([]GenericChunk, 0, numChunks)
		offset    = 30
		samples   = 100
	)
	for _, enc := range []chunk.Encoding{chunk.PrometheusXorChunk, chunk.PrometheusHistogramChunk, chunk.PrometheusFloatHistogramChunk} {
		t.Run(enc.String(), func(t *testing.T) {
			chunks = chunks[:0]
			from := model.Time(0)
			for i := 0; i < numChunks; i++ {
				chunks = append(chunks, mkGenericChunk(t, from, samples, enc))
				from = from.Add(time.Duration(offset) * time.Second)
			}
			iter := newMergeIterator(nil, chunks)
			testIter(t, offset*numChunks+samples-offset, newIteratorAdapter(nil, iter, labels.EmptyLabels()), enc)

			iter = newMergeIterator(nil, chunks)
			testSeek(t, offset*numChunks+samples-offset, newIteratorAdapter(nil, iter, labels.EmptyLabels()), enc)
		})
	}
}

// TestMergeIteratorSeek tests a bug while calling Seek() on mergeIterator.
func TestMergeIteratorSeek(t *testing.T) {
	// Samples for 3 chunks.
	chunkSamples := [][]int64{
		{10, 20, 30, 40},
		{50, 60, 70, 80, 90, 100},
		{110, 120},
	}

	var genericChunks []GenericChunk
	for _, samples := range chunkSamples {
		ch := chunkenc.NewXORChunk()
		app, err := ch.Appender()
		require.NoError(t, err)
		for _, ts := range samples {
			app.Append(ts, 1)
		}

		genericChunks = append(genericChunks, NewGenericChunk(samples[0], samples[len(samples)-1], 0, func(reuse chunk.Iterator) chunk.Iterator {
			chk, err := chunk.NewForEncoding(chunk.PrometheusXorChunk)
			require.NoError(t, err)
			require.NoError(t, chk.UnmarshalFromBuf(ch.Bytes()))
			return chk.NewIterator(reuse)
		}))
	}

	c3It := NewGenericChunkMergeIterator(nil, labels.EmptyLabels(), genericChunks)

	c3It.Seek(15)
	// These Next() calls are necessary to reproduce the bug.
	c3It.Next()
	c3It.Next()
	c3It.Next()
	c3It.Seek(75)
	// Without the bug fix, this Seek() call skips an additional sample than desired.
	// Instead of stopping at 100, which is the last sample of chunk 2,
	// it would go to 110, which is the first sample of chunk 3.
	// This was happening because in the Seek() method we were discarding the current
	// batch from mergeIterator if the batch's first sample was after the seek time.
	c3It.Seek(95)

	require.Equal(t, int64(100), c3It.AtT())
}

func TestMergeIteratorCounterResets(t *testing.T) {
	h40 := histogram.Histogram{
		Count: 40,
		Sum:   40,
		PositiveSpans: []histogram.Span{
			{Offset: 0, Length: 1},
		},
		PositiveBuckets: []int64{40},
	}
	h30 := histogram.Histogram{
		Count: 30,
		Sum:   30,
		PositiveSpans: []histogram.Span{
			{Offset: 0, Length: 1},
		},
		PositiveBuckets: []int64{30},
	}
	h20 := histogram.Histogram{
		Count: 20,
		Sum:   20,
		PositiveSpans: []histogram.Span{
			{Offset: 0, Length: 1},
		},
		PositiveBuckets: []int64{20},
	}

	type hSample struct {
		t int64
		h *histogram.Histogram
	}

	// Make chunks from samples and simulate that they came
	// from the same source - the prevMaxT is set correctly.
	sampleToChunks := func(samples []hSample) []chunk.Chunk {
		chunks := []chunk.Chunk{}
		pc, err := chunk.NewForEncoding(chunk.PrometheusHistogramChunk)
		require.NoError(t, err)
		mint := samples[0].t
		maxt := samples[0].t
		prevMaxT := int64(0)
		for _, s := range samples {
			overFlow, err := pc.AddHistogram(s.t, s.h)
			require.NoError(t, err)
			if overFlow != nil {
				chunks = append(chunks, chunk.NewChunk(labels.EmptyLabels(), pc, model.Time(mint), model.Time(maxt), prevMaxT))
				prevMaxT = maxt
				mint = s.t
				pc = overFlow
			}
			maxt = s.t
		}
		chunks = append(chunks, chunk.NewChunk(labels.EmptyLabels(), pc, model.Time(mint), model.Time(maxt), prevMaxT))
		return chunks
	}

	type tc struct {
		chunks []chunk.Chunk
		expect []histogram.CounterResetHint
	}

	testCases := map[string]tc{
		"clear reset when streams end differently": {
			chunks: append(sampleToChunks([]hSample{{1, &h40}, {3, &h30}}), sampleToChunks([]hSample{{1, &h40}, {2, &h20}})...),
			expect: []histogram.CounterResetHint{
				histogram.UnknownCounterReset,
				histogram.CounterReset,
				histogram.UnknownCounterReset,
			},
		},
		"clear reset when stream differ in the middle": {
			chunks: append(sampleToChunks([]hSample{{1, &h40}, {3, &h30}}), sampleToChunks([]hSample{{1, &h40}, {2, &h20}, {3, &h30}})...),
			expect: []histogram.CounterResetHint{
				histogram.UnknownCounterReset,
				histogram.CounterReset,
				histogram.UnknownCounterReset,
			},
		},
		"keep reset when streams are the same": {
			chunks: append(sampleToChunks([]hSample{{1, &h40}, {2, &h20}, {3, &h30}}), sampleToChunks([]hSample{{1, &h40}, {2, &h20}, {3, &h30}})...),
			expect: []histogram.CounterResetHint{
				histogram.UnknownCounterReset,
				histogram.CounterReset,
				histogram.NotCounterReset,
			},
		},
	}

	// Set up multiple test cases to test that time distance between chunks
	// does not affect the counter reset hint. Merge works over batch reuse.
	for i := 3; i <= chunk.BatchSize*2; i++ {
		stream1 := []hSample{{1, &h40}, {int64(i), &h30}}
		stream2 := []hSample{}
		expect := []histogram.CounterResetHint{}
		for j := 1; j <= i-2; j++ {
			if j == 1 {
				expect = append(expect, histogram.UnknownCounterReset)
			} else {
				expect = append(expect, histogram.NotCounterReset)
			}
			stream2 = append(stream2, hSample{int64(j), &h40})
		}
		stream2 = append(stream2, hSample{int64(i - 1), &h20})
		expect = append(expect, histogram.CounterReset)
		expect = append(expect, histogram.UnknownCounterReset)
		testCases[fmt.Sprintf("distance between samples %d", i-1)] = tc{
			chunks: append(sampleToChunks(stream1), sampleToChunks(stream2)...),
			expect: expect,
		}
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			iter := NewChunkMergeIterator(nil, labels.EmptyLabels(), tc.chunks)
			i := 0
			for iter.Next() != chunkenc.ValNone {
				ts, h := iter.AtHistogram(nil)
				require.Less(t, i, len(tc.expect))
				require.Equal(t, tc.expect[i], h.CounterResetHint, ts)
				i++
			}
			require.Equal(t, len(tc.expect), i)
		})
	}
}
