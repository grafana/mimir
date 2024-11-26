// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/batch/merge_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package batch

import (
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/prometheus/prometheus/model/histogram"
	"testing"
	"time"

	"github.com/prometheus/common/model"
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
			testIter(t, 200, iter, enc, setNotCounterResetHintsAsUnknown)
			iter = NewGenericChunkMergeIterator(nil, labels.EmptyLabels(), []GenericChunk{chunk1, chunk2, chunk3, chunk4, chunk5})
			testSeek(t, 200, iter, enc, setNotCounterResetHintsAsUnknown)

			// Re-use iterator.
			iter = NewGenericChunkMergeIterator(iter, labels.EmptyLabels(), []GenericChunk{chunk1, chunk2, chunk3, chunk4, chunk5})
			testIter(t, 200, iter, enc, setNotCounterResetHintsAsUnknown)
			iter = NewGenericChunkMergeIterator(iter, labels.EmptyLabels(), []GenericChunk{chunk1, chunk2, chunk3, chunk4, chunk5})
			testSeek(t, 200, iter, enc, setNotCounterResetHintsAsUnknown)
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
			testIter(t, offset*numChunks+samples-offset, newIteratorAdapter(nil, iter, labels.EmptyLabels()), enc, setNotCounterResetHintsAsUnknown)

			iter = newMergeIterator(nil, chunks)
			testSeek(t, offset*numChunks+samples-offset, newIteratorAdapter(nil, iter, labels.EmptyLabels()), enc, setNotCounterResetHintsAsUnknown)
		})
	}
}

type histSample struct {
	t    int64
	v    int
	hint histogram.CounterResetHint
}

func TestMergeHistogramCheckHints(t *testing.T) {
	for _, enc := range []chunk.Encoding{chunk.PrometheusHistogramChunk, chunk.PrometheusFloatHistogramChunk} {
		var addFunc func(chk chunk.EncodedChunk, ts, val int)
		if enc == chunk.PrometheusHistogramChunk {
			addFunc = func(chk chunk.EncodedChunk, ts, val int) {
				overflow, err := chk.AddHistogram(int64(ts), test.GenerateTestHistogram(val))
				require.NoError(t, err)
				require.Nil(t, overflow)
			}
		} else {
			addFunc = func(chk chunk.EncodedChunk, ts, val int) {
				overflow, err := chk.AddFloatHistogram(int64(ts), test.GenerateTestFloatHistogram(val))
				require.NoError(t, err)
				require.Nil(t, overflow)
			}
		}
		t.Run(enc.String(), func(t *testing.T) {
			for _, tc := range []struct {
				name            string
				chunks          []GenericChunk
				expectedSamples []histSample
			}{
				{
					name: "no overlapping iterators",
					chunks: []GenericChunk{
						mkGenericChunk(t, 0, 5, enc),
						mkGenericChunk(t, model.TimeFromUnix(5), 5, enc),
					},
					expectedSamples: []histSample{
						{t: 0, v: 0, hint: histogram.UnknownCounterReset},
						{t: 1000, v: 1000, hint: histogram.NotCounterReset},
						{t: 2000, v: 2000, hint: histogram.NotCounterReset},
						{t: 3000, v: 3000, hint: histogram.NotCounterReset},
						{t: 4000, v: 4000, hint: histogram.NotCounterReset},
						{t: 5000, v: 5000, hint: histogram.UnknownCounterReset},
						{t: 6000, v: 6000, hint: histogram.NotCounterReset},
						{t: 7000, v: 7000, hint: histogram.NotCounterReset},
						{t: 8000, v: 8000, hint: histogram.NotCounterReset},
						{t: 9000, v: 9000, hint: histogram.NotCounterReset},
					},
				},
				{
					name: "duplicated chunks",
					chunks: []GenericChunk{
						mkGenericChunk(t, 0, 10, enc),
						mkGenericChunk(t, 0, 10, enc),
					},
					expectedSamples: []histSample{
						{t: 0, v: 0, hint: histogram.UnknownCounterReset},       // 1 sample from c0
						{t: 1000, v: 1000, hint: histogram.UnknownCounterReset}, // 1 sample from c1
						{t: 2000, v: 2000, hint: histogram.UnknownCounterReset}, // 2 samples from c0
						{t: 3000, v: 3000, hint: histogram.NotCounterReset},
						{t: 4000, v: 4000, hint: histogram.UnknownCounterReset}, // 4 samples from c1
						{t: 5000, v: 5000, hint: histogram.NotCounterReset},
						{t: 6000, v: 6000, hint: histogram.NotCounterReset},
						{t: 7000, v: 7000, hint: histogram.NotCounterReset},
						{t: 8000, v: 8000, hint: histogram.UnknownCounterReset}, // 2 samples from c0
						{t: 9000, v: 9000, hint: histogram.NotCounterReset},
					},
				},
				{
					name: "overlapping chunks",
					chunks: []GenericChunk{
						mkGenericChunk(t, 0, 11, enc),
						mkGenericChunk(t, 3000, 7, enc),
					},
					expectedSamples: []histSample{
						{t: 0, v: 0, hint: histogram.UnknownCounterReset},   // 1 sample from c0
						{t: 1000, v: 1000, hint: histogram.NotCounterReset}, // 1 sample from c0, previous iterator was also c0, so keep the NCR hint
						{t: 2000, v: 2000, hint: histogram.NotCounterReset}, // 2 samples from c0, previous iterator was also c0, so keep the NCR hint
						{t: 3000, v: 3000, hint: histogram.NotCounterReset},
						{t: 4000, v: 4000, hint: histogram.UnknownCounterReset}, // 4 samples from c1
						{t: 5000, v: 5000, hint: histogram.NotCounterReset},
						{t: 6000, v: 6000, hint: histogram.NotCounterReset},
						{t: 7000, v: 7000, hint: histogram.NotCounterReset},
						{t: 8000, v: 8000, hint: histogram.UnknownCounterReset}, // 3 samples from c1
						{t: 9000, v: 9000, hint: histogram.NotCounterReset},
						{t: 10000, v: 10000, hint: histogram.NotCounterReset},
					},
				},
				{
					name: "different values at same timestamp",
					chunks: func() []GenericChunk {
						chk1, err := chunk.NewForEncoding(enc)
						require.NoError(t, err)
						addFunc(chk1, 0, 0)
						addFunc(chk1, 1, 1)
						addFunc(chk1, 4, 2)
						addFunc(chk1, 5, 3)
						addFunc(chk1, 7, 4)
						chk2, err := chunk.NewForEncoding(enc)
						require.NoError(t, err)
						addFunc(chk2, 0, 7)
						addFunc(chk2, 1, 8)
						addFunc(chk2, 2, 9)
						addFunc(chk2, 3, 10)
						addFunc(chk2, 4, 11)
						return []GenericChunk{
							NewGenericChunk(0, 7, chunk.NewChunk(labels.FromStrings(model.MetricNameLabel, "foo"), chk1, 0, 7).Data.NewIterator),
							NewGenericChunk(0, 4, chunk.NewChunk(labels.FromStrings(model.MetricNameLabel, "foo"), chk2, 0, 4).Data.NewIterator),
						}
					}(),
					expectedSamples: []histSample{
						{t: 0, v: 0, hint: histogram.UnknownCounterReset}, // 1 sample from c0
						{t: 1, v: 8, hint: histogram.UnknownCounterReset}, // 1 sample from c1
						{t: 2, v: 9, hint: histogram.NotCounterReset},     // 2 samples from c1, previous iterator was also c0, so keep the NCR hint
						{t: 3, v: 10, hint: histogram.NotCounterReset},
						{t: 4, v: 11, hint: histogram.NotCounterReset},    // 1 sample from c1, previous iterator was also c0, so keep the NCR hint, also end of iterator
						{t: 5, v: 3, hint: histogram.UnknownCounterReset}, // 2 samples from c0
						{t: 7, v: 4, hint: histogram.NotCounterReset},
					},
				},
				{
					name: "overlapping and interleaved samples",
					chunks: func() []GenericChunk {
						chk1, err := chunk.NewForEncoding(enc)
						require.NoError(t, err)
						addFunc(chk1, 0, 0)
						addFunc(chk1, 3, 1)
						addFunc(chk1, 4, 2)
						addFunc(chk1, 5, 3)
						addFunc(chk1, 7, 4)
						chk2, err := chunk.NewForEncoding(enc)
						require.NoError(t, err)
						addFunc(chk2, 1, 1)
						addFunc(chk2, 2, 7)
						addFunc(chk2, 5, 8)
						addFunc(chk2, 6, 9)
						addFunc(chk2, 7, 10)
						addFunc(chk2, 8, 11)
						return []GenericChunk{
							NewGenericChunk(0, 7, chunk.NewChunk(labels.FromStrings(model.MetricNameLabel, "foo"), chk1, 0, 7).Data.NewIterator),
							NewGenericChunk(0, 4, chunk.NewChunk(labels.FromStrings(model.MetricNameLabel, "foo"), chk2, 0, 4).Data.NewIterator),
						}
					}(),
					expectedSamples: []histSample{
						{t: 0, v: 0, hint: histogram.UnknownCounterReset},  // c0
						{t: 1, v: 1, hint: histogram.UnknownCounterReset},  // c1
						{t: 2, v: 7, hint: histogram.NotCounterReset},      // c1
						{t: 3, v: 1, hint: histogram.UnknownCounterReset},  // c0
						{t: 4, v: 2, hint: histogram.NotCounterReset},      // c0
						{t: 5, v: 8, hint: histogram.UnknownCounterReset},  // c1
						{t: 6, v: 9, hint: histogram.UnknownCounterReset},  // c1 - consecutive sample, but the batch with the previous sample is removed before this sample is merged in so can't detect it's from the same iterator
						{t: 7, v: 4, hint: histogram.UnknownCounterReset},  // c0
						{t: 8, v: 11, hint: histogram.UnknownCounterReset}, // c1
					},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					iter := NewGenericChunkMergeIterator(nil, labels.EmptyLabels(), tc.chunks)
					for i, s := range tc.expectedSamples {
						valType := iter.Next()
						require.NotEqual(t, chunkenc.ValNone, valType, "expectedSamples has extra samples")
						require.Nil(t, iter.Err())
						require.Equal(t, s.t, iter.AtT())
						switch enc {
						case chunk.PrometheusHistogramChunk:
							expH := test.GenerateTestHistogram(s.v)
							expH.CounterResetHint = s.hint
							_, actH := iter.AtHistogram(nil)
							test.RequireHistogramEqual(t, expH, actH, "expected sample %d does not match", i)
						case chunk.PrometheusFloatHistogramChunk:
							expH := test.GenerateTestFloatHistogram(s.v)
							expH.CounterResetHint = s.hint
							_, actH := iter.AtFloatHistogram(nil)
							test.RequireFloatHistogramEqual(t, expH, actH, "expected sample with idx %d does not match", i)
						default:
							t.Errorf("checkHints - internal error, unhandled expected type: %T", s)
						}
					}
					require.Equal(t, chunkenc.ValNone, iter.Next(), "iter has extra samples")
					require.Nil(t, iter.Err())
				})

			}
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

		genericChunks = append(genericChunks, NewGenericChunk(samples[0], samples[len(samples)-1], func(reuse chunk.Iterator) chunk.Iterator {
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
