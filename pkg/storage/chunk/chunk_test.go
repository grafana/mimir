// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

// Note: this file has tests for code in both delta.go and doubledelta.go --
// it may make sense to split those out later, but given that the tests are
// near-identical and share a helper, this feels simpler for now.

package chunk

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/test"
)

// TSDB updates the reset hint automatically for histograms inserted into TSDB
func expectedHistogram(i int) *histogram.Histogram {
	h := test.GenerateTestHistogram(i)
	if i > 0 {
		h.CounterResetHint = histogram.NotCounterReset
	}
	return h
}

// TSDB updates the reset hint automatically for histograms inserted into TSDB
func expectedFloatHistogram(i int) *histogram.FloatHistogram {
	h := test.GenerateTestFloatHistogram(i)
	if i > 0 {
		h.CounterResetHint = histogram.NotCounterReset
	}
	return h
}

func TestLen(t *testing.T) {
	c, err := NewForEncoding(PrometheusXorChunk)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i <= 10; i++ {
		if c.Len() != i {
			t.Errorf("chunk type %s should have %d samples, had %d", c.Encoding(), i, c.Len())
		}

		cs, err := c.Add(model.SamplePair{
			Timestamp: model.Time(i),
			Value:     model.SampleValue(i),
		})
		require.NoError(t, err)
		require.Nil(t, cs)
	}
}

var step = int(15 * time.Second / time.Millisecond)

func TestChunk(t *testing.T) {
	const maxSamples = 240 // Twice as big as current TSDB

	for _, enc := range []Encoding{PrometheusXorChunk, PrometheusHistogramChunk, PrometheusFloatHistogramChunk} {
		for samples := maxSamples / 10; samples < maxSamples; samples += maxSamples / 10 {
			t.Run(fmt.Sprintf("testChunkEncoding/%s/%d", enc.String(), samples), func(t *testing.T) {
				testChunkEncoding(t, enc, samples)
			})

			t.Run(fmt.Sprintf("testChunkSeek/%s/%d", enc.String(), samples), func(t *testing.T) {
				testChunkSeek(t, enc, samples)
			})

			t.Run(fmt.Sprintf("testChunkSeekForward/%s/%d", enc.String(), samples), func(t *testing.T) {
				testChunkSeekForward(t, enc, samples)
			})

			t.Run(fmt.Sprintf("testChunkBatch/%s/%d", enc.String(), samples), func(t *testing.T) {
				testChunkBatch(t, enc, samples)
			})
		}
	}
}

func mkChunk(t *testing.T, encoding Encoding, samples int) EncodedChunk {
	chunk, err := NewForEncoding(encoding)
	require.NoError(t, err)

	for i := 0; i < samples; i++ {
		var overflowChunk EncodedChunk
		switch encoding {
		case PrometheusXorChunk:
			overflowChunk, err = chunk.Add(model.SamplePair{
				Timestamp: model.Time(i * step),
				Value:     model.SampleValue(i),
			})
		case PrometheusHistogramChunk:
			overflowChunk, err = chunk.AddHistogram(int64(i*step), test.GenerateTestHistogram(i))
		case PrometheusFloatHistogramChunk:
			overflowChunk, err = chunk.AddFloatHistogram(int64(i*step), test.GenerateTestFloatHistogram(i))
		default:
			require.FailNowf(t, "Unexpected encoding", "%v", encoding)
		}

		require.NoError(t, err)
		require.Nil(t, overflowChunk)
	}

	return chunk
}

// testChunkEncoding checks chunks roundtrip and contain all their samples.
func testChunkEncoding(t *testing.T, encoding Encoding, samples int) {
	chunk := mkChunk(t, encoding, samples)

	var buf bytes.Buffer
	err := chunk.Marshal(&buf)
	require.NoError(t, err)

	bs1 := buf.Bytes()
	chunk, err = NewForEncoding(encoding)
	require.NoError(t, err)

	err = chunk.UnmarshalFromBuf(bs1)
	require.NoError(t, err)

	// Check all the samples are in there.
	iter := chunk.NewIterator(nil)
	var (
		h  *histogram.Histogram
		fh *histogram.FloatHistogram
		ts int64
	)
	for i := 0; i < samples; i++ {
		switch encoding {
		case PrometheusXorChunk:
			require.Equal(t, chunkenc.ValFloat, iter.Scan())
			sample := iter.Value()
			require.EqualValues(t, model.Time(i*step), sample.Timestamp)
			require.EqualValues(t, model.SampleValue(i), sample.Value)
		case PrometheusHistogramChunk:
			require.Equal(t, chunkenc.ValHistogram, iter.Scan())
			ts, h = iter.AtHistogram(h)
			require.EqualValues(t, model.Time(i*step), ts)
			require.EqualValues(t, expectedHistogram(i), h)
		case PrometheusFloatHistogramChunk:
			require.Equal(t, chunkenc.ValFloatHistogram, iter.Scan())
			ts, fh = iter.AtFloatHistogram(fh)
			require.EqualValues(t, model.Time(i*step), ts)
			require.EqualValues(t, expectedFloatHistogram(i), fh)
		default:
			require.FailNowf(t, "Unexpected encoding", "%v", encoding)
		}
	}
	require.Equal(t, chunkenc.ValNone, iter.Scan())
	require.NoError(t, iter.Err())

	// Check seek works after unmarshal
	iter = chunk.NewIterator(iter)
	for i := 0; i < samples; i += samples / 10 {
		val := iter.FindAtOrAfter(model.Time(i * step))
		require.NotEqual(t, chunkenc.ValNone, val)
	}

	// Check the byte representation after another Marshall is the same.
	buf = bytes.Buffer{}
	err = chunk.Marshal(&buf)
	require.NoError(t, err)
	bs2 := buf.Bytes()

	require.Equal(t, bs1, bs2)
}

// testChunkSeek checks seek works as expected.
// This version of the test will seek backwards.
func testChunkSeek(t *testing.T, encoding Encoding, samples int) {
	chunk := mkChunk(t, encoding, samples)

	iter := chunk.NewIterator(nil)
	for i := 0; i < samples; i += samples / 10 {
		var (
			h  *histogram.Histogram
			fh *histogram.FloatHistogram
			ts int64
		)
		if i > 0 {
			// Seek one millisecond before the actual time
			require.NotEqual(t, chunkenc.ValNone, iter.FindAtOrAfter(model.Time(i*step-1)), "1ms before step %d not found", i)
			switch encoding {
			case PrometheusXorChunk:
				sample := iter.Value()
				require.EqualValues(t, model.Time(i*step), sample.Timestamp)
				require.EqualValues(t, model.SampleValue(i), sample.Value)
			case PrometheusHistogramChunk:
				ts, h = iter.AtHistogram(h)
				require.EqualValues(t, model.Time(i*step), ts)
				require.EqualValues(t, expectedHistogram(i), h)
			case PrometheusFloatHistogramChunk:
				ts, fh = iter.AtFloatHistogram(fh)
				require.EqualValues(t, model.Time(i*step), ts)
				require.EqualValues(t, expectedFloatHistogram(i), fh)
			default:
				require.FailNowf(t, "Unexpected encoding", "%v", encoding)
			}
		}
		// Now seek to exactly the right time
		require.NotEqual(t, chunkenc.ValNone, iter.FindAtOrAfter(model.Time(i*step)))
		switch encoding {
		case PrometheusXorChunk:
			sample := iter.Value()
			require.EqualValues(t, model.Time(i*step), sample.Timestamp)
			require.EqualValues(t, model.SampleValue(i), sample.Value)
		case PrometheusHistogramChunk:
			ts, h = iter.AtHistogram(h)
			require.EqualValues(t, model.Time(i*step), ts)
			require.EqualValues(t, expectedHistogram(i), h)
		case PrometheusFloatHistogramChunk:
			ts, fh = iter.AtFloatHistogram(fh)
			require.EqualValues(t, model.Time(i*step), ts)
			require.EqualValues(t, expectedFloatHistogram(i), fh)
		default:
			require.FailNowf(t, "Unexpected encoding", "%v", encoding)
		}

		j := i + 1
		for ; j < samples; j++ {
			require.NotEqual(t, chunkenc.ValNone, iter.Scan())
			switch encoding {
			case PrometheusXorChunk:
				sample := iter.Value()
				require.EqualValues(t, model.Time(j*step), sample.Timestamp)
				require.EqualValues(t, model.SampleValue(j), sample.Value)
			case PrometheusHistogramChunk:
				ts, h = iter.AtHistogram(h)
				require.EqualValues(t, model.Time(j*step), ts)
				require.EqualValues(t, expectedHistogram(j), h)
			case PrometheusFloatHistogramChunk:
				ts, fh = iter.AtFloatHistogram(fh)
				require.EqualValues(t, model.Time(j*step), ts)
				require.EqualValues(t, expectedFloatHistogram(j), fh)
			default:
				require.FailNowf(t, "Unexpected encoding", "%v", encoding)
			}
		}
		require.Equal(t, chunkenc.ValNone, iter.Scan())
		require.NoError(t, iter.Err())
	}
	// Check seek past the end of the chunk returns failure
	require.Equal(t, chunkenc.ValNone, iter.FindAtOrAfter(model.Time(samples*step+1)))
}

func testChunkSeekForward(t *testing.T, encoding Encoding, samples int) {
	chunk := mkChunk(t, encoding, samples)

	iter := chunk.NewIterator(nil)
	var (
		h  *histogram.Histogram
		fh *histogram.FloatHistogram
		ts int64
	)
	for i := 0; i < samples; i += samples / 10 {
		require.NotEqual(t, chunkenc.ValNone, iter.FindAtOrAfter(model.Time(i*step)))
		switch encoding {
		case PrometheusXorChunk:
			sample := iter.Value()
			require.EqualValues(t, model.Time(i*step), sample.Timestamp)
			require.EqualValues(t, model.SampleValue(i), sample.Value)
		case PrometheusHistogramChunk:
			ts, h = iter.AtHistogram(h)
			require.EqualValues(t, model.Time(i*step), ts)
			require.EqualValues(t, expectedHistogram(i), h)
		case PrometheusFloatHistogramChunk:
			ts, fh = iter.AtFloatHistogram(fh)
			require.EqualValues(t, model.Time(i*step), ts)
			require.EqualValues(t, expectedFloatHistogram(i), fh)
		default:
			require.FailNowf(t, "Unexpected encoding", "%v", encoding)
		}

		j := i + 1
		for ; j < (i+samples/10) && j < samples; j++ {
			require.NotEqual(t, chunkenc.ValNone, iter.Scan())
			switch encoding {
			case PrometheusXorChunk:
				sample := iter.Value()
				require.EqualValues(t, model.Time(j*step), sample.Timestamp)
				require.EqualValues(t, model.SampleValue(j), sample.Value)
			case PrometheusHistogramChunk:
				ts, h = iter.AtHistogram(h)
				require.EqualValues(t, model.Time(j*step), ts)
				require.EqualValues(t, expectedHistogram(j), h)
			case PrometheusFloatHistogramChunk:
				ts, fh = iter.AtFloatHistogram(nil)
				require.EqualValues(t, model.Time(j*step), ts)
				require.EqualValues(t, expectedFloatHistogram(j), fh)
			default:
				require.FailNowf(t, "Unexpected encoding", "%v", encoding)
			}
		}
	}
	require.Equal(t, chunkenc.ValNone, iter.Scan())
	require.NoError(t, iter.Err())
}

func testChunkBatch(t *testing.T, encoding Encoding, samples int) {
	chunk := mkChunk(t, encoding, samples)

	// Check all the samples are in there.
	iter := chunk.NewIterator(nil)
	for i := 0; i < samples; {
		chunkType := iter.Scan()
		var batch Batch
		switch encoding {
		case PrometheusXorChunk:
			require.Equal(t, chunkenc.ValFloat, chunkType)
			batch = iter.BatchFloats(BatchSize)
			require.Equal(t, chunkenc.ValFloat, batch.ValueType, "Batch contains floats")
			for j := 0; j < batch.Length; j++ {
				require.Equal(t, int64((i+j)*step), batch.AtTime())
				ts, v := batch.At()
				require.Equal(t, int64((i+j)*step), ts)
				require.Equal(t, float64(i+j), v)
				batch.Index++
			}
		case PrometheusHistogramChunk:
			require.Equal(t, chunkenc.ValHistogram, chunkType)
			batch = iter.Batch(BatchSize, chunkenc.ValHistogram, 42, nil, nil)
			require.Equal(t, chunkenc.ValHistogram, batch.ValueType, "Batch contains histograms")
			for j := 0; j < batch.Length; j++ {
				require.Equal(t, int64((i+j)*step), batch.AtTime())
				ts, h := batch.AtHistogram()
				require.EqualValues(t, expectedHistogram(i+j), (*histogram.Histogram)(h))
				require.Equal(t, int64((i+j)*step), ts)
				if j == 0 {
					require.Equal(t, int64(42), batch.PrevT())
				} else {
					require.Equal(t, int64((i+j-1)*step), batch.PrevT())
				}
				batch.Index++
			}
		case PrometheusFloatHistogramChunk:
			require.Equal(t, chunkenc.ValFloatHistogram, chunkType)
			batch = iter.Batch(BatchSize, chunkenc.ValFloatHistogram, 42, nil, nil)
			require.Equal(t, chunkenc.ValFloatHistogram, batch.ValueType, "Batch contains float histograms")
			for j := 0; j < batch.Length; j++ {
				require.Equal(t, int64((i+j)*step), batch.AtTime())
				ts, fh := batch.AtFloatHistogram()
				require.EqualValues(t, expectedFloatHistogram(i+j), (*histogram.FloatHistogram)(fh))
				require.Equal(t, int64((i+j)*step), ts)
				if j == 0 {
					require.Equal(t, int64(42), batch.PrevT())
				} else {
					require.Equal(t, int64((i+j-1)*step), batch.PrevT())
				}
				batch.Index++
			}
		default:
			require.FailNowf(t, "Unexpected encoding", "%v", encoding)
		}
		batch.Index = 0
		i += batch.Length
	}
	require.Equal(t, chunkenc.ValNone, iter.Scan())
	require.NoError(t, iter.Err())
}
