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
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
)

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
	const (
		encoding   = PrometheusXorChunk
		maxSamples = 2048
	)

	for samples := maxSamples / 10; samples < maxSamples; samples += maxSamples / 10 {
		t.Run(fmt.Sprintf("testChunkEncoding/%s/%d", encoding.String(), samples), func(t *testing.T) {
			testChunkEncoding(t, encoding, samples)
		})

		t.Run(fmt.Sprintf("testChunkSeek/%s/%d", encoding.String(), samples), func(t *testing.T) {
			testChunkSeek(t, encoding, samples)
		})

		t.Run(fmt.Sprintf("testChunkSeekForward/%s/%d", encoding.String(), samples), func(t *testing.T) {
			testChunkSeekForward(t, encoding, samples)
		})

		t.Run(fmt.Sprintf("testChunkBatch/%s/%d", encoding.String(), samples), func(t *testing.T) {
			testChunkBatch(t, encoding, samples)
		})
	}
}

func mkChunk(t *testing.T, encoding Encoding, samples int) EncodedChunk {
	chunk, err := NewForEncoding(encoding)
	require.NoError(t, err)

	for i := 0; i < samples; i++ {
		newChunk, err := chunk.Add(model.SamplePair{
			Timestamp: model.Time(i * step),
			Value:     model.SampleValue(i),
		})
		require.NoError(t, err)
		require.Nil(t, newChunk)
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
	for i := 0; i < samples; i++ {
		require.True(t, iter.Scan() == chunkenc.ValFloat)
		sample := iter.Value()
		require.EqualValues(t, model.Time(i*step), sample.Timestamp)
		require.EqualValues(t, model.SampleValue(i), sample.Value)
	}
	require.False(t, iter.Scan() == chunkenc.ValFloat)
	require.NoError(t, iter.Err())

	// Check seek works after unmarshal
	iter = chunk.NewIterator(iter)
	for i := 0; i < samples; i += samples / 10 {
		require.True(t, iter.FindAtOrAfter(model.Time(i*step)) == chunkenc.ValFloat)
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
		if i > 0 {
			// Seek one millisecond before the actual time
			require.True(t, iter.FindAtOrAfter(model.Time(i*step-1)) == chunkenc.ValFloat, "1ms before step %d not found", i)
			sample := iter.Value()
			require.EqualValues(t, model.Time(i*step), sample.Timestamp)
			require.EqualValues(t, model.SampleValue(i), sample.Value)
		}
		// Now seek to exactly the right time
		require.True(t, iter.FindAtOrAfter(model.Time(i*step)) == chunkenc.ValFloat)
		sample := iter.Value()
		require.EqualValues(t, model.Time(i*step), sample.Timestamp)
		require.EqualValues(t, model.SampleValue(i), sample.Value)

		j := i + 1
		for ; j < samples; j++ {
			require.True(t, iter.Scan() == chunkenc.ValFloat)
			sample := iter.Value()
			require.EqualValues(t, model.Time(j*step), sample.Timestamp)
			require.EqualValues(t, model.SampleValue(j), sample.Value)
		}
		require.False(t, iter.Scan() == chunkenc.ValFloat)
		require.NoError(t, iter.Err())
	}
	// Check seek past the end of the chunk returns failure
	require.False(t, iter.FindAtOrAfter(model.Time(samples*step+1)) == chunkenc.ValFloat)
}

func testChunkSeekForward(t *testing.T, encoding Encoding, samples int) {
	chunk := mkChunk(t, encoding, samples)

	iter := chunk.NewIterator(nil)
	for i := 0; i < samples; i += samples / 10 {
		require.True(t, iter.FindAtOrAfter(model.Time(i*step)) == chunkenc.ValFloat)
		sample := iter.Value()
		require.EqualValues(t, model.Time(i*step), sample.Timestamp)
		require.EqualValues(t, model.SampleValue(i), sample.Value)

		j := i + 1
		for ; j < (i+samples/10) && j < samples; j++ {
			require.True(t, iter.Scan() == chunkenc.ValFloat)
			sample := iter.Value()
			require.EqualValues(t, model.Time(j*step), sample.Timestamp)
			require.EqualValues(t, model.SampleValue(j), sample.Value)
		}
	}
	require.False(t, iter.Scan() == chunkenc.ValFloat)
	require.NoError(t, iter.Err())
}

func testChunkBatch(t *testing.T, encoding Encoding, samples int) {
	chunk := mkChunk(t, encoding, samples)

	// Check all the samples are in there.
	iter := chunk.NewIterator(nil)
	for i := 0; i < samples; {
		require.True(t, iter.Scan() == chunkenc.ValFloat)
		batch := iter.Batch(BatchSize, chunkenc.ValFloat)
		for j := 0; j < batch.Length; j++ {
			require.EqualValues(t, int64((i+j)*step), batch.Timestamps[j])
			require.EqualValues(t, float64(i+j), batch.SampleValues[j])
		}
		i += batch.Length
	}
	require.False(t, iter.Scan() == chunkenc.ValFloat)
	require.NoError(t, iter.Err())
}
