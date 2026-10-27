// SPDX-License-Identifier: AGPL-3.0-only

package chunks

import (
	"bytes"
	"math"
	"testing"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
)

// xorshift is the Rust tests' random source, so their sequences carry over.
type xorshift uint64

func (x *xorshift) next() uint64 {
	*x ^= *x << 13
	*x ^= *x >> 7
	*x ^= *x << 17
	return uint64(*x)
}

func TestBitsAreWrittenLikeOneAtATime(t *testing.T) {
	// One bit at a time, which writing in bulk must match.
	reference := func(w *bitWriter, value uint64, count uint) {
		for shift := int(count) - 1; shift >= 0; shift-- {
			w.bit((value>>uint(shift))&1 != 0)
		}
	}
	random := xorshift(0x9e37_79b9_7f4a_7c15)
	for range 200 {
		bulk, single := bitWriter{bytes: []byte{0, 0}}, bitWriter{bytes: []byte{0, 0}}
		for range 200 {
			value := random.next()
			switch random.next() % 4 {
			case 0:
				bulk.bit(value&1 == 1)
				single.bit(value&1 == 1)
			case 1:
				bulk.byte(byte(value))
				single.byte(byte(value))
			default:
				count := uint(random.next() % 65)
				bulk.bits(value, count)
				reference(&single, value, count)
			}
			require.Equal(t, single.bytes, bulk.bytes)
			require.Equal(t, single.free, bulk.free)
		}
	}
}

// The appender writes what Prometheus's XOR appender writes, sample for sample.
func TestXORAppenderMatchesChunkenc(t *testing.T) {
	random := xorshift(0x2545_f491_4f6c_dd1d)
	for series := range 200 {
		ours := NewXORAppender()
		chunk := chunkenc.NewXORChunk()
		theirs, err := chunk.Appender()
		require.NoError(t, err)
		timestamp := int64(random.next()%1_000_000) - 500_000
		value := float64(random.next()%1000) / 7
		for range 120 {
			switch random.next() % 5 {
			case 0:
				timestamp += 15_000
			case 1:
				timestamp += int64(random.next() % 100_000_000)
			case 2:
				timestamp += int64(random.next()%(1<<40)) + 1
			default:
				timestamp += int64(random.next()%30_000) + 1
			}
			switch random.next() % 4 {
			case 0:
			case 1:
				value = math.Float64frombits(random.next())
			default:
				value += float64(random.next()%100) / 3
			}
			ours.Append(timestamp, value)
			theirs.Append(0, timestamp, value)
			require.True(t, bytes.Equal(chunk.Bytes(), ours.Bytes()), "series %d", series)
		}
	}
}

func TestXORAppenderMatchesBatchEncodingAndRoundTrips(t *testing.T) {
	samples := []FloatSample{
		{1_000, 1.5},
		{16_000, 1.5},
		{31_000, 2.25},
		{46_001, -7.0},
		{1_000_000, math.Float64frombits(0x7ff0_0000_0000_0002)},
		{1_000_015, 1e300},
	}
	appender := NewXORAppender()
	for _, sample := range samples {
		appender.Append(sample.T, sample.V)
	}
	last, ok := appender.LastTimestamp()
	require.True(t, ok)
	require.Equal(t, int64(1_000_015), last)
	require.Equal(t, EncodeXOR(samples), appender.Bytes())
	var state bytes.Buffer
	require.NoError(t, appender.WriteState(&state))
	restored, err := ReadXORAppenderState(&state)
	require.NoError(t, err)
	restored.Append(1_000_030, 2.0)
	appender.Append(1_000_030, 2.0)
	require.Equal(t, appender.Bytes(), restored.Bytes())
	decoded, err := DecodeXOR(appender.Bytes())
	require.NoError(t, err)
	for index, sample := range samples {
		require.Equal(t, sample.T, decoded[index].T)
		require.Equal(t, math.Float64bits(sample.V), math.Float64bits(decoded[index].V))
	}
}

func BenchmarkXORAppend(b *testing.B) {
	for b.Loop() {
		appender := NewXORAppender()
		for index := range 120 {
			appender.Append(int64(index)*15_000, float64(index)*1.7)
		}
	}
}
