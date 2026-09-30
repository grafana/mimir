// SPDX-License-Identifier: AGPL-3.0-only

package chunks

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// FloatSample is a float sample of a series.
type FloatSample struct {
	T int64
	V float64
}

// bitWriter writes a Prometheus bit stream: bytes filled from the most significant bit.
type bitWriter struct {
	bytes []byte
	free  uint8
}

func (w *bitWriter) bit(value bool) {
	if w.free == 0 {
		w.bytes = append(w.bytes, 0)
		w.free = 8
	}
	if value {
		w.bytes[len(w.bytes)-1] |= 1 << (w.free - 1)
	}
	w.free--
}

func (w *bitWriter) byte(value byte) {
	if w.free == 0 {
		w.bytes = append(w.bytes, value)
		return
	}
	w.bytes[len(w.bytes)-1] |= value >> (8 - w.free)
	w.bytes = append(w.bytes, value<<w.free)
}

// bits writes the low count bits of value, most significant first, filling the last byte's free
// bits at once: writing them one by one was most of what appending a sample cost.
func (w *bitWriter) bits(value uint64, count uint) {
	for count > 0 {
		if w.free == 0 {
			w.bytes = append(w.bytes, 0)
			w.free = 8
		}
		take := min(count, uint(w.free))
		count -= take
		chunk := byte((value >> count) & (1<<take - 1))
		w.bytes[len(w.bytes)-1] |= chunk << (uint(w.free) - take)
		w.free -= uint8(take)
	}
}

// XORAppender incrementally encodes a Prometheus XOR chunk, byte-identical to chunkenc's, so an
// open chunk costs about two bytes per sample. Unlike chunkenc's appender its state can be
// written and read back, which head snapshots need.
type XORAppender struct {
	out           bitWriter
	count         uint16
	previousTime  int64
	previousDelta uint64
	previousValue float64
	leading       uint8
	trailing      uint8
}

// NewXORAppender returns an empty chunk's appender.
func NewXORAppender() XORAppender {
	return XORAppender{
		out:          bitWriter{bytes: make([]byte, 2, 128)},
		previousTime: math.MinInt64,
		leading:      math.MaxUint8,
	}
}

func (a *XORAppender) Len() int {
	return int(a.count)
}

func (a *XORAppender) IsEmpty() bool {
	return a.count == 0
}

// ByteCapacity is the heap bytes the open chunk holds.
func (a *XORAppender) ByteCapacity() int {
	return cap(a.out.bytes)
}

// LastTimestamp is the newest sample's timestamp, false without samples.
func (a *XORAppender) LastTimestamp() (int64, bool) {
	return a.previousTime, a.count > 0
}

// LastValue is the newest sample's value, false without samples.
func (a *XORAppender) LastValue() (float64, bool) {
	return a.previousValue, a.count > 0
}

// Append adds a sample, which must be newer than the previous one.
func (a *XORAppender) Append(timestamp int64, value float64) {
	if a.count == math.MaxUint16 {
		panic("XOR chunk sample count exceeds u16")
	}
	out := &a.out
	var delta uint64
	switch a.count {
	case 0:
		writeVarint(out, timestamp)
		out.bits(math.Float64bits(value), 64)
	case 1:
		delta = uint64(timestamp - a.previousTime)
		writeUvarint(out, delta)
		writeXORValue(out, value, a.previousValue, &a.leading, &a.trailing)
	default:
		delta = uint64(timestamp - a.previousTime)
		dod := int64(delta - a.previousDelta)
		switch {
		case dod == 0:
			out.bit(false)
		case bitRange(dod, 14):
			out.byte(0b1000_0000 | byte(dod>>8)&0x3f)
			out.byte(byte(dod))
		case bitRange(dod, 17):
			out.bits(0b110, 3)
			out.bits(uint64(dod), 17)
		case bitRange(dod, 20):
			out.bits(0b1110, 4)
			out.bits(uint64(dod), 20)
		default:
			out.bits(0b1111, 4)
			out.bits(uint64(dod), 64)
		}
		writeXORValue(out, value, a.previousValue, &a.leading, &a.trailing)
	}
	a.previousTime = timestamp
	a.previousDelta = delta
	a.previousValue = value
	a.count++
	binary.BigEndian.PutUint16(a.out.bytes, a.count)
}

// Bytes is the chunk, valid until the next Append.
func (a *XORAppender) Bytes() []byte {
	return a.out.bytes
}

const xorStateFixed = 2 + 8 + 8 + 8 + 3 + 4

// WriteState writes the appender for ReadXORAppenderState, in the Rust ingester's head snapshot
// layout.
func (a *XORAppender) WriteState(w io.Writer) error {
	var fixed [xorStateFixed]byte
	binary.LittleEndian.PutUint16(fixed[0:], a.count)
	binary.LittleEndian.PutUint64(fixed[2:], uint64(a.previousTime))
	binary.LittleEndian.PutUint64(fixed[10:], a.previousDelta)
	binary.LittleEndian.PutUint64(fixed[18:], math.Float64bits(a.previousValue))
	fixed[26], fixed[27], fixed[28] = a.leading, a.trailing, a.out.free
	binary.LittleEndian.PutUint32(fixed[29:], uint32(len(a.out.bytes)))
	if _, err := w.Write(fixed[:]); err != nil {
		return err
	}
	_, err := w.Write(a.out.bytes)
	return err
}

// ReadXORAppenderState reads what WriteState wrote.
func ReadXORAppenderState(r io.Reader) (XORAppender, error) {
	var fixed [xorStateFixed]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return XORAppender{}, err
	}
	length := binary.LittleEndian.Uint32(fixed[29:])
	if length < 2 || length > 64*1024 {
		return XORAppender{}, errors.New("invalid XOR appender length")
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return XORAppender{}, err
	}
	return XORAppender{
		out:           bitWriter{bytes: data, free: fixed[28]},
		count:         binary.LittleEndian.Uint16(fixed[0:]),
		previousTime:  int64(binary.LittleEndian.Uint64(fixed[2:])),
		previousDelta: binary.LittleEndian.Uint64(fixed[10:]),
		previousValue: math.Float64frombits(binary.LittleEndian.Uint64(fixed[18:])),
		leading:       fixed[26],
		trailing:      fixed[27],
	}, nil
}

func bitRange(value int64, nbits uint8) bool {
	return -((int64(1)<<(nbits-1))-1) <= value && value <= int64(1)<<(nbits-1)
}

func writeXORValue(out *bitWriter, value, previous float64, leading, trailing *uint8) {
	delta := math.Float64bits(value) ^ math.Float64bits(previous)
	if delta == 0 {
		out.bit(false)
		return
	}
	out.bit(true)
	newLeading := min(uint8(bits.LeadingZeros64(delta)), 31)
	newTrailing := uint8(bits.TrailingZeros64(delta))
	if *leading != math.MaxUint8 && newLeading >= *leading && newTrailing >= *trailing {
		out.bit(false)
		out.bits(delta>>*trailing, uint(64-*leading-*trailing))
		return
	}
	*leading, *trailing = newLeading, newTrailing
	out.bit(true)
	out.bits(uint64(newLeading), 5)
	significant := 64 - newLeading - newTrailing
	out.bits(uint64(significant&0x3f), 6)
	out.bits(delta>>newTrailing, uint(significant))
}

func writeUvarint(out *bitWriter, value uint64) {
	for value >= 0x80 {
		out.byte(byte(value) | 0x80)
		value >>= 7
	}
	out.byte(byte(value))
}

func writeVarint(out *bitWriter, value int64) {
	encoded := uint64(value) << 1
	if value < 0 {
		encoded = ^encoded
	}
	writeUvarint(out, encoded)
}

// EncodeXOR encodes samples into one XOR chunk.
func EncodeXOR(samples []FloatSample) []byte {
	if len(samples) > math.MaxUint16 {
		panic("XOR chunk sample count exceeds u16")
	}
	appender := NewXORAppender()
	for _, sample := range samples {
		appender.Append(sample.T, sample.V)
	}
	return appender.Bytes()
}

// DecodeXOR returns an XOR chunk's samples.
func DecodeXOR(data []byte) ([]FloatSample, error) {
	chunk, err := chunkenc.FromData(chunkenc.EncXOR, data)
	if err != nil {
		return nil, fmt.Errorf("decode XOR chunk: %w", err)
	}
	samples := make([]FloatSample, 0, chunk.NumSamples())
	iterator := chunk.Iterator(nil)
	for iterator.Next() == chunkenc.ValFloat {
		t, v := iterator.At()
		samples = append(samples, FloatSample{T: t, V: v})
	}
	return samples, iterator.Err()
}
