package csproto

import (
	"encoding/binary"
	"math"
	"unsafe"
)

// Encoder implements a binary Protobuf Encoder by sequentially writing to a wrapped []byte.
type Encoder struct {
	p      []byte
	offset int
}

// NewEncoder initializes a new Protobuf encoder to write to the specified buffer, which must be
// pre-allocated by the caller with sufficient space to hold the message(s) being written.
func NewEncoder(p []byte) *Encoder {
	return &Encoder{
		p:      p,
		offset: 0,
	}
}

// EncodeBool writes a varint-encoded boolean value to the buffer preceded by the varint-encoded tag key.
func (e *Encoder) EncodeBool(tag int, v bool) {
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeVarint)
	if v {
		e.p[e.offset] = 1
	} else {
		e.p[e.offset] = 0
	}
	e.offset++
}

// EncodeString writes a length-delimited string value to the buffer preceded by the varint-encoded tag key.
func (e *Encoder) EncodeString(tag int, s string) {
	b := e.stringToBytes(s)
	e.EncodeBytes(tag, b)
}

// EncodeBytes writes a length-delimited byte slice to the buffer preceded by the varint-encoded tag key.
func (e *Encoder) EncodeBytes(tag int, v []byte) {
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeLengthDelimited)
	e.offset += EncodeVarint(e.p[e.offset:], uint64(len(v)))
	copy(e.p[e.offset:], v)
	e.offset += len(v)
}

// EncodeUInt32 writes a varint-encoded 32-bit unsigned integer value to the buffer preceded by the varint-encoded tag key.
func (e *Encoder) EncodeUInt32(tag int, v uint32) {
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeVarint)
	e.offset += EncodeVarint(e.p[e.offset:], uint64(v))
}

// EncodeUInt64 writes a varint-encoded 64-bit unsigned integer value to the buffer preceded by the varint-encoded tag key.
func (e *Encoder) EncodeUInt64(tag int, v uint64) {
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeVarint)
	e.offset += EncodeVarint(e.p[e.offset:], v)
}

// EncodeInt32 writes a varint-encoded 32-bit signed integer value to the buffer preceded by the varint-encoded tag key.
func (e *Encoder) EncodeInt32(tag int, v int32) {
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeVarint)
	e.offset += EncodeVarint(e.p[e.offset:], uint64(v))
}

// EncodeInt64 writes a varint-encoded 64-bit signed integer value to the buffer preceded by the varint-encoded tag key.
func (e *Encoder) EncodeInt64(tag int, v int64) {
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeVarint)
	e.offset += EncodeVarint(e.p[e.offset:], uint64(v))
}

// EncodeSInt32 writes a zigzag-encoded 32-bit signed integer value to the buffer preceded by the varint-encoded tag key.
func (e *Encoder) EncodeSInt32(tag int, v int32) {
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeVarint)
	e.offset += EncodeZigZag32(e.p[e.offset:], v)
}

// EncodeSInt64 writes a zigzag-encoded 64-bit signed integer value to the buffer preceded by the varint-encoded tag key.
func (e *Encoder) EncodeSInt64(tag int, v int64) {
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeVarint)
	e.offset += EncodeZigZag64(e.p[e.offset:], v)
}

// EncodeFixed32 writes a 32-bit unsigned integer value to the buffer using 4 bytes in little endian format,
// preceded by the varint-encoded tag key.
func (e *Encoder) EncodeFixed32(tag int, v uint32) {
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeFixed32)
	e.offset += EncodeFixed32(e.p[e.offset:], v)
}

// EncodeFixed64 writes a 64-bit unsigned integer value to the buffer using 8 bytes in little endian format,
// preceded by the varint-encoded tag key.
func (e *Encoder) EncodeFixed64(tag int, v uint64) {
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeFixed64)
	e.offset += EncodeFixed64(e.p[e.offset:], v)
}

// EncodeFloat32 writes a 32-bit IEEE 754 floating point value to the buffer using 4 bytes in little endian format,
// preceded by the varint-encoded tag key.
func (e *Encoder) EncodeFloat32(tag int, v float32) {
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeFixed32)
	binary.LittleEndian.PutUint32(e.p[e.offset:], math.Float32bits(v))
	e.offset += 4
}

// EncodeFloat64 writes a 64-bit IEEE 754 floating point value to the buffer using 8 bytes in little endian format,
// preceded by the varint-encoded tag key.
func (e *Encoder) EncodeFloat64(tag int, v float64) {
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeFixed64)
	binary.LittleEndian.PutUint64(e.p[e.offset:], math.Float64bits(v))
	e.offset += 8
}

// EncodePackedBool writes a list of booleans to the buffer using packed encoding, preceded by
// the varint-encoded tag key.
func (e *Encoder) EncodePackedBool(tag int, vs []bool) {
	if len(vs) == 0 {
		return
	}
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeLengthDelimited)
	e.offset += EncodeVarint(e.p[e.offset:], uint64(len(vs)))
	for _, v := range vs {
		if v {
			e.p[e.offset] = 1
		} else {
			e.p[e.offset] = 0
		}
		e.offset++
	}
}

// EncodePackedInt32 writes a list of 32-bit integers to the buffer using packed encoding, preceded by
// the varint-encoded tag key.
//
// This operation is O(n^2) because we have to traverse the list of values to calculate the total
// encoded size and write that size *before* the actual encoded values.
func (e *Encoder) EncodePackedInt32(tag int, vs []int32) {
	if len(vs) == 0 {
		return
	}
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeLengthDelimited)
	sz := 0
	for _, v := range vs {
		sz += SizeOfVarint(uint64(v))
	}
	e.offset += EncodeVarint(e.p[e.offset:], uint64(sz))
	for _, v := range vs {
		e.offset += EncodeVarint(e.p[e.offset:], uint64(v))
	}
}

// EncodePackedInt64 writes a list of 64-bit integers to the buffer using packed encoding, preceded by
// the varint-encoded tag key.
//
// This operation is O(n^2) because we have to traverse the list of values to calculate the total
// encoded size and write that size *before* the actual encoded values.
func (e *Encoder) EncodePackedInt64(tag int, vs []int64) {
	if len(vs) == 0 {
		return
	}
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeLengthDelimited)
	sz := 0
	for _, v := range vs {
		sz += SizeOfVarint(uint64(v))
	}
	e.offset += EncodeVarint(e.p[e.offset:], uint64(sz))
	for _, v := range vs {
		e.offset += EncodeVarint(e.p[e.offset:], uint64(v))
	}
}

// EncodePackedUInt32 writes a list of 32-bit unsigned integers to the buffer using packed encoding,
// preceded by the varint-encoded tag key.
//
// This operation is O(n^2) because we have to traverse the list of values to calculate the total
// encoded size and write that size *before* the actual encoded values.
func (e *Encoder) EncodePackedUInt32(tag int, vs []uint32) {
	if len(vs) == 0 {
		return
	}
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeLengthDelimited)
	sz := 0
	for _, v := range vs {
		sz += SizeOfVarint(uint64(v))
	}
	e.offset += EncodeVarint(e.p[e.offset:], uint64(sz))
	for _, v := range vs {
		e.offset += EncodeVarint(e.p[e.offset:], uint64(v))
	}
}

// EncodePackedUInt64 writes a list of 64-bit unsigned integers to the buffer using packed encoding,
// preceded by the varint-encoded tag key.
//
// This operation is O(n^2) because we have to traverse the list of values to calculate the total
// encoded size and write that size *before* the actual encoded values.
func (e *Encoder) EncodePackedUInt64(tag int, vs []uint64) {
	if len(vs) == 0 {
		return
	}
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeLengthDelimited)
	sz := 0
	for _, v := range vs {
		sz += SizeOfVarint(v)
	}
	e.offset += EncodeVarint(e.p[e.offset:], uint64(sz))
	for _, v := range vs {
		e.offset += EncodeVarint(e.p[e.offset:], v)
	}
}

// EncodePackedSInt32 writes a list of 32-bit signed integers to the buffer using packed encoding,
// preceded by the varint-encoded tag key.
//
// This operation is O(n^2) because we have to traverse the list of values to calculate the total
// encoded size and write that size *before* the actual encoded values.
func (e *Encoder) EncodePackedSInt32(tag int, vs []int32) {
	if len(vs) == 0 {
		return
	}
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeLengthDelimited)
	sz := 0
	for _, v := range vs {
		sz += SizeOfZigZag(uint64(v))
	}
	e.offset += EncodeVarint(e.p[e.offset:], uint64(sz))
	for _, v := range vs {
		e.offset += EncodeZigZag32(e.p[e.offset:], v)
	}
}

// EncodePackedSInt64 writes a list of 64-bit signed integers to the buffer using packed encoding,
// preceded by the varint-encoded tag key.
//
// This operation is O(n^2) because we have to traverse the list of values to calculate the total
// encoded size and write that size *before* the actual encoded values.
func (e *Encoder) EncodePackedSInt64(tag int, vs []int64) {
	if len(vs) == 0 {
		return
	}
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeLengthDelimited)
	sz := 0
	for _, v := range vs {
		sz += SizeOfZigZag(uint64(v))
	}
	e.offset += EncodeVarint(e.p[e.offset:], uint64(sz))
	for _, v := range vs {
		e.offset += EncodeZigZag64(e.p[e.offset:], v)
	}
}

// EncodePackedFixed32 writes a list of 32-bit fixed-width unsigned integers to the buffer using packed
// encoding, preceded by the varint-encoded tag key.
func (e *Encoder) EncodePackedFixed32(tag int, vs []uint32) {
	if len(vs) == 0 {
		return
	}
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeLengthDelimited)
	e.offset += EncodeVarint(e.p[e.offset:], uint64(len(vs)*4))
	for _, v := range vs {
		binary.LittleEndian.PutUint32(e.p[e.offset:], v)
		e.offset += 4
	}
}

// EncodePackedFixed64 writes a list of 64-bit fixed-width unsigned integers to the buffer using packed
// encoding, preceded by the varint-encoded tag key.
func (e *Encoder) EncodePackedFixed64(tag int, vs []uint64) {
	if len(vs) == 0 {
		return
	}
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeLengthDelimited)
	e.offset += EncodeVarint(e.p[e.offset:], uint64(len(vs)*8))
	for _, v := range vs {
		binary.LittleEndian.PutUint64(e.p[e.offset:], v)
		e.offset += 8
	}
}

// EncodePackedSFixed32 writes a list of 32-bit fixed-width signed integers to the buffer using packed
// encoding, preceded by the varint-encoded tag key.
func (e *Encoder) EncodePackedSFixed32(tag int, vs []int32) {
	if len(vs) == 0 {
		return
	}
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeLengthDelimited)
	e.offset += EncodeVarint(e.p[e.offset:], uint64(len(vs)*4))
	for _, v := range vs {
		binary.LittleEndian.PutUint32(e.p[e.offset:], uint32(v))
		e.offset += 4
	}
}

// EncodePackedSFixed64 writes a list of 64-bit fixed-width signed integers to the buffer using packed
// encoding, preceded by the varint-encoded tag key.
func (e *Encoder) EncodePackedSFixed64(tag int, vs []int64) {
	if len(vs) == 0 {
		return
	}
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeLengthDelimited)
	e.offset += EncodeVarint(e.p[e.offset:], uint64(len(vs)*8))
	for _, v := range vs {
		binary.LittleEndian.PutUint64(e.p[e.offset:], uint64(v))
		e.offset += 8
	}
}

// EncodePackedFloat32 writes a list of 32-bit floating point numbers to the buffer using packed
// encoding, preceded by the varint-encoded tag key.
func (e *Encoder) EncodePackedFloat32(tag int, vs []float32) {
	if len(vs) == 0 {
		return
	}
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeLengthDelimited)
	e.offset += EncodeVarint(e.p[e.offset:], uint64(len(vs)*4))
	for _, v := range vs {
		binary.LittleEndian.PutUint32(e.p[e.offset:], math.Float32bits(v))
		e.offset += 4
	}
}

// EncodePackedFloat64 writes a list of 64-bit floating point numbers to the buffer using packed
// encoding, preceded by the varint-encoded tag key.
func (e *Encoder) EncodePackedFloat64(tag int, vs []float64) {
	if len(vs) == 0 {
		return
	}
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeLengthDelimited)
	e.offset += EncodeVarint(e.p[e.offset:], uint64(len(vs)*8))
	for _, v := range vs {
		binary.LittleEndian.PutUint64(e.p[e.offset:], math.Float64bits(v))
		e.offset += 8
	}
}

// EncodeNested writes a nested message to the buffer preceded by the varint-encoded tag key.
func (e *Encoder) EncodeNested(tag int, m interface{}) error {
	sz := Size(m)
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeLengthDelimited)
	e.offset += EncodeVarint(e.p[e.offset:], uint64(sz))
	switch tv := m.(type) {
	case MarshalerTo:
		if err := tv.MarshalTo(e.p[e.offset:]); err != nil {
			return err
		}
		e.offset += sz
		return nil
	case Marshaler:
		buf, err := tv.Marshal()
		if err != nil {
			return err
		}
		copy(e.p[e.offset:], buf)
		e.offset += sz
		return nil
	default:
		buf, err := Marshal(tv)
		if err != nil {
			return err
		}
		copy(e.p[e.offset:], buf)
		e.offset += sz
		return nil
	}
}

// EncodeRaw writes the raw bytes of d into the buffer at the current offset
func (e *Encoder) EncodeRaw(d []byte) {
	if l := len(d); l > 0 {
		copy(e.p[e.offset:], d)
		e.offset += l
	}
}

// EncodeMapEntryHeader writes a map entry header into the buffer, which consists of the specified
// tag with a wire type of WireTypeLengthDelimited followed by the varint encoded entry size.
func (e *Encoder) EncodeMapEntryHeader(tag int, size int) {
	e.offset += EncodeTag(e.p[e.offset:], tag, WireTypeLengthDelimited)
	e.offset += EncodeVarint(e.p[e.offset:], uint64(size))
}

// stringToBytes is an optimized convert from a string to a []byte using unsafe.Pointer
func (e *Encoder) stringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}

// EncodeTag combines tag and wireType then encodes the result into dest using the Protobuf
// varint format and returns the number of bytes written.
func EncodeTag(dest []byte, tag int, wireType WireType) int {
	// per the Protobuf spec, the field tag is: (tag << 3) | wireType
	// ex:
	//   field #1,  wire type varint (0)  ->   8 (1000 in binary, 0x8 in hex)
	//   field #13, wire type fixed32 (5) -> 109 (1101101 in binary, 0x6d in hex)
	k := (uint64(tag) << 3) | uint64(wireType)
	return EncodeVarint(dest, k)
}

// EncodeVarint encodes v into dest using the Protobuf base-128 varint format and returns the number
// of bytes written
func EncodeVarint(dest []byte, v uint64) int {
	n := 0
	for v >= 1<<7 {
		dest[n] = uint8(v&0x7f | 0x80)
		v >>= 7
		n++
	}
	dest[n] = uint8(v)
	return n + 1
}

// EncodeFixed32 encodes v into dest using the Protobuf fixed 32-bit encoding, which is just the 4 bytes
// of the value in little-endian format, and returns the number of bytes written
func EncodeFixed32(dest []byte, v uint32) int {
	binary.LittleEndian.PutUint32(dest, v)
	return 4
}

// EncodeFixed64 encodes v into dest using the Protobuf fixed 64-bit encoding, which is just the 8 bytes
// of the value in little-endian format, and returns the number of bytes written
func EncodeFixed64(dest []byte, v uint64) int {
	binary.LittleEndian.PutUint64(dest, v)
	return 8
}

// EncodeZigZag32 encodes v into dest using the Protobuf zig/zag encoding for more efficient encoding
// of negative numbers, and returns the number of bytes written.
func EncodeZigZag32(dest []byte, v int32) int {
	zz := uint64((uint32(v) << 1) ^ uint32((v >> 31)))
	return EncodeVarint(dest, zz)
}

// EncodeZigZag64 encodes v into dest using the Protobuf zig/zag encoding for more efficient encoding
// of negative numbers, and returns the number of bytes written.
func EncodeZigZag64(dest []byte, v int64) int {
	zz := uint64(v<<1) ^ uint64((v >> 63))
	return EncodeVarint(dest, zz)
}
