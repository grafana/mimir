package csproto

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"unsafe"
)

var (
	// ErrInvalidFieldTag is returned by the decoder when it fails to read a varint-encoded field tag/wire type value.
	ErrInvalidFieldTag = errors.New("unable to read protobuf field tag")
	// ErrInvalidVarintData is returned by the decoder when it fails to read a varint-encoded value.
	ErrInvalidVarintData = errors.New("unable to read protobuf varint value")
	// ErrValueOverflow is returned by DecodeUInt32() or DecodeInt32() when the decoded value is too large for a 32-bit value.
	ErrValueOverflow = errors.New("value overflow trying to read protobuf varint value")
	// ErrLenOverflow is returned when the LEN portion of a length-delimited field is larger than 2GB
	ErrLenOverflow = errors.New("field length cannot be more than 2GB")
	// ErrInvalidZigZagData is returned by the decoder when it fails to read a zigzag-encoded value.
	ErrInvalidZigZagData = errors.New("unable to read protobuf zigzag value")
	// ErrInvalidFixed32Data is returned by the decoder when it fails to read a fixed-size 32-bit value.
	ErrInvalidFixed32Data = errors.New("unable to read protobuf fixed 32-bit value")
	// ErrInvalidFixed64Data is returned by the decoder when it fails to read a fixed-size 64-bit value.
	ErrInvalidFixed64Data = errors.New("unable to read protobuf fixed 64-bit value")
	// ErrInvalidPackedData is returned by the decoder when it fails to read a packed repeated value.
	ErrInvalidPackedData = errors.New("unable to read protobuf packed value")
)

// MaxTagValue is the largest supported protobuf field tag, which is 2^29 - 1 (or 536,870,911)
const MaxTagValue = 536870911

// length-delimited fields cannot contain more than 2GB
const maxFieldLen = math.MaxInt32

// DecoderMode defines the behavior of the decoder (safe vs fastest).
type DecoderMode int

const (
	// DecoderModeSafe instructs the decoder to only use safe operations when decoding values.
	DecoderModeSafe DecoderMode = iota
	// DecoderModeFast instructs the decoder to use unsafe operations to avoid allocations and copying data
	// for the fastest throughput.
	//
	// When using DecoderModeFast, the byte slice passed to the decoder must not be modified after
	// using the decoder to extract values.  The behavior is undefined if the slice is modified.
	DecoderModeFast
)

// String returns a string representation of m, "safe" or "fast".
func (m DecoderMode) String() string {
	if m == DecoderModeSafe {
		return "safe"
	}
	return "fast"
}

// Decoder implements a binary Protobuf Decoder by sequentially reading from a provided []byte.
type Decoder struct {
	p      []byte
	offset int
	mode   DecoderMode
}

// NewDecoder initializes a new Protobuf decoder to read the provided buffer.
func NewDecoder(p []byte) *Decoder {
	return &Decoder{
		p:      p,
		offset: 0,
	}
}

// Mode returns the current decoding mode, safe vs fastest.
func (d *Decoder) Mode() DecoderMode {
	return d.mode
}

// SetMode configures the decoding behavior, safe vs fastest.
func (d *Decoder) SetMode(m DecoderMode) {
	d.mode = m
}

// Seek sets the position of the next read operation to [offset], interpreted according to [whence]:
// [io.SeekStart] means relative to the start of the data, [io.SeekCurrent] means relative to the
// current offset, and [io.SeekEnd] means relative to the end.
//
// This low-level operation is provided to support advanced/custom usages of the decoder and it is up
// to the caller to ensure that the resulting offset will point to a valid location in the data stream.
func (d *Decoder) Seek(offset int64, whence int) (int64, error) {
	pos := int(offset)
	switch whence {
	case io.SeekStart:
		// no adjustment needed
	case io.SeekCurrent:
		// shift relative to current read offset
		pos += d.offset
	case io.SeekEnd:
		// shift relative to EOF
		pos += len(d.p)
	default:
		return int64(d.offset), fmt.Errorf("invalid value (%d) for whence", whence)
	}
	// verify bounds then update the read position
	if pos < 0 || pos > len(d.p) {
		return int64(d.offset), fmt.Errorf("seek position (%d) out of bounds", pos)
	}
	d.offset = pos
	return int64(d.offset), nil
}

// Reset moves the read offset back to the beginning of the encoded data
func (d *Decoder) Reset() {
	d.offset = 0
}

// More indicates if there is more data to be read in the buffer.
func (d *Decoder) More() bool {
	return d.offset < len(d.p)
}

// Offset returns the current read offset
func (d *Decoder) Offset() int {
	return d.offset
}

// DecodeTag decodes a field tag and Protobuf wire type from the stream and returns the values.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodeTag() (tag int, wireType WireType, err error) {
	if d.offset >= len(d.p) {
		return 0, WireTypeVarint, io.ErrUnexpectedEOF
	}
	v, n, err := DecodeVarint(d.p[d.offset:])
	if err != nil {
		return 0, -1, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	}
	if n < 1 || v < 1 || v > MaxTagValue {
		return 0, -1, fmt.Errorf("invalid tag value (%d) at byte %d: %w", v, d.offset, ErrInvalidFieldTag)
	}
	d.offset += n
	return int(v >> 3), WireType(v & 0x7), nil
}

// DecodeBool decodes a boolean value from the stream and returns the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodeBool() (b bool, err error) {
	if d.offset >= len(d.p) {
		return false, io.ErrUnexpectedEOF
	}
	v, n, err := DecodeVarint(d.p[d.offset:])
	if err != nil {
		return false, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	}
	if n == 0 {
		return false, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
	}
	d.offset += n
	return (v != 0), nil
}

// DecodeString decodes a length-delimited string from the stream and returns the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodeString() (string, error) {
	if d.offset >= len(d.p) {
		return "", io.ErrUnexpectedEOF
	}
	b, err := d.DecodeBytes()
	if err != nil {
		return "", fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	}
	switch d.mode {
	case DecoderModeFast:
		return *(*string)(unsafe.Pointer(&b)), nil //nolint: gosec // using unsafe on purpose

	default:
		// safe mode by default
		return string(b), nil
	}
}

// DecodeBytes decodes a length-delimited slice of bytes from the stream and returns the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodeBytes() ([]byte, error) {
	if d.offset >= len(d.p) {
		return nil, io.ErrUnexpectedEOF
	}

	l, n, err := DecodeVarint(d.p[d.offset:])
	switch {
	case err != nil:
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	case n == 0:
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
	case l > maxFieldLen:
		return nil, fmt.Errorf("invalid length (%d) for length-delimited field at byte %d: %w", l, d.offset, ErrLenOverflow)
	default:
		// length is good
	}

	nb := int(l)
	if d.offset+n+nb > len(d.p) {
		return nil, io.ErrUnexpectedEOF
	}
	b := d.p[d.offset+n : d.offset+n+nb]
	d.offset += n + nb
	return b, nil
}

// DecodeUInt32 decodes a varint-encoded 32-bit unsigned integer from the stream and returns the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodeUInt32() (uint32, error) {
	if d.offset >= len(d.p) {
		return 0, io.ErrUnexpectedEOF
	}
	v, n, err := DecodeVarint(d.p[d.offset:])
	if err != nil {
		return 0, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	}
	if n == 0 {
		return 0, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
	}
	if v > math.MaxUint32 {
		return 0, ErrValueOverflow
	}
	d.offset += n
	return uint32(v), nil
}

// DecodeUInt64 decodes a varint-encoded 64-bit unsigned integer from the stream and returns the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodeUInt64() (uint64, error) {
	if d.offset >= len(d.p) {
		return 0, io.ErrUnexpectedEOF
	}
	v, n, err := DecodeVarint(d.p[d.offset:])
	if err != nil {
		return 0, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	}
	if n == 0 {
		return 0, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
	}
	d.offset += n
	return v, nil
}

// DecodeInt32 decodes a varint-encoded 32-bit integer from the stream and returns the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodeInt32() (int32, error) {
	if d.offset >= len(d.p) {
		return 0, io.ErrUnexpectedEOF
	}
	v, n, err := DecodeVarint(d.p[d.offset:])
	if err != nil {
		return 0, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	}
	if n == 0 {
		return 0, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
	}
	// ensure the result is within [-math.MaxInt32, math.MaxInt32] when converted to a signed value
	if i64 := int64(v); i64 > math.MaxInt32 || i64 < math.MinInt32 {
		return 0, ErrValueOverflow
	}
	d.offset += n
	return int32(v), nil
}

// DecodeInt64 decodes a varint-encoded 64-bit integer from the stream and returns the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodeInt64() (int64, error) {
	if d.offset >= len(d.p) {
		return 0, io.ErrUnexpectedEOF
	}
	v, n, err := DecodeVarint(d.p[d.offset:])
	if err != nil {
		return 0, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	}
	if n == 0 {
		return 0, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
	}
	d.offset += n
	return int64(v), nil
}

// DecodeSInt32 decodes a zigzag-encoded 32-bit integer from the stream and returns the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodeSInt32() (int32, error) {
	if d.offset >= len(d.p) {
		return 0, io.ErrUnexpectedEOF
	}
	v, n, err := DecodeZigZag32(d.p[d.offset:])
	if err != nil {
		return 0, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	}
	if n == 0 {
		return 0, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidZigZagData)
	}
	d.offset += n
	return v, nil
}

// DecodeSInt64 decodes a zigzag-encoded 32-bit integer from the stream and returns the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodeSInt64() (int64, error) {
	if d.offset >= len(d.p) {
		return 0, io.ErrUnexpectedEOF
	}
	v, n, err := DecodeZigZag64(d.p[d.offset:])
	if err != nil {
		return 0, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	}
	if n == 0 {
		return 0, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidZigZagData)
	}
	d.offset += n
	return v, nil
}

// DecodeFixed32 decodes a 4-byte integer from the stream and returns the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodeFixed32() (uint32, error) {
	if d.offset >= len(d.p) {
		return 0, io.ErrUnexpectedEOF
	}
	v, n, err := DecodeFixed32(d.p[d.offset:])
	if err != nil {
		return 0, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	}
	if n == 0 {
		return 0, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidFixed32Data)
	}
	d.offset += n
	return v, nil
}

// DecodeFixed64 decodes an 8-byte integer from the stream and returns the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodeFixed64() (uint64, error) {
	if d.offset >= len(d.p) {
		return 0, io.ErrUnexpectedEOF
	}
	v, n, err := DecodeFixed64(d.p[d.offset:])
	if err != nil {
		return 0, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	}
	if n == 0 {
		return 0, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidFixed64Data)
	}
	d.offset += n
	return v, nil
}

// DecodeFloat32 decodes a 4-byte IEEE 754 floating point value from the stream and returns the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodeFloat32() (float32, error) {
	if d.offset >= len(d.p) {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.LittleEndian.Uint32(d.p[d.offset:])
	fv := math.Float32frombits(v)
	d.offset += 4
	return fv, nil
}

// DecodeFloat64 decodes an 8-byte IEEE 754 floating point value from the stream and returns the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodeFloat64() (float64, error) {
	if d.offset >= len(d.p) {
		return 0, io.ErrUnexpectedEOF
	}
	v := binary.LittleEndian.Uint64(d.p[d.offset:])
	fv := math.Float64frombits(v)
	d.offset += 8
	return fv, nil
}

// DecodePackedBool decodes a packed encoded list of boolean values from the stream and returns the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodePackedBool() ([]bool, error) {
	if d.offset >= len(d.p) {
		return nil, io.ErrUnexpectedEOF
	}
	var (
		l, nRead uint64
		n        int
		err      error
		res      []bool
	)
	l, n, err = DecodeVarint(d.p[d.offset:])
	if err != nil {
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	}
	if n == 0 {
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
	}
	d.offset += n
	packedDataStart := d.offset
	for nRead < l {
		if d.offset >= len(d.p) {
			return nil, io.ErrUnexpectedEOF
		}
		v, n, err := DecodeVarint(d.p[d.offset:])
		if err != nil {
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
		}
		if n == 0 {
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
		}
		nRead += uint64(n)
		d.offset += n
		res = append(res, (v != 0))
	}
	if nRead != l {
		return nil, fmt.Errorf("invalid packed data at byte %d: %w", packedDataStart, ErrInvalidPackedData)
	}
	return res, nil
}

// DecodePackedInt32 decodes a packed encoded list of 32-bit integers from the stream and returns the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodePackedInt32() ([]int32, error) { //nolint: dupl // FALSE POSITIVE: this function is NOT a duplicate
	if d.offset >= len(d.p) {
		return nil, io.ErrUnexpectedEOF
	}
	var (
		l, nRead uint64
		n        int
		err      error
		res      []int32
	)
	l, n, err = DecodeVarint(d.p[d.offset:])
	if err != nil {
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	}
	if n == 0 {
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
	}
	d.offset += n
	packedDataStart := d.offset
	for nRead < l {
		if d.offset >= len(d.p) {
			return nil, io.ErrUnexpectedEOF
		}
		v, n, err := DecodeVarint(d.p[d.offset:])
		if err != nil {
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
		}
		if n == 0 {
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
		}
		// ensure the result is within [-math.MaxInt32, math.MaxInt32] when converted to a signed value
		if v > math.MaxInt32 {
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrValueOverflow)
		}
		nRead += uint64(n)
		d.offset += n
		res = append(res, int32(v))
	}
	if nRead != l {
		return nil, fmt.Errorf("invalid packed data at byte %d: %w", packedDataStart, ErrInvalidPackedData)
	}
	return res, nil
}

// DecodePackedInt64 decodes a packed encoded list of 64-bit integers from the stream and returns the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodePackedInt64() ([]int64, error) { //nolint: dupl // FALSE POSITIVE: this function is NOT a duplicate
	if d.offset >= len(d.p) {
		return nil, io.ErrUnexpectedEOF
	}
	var (
		l, nRead uint64
		n        int
		err      error
		res      []int64
	)
	l, n, err = DecodeVarint(d.p[d.offset:])
	if err != nil {
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	}
	if n == 0 {
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
	}
	d.offset += n
	packedDataStart := d.offset
	for nRead < l {
		if d.offset >= len(d.p) {
			return nil, io.ErrUnexpectedEOF
		}
		v, n, err := DecodeVarint(d.p[d.offset:])
		if err != nil {
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
		}
		if n == 0 {
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
		}
		if v > math.MaxInt64 {
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrValueOverflow)
		}
		nRead += uint64(n)
		d.offset += n
		res = append(res, int64(v))
	}
	if nRead != l {
		return nil, fmt.Errorf("invalid packed data at byte %d: %w", packedDataStart, ErrInvalidPackedData)
	}
	return res, nil
}

// DecodePackedUint32 decodes a packed encoded list of unsigned 32-bit integers from the stream and
// returns the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodePackedUint32() ([]uint32, error) { //nolint: dupl // FALSE POSITIVE: this function is NOT a duplicate
	if d.offset >= len(d.p) {
		return nil, io.ErrUnexpectedEOF
	}
	var (
		l, nRead uint64
		n        int
		err      error
		res      []uint32
	)
	l, n, err = DecodeVarint(d.p[d.offset:])
	if err != nil {
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	}
	if n == 0 {
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
	}
	d.offset += n
	packedDataStart := d.offset
	for nRead < l {
		if d.offset >= len(d.p) {
			return nil, io.ErrUnexpectedEOF
		}
		v, n, err := DecodeVarint(d.p[d.offset:])
		if err != nil {
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
		}
		if n == 0 {
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
		}
		// ensure the result is within [0, math.MaxUInt32]
		if v > math.MaxUint32 {
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrValueOverflow)
		}
		nRead += uint64(n)
		d.offset += n
		res = append(res, uint32(v))
	}
	if nRead != l {
		return nil, fmt.Errorf("invalid packed data at byte %d: %w", packedDataStart, ErrInvalidPackedData)
	}
	return res, nil
}

// DecodePackedUint64 decodes a packed encoded list of unsigned 64-bit integers from the stream and
// returns the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodePackedUint64() ([]uint64, error) { //nolint: dupl // FALSE POSITIVE: this function is NOT a duplicate
	if d.offset >= len(d.p) {
		return nil, io.ErrUnexpectedEOF
	}
	var (
		l, nRead uint64
		n        int
		err      error
		res      []uint64
	)
	l, n, err = DecodeVarint(d.p[d.offset:])
	if err != nil {
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	}
	if n == 0 {
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
	}
	d.offset += n
	packedDataStart := d.offset
	for nRead < l {
		if d.offset >= len(d.p) {
			return nil, io.ErrUnexpectedEOF
		}
		v, n, err := DecodeVarint(d.p[d.offset:])
		if err != nil {
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
		}
		if n == 0 {
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
		}
		nRead += uint64(n)
		d.offset += n
		res = append(res, v)
	}
	if nRead != l {
		return nil, fmt.Errorf("invalid packed data at byte %d: %w", packedDataStart, ErrInvalidPackedData)
	}
	return res, nil
}

// DecodePackedSint32 decodes a packed encoded list of 32-bit signed integers from the stream and returns
// the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodePackedSint32() ([]int32, error) { //nolint: dupl // FALSE POSITIVE: this function is NOT a duplicate
	if d.offset >= len(d.p) {
		return nil, io.ErrUnexpectedEOF
	}
	var (
		l, nRead uint64
		n        int
		err      error
		res      []int32
	)
	l, n, err = DecodeVarint(d.p[d.offset:])
	if err != nil {
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	}
	if n == 0 {
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
	}
	d.offset += n
	packedDataStart := d.offset
	for nRead < l {
		if d.offset >= len(d.p) {
			return nil, io.ErrUnexpectedEOF
		}
		v, n, err := DecodeZigZag32(d.p[d.offset:])
		if err != nil {
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
		}
		if n == 0 {
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
		}
		nRead += uint64(n)
		d.offset += n
		res = append(res, v)
	}
	if nRead != l {
		return nil, fmt.Errorf("invalid packed data at byte %d: %w", packedDataStart, ErrInvalidPackedData)
	}
	return res, nil
}

// DecodePackedSint64 decodes a packed encoded list of 64-bit signed integers from the stream and returns
// the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodePackedSint64() ([]int64, error) { //nolint: dupl // FALSE POSITIVE: this function is NOT a duplicate
	if d.offset >= len(d.p) {
		return nil, io.ErrUnexpectedEOF
	}
	var (
		l, nRead uint64
		n        int
		err      error
		res      []int64
	)
	l, n, err = DecodeVarint(d.p[d.offset:])
	if err != nil {
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	}
	if n == 0 {
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
	}
	d.offset += n
	packedDataStart := d.offset
	for nRead < l {
		if d.offset >= len(d.p) {
			return nil, io.ErrUnexpectedEOF
		}
		v, n, err := DecodeZigZag64(d.p[d.offset:])
		if err != nil {
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
		}
		if n == 0 {
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
		}
		nRead += uint64(n)
		d.offset += n
		res = append(res, v)
	}
	if nRead != l {
		return nil, fmt.Errorf("invalid packed data at byte %d: %w", packedDataStart, ErrInvalidPackedData)
	}
	return res, nil
}

// DecodePackedFixed32 decodes a packed encoded list of 32-bit fixed-width integers from the stream
// and returns the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodePackedFixed32() ([]uint32, error) { //nolint: dupl // FALSE POSITIVE: this function is NOT a duplicate
	if d.offset >= len(d.p) {
		return nil, io.ErrUnexpectedEOF
	}
	var (
		l, nRead uint64
		n        int
		err      error
		res      []uint32
	)
	l, n, err = DecodeVarint(d.p[d.offset:])
	if err != nil {
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	}
	if n == 0 {
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
	}
	d.offset += n
	packedDataStart := d.offset
	for nRead < l {
		if d.offset >= len(d.p) {
			return nil, io.ErrUnexpectedEOF
		}
		v, n, err := DecodeFixed32(d.p[d.offset:])
		if err != nil {
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
		}
		if n == 0 {
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
		}
		nRead += uint64(n)
		d.offset += n
		res = append(res, v)
	}
	if nRead != l {
		return nil, fmt.Errorf("invalid packed data at byte %d: %w", packedDataStart, ErrInvalidPackedData)
	}
	return res, nil
}

// DecodePackedFixed64 decodes a packed encoded list of 64-bit fixed-width integers from the stream
// and returns the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodePackedFixed64() ([]uint64, error) { //nolint: dupl // FALSE POSITIVE: this function is NOT a duplicate
	if d.offset >= len(d.p) {
		return nil, io.ErrUnexpectedEOF
	}
	var (
		l, nRead uint64
		n        int
		err      error
		res      []uint64
	)
	l, n, err = DecodeVarint(d.p[d.offset:])
	if err != nil {
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	}
	if n == 0 {
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
	}
	d.offset += n
	packedDataStart := d.offset
	for nRead < l {
		if d.offset >= len(d.p) {
			return nil, io.ErrUnexpectedEOF
		}
		v, n, err := DecodeFixed64(d.p[d.offset:])
		if err != nil {
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
		}
		if n == 0 {
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
		}
		nRead += uint64(n)
		d.offset += n
		res = append(res, v)
	}
	if nRead != l {
		return nil, fmt.Errorf("invalid packed data at byte %d: %w", packedDataStart, ErrInvalidPackedData)
	}
	return res, nil
}

// DecodePackedFloat32 decodes a packed encoded list of 32-bit floating point numbers from the stream
// and returns the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodePackedFloat32() ([]float32, error) { //nolint: dupl // FALSE POSITIVE: this function is NOT a duplicate
	if d.offset >= len(d.p) {
		return nil, io.ErrUnexpectedEOF
	}
	var (
		l, nRead uint64
		n        int
		err      error
		res      []float32
	)
	l, n, err = DecodeVarint(d.p[d.offset:])
	if err != nil {
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	}
	if n == 0 {
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
	}
	d.offset += n
	packedDataStart := d.offset
	res = make([]float32, 0, l/4)
	for nRead < l {
		if d.offset >= len(d.p) {
			return nil, io.ErrUnexpectedEOF
		}
		v := binary.LittleEndian.Uint32(d.p[d.offset:])
		nRead += 4
		d.offset += 4
		res = append(res, math.Float32frombits(v))
	}
	if nRead != l {
		return nil, fmt.Errorf("invalid packed data at byte %d: %w", packedDataStart, ErrInvalidPackedData)
	}
	return res, nil
}

// DecodePackedFloat64 decodes a packed encoded list of 64-bit floating point numbers from the stream
// and returns the value.
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodePackedFloat64() ([]float64, error) {
	if d.offset >= len(d.p) {
		return nil, io.ErrUnexpectedEOF
	}
	var (
		l, nRead uint64
		n        int
		err      error
		res      []float64
	)
	l, n, err = DecodeVarint(d.p[d.offset:])
	if err != nil {
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	}
	if n == 0 {
		return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
	}
	d.offset += n
	packedDataStart := d.offset
	for nRead < l {
		if d.offset >= len(d.p) {
			return nil, io.ErrUnexpectedEOF
		}
		v := binary.LittleEndian.Uint64(d.p[d.offset:])
		nRead += 8
		d.offset += 8
		res = append(res, math.Float64frombits(v))
	}
	if nRead != l {
		return nil, fmt.Errorf("invalid packed data at byte %d: %w", packedDataStart, ErrInvalidPackedData)
	}
	return res, nil
}

// DecodeNested decodes a nested Protobuf message from the stream into m.  If m satisfies our csproto.Unmarshaler
// interface its Unmarshal() method will be called.  Otherwise, this method delegates to Marshal().
//
// io.ErrUnexpectedEOF is returned if the operation would read past the end of the data.
func (d *Decoder) DecodeNested(m interface{}) error {
	if d.offset >= len(d.p) {
		return io.ErrUnexpectedEOF
	}

	l, n, err := DecodeVarint(d.p[d.offset:])
	switch {
	case err != nil:
		return fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
	case n == 0:
		return fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
	case l > maxFieldLen:
		return fmt.Errorf("invalid length (%d) for length-delimited field at byte %d: %w", l, d.offset, ErrLenOverflow)
	default:
		// length is good
	}

	nb := int(l)
	if nb < 0 {
		return fmt.Errorf("csproto: bad byte length %d at byte %d", nb, d.offset)
	}
	if d.offset+n+nb > len(d.p) {
		return io.ErrUnexpectedEOF
	}
	switch tv := m.(type) {
	case Unmarshaler:
		if err := tv.Unmarshal(d.p[d.offset+n : d.offset+n+nb]); err != nil {
			return err
		}
	default:
		if err := Unmarshal(d.p[d.offset+n:d.offset+n+nb], m); err != nil {
			return err
		}
	}
	d.offset += n + nb
	return nil
}

// Skip skips over the encoded field value at the current offset, returning the raw bytes so that the
// caller can decide what to do with the data.
//
// The tag and wire type are validated against the provided values and a DecoderSkipError error is
// returned if they do not match.  This check is skipped when using "fast" mode.
//
// io.ErrUnexpectedEOF is returned if the operation would advance past the end of the data.
func (d *Decoder) Skip(tag int, wt WireType) ([]byte, error) {
	if d.offset >= len(d.p) {
		return nil, io.ErrUnexpectedEOF
	}
	sz := SizeOfTagKey(tag)
	bof := d.offset - sz
	// account for skipping the first field
	if bof < 0 {
		bof = 0
	}
	// validate that the field we're skipping matches the specified tag and wire type
	// . skip validation in fast mode
	if d.mode == DecoderModeSafe {
		v, n, err := DecodeVarint(d.p[bof:])
		if err != nil {
			return nil, fmt.Errorf("invalid data at byte %d: %w", bof, err)
		}
		if n != sz {
			return nil, fmt.Errorf("invalid data at byte %d: %w", bof, ErrInvalidVarintData)
		}
		thisTag, thisWireType := int(v>>3), WireType(v&0x7)
		if thisTag != tag || thisWireType != wt {
			return nil, &DecoderSkipError{
				ExpectedTag:      tag,
				ExpectedWireType: wt,
				ActualTag:        thisTag,
				ActualWireType:   thisWireType,
			}
		}
	}
	skipped := 0
	switch wt {
	case WireTypeVarint:
		_, n, err := DecodeVarint(d.p[d.offset:])
		if err != nil {
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
		}
		skipped = n
	case WireTypeFixed64:
		skipped = 8
	case WireTypeLengthDelimited:
		l, n, err := DecodeVarint(d.p[d.offset:])
		switch {
		case err != nil:
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, err)
		case n == 0:
			return nil, fmt.Errorf("invalid data at byte %d: %w", d.offset, ErrInvalidVarintData)
		case l > maxFieldLen:
			return nil, fmt.Errorf("invalid length (%d) for length-delimited field at byte %d: %w", l, d.offset, ErrLenOverflow)
		default:
			// length is good
		}

		skipped = n + int(l)

	case WireTypeFixed32:
		skipped = 4
	default:
		return nil, fmt.Errorf("unsupported wire type value %v at byte %d", wt, d.offset)
	}
	if d.offset+skipped > len(d.p) {
		return nil, io.ErrUnexpectedEOF
	}
	d.offset += skipped
	return d.p[bof:d.offset], nil
}

// DecodeVarint reads a base-128 [varint encoded] integer from p and returns the value and the number
// of bytes that were consumed.
//
// [varint encoded]: https://developers.google.com/protocol-buffers/docs/encoding#varints
func DecodeVarint(p []byte) (v uint64, n int, err error) {
	if len(p) == 0 {
		return 0, 0, ErrInvalidVarintData
	}
	// single-byte values don't need any processing
	if p[0] < 0x80 {
		return uint64(p[0]), 1, nil
	}
	// 2-9 byte values
	if len(p) < 10 {
		for shift := uint(0); shift < 64; shift += 7 {
			if n >= len(p) {
				return 0, 0, io.ErrUnexpectedEOF
			}
			b := uint64(p[n])
			n++
			v |= (b & 0x7f << shift)
			if (b & 0x80) == 0 {
				return v, n, nil
			}
		}
		return 0, 0, ErrValueOverflow
	}
	// 10-byte values
	// . we already know the first byte has the high-bit set, so grab it's value then walk bytes 2-10
	v = uint64(p[0] & 0x7f)
	for i, shift := 1, 7; i < 10; i, shift = i+1, shift+7 {
		b := uint64(p[i])
		v |= (b & 0x7f) << shift
		if (b & 0x80) == 0 {
			return v, i + 1, nil
		}
	}
	return 0, 0, ErrValueOverflow
}

// DecodeZigZag32 reads a base-128 [zig zag encoded] 32-bit integer from p and returns the value and the
// number of bytes that were consumed.
//
// [zig zag encoded]: https://developers.google.com/protocol-buffers/docs/encoding#signed-ints
func DecodeZigZag32(p []byte) (v int32, n int, err error) {
	var dv uint64
	dv, n, err = DecodeVarint(p)
	if err != nil {
		return 0, 0, err
	}
	if n == 0 {
		return 0, 0, ErrInvalidVarintData
	}
	dv = uint64((uint32(dv) >> 1) ^ uint32((int32(dv&1)<<31)>>31))
	return int32(dv), n, nil
}

// DecodeZigZag64 reads a base-128 [zig zag encoded] 64-bit integer from p and returns the value and the
// number of bytes that were consumed.
//
// [zig zag encoded]: https://developers.google.com/protocol-buffers/docs/encoding#signed-ints
func DecodeZigZag64(p []byte) (v int64, n int, err error) {
	var dv uint64
	dv, n, err = DecodeVarint(p)
	if err != nil {
		return 0, 0, err
	}
	if n == 0 {
		return 0, 0, ErrInvalidVarintData
	}
	dv = (dv >> 1) ^ uint64((int64(dv&1)<<63)>>63)
	return int64(dv), n, nil
}

// DecodeFixed32 reads a Protobuf fixed32 or float (4 byte, little endian) value from p and returns the value
// and the number of bytes consumed.
func DecodeFixed32(p []byte) (v uint32, n int, err error) {
	if len(p) < 4 {
		return 0, 0, io.ErrUnexpectedEOF
	}
	// we only care about the first 4 bytes, so help the compiler eliminate bounds checks
	p = p[:4]
	v = uint32(p[0])
	v |= uint32(p[1]) << 8
	v |= uint32(p[2]) << 16
	v |= uint32(p[3]) << 24
	return v, 4, nil
}

// DecodeFixed64 reads a Protobuf fixed64 or double (8 byte, little endian) value from p and returns the value
// and the number of bytes consumed.
func DecodeFixed64(p []byte) (v uint64, n int, err error) {
	if len(p) < 8 {
		return 0, 0, io.ErrUnexpectedEOF
	}
	// we only care about the first 8 bytes, so help the compiler eliminate bounds checks
	p = p[:8]
	v = uint64(p[0])
	v |= uint64(p[1]) << 8
	v |= uint64(p[2]) << 16
	v |= uint64(p[3]) << 24
	v |= uint64(p[4]) << 32
	v |= uint64(p[5]) << 40
	v |= uint64(p[6]) << 48
	v |= uint64(p[7]) << 56
	return v, 8, nil
}

// DecoderSkipError defines an error returned by the decoder's Skip() method when the specified tag and
// wire type do not match the data in the stream at the current decoder offset.
type DecoderSkipError struct {
	ExpectedTag      int
	ExpectedWireType WireType
	ActualTag        int
	ActualWireType   WireType
}

// Error satisfies the error interface
func (e *DecoderSkipError) Error() string {
	return fmt.Sprintf("unexpected tag/wire type (%d, %s), expected (%d, %s)", e.ActualTag, e.ActualWireType, e.ExpectedTag, e.ExpectedWireType)
}
