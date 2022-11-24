// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"strconv"
	"testing"

	prom_encoding "github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/stretchr/testify/require"
)

func TestBe32(t *testing.T) {
	cases := []uint32{
		0,
		1,
		0xFFFF_FFFF,
	}

	for _, c := range cases {
		t.Run(strconv.FormatInt(int64(c), 10), func(t *testing.T) {
			enc := prom_encoding.Encbuf{}
			enc.PutBE32(c)

			dec := NewDecbufRaw(realByteSlice(enc.Get()))
			require.Equal(t, dec.Len(), 4)

			actual := dec.Be32()
			require.NoError(t, dec.Err())
			require.Equal(t, actual, c)
			require.Equal(t, dec.Len(), 0)
		})
	}
}

func TestBe32int(t *testing.T) {
	cases := []int{
		0,
		1,
		0xFFFF_FFFF,
	}

	for _, c := range cases {
		t.Run(strconv.Itoa(c), func(t *testing.T) {
			enc := prom_encoding.Encbuf{}
			enc.PutBE32int(c)

			dec := NewDecbufRaw(realByteSlice(enc.Get()))
			require.Equal(t, dec.Len(), 4)

			actual := dec.Be32int()
			require.NoError(t, dec.Err())
			require.Equal(t, actual, c)
			require.Equal(t, dec.Len(), 0)
		})
	}
}

func TestSkip(t *testing.T) {
	expected := uint32(0x12345678)

	enc := prom_encoding.Encbuf{}
	enc.PutBE32(0xFFFF_FFFF)
	enc.PutBE32(expected)

	dec := NewDecbufRaw(realByteSlice(enc.Get()))
	require.Equal(t, dec.Len(), 8)

	dec.Skip(4)
	require.NoError(t, dec.Err())
	require.Equal(t, dec.Len(), 4)

	actual := dec.Be32()
	require.NoError(t, dec.Err())
	require.Equal(t, actual, expected)
	require.Equal(t, dec.Len(), 0)
}

func TestUvarint(t *testing.T) {
	cases := []struct {
		value int
		bytes int
	}{
		{value: 0, bytes: 1},
		{value: 1, bytes: 1},
		{value: 127, bytes: 1},
		{value: 128, bytes: 2},
		{value: 0xFFFF_FFFF, bytes: 5},
	}

	for _, c := range cases {
		t.Run(strconv.Itoa(c.value), func(t *testing.T) {
			enc := prom_encoding.Encbuf{}
			enc.PutUvarint(c.value)

			dec := NewDecbufRaw(realByteSlice(enc.Get()))
			require.Equal(t, dec.Len(), c.bytes)

			actual := dec.Uvarint()
			require.NoError(t, dec.Err())
			require.Equal(t, actual, c.value)
			require.Equal(t, dec.Len(), 0)
		})
	}
}

func TestUvarint64(t *testing.T) {
	cases := []struct {
		value uint64
		bytes int
	}{
		{value: 0, bytes: 1},
		{value: 1, bytes: 1},
		{value: 127, bytes: 1},
		{value: 128, bytes: 2},
		{value: 0xFFFF_FFFF, bytes: 5},
		{value: 0xFFFF_FFFF_FFFF_FFFF, bytes: 10},
	}

	for _, c := range cases {
		t.Run(strconv.FormatUint(c.value, 10), func(t *testing.T) {
			enc := prom_encoding.Encbuf{}
			enc.PutUvarint64(c.value)

			dec := NewDecbufRaw(realByteSlice(enc.Get()))
			require.Equal(t, dec.Len(), c.bytes)

			actual := dec.Uvarint64()
			require.NoError(t, dec.Err())
			require.Equal(t, actual, c.value)
			require.Equal(t, dec.Len(), 0)
		})
	}
}

func TestUvarintBytes(t *testing.T) {
	cases := []struct {
		name              string
		value             []byte
		encodedSizeLength int
	}{
		{name: "empty slice", value: []byte{}, encodedSizeLength: 1},
		{name: "single byte", value: []byte{0x12}, encodedSizeLength: 1},
		{name: "127 bytes", value: []byte("1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567"), encodedSizeLength: 1},
		{name: "128 bytes", value: []byte("12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678"), encodedSizeLength: 2},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			enc := prom_encoding.Encbuf{}
			enc.PutUvarintBytes(c.value)

			dec := NewDecbufRaw(realByteSlice(enc.Get()))
			require.Equal(t, dec.Len(), c.encodedSizeLength+len(c.value))

			actual := dec.UvarintBytes()
			require.NoError(t, dec.Err())
			require.Equal(t, actual, c.value)
			require.Equal(t, dec.Len(), 0)
		})
	}
}

func TestUvarintString(t *testing.T) {
	cases := []struct {
		name              string
		value             string
		encodedSizeLength int
	}{
		{name: "empty string", value: "", encodedSizeLength: 1},
		{name: "single byte", value: "a", encodedSizeLength: 1},
		{name: "127 bytes", value: "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567", encodedSizeLength: 1},
		{name: "128 bytes", value: "12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678", encodedSizeLength: 2},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			enc := prom_encoding.Encbuf{}
			enc.PutUvarintStr(c.value)

			dec := NewDecbufRaw(realByteSlice(enc.Get()))
			require.Equal(t, dec.Len(), c.encodedSizeLength+len(c.value))

			actual := dec.UvarintStr()
			require.NoError(t, dec.Err())
			require.Equal(t, actual, c.value)
			require.Equal(t, dec.Len(), 0)
		})
	}
}

type realByteSlice []byte

func (b realByteSlice) Len() int {
	return len(b)
}

func (b realByteSlice) Range(start, end int) []byte {
	return b[start:end]
}

func (b realByteSlice) Sub(start, end int) ByteSlice {
	return b[start:end]
}
