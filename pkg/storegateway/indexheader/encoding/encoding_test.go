// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"strconv"
	"testing"

	prom_encoding "github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/stretchr/testify/require"
)

func TestDecbuf_Be32HappyPath(t *testing.T) {
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
			require.Equal(t, 4, dec.Len())

			actual := dec.Be32()
			require.NoError(t, dec.Err())
			require.Equal(t, c, actual)
			require.Equal(t, 0, dec.Len())
		})
	}
}

func TestDecbuf_Be32EmptyBuffer(t *testing.T) {
	dec := NewDecbufRaw(realByteSlice(nil))
	_ = dec.Be32()
	require.ErrorIs(t, dec.Err(), ErrInvalidSize)
}

func TestDecbuf_Be32InsufficientBuffer(t *testing.T) {
	enc := prom_encoding.Encbuf{}
	enc.PutBE32(0xFFFF_FFFF)

	dec := NewDecbufRaw(realByteSlice(enc.Get()[:2]))
	_ = dec.Be32()
	require.ErrorIs(t, dec.Err(), ErrInvalidSize)
}

func FuzzDecbuf_Be32(f *testing.F) {
	f.Add(uint32(0))
	f.Add(uint32(1))
	f.Add(uint32(0xFFFF_FFFF))

	f.Fuzz(func(t *testing.T, n uint32) {
		enc := prom_encoding.Encbuf{}
		enc.PutBE32(n)

		dec := NewDecbufRaw(realByteSlice(enc.Get()))
		require.Equal(t, 4, dec.Len())

		actual := dec.Be32()
		require.NoError(t, dec.Err())
		require.Equal(t, n, actual)
		require.Equal(t, 0, dec.Len())
	})
}

func TestDecbuf_Be32intHappyPath(t *testing.T) {
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
			require.Equal(t, 4, dec.Len())

			actual := dec.Be32int()
			require.NoError(t, dec.Err())
			require.Equal(t, c, actual)
			require.Equal(t, 0, dec.Len())
		})
	}
}

func TestDecbuf_Be32intEmptyBuffer(t *testing.T) {
	dec := NewDecbufRaw(realByteSlice(nil))
	_ = dec.Be32int()
	require.ErrorIs(t, dec.Err(), ErrInvalidSize)
}

func TestDecbuf_Be32intInsufficientBuffer(t *testing.T) {
	enc := prom_encoding.Encbuf{}
	enc.PutBE32int(0xFFFF_FFFF)

	dec := NewDecbufRaw(realByteSlice(enc.Get()[:2]))
	_ = dec.Be32int()
	require.ErrorIs(t, dec.Err(), ErrInvalidSize)
}

func FuzzDecbuf_Be32int(f *testing.F) {
	f.Add(0)
	f.Add(1)
	f.Add(0xFFFF_FFFF)

	f.Fuzz(func(t *testing.T, n int) {
		if n < 0 || n > 0xFFFF_FFFF {
			t.Skip()
		}

		enc := prom_encoding.Encbuf{}
		enc.PutBE32int(n)

		dec := NewDecbufRaw(realByteSlice(enc.Get()))
		require.Equal(t, 4, dec.Len())

		actual := dec.Be32int()
		require.NoError(t, dec.Err())
		require.Equal(t, n, actual)
		require.Equal(t, 0, dec.Len())
	})
}

func TestDecbuf_Be64HappyPath(t *testing.T) {
	cases := []uint64{
		0,
		1,
		0xFFFF_FFFF_FFFF_FFFF,
	}

	for _, c := range cases {
		t.Run(strconv.FormatUint(c, 10), func(t *testing.T) {
			enc := prom_encoding.Encbuf{}
			enc.PutBE64(c)

			dec := NewDecbufRaw(realByteSlice(enc.Get()))
			require.Equal(t, 8, dec.Len())

			actual := dec.Be64()
			require.NoError(t, dec.Err())
			require.Equal(t, c, actual)
			require.Equal(t, 0, dec.Len())
		})
	}
}

func TestDecbuf_Be64EmptyBuffer(t *testing.T) {
	dec := NewDecbufRaw(realByteSlice(nil))
	_ = dec.Be64()
	require.ErrorIs(t, dec.Err(), ErrInvalidSize)
}

func TestDecbuf_Be64InsufficientBuffer(t *testing.T) {
	dec := NewDecbufRaw(realByteSlice([]byte{0x01}))
	_ = dec.Be64()
	require.ErrorIs(t, dec.Err(), ErrInvalidSize)
}

func FuzzDecbuf_Be64(f *testing.F) {
	f.Add(uint64(0))
	f.Add(uint64(1))
	f.Add(uint64(0xFFFF_FFFF_FFFF_FFFF))

	f.Fuzz(func(t *testing.T, n uint64) {
		enc := prom_encoding.Encbuf{}
		enc.PutBE64(n)

		dec := NewDecbufRaw(realByteSlice(enc.Get()))
		require.Equal(t, 8, dec.Len())

		actual := dec.Be64()
		require.NoError(t, dec.Err())
		require.Equal(t, n, actual)
		require.Equal(t, 0, dec.Len())
	})
}

func TestDecbuf_SkipHappyPath(t *testing.T) {
	expected := uint32(0x12345678)

	enc := prom_encoding.Encbuf{}
	enc.PutBE32(0xFFFF_FFFF)
	enc.PutBE32(expected)

	dec := NewDecbufRaw(realByteSlice(enc.Get()))
	require.Equal(t, 8, dec.Len())

	dec.Skip(4)
	require.NoError(t, dec.Err())
	require.Equal(t, 4, dec.Len())

	actual := dec.Be32()
	require.NoError(t, dec.Err())
	require.Equal(t, expected, actual)
	require.Equal(t, 0, dec.Len())
}

func TestDecbuf_SkipEmptyBuffer(t *testing.T) {
	dec := NewDecbufRaw(realByteSlice(nil))
	dec.Skip(2)
	require.ErrorIs(t, dec.Err(), ErrInvalidSize)
}

func TestDecbuf_SkipInsufficientBuffer(t *testing.T) {
	dec := NewDecbufRaw(realByteSlice([]byte{0x01}))
	dec.Skip(2)
	require.ErrorIs(t, dec.Err(), ErrInvalidSize)
}

func TestDecbuf_UvarintHappyPath(t *testing.T) {
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
			require.Equal(t, c.bytes, dec.Len())

			actual := dec.Uvarint()
			require.NoError(t, dec.Err())
			require.Equal(t, c.value, actual)
			require.Equal(t, 0, dec.Len())
		})
	}
}

func TestDecbuf_UvarintEmptyBuffer(t *testing.T) {
	dec := NewDecbufRaw(realByteSlice(nil))
	_ = dec.Uvarint()
	require.ErrorIs(t, dec.Err(), ErrInvalidSize)
}

func TestDecbuf_UvarintInsufficientBuffer(t *testing.T) {
	enc := prom_encoding.Encbuf{}
	enc.PutUvarint(0xFFFF_FFFF)

	dec := NewDecbufRaw(realByteSlice(enc.Get()[:2]))
	_ = dec.Uvarint()
	require.ErrorIs(t, dec.Err(), ErrInvalidSize)
}

func FuzzDecbuf_Uvarint(f *testing.F) {
	f.Add(0)
	f.Add(1)
	f.Add(0xFFFF_FFFF)

	f.Fuzz(func(t *testing.T, n int) {
		if n < 0 {
			t.Skip()
		}

		enc := prom_encoding.Encbuf{}
		enc.PutUvarint(n)

		dec := NewDecbufRaw(realByteSlice(enc.Get()))
		actual := dec.Uvarint()
		require.NoError(t, dec.Err())
		require.Equal(t, n, actual)
		require.Equal(t, 0, dec.Len())
	})
}

func TestDecbuf_Uvarint64HappyPath(t *testing.T) {
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
			require.Equal(t, c.bytes, dec.Len())

			actual := dec.Uvarint64()
			require.NoError(t, dec.Err())
			require.Equal(t, c.value, actual)
			require.Equal(t, 0, dec.Len())
		})
	}
}

func TestDecbuf_Uvarint64EmptyBuffer(t *testing.T) {
	dec := NewDecbufRaw(realByteSlice(nil))
	_ = dec.Uvarint64()
	require.ErrorIs(t, dec.Err(), ErrInvalidSize)
}

func TestDecbuf_Uvarint64InsufficientBuffer(t *testing.T) {
	enc := prom_encoding.Encbuf{}
	enc.PutUvarint64(0xFFFF_FFFF)

	dec := NewDecbufRaw(realByteSlice(enc.Get()[:2]))
	_ = dec.Uvarint64()
	require.ErrorIs(t, dec.Err(), ErrInvalidSize)
}

func FuzzDecbuf_Uvarint64(f *testing.F) {
	f.Add(uint64(0))
	f.Add(uint64(1))
	f.Add(uint64(127))
	f.Add(uint64(128))
	f.Add(uint64(0xFFFF_FFFF))
	f.Add(uint64(0xFFFF_FFFF_FFFF_FFFF))

	f.Fuzz(func(t *testing.T, n uint64) {
		enc := prom_encoding.Encbuf{}
		enc.PutUvarint64(n)

		dec := NewDecbufRaw(realByteSlice(enc.Get()))
		actual := dec.Uvarint64()
		require.NoError(t, dec.Err())
		require.Equal(t, n, actual)
		require.Equal(t, 0, dec.Len())
	})
}

func TestDecbuf_UvarintBytesHappyPath(t *testing.T) {
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
			require.Equal(t, c.value, actual)
			require.Equal(t, 0, dec.Len())
		})
	}
}

func TestDecbuf_UvarintBytesEmptyBuffer(t *testing.T) {
	dec := NewDecbufRaw(realByteSlice(nil))
	_ = dec.UvarintBytes()
	require.ErrorIs(t, dec.Err(), ErrInvalidSize)
}

func TestDecbuf_UvarintBytesInsufficientBuffer(t *testing.T) {
	enc := prom_encoding.Encbuf{}
	enc.PutUvarintBytes([]byte("123456"))

	dec := NewDecbufRaw(realByteSlice(enc.Get()[:2]))
	_ = dec.UvarintBytes()
	require.ErrorIs(t, dec.Err(), ErrInvalidSize)
}

func FuzzDecbuf_UvarintBytes(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x12})
	f.Add([]byte("1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567"))
	f.Add([]byte("12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678"))

	f.Fuzz(func(t *testing.T, b []byte) {
		enc := prom_encoding.Encbuf{}
		enc.PutUvarintBytes(b)

		dec := NewDecbufRaw(realByteSlice(enc.Get()))
		actual := dec.UvarintBytes()
		require.NoError(t, dec.Err())
		require.Equal(t, b, actual)
		require.Equal(t, 0, dec.Len())
	})
}

func TestDecbuf_UvarintStrHappyPath(t *testing.T) {
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
			require.Equal(t, c.value, actual)
			require.Equal(t, 0, dec.Len())
		})
	}
}

func TestDecbuf_UvarintStrEmptyBuffer(t *testing.T) {
	dec := NewDecbufRaw(realByteSlice(nil))
	_ = dec.UvarintStr()
	require.ErrorIs(t, dec.Err(), ErrInvalidSize)
}

func TestDecbuf_UvarintStringInsufficientBuffer(t *testing.T) {
	enc := prom_encoding.Encbuf{}
	enc.PutUvarintStr("123456")

	dec := NewDecbufRaw(realByteSlice(enc.Get()[:2]))
	_ = dec.UvarintStr()
	require.ErrorIs(t, dec.Err(), ErrInvalidSize)
}

func FuzzDecbuf_UvarintStr(f *testing.F) {
	f.Add("")
	f.Add("a")
	f.Add("1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567")
	f.Add("12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678")

	f.Fuzz(func(t *testing.T, s string) {
		enc := prom_encoding.Encbuf{}
		enc.PutUvarintStr(s)

		dec := NewDecbufRaw(realByteSlice(enc.Get()))
		actual := dec.UvarintStr()
		require.NoError(t, dec.Err())
		require.Equal(t, s, actual)
		require.Equal(t, 0, dec.Len())
	})
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
