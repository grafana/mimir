// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"hash/crc32"
	"os"
	"path"
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	promencoding "github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/test"
)

func TestDecbuf_Be32HappyPath(t *testing.T) {
	cases := []uint32{
		0,
		1,
		0xFFFF_FFFF,
	}

	for _, c := range cases {
		t.Run(strconv.FormatInt(int64(c), 10), func(t *testing.T) {
			enc := promencoding.Encbuf{}
			enc.PutBE32(c)

			dec := createDecbufWithBytes(t, enc.Get())
			require.Equal(t, 4, dec.Len())
			require.Equal(t, 0, dec.Position())

			actual := dec.Be32()
			require.NoError(t, dec.Err())
			require.Equal(t, c, actual)
			require.Equal(t, 0, dec.Len())
			require.Equal(t, 4, dec.Position())
		})
	}
}

func TestDecbuf_Be32InsufficientBuffer(t *testing.T) {
	enc := promencoding.Encbuf{}
	enc.PutBE32(0xFFFF_FFFF)

	dec := createDecbufWithBytes(t, enc.Get()[:2])
	_ = dec.Be32()
	require.ErrorIs(t, dec.Err(), ErrInvalidSize)
}

func FuzzDecbuf_Be32(f *testing.F) {
	f.Add(uint32(0))
	f.Add(uint32(1))
	f.Add(uint32(0xFFFF_FFFF))

	f.Fuzz(func(t *testing.T, n uint32) {
		enc := promencoding.Encbuf{}
		enc.PutBE32(n)

		dec := createDecbufWithBytes(t, enc.Get())
		require.NoError(t, dec.Err())
		require.Equal(t, 4, dec.Len())
		require.Equal(t, 0, dec.Position())

		actual := dec.Be32()
		require.NoError(t, dec.Err())
		require.Equal(t, n, actual)
		require.Equal(t, 0, dec.Len())
		require.Equal(t, 4, dec.Position())
	})
}

func BenchmarkDecbuf_Be32(t *testing.B) {
	enc := promencoding.Encbuf{}
	enc.PutBE32(uint32(0))
	enc.PutBE32(uint32(1))
	enc.PutBE32(uint32(0xFFFF_FFFF))

	dec := createDecbufWithBytes(t, enc.Get())
	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		v1 := dec.Be32()
		v2 := dec.Be32()
		v3 := dec.Be32()

		if err := dec.Err(); err != nil {
			require.NoError(t, err)
		}

		if v1 != 0 || v2 != 0 || v3 != 0xFFFF_FFFF {
			require.Equal(t, uint32(0), v1)
			require.Equal(t, uint32(1), v2)
			require.Equal(t, uint32(0xFFFF_FFFF), v3)
		}

		dec.ResetAt(0)
	}
}

func TestDecbuf_Be32intHappyPath(t *testing.T) {
	cases := []int{
		0,
		1,
		0xFFFF_FFFF,
	}

	for _, c := range cases {
		t.Run(strconv.Itoa(c), func(t *testing.T) {
			enc := promencoding.Encbuf{}
			enc.PutBE32int(c)

			dec := createDecbufWithBytes(t, enc.Get())
			require.Equal(t, 4, dec.Len())
			require.Equal(t, 0, dec.Position())

			actual := dec.Be32int()
			require.NoError(t, dec.Err())
			require.Equal(t, c, actual)
			require.Equal(t, 0, dec.Len())
			require.Equal(t, 4, dec.Position())
		})
	}
}

func TestDecbuf_Be32intInsufficientBuffer(t *testing.T) {
	enc := promencoding.Encbuf{}
	enc.PutBE32int(0xFFFF_FFFF)

	dec := createDecbufWithBytes(t, enc.Get()[:2])
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

		enc := promencoding.Encbuf{}
		enc.PutBE32int(n)

		dec := createDecbufWithBytes(t, enc.Get())
		require.NoError(t, dec.Err())
		require.Equal(t, 4, dec.Len())
		require.Equal(t, 0, dec.Position())

		actual := dec.Be32int()
		require.NoError(t, dec.Err())
		require.Equal(t, n, actual)
		require.Equal(t, 0, dec.Len())
		require.Equal(t, 4, dec.Position())
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
			enc := promencoding.Encbuf{}
			enc.PutBE64(c)

			dec := createDecbufWithBytes(t, enc.Get())
			require.Equal(t, 8, dec.Len())
			require.Equal(t, 0, dec.Position())

			actual := dec.Be64()
			require.NoError(t, dec.Err())
			require.Equal(t, c, actual)
			require.Equal(t, 0, dec.Len())
			require.Equal(t, 8, dec.Position())
		})
	}
}

func TestDecbuf_Be64InsufficientBuffer(t *testing.T) {
	dec := createDecbufWithBytes(t, []byte{0x01})
	_ = dec.Be64()
	require.ErrorIs(t, dec.Err(), ErrInvalidSize)
}

func FuzzDecbuf_Be64(f *testing.F) {
	f.Add(uint64(0))
	f.Add(uint64(1))
	f.Add(uint64(0xFFFF_FFFF_FFFF_FFFF))

	f.Fuzz(func(t *testing.T, n uint64) {
		enc := promencoding.Encbuf{}
		enc.PutBE64(n)

		dec := createDecbufWithBytes(t, enc.Get())
		require.NoError(t, dec.Err())
		require.Equal(t, 8, dec.Len())
		require.Equal(t, 0, dec.Position())

		actual := dec.Be64()
		require.NoError(t, dec.Err())
		require.Equal(t, n, actual)
		require.Equal(t, 0, dec.Len())
		require.Equal(t, 8, dec.Position())
	})
}

func BenchmarkDecbuf_Be64(t *testing.B) {
	enc := promencoding.Encbuf{}
	enc.PutBE64(uint64(0))
	enc.PutBE64(uint64(1))
	enc.PutBE64(uint64(0xFFFF_FFFF_FFFF_FFFF))

	dec := createDecbufWithBytes(t, enc.Get())
	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		v1 := dec.Be64()
		v2 := dec.Be64()
		v3 := dec.Be64()

		if err := dec.Err(); err != nil {
			require.NoError(t, err)
		}

		if v1 != 0 || v2 != 0 || v3 != 0xFFFF_FFFF_FFFF_FFFF {
			require.Equal(t, uint64(0), v1)
			require.Equal(t, uint64(1), v2)
			require.Equal(t, uint64(0xFFFF_FFFF_FFFF_FFFF), v3)
		}

		dec.ResetAt(0)
	}
}

func TestDecbuf_SkipHappyPath(t *testing.T) {
	expected := uint32(0x12345678)

	enc := promencoding.Encbuf{}
	enc.PutBE32(0xFFFF_FFFF)
	enc.PutBE32(expected)

	dec := createDecbufWithBytes(t, enc.Get())
	require.Equal(t, 8, dec.Len())
	require.Equal(t, 0, dec.Position())

	dec.Skip(4)
	require.NoError(t, dec.Err())
	require.Equal(t, 4, dec.Len())
	require.Equal(t, 4, dec.Position())

	actual := dec.Be32()
	require.NoError(t, dec.Err())
	require.Equal(t, expected, actual)
	require.Equal(t, 0, dec.Len())
	require.Equal(t, 8, dec.Position())
}

func TestDecbuf_SkipMultipleBufferReads(t *testing.T) {
	// The underlying fileReader buffers the file 4k bytes at a time. Ensure
	// that we can skip multiple 4k chunks without ending up with a short read.
	bytes := make([]byte, 4096*5)
	for i := 0; i < len(bytes); i++ {
		bytes[i] = 0x01
	}

	dec := createDecbufWithBytes(t, bytes)
	dec.Skip(4096 * 4)

	require.NoError(t, dec.Err())
	require.Equal(t, dec.Len(), 4096)
	require.Equal(t, 4096*4, dec.Position())
	require.Equal(t, byte(0x01), dec.Byte())
}

func TestDecbuf_SkipInsufficientBuffer(t *testing.T) {
	dec := createDecbufWithBytes(t, []byte{0x01})
	dec.Skip(2)
	require.ErrorIs(t, dec.Err(), ErrInvalidSize)
}

func TestDecbuf_SkipUvarintBytesHappyPath(t *testing.T) {
	expected := uint32(0x567890AB)
	enc := promencoding.Encbuf{}
	enc.PutUvarintBytes([]byte{0x12, 0x34})
	enc.PutBE32(expected)

	dec := createDecbufWithBytes(t, enc.Get())
	require.Equal(t, 7, dec.Len())
	require.Equal(t, 0, dec.Position())

	dec.SkipUvarintBytes()
	require.NoError(t, dec.Err())
	require.Equal(t, 4, dec.Len())
	require.Equal(t, 3, dec.Position())

	actual := dec.Be32()
	require.NoError(t, dec.Err())
	require.Equal(t, expected, actual)
}

func TestDecbuf_SkipUvarintBytesEndOfFile(t *testing.T) {
	enc := promencoding.Encbuf{}
	enc.PutBE32(0x12345678)

	dec := createDecbufWithBytes(t, enc.Get())
	dec.Be32()
	require.NoError(t, dec.Err())
	require.Equal(t, 0, dec.Len())
	require.Equal(t, 4, dec.Position())

	dec.SkipUvarintBytes()
	require.ErrorIs(t, dec.Err(), ErrInvalidSize)
}

func TestDecbuf_SkipUvarintBytesOnlyHaveLength(t *testing.T) {
	enc := promencoding.Encbuf{}
	enc.PutUvarintBytes([]byte{0x12, 0x34})

	bytes := enc.Get()
	dec := createDecbufWithBytes(t, bytes[:len(bytes)-2])
	require.Equal(t, 1, dec.Len())
	require.Equal(t, 0, dec.Position())

	dec.SkipUvarintBytes()
	require.ErrorIs(t, dec.Err(), ErrInvalidSize)
}

func TestDecbuf_SkipUvarintBytesPartialValue(t *testing.T) {
	enc := promencoding.Encbuf{}
	enc.PutUvarintBytes([]byte{0x12, 0x34})

	bytes := enc.Get()
	dec := createDecbufWithBytes(t, bytes[:len(bytes)-1])
	require.Equal(t, 2, dec.Len())
	require.Equal(t, 0, dec.Position())

	dec.SkipUvarintBytes()
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
			enc := promencoding.Encbuf{}
			enc.PutUvarint(c.value)

			dec := createDecbufWithBytes(t, enc.Get())
			require.Equal(t, c.bytes, dec.Len())
			require.Equal(t, 0, dec.Position())

			actual := dec.Uvarint()
			require.NoError(t, dec.Err())
			require.Equal(t, c.value, actual)
			require.Equal(t, 0, dec.Len())
			require.Equal(t, c.bytes, dec.Position())
		})
	}
}

func TestDecbuf_UvarintInsufficientBuffer(t *testing.T) {
	enc := promencoding.Encbuf{}
	enc.PutUvarint(0xFFFF_FFFF)

	dec := createDecbufWithBytes(t, enc.Get()[:2])
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

		enc := promencoding.Encbuf{}
		enc.PutUvarint(n)

		dec := createDecbufWithBytes(t, enc.Get())
		require.NoError(t, dec.Err())
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
			enc := promencoding.Encbuf{}
			enc.PutUvarint64(c.value)

			dec := createDecbufWithBytes(t, enc.Get())
			require.Equal(t, c.bytes, dec.Len())
			require.Equal(t, 0, dec.Position())

			actual := dec.Uvarint64()
			require.NoError(t, dec.Err())
			require.Equal(t, c.value, actual)
			require.Equal(t, 0, dec.Len())
			require.Equal(t, c.bytes, dec.Position())
		})
	}
}

func TestDecbuf_Uvarint64InsufficientBuffer(t *testing.T) {
	enc := promencoding.Encbuf{}
	enc.PutUvarint64(0xFFFF_FFFF)

	dec := createDecbufWithBytes(t, enc.Get()[:2])
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
		enc := promencoding.Encbuf{}
		enc.PutUvarint64(n)

		dec := createDecbufWithBytes(t, enc.Get())
		require.NoError(t, dec.Err())
		actual := dec.Uvarint64()
		require.NoError(t, dec.Err())
		require.Equal(t, n, actual)
		require.Equal(t, 0, dec.Len())
	})
}

func TestDecbuf_UnsafeUvarintBytesHappyPath(t *testing.T) {
	cases := []struct {
		name              string
		value             []byte
		encodedSizeLength int
	}{
		{name: "nil slice", value: []byte(nil), encodedSizeLength: 1},
		{name: "empty slice", value: []byte{}, encodedSizeLength: 1},
		{name: "single byte", value: []byte{0x12}, encodedSizeLength: 1},
		{name: "127 bytes", value: []byte("1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567"), encodedSizeLength: 1},
		{name: "128 bytes", value: []byte("12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678"), encodedSizeLength: 2},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			enc := promencoding.Encbuf{}
			enc.PutUvarintBytes(c.value)

			dec := createDecbufWithBytes(t, enc.Get())
			size := c.encodedSizeLength + len(c.value)
			require.Equal(t, size, dec.Len())
			require.Equal(t, 0, dec.Position())

			actual := dec.UnsafeUvarintBytes()
			require.NoError(t, dec.Err())
			require.Condition(t, test.EqualSlices(c.value, actual), "%#v != %#v", c.value, actual)
			require.Equal(t, 0, dec.Len())
			require.Equal(t, size, dec.Position())
		})
	}
}

func TestDecbuf_UnsafeUvarintBytesInsufficientBuffer(t *testing.T) {
	enc := promencoding.Encbuf{}
	enc.PutUvarintBytes([]byte("123456"))

	dec := createDecbufWithBytes(t, enc.Get()[:2])
	_ = dec.UnsafeUvarintBytes()
	require.ErrorIs(t, dec.Err(), ErrInvalidSize)
}

func TestDecbuf_UnsafeUvarintBytesSkipDoesNotCauseBufferFill(t *testing.T) {
	const (
		expectedSlices = 32
		expectedBytes  = 983
	)

	// This test verifies that when bytes are read in UnsafeUvarintBytes, the peek(n) and
	// subsequent skip(len(b)) does not cause a read from disk that invalidates the slice
	// returned. It does this by creating multiple uvarint byte slices in the encoding
	// buffer each with different content _and_ by ensuring there are more bytes written
	// in total than the size of the underlying buffer (currently 4k).

	enc := promencoding.Encbuf{}
	for base := 0; base < expectedSlices; base++ {
		var bytes []byte
		for i := 0; i < expectedBytes; i++ {
			bytes = append(bytes, byte(base))
		}

		enc.PutUvarintBytes(bytes)
	}

	dec := createDecbufWithBytes(t, enc.Get())
	for base := 0; base < expectedSlices; base++ {
		bytes := dec.UnsafeUvarintBytes()

		require.NoError(t, dec.Err())
		require.Len(t, bytes, expectedBytes)

		for _, v := range bytes {
			require.Equal(t, byte(base), v)
		}
	}
}

func FuzzDecbuf_UnsafeUvarintBytes(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x12})
	f.Add([]byte("1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567"))
	f.Add([]byte("12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678"))

	f.Fuzz(func(t *testing.T, b []byte) {
		enc := promencoding.Encbuf{}
		enc.PutUvarintBytes(b)

		dec := createDecbufWithBytes(t, enc.Get())
		require.NoError(t, dec.Err())
		actual := dec.UnsafeUvarintBytes()
		require.NoError(t, dec.Err())
		require.Condition(t, test.EqualSlices(b, actual), "%#v != %#v", b, actual)
		require.Equal(t, 0, dec.Len())
	})
}

func BenchmarkDecbuf_UnsafeUvarintBytes(t *testing.B) {
	// 127 bytes, the varint size will be 1 byte.
	val := []byte("1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567")
	enc := promencoding.Encbuf{}
	enc.PutUvarintBytes(val)

	dec := createDecbufWithBytes(t, enc.Get())
	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		b := dec.UnsafeUvarintBytes()
		if err := dec.Err(); err != nil {
			require.NoError(t, err)
		}

		if len(b) != len(val) {
			require.Len(t, b, len(val))
		}

		dec.ResetAt(0)
		if err := dec.Err(); err != nil {
			require.NoError(t, err)
		}
	}
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
			enc := promencoding.Encbuf{}
			enc.PutUvarintStr(c.value)

			dec := createDecbufWithBytes(t, enc.Get())
			size := c.encodedSizeLength + len(c.value)
			require.Equal(t, size, dec.Len())
			require.Equal(t, 0, dec.Position())

			actual := dec.UvarintStr()
			require.NoError(t, dec.Err())
			require.Equal(t, c.value, actual)
			require.Equal(t, 0, dec.Len())
			require.Equal(t, size, dec.Position())
		})
	}
}

func TestDecbuf_UvarintStrInsufficientBuffer(t *testing.T) {
	enc := promencoding.Encbuf{}
	enc.PutUvarintStr("123456")

	dec := createDecbufWithBytes(t, enc.Get()[:2])
	_ = dec.UvarintStr()
	require.ErrorIs(t, dec.Err(), ErrInvalidSize)
}

func FuzzDecbuf_UvarintStr(f *testing.F) {
	f.Add("")
	f.Add("a")
	f.Add("1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567")
	f.Add("12345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678")

	f.Fuzz(func(t *testing.T, s string) {
		enc := promencoding.Encbuf{}
		enc.PutUvarintStr(s)

		dec := createDecbufWithBytes(t, enc.Get())
		require.NoError(t, dec.Err())
		actual := dec.UvarintStr()
		require.NoError(t, dec.Err())
		require.Equal(t, s, actual)
		require.Equal(t, 0, dec.Len())
	})
}

func TestDecbuf_Crc32(t *testing.T) {
	table := crc32.MakeTable(crc32.Castagnoli)

	t.Run("matches checksum (small buffer)", func(t *testing.T) {
		dec := createDecbufWithBytes(t, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x4f, 0x4d, 0xfb, 0xab})
		dec.CheckCrc32(table)
		require.NoError(t, dec.Err())
	})

	t.Run("matches checksum (buffer larger than single read)", func(t *testing.T) {
		bufferSize := 4*1024*1024 + 1
		enc := promencoding.Encbuf{}

		for enc.Len() < bufferSize {
			enc.PutByte(0x01)
		}

		enc.PutHash(crc32.New(crc32.MakeTable(crc32.Castagnoli)))

		dec := createDecbufWithBytes(t, enc.Get())
		dec.CheckCrc32(table)
		require.NoError(t, dec.Err())
	})

	t.Run("does not match checksum (small buffer)", func(t *testing.T) {
		dec := createDecbufWithBytes(t, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x4f, 0x4d, 0xfb, 0xff})
		dec.CheckCrc32(table)
		require.ErrorIs(t, dec.Err(), ErrInvalidChecksum)
	})

	t.Run("does not match checksum (buffer larger than single read)", func(t *testing.T) {
		bufferSize := 4*1024*1024 + 1
		enc := promencoding.Encbuf{}

		for enc.Len() < bufferSize {
			enc.PutByte(0x01)
		}

		b := enc.Get()
		b = append(b, 0x00, 0x01, 0x02, 0x03)

		dec := createDecbufWithBytes(t, b)
		dec.CheckCrc32(table)
		require.ErrorIs(t, dec.Err(), ErrInvalidChecksum)
	})

	t.Run("buffer only contains checksum", func(t *testing.T) {
		dec := createDecbufWithBytes(t, []byte{0x4f, 0x4d, 0xfb, 0xab})
		dec.CheckCrc32(table)
		require.ErrorIs(t, dec.Err(), ErrInvalidSize)
	})

	t.Run("buffer too short for checksum", func(t *testing.T) {
		dec := createDecbufWithBytes(t, []byte{0x4f, 0x4d, 0xfb})
		dec.CheckCrc32(table)
		require.ErrorIs(t, dec.Err(), ErrInvalidSize)
	})
}

func createDecbufWithBytes(t testing.TB, b []byte) Decbuf {
	dir := t.TempDir()
	filePath := path.Join(dir, "test-file")
	require.NoError(t, os.WriteFile(filePath, b, 0700))

	reg := prometheus.NewPedanticRegistry()
	factory := NewDecbufFactory(filePath, 0, NewDecbufFactoryMetrics(reg))
	decbuf := factory.NewRawDecbuf()
	t.Cleanup(func() {
		require.NoError(t, decbuf.Close())
	})

	require.NoError(t, decbuf.Err())
	return decbuf
}
