// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path"
	"testing"

	promencoding "github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/stretchr/testify/require"
)

func TestDecbufFactory_NewDecbufAtChecked(t *testing.T) {
	table := crc32.MakeTable(crc32.Castagnoli)

	t.Run("invalid CRC", func(t *testing.T) {
		contentLength := 4096
		enc := promencoding.Encbuf{}

		for i := 0; i < contentLength; i++ {
			enc.PutByte(0x01)
		}

		enc.PutBytes([]byte{0, 0, 0, 0})

		factory := createDecbufFactoryWithBytes(t, contentLength, enc.Get())
		d := factory.NewDecbufAtChecked(0, table)
		t.Cleanup(func() {
			require.NoError(t, factory.Close(d))
		})

		require.ErrorIs(t, d.Err(), ErrInvalidChecksum)
	})

	t.Run("invalid length", func(t *testing.T) {
		contentLength := 4096
		enc := promencoding.Encbuf{}

		for i := 0; i < contentLength; i++ {
			enc.PutByte(0x01)
		}

		enc.PutHash(crc32.New(table))

		factory := createDecbufFactoryWithBytes(t, contentLength+1000, enc.Get())
		d := factory.NewDecbufAtChecked(0, table)
		t.Cleanup(func() {
			require.NoError(t, factory.Close(d))
		})

		require.ErrorIs(t, d.Err(), ErrInvalidSize)
	})

	t.Run("happy path", func(t *testing.T) {
		contentLength := 4096
		enc := promencoding.Encbuf{}

		for i := 0; i < contentLength; i++ {
			enc.PutByte(0x01)
		}

		enc.PutHash(crc32.New(table))

		factory := createDecbufFactoryWithBytes(t, contentLength, enc.Get())
		d := factory.NewDecbufAtChecked(0, table)
		t.Cleanup(func() {
			require.NoError(t, factory.Close(d))
		})

		require.NoError(t, d.Err())
		require.Equal(t, contentLength+4, d.Len())
	})
}

func TestDecbufFactory_NewDecbufAtUnchecked(t *testing.T) {
	table := crc32.MakeTable(crc32.Castagnoli)

	t.Run("happy path", func(t *testing.T) {
		contentLength := 4096
		enc := promencoding.Encbuf{}

		for i := 0; i < contentLength; i++ {
			enc.PutByte(0x01)
		}

		enc.PutHash(crc32.New(table))

		factory := createDecbufFactoryWithBytes(t, contentLength, enc.Get())
		d := factory.NewDecbufAtUnchecked(0)
		t.Cleanup(func() {
			require.NoError(t, factory.Close(d))
		})

		require.NoError(t, d.Err())
		require.Equal(t, contentLength+4, d.Len())
	})
}

func TestDecbufFactory_NewDecbufRaw(t *testing.T) {
	table := crc32.MakeTable(crc32.Castagnoli)

	t.Run("happy path", func(t *testing.T) {
		contentLength := 4096
		enc := promencoding.Encbuf{}

		for i := 0; i < contentLength; i++ {
			enc.PutByte(0x01)
		}

		enc.PutHash(crc32.New(table))

		factory := createDecbufFactoryWithBytes(t, contentLength, enc.Get())
		d := factory.NewRawDecbuf()
		t.Cleanup(func() {
			require.NoError(t, factory.Close(d))
		})

		require.NoError(t, d.Err())
		require.Equal(t, 4+contentLength+4, d.Len())
	})
}
func createDecbufFactoryWithBytes(t testing.TB, len int, b []byte) *DecbufFactory {
	// Prepend the contents of the buffer with the length of the content portion
	// which does not include the trailing 4 bytes for a CRC 32.
	lenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBytes, uint32(len))
	b = append(lenBytes, b...)

	dir := t.TempDir()
	filePath := path.Join(dir, "test-file")
	require.NoError(t, os.WriteFile(filePath, b, 0700))

	return NewDecbufFactory(filePath)
}
func BenchmarkDecbufFactory_NewDecbufAtUnchecked(t *testing.B) {
	table := crc32.MakeTable(crc32.Castagnoli)
	contentLength := 4096
	enc := promencoding.Encbuf{}

	for i := 0; i < contentLength; i++ {
		enc.PutByte(0x01)
	}

	enc.PutHash(crc32.New(table))
	factory := createDecbufFactoryWithBytes(t, contentLength, enc.Get())
	t.ResetTimer()

	for i := 0; i < t.N; i++ {
		d := factory.NewDecbufAtUnchecked(0)

		if err := d.Err(); err != nil {
			require.NoError(t, err)
		}

		if err := factory.Close(d); err != nil {
			require.NoError(t, err)
		}
	}
}
