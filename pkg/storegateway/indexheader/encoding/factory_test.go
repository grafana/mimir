// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path"
	"testing"

	prom_encoding "github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/stretchr/testify/require"
)

func TestNewDecbufFactory_NewDecbufAtChecked(t *testing.T) {
	table := crc32.MakeTable(crc32.Castagnoli)

	t.Run("invalid CRC", func(t *testing.T) {
		contentLength := 4096
		enc := prom_encoding.Encbuf{}

		for i := 0; i < contentLength; i++ {
			enc.PutByte(0x01)
		}

		enc.PutBytes([]byte{0, 0, 0, 0})

		factory := createDecbufFactoryWithBytes(t, contentLength, enc.Get())
		d := factory.NewDecbufAtChecked(0, table)
		t.Cleanup(func() {
			require.NoError(t, d.Close())
		})

		require.ErrorIs(t, d.Err(), ErrInvalidChecksum)
	})

	t.Run("invalid length", func(t *testing.T) {
		contentLength := 4096
		enc := prom_encoding.Encbuf{}

		for i := 0; i < contentLength; i++ {
			enc.PutByte(0x01)
		}

		enc.PutHash(crc32.New(table))

		factory := createDecbufFactoryWithBytes(t, contentLength+1000, enc.Get())
		d := factory.NewDecbufAtChecked(0, table)
		t.Cleanup(func() {
			require.NoError(t, d.Close())
		})

		require.ErrorIs(t, d.Err(), ErrInvalidSize)
	})

	t.Run("happy path", func(t *testing.T) {
		contentLength := 4096
		enc := prom_encoding.Encbuf{}

		for i := 0; i < contentLength; i++ {
			enc.PutByte(0x01)
		}

		enc.PutHash(crc32.New(table))

		factory := createDecbufFactoryWithBytes(t, contentLength, enc.Get())
		d := factory.NewDecbufAtChecked(0, table)
		t.Cleanup(func() {
			require.NoError(t, d.Close())
		})

		require.NoError(t, d.Err())
		require.Equal(t, contentLength+4, d.Len())
	})
}

func TestNewDecbufFactory_NewDecbufAtUnchecked(t *testing.T) {
	table := crc32.MakeTable(crc32.Castagnoli)

	t.Run("happy path", func(t *testing.T) {
		contentLength := 4096
		enc := prom_encoding.Encbuf{}

		for i := 0; i < contentLength; i++ {
			enc.PutByte(0x01)
		}

		enc.PutHash(crc32.New(table))

		factory := createDecbufFactoryWithBytes(t, contentLength, enc.Get())
		d := factory.NewDecbufAtUnchecked(0)
		t.Cleanup(func() {
			require.NoError(t, d.Close())
		})

		require.NoError(t, d.Err())
		require.Equal(t, contentLength+4, d.Len())
	})
}

func TestNewDecbufFactory_NewDecbufRaw(t *testing.T) {
	table := crc32.MakeTable(crc32.Castagnoli)

	t.Run("happy path", func(t *testing.T) {
		contentLength := 4096
		enc := prom_encoding.Encbuf{}

		for i := 0; i < contentLength; i++ {
			enc.PutByte(0x01)
		}

		enc.PutHash(crc32.New(table))

		factory := createDecbufFactoryWithBytes(t, contentLength, enc.Get())
		d := factory.NewRawDecbuf()
		t.Cleanup(func() {
			require.NoError(t, d.Close())
		})

		require.NoError(t, d.Err())
		require.Equal(t, 4+contentLength+4, d.Len())
	})
}
func createDecbufFactoryWithBytes(t *testing.T, len int, b []byte) *DecbufFactory {
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
