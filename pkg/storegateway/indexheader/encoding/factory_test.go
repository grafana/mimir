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

const testContentSize = 4096

var table = crc32.MakeTable(crc32.Castagnoli)

func BenchmarkDecbufFactory_NewDecbufAtUnchecked(t *testing.B) {
	enc := createTestEncoder(testContentSize)
	enc.PutHash(crc32.New(table))

	factory := createDecbufFactoryWithBytes(t, 1, testContentSize, enc)
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

func TestDecbufFactory_NewDecbufAtChecked_InvalidCRC(t *testing.T) {
	enc := createTestEncoder(testContentSize)
	enc.PutBytes([]byte{0, 0, 0, 0})

	testDecbufFactory(t, testContentSize, enc, func(t *testing.T, factory *DecbufFactory) {
		d := factory.NewDecbufAtChecked(0, table)
		t.Cleanup(func() {
			require.NoError(t, factory.Close(d))
		})

		require.ErrorIs(t, d.Err(), ErrInvalidChecksum)
	})
}

func TestDecbufFactory_NewDecbufAtChecked_InvalidLength(t *testing.T) {
	enc := createTestEncoder(testContentSize)
	enc.PutHash(crc32.New(table))

	testDecbufFactory(t, testContentSize+1000, enc, func(t *testing.T, factory *DecbufFactory) {
		d := factory.NewDecbufAtChecked(0, table)
		t.Cleanup(func() {
			require.NoError(t, factory.Close(d))
		})

		require.ErrorIs(t, d.Err(), ErrInvalidSize)
	})
}

func TestDecbufFactory_NewDecbufAtChecked_HappyPath(t *testing.T) {
	enc := createTestEncoder(testContentSize)
	enc.PutHash(crc32.New(table))

	testDecbufFactory(t, testContentSize, enc, func(t *testing.T, factory *DecbufFactory) {
		d := factory.NewDecbufAtChecked(0, table)
		t.Cleanup(func() {
			require.NoError(t, factory.Close(d))
		})

		require.NoError(t, d.Err())
		require.Equal(t, testContentSize+crc32.Size, d.Len())
	})
}

func TestDecbufFactory_NewDecbufAtUnchecked_HappyPath(t *testing.T) {
	enc := createTestEncoder(testContentSize)
	enc.PutHash(crc32.New(table))

	testDecbufFactory(t, testContentSize, enc, func(t *testing.T, factory *DecbufFactory) {
		d := factory.NewDecbufAtUnchecked(0)
		t.Cleanup(func() {
			require.NoError(t, factory.Close(d))
		})

		require.NoError(t, d.Err())
		require.Equal(t, testContentSize+crc32.Size, d.Len())
	})
}

func TestDecbufFactory_NewDecbufRaw_HappyPath(t *testing.T) {
	enc := createTestEncoder(testContentSize)
	enc.PutHash(crc32.New(table))

	testDecbufFactory(t, testContentSize, enc, func(t *testing.T, factory *DecbufFactory) {
		d := factory.NewRawDecbuf()
		t.Cleanup(func() {
			require.NoError(t, factory.Close(d))
		})

		require.NoError(t, d.Err())
		require.Equal(t, 4+testContentSize+crc32.Size, d.Len())
	})
}

func testDecbufFactory(t *testing.T, len int, enc promencoding.Encbuf, test func(t *testing.T, factory *DecbufFactory)) {
	t.Run("pooling file handles", func(t *testing.T) {
		factory := createDecbufFactoryWithBytes(t, 1, len, enc)
		test(t, factory)
	})

	t.Run("no pooling file handles", func(t *testing.T) {
		factory := createDecbufFactoryWithBytes(t, 0, len, enc)
		test(t, factory)
	})
}

func createTestEncoder(numBytes int) promencoding.Encbuf {
	enc := promencoding.Encbuf{}

	for i := 0; i < numBytes; i++ {
		enc.PutByte(0x01)
	}

	return enc
}

func createDecbufFactoryWithBytes(t testing.TB, filePoolSize uint, len int, enc promencoding.Encbuf) *DecbufFactory {
	// Prepend the contents of the buffer with the length of the content portion
	// which does not include the trailing 4 bytes for a CRC 32.
	lenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBytes, uint32(len))
	bytes := append(lenBytes, enc.Get()...)

	dir := t.TempDir()
	filePath := path.Join(dir, "test-file")
	require.NoError(t, os.WriteFile(filePath, bytes, 0700))

	factory := NewDecbufFactory(filePath, filePoolSize)
	t.Cleanup(func() {
		factory.Stop()
	})

	return factory
}
