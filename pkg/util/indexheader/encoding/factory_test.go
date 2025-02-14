// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	promencoding "github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
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

		if err := d.Close(); err != nil {
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
			require.NoError(t, d.Close())
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
			require.NoError(t, d.Close())
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
			require.NoError(t, d.Close())
		})

		require.NoError(t, d.Err())
		require.Equal(t, testContentSize+crc32.Size, d.Len())
	})
}

func TestDecbufFactory_NewDecbufAtChecked_MultipleInstances(t *testing.T) {
	enc := createTestEncoder(testContentSize)
	enc.PutHash(crc32.New(table))

	// Note that we create the factory ourselves instead of using testDecbufFactory because
	// we only want to test the case where file handles are pooled and hence will be reused
	// between different Decbuf instances.
	factory := createDecbufFactoryWithBytes(t, 1, testContentSize, enc)
	t.Cleanup(func() {
		factory.Stop()
	})

	d1 := factory.NewDecbufAtChecked(0, table)
	require.NoError(t, d1.Err())
	fd1 := d1.r.file.Fd()
	require.NoError(t, d1.Close())

	d2 := factory.NewDecbufAtChecked(0, table)
	require.NoError(t, d2.Err())
	fd2 := d2.r.file.Fd()
	require.NoError(t, d2.Close())

	require.Equal(t, fd1, fd2, "expected Decbuf instances to use the same file descriptor")
}

func TestDecbufFactory_NewDecbufAtChecked_Concurrent(t *testing.T) {
	enc := createTestEncoder(testContentSize)
	enc.PutHash(crc32.New(table))

	const (
		runs        = 100
		concurrency = 10
	)

	testDecbufFactory(t, testContentSize, enc, func(t *testing.T, factory *DecbufFactory) {
		g, _ := errgroup.WithContext(context.Background())

		for i := 0; i < concurrency; i++ {
			g.Go(func() error {
				for run := 0; run < runs; run++ {
					d := factory.NewDecbufAtChecked(0, table)

					if err := d.Err(); err != nil {
						_ = d.Close()
						return err
					}

					if err := d.Close(); err != nil {
						return err
					}
				}

				return nil
			})
		}

		require.NoError(t, g.Wait())
	})
}

func TestDecbufFactory_NewDecbufAtUnchecked_HappyPath(t *testing.T) {
	enc := createTestEncoder(testContentSize)
	enc.PutHash(crc32.New(table))

	testDecbufFactory(t, testContentSize, enc, func(t *testing.T, factory *DecbufFactory) {
		d := factory.NewDecbufAtUnchecked(0)
		t.Cleanup(func() {
			require.NoError(t, d.Close())
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
			require.NoError(t, d.Close())
		})

		require.NoError(t, d.Err())
		require.Equal(t, 4+testContentSize+crc32.Size, d.Len())
	})
}

func TestDecbufFactory_Stop(t *testing.T) {
	enc := createTestEncoder(testContentSize)
	enc.PutHash(crc32.New(table))

	testDecbufFactory(t, testContentSize, enc, func(t *testing.T, factory *DecbufFactory) {
		factory.Stop()

		d := factory.NewRawDecbuf()
		t.Cleanup(func() {
			require.NoError(t, d.Close())
		})

		require.ErrorIs(t, d.Err(), ErrPoolStopped)
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

	reg := prometheus.NewPedanticRegistry()
	factory := NewDecbufFactory(filePath, filePoolSize, NewDecbufFactoryMetrics(reg))
	t.Cleanup(func() {
		factory.Stop()
	})

	return factory
}
