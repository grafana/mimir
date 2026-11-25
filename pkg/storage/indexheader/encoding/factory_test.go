// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"testing"

	promencoding "github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"golang.org/x/sync/errgroup"
)

const testContentSize = 4096

var table = crc32.MakeTable(crc32.Castagnoli)

func BenchmarkDecbufFactory_NewDecbufAtUnchecked(b *testing.B) {
	enc := createTestEncoder(testContentSize)
	enc.PutHash(crc32.New(table))

	diskFactory, bucketFactory := createDecbufFactoriesWithBytes(b, testContentSize, enc)
	factories := map[string]DecbufFactory{
		"disk":   diskFactory,
		"bucket": bucketFactory,
	}
	b.ResetTimer()

	for factoryName, factory := range factories {
		b.Run(fmt.Sprintf("DecbufFactory=%s", factoryName), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				d := factory.NewDecbufAtUnchecked(context.Background(), 0)

				if err := d.Err(); err != nil {
					require.NoError(b, err)
				}

				if err := d.Close(); err != nil {
					require.NoError(b, err)
				}
			}
		})
	}
}

func TestDecbufFactory_NewDecbufAtChecked_InvalidCRC(t *testing.T) {
	enc := createTestEncoder(testContentSize)
	enc.PutBytes([]byte{0, 0, 0, 0})

	testDecbufFactory(t, testContentSize, enc, true, true, func(t *testing.T, factory DecbufFactory) {
		d := factory.NewDecbufAtChecked(context.Background(), 0, table)
		t.Cleanup(func() {
			require.NoError(t, d.Close())
		})

		require.ErrorIs(t, d.Err(), ErrInvalidChecksum)
	})
}

func TestDecbufFactory_NewDecbufAtChecked_InvalidLength(t *testing.T) {
	enc := createTestEncoder(testContentSize)
	enc.PutHash(crc32.New(table))

	testDecbufFactory(t, testContentSize+1000, enc, true, true, func(t *testing.T, factory DecbufFactory) {
		d := factory.NewDecbufAtChecked(context.Background(), 0, table)
		t.Cleanup(func() {
			require.NoError(t, d.Close())
		})

		require.ErrorIs(t, d.Err(), ErrInvalidSize)
	})
}

func TestDecbufFactory_NewDecbufAtChecked_HappyPath(t *testing.T) {
	enc := createTestEncoder(testContentSize)
	enc.PutHash(crc32.New(table))

	testDecbufFactory(t, testContentSize, enc, true, true, func(t *testing.T, factory DecbufFactory) {
		d := factory.NewDecbufAtChecked(context.Background(), 0, table)
		t.Cleanup(func() {
			require.NoError(t, d.Close())
		})

		require.NoError(t, d.Err())
		require.Equal(t, testContentSize+crc32.Size, d.Len())
	})
}

func TestFileDecbufFactory_SharesFileAcrossDecbufs(t *testing.T) {
	enc := createTestEncoder(testContentSize)
	enc.PutHash(crc32.New(table))

	factory, _ := createDecbufFactoriesWithBytes(t, testContentSize, enc)

	d1 := factory.NewDecbufAtChecked(t.Context(), 0, table)
	require.NoError(t, d1.Err())
	d2 := factory.NewDecbufAtChecked(t.Context(), 0, table)
	require.NoError(t, d2.Err())

	fr1, ok := d1.r.(*FileReader)
	require.True(t, ok, "expected FileReader")
	fr2, ok := d2.r.(*FileReader)
	require.True(t, ok, "expected FileReader")
	require.Same(t, factory.file, fr1.src.r)
	require.Same(t, factory.file, fr2.src.r)

	require.NoError(t, d1.Close())
	require.NoError(t, d2.Close())
}

func TestFileDecbufFactory_ConcurrentReadsAtDifferentOffsets(t *testing.T) {
	const (
		contentSize = 64 * 1024
		readSize    = 64
		concurrency = 16
		runs        = 200
	)

	enc := promencoding.Encbuf{}
	for i := 0; i < contentSize; i++ {
		enc.PutByte(byte(i % 251))
	}
	enc.PutHash(crc32.New(table))
	factory, _ := createDecbufFactoriesWithBytes(t, contentSize, enc)

	g, ctx := errgroup.WithContext(t.Context())
	for w := 0; w < concurrency; w++ {
		g.Go(func() error {
			for run := 0; run < runs; run++ {
				d := factory.NewDecbufAtUnchecked(ctx, 0)
				off := ((w*runs + run) * 4099) % (contentSize - readSize)
				d.ResetAt(numLenBytes + off)
				for i := 0; i < readSize; i++ {
					b := d.Byte()
					if err := d.Err(); err != nil {
						_ = d.Close()
						return err
					}
					if want := byte((off + i) % 251); b != want {
						_ = d.Close()
						return fmt.Errorf("offset %d: got byte %d, want %d", off+i, b, want)
					}
				}
				if err := d.Close(); err != nil {
					return err
				}
			}
			return nil
		})
	}
	require.NoError(t, g.Wait())
}

func TestFileDecbufFactory_CloseWithDecbufsInFlight(t *testing.T) {
	enc := createTestEncoder(testContentSize)
	enc.PutHash(crc32.New(table))
	factory, _ := createDecbufFactoriesWithBytes(t, testContentSize, enc)

	d1 := factory.NewDecbufAtChecked(t.Context(), 0, table)
	require.NoError(t, d1.Err())
	d2 := factory.NewRawDecbuf(t.Context())
	require.NoError(t, d2.Err())

	require.NoError(t, factory.Close())

	d3 := factory.NewDecbufAtUnchecked(t.Context(), 0)
	require.ErrorIs(t, d3.Err(), ErrFileDecbufFactoryClosed)
	require.NoError(t, d3.Close())

	// Decbufs created before Close keep reading from the still-open file.
	d1.Skip(testContentSize)
	require.NoError(t, d1.Err())
	require.Equal(t, crc32.Size, d1.Len())
	require.NoError(t, d1.Close())
	// Closing the same Decbuf twice must not release the file early.
	require.NoError(t, d1.Close())
	_, err := factory.file.Stat()
	require.NoError(t, err, "file should stay open while a Decbuf is in flight")

	d2.ResetAt(numLenBytes)
	require.Equal(t, byte(0x01), d2.Byte())
	require.NoError(t, d2.Err())
	require.NoError(t, d2.Close())

	_, err = factory.file.Stat()
	require.ErrorIs(t, err, os.ErrClosed, "file should be closed once the last Decbuf is closed")
	require.NoError(t, factory.Close())
}

func TestFileDecbufFactory_CloseDuringConcurrentReads(t *testing.T) {
	enc := createTestEncoder(testContentSize)
	enc.PutHash(crc32.New(table))
	factory, _ := createDecbufFactoriesWithBytes(t, testContentSize, enc)

	const concurrency = 16
	started := make(chan struct{}, concurrency)
	g, ctx := errgroup.WithContext(t.Context())
	for w := 0; w < concurrency; w++ {
		g.Go(func() error {
			signalled := false
			for {
				d := factory.NewDecbufAtChecked(ctx, 0, table)
				if errors.Is(d.Err(), ErrFileDecbufFactoryClosed) {
					return d.Close()
				}
				if !signalled {
					started <- struct{}{}
					signalled = true
				}
				if err := d.Err(); err != nil {
					_ = d.Close()
					return err
				}
				if b := d.Byte(); b != 0x01 {
					_ = d.Close()
					return fmt.Errorf("got byte %d, want 1", b)
				}
				if err := d.Close(); err != nil {
					return err
				}
			}
		})
	}

	for w := 0; w < concurrency; w++ {
		<-started
	}
	require.NoError(t, factory.Close())
	require.NoError(t, g.Wait())

	_, err := factory.file.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
}

func TestFileDecbufFactory_MissingFile(t *testing.T) {
	factory := NewFileDecbufFactory(path.Join(t.TempDir(), "missing"))

	d := factory.NewRawDecbuf(t.Context())
	require.ErrorIs(t, d.Err(), os.ErrNotExist)
	require.NoError(t, d.Close())
	require.NoError(t, factory.Close())
}

func TestDecbufFactory_NewDecbufAtChecked_Concurrent(t *testing.T) {
	enc := createTestEncoder(testContentSize)
	enc.PutHash(crc32.New(table))

	const (
		runs        = 100
		concurrency = 10
	)

	testDecbufFactory(t, testContentSize, enc, true, true, func(t *testing.T, factory DecbufFactory) {
		g, ctx := errgroup.WithContext(context.Background())

		for i := 0; i < concurrency; i++ {
			g.Go(func() error {
				for run := 0; run < runs; run++ {
					d := factory.NewDecbufAtChecked(ctx, 0, table)

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

	testDecbufFactory(t, testContentSize, enc, true, true, func(t *testing.T, factory DecbufFactory) {
		d := factory.NewDecbufAtUnchecked(context.Background(), 0)
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

	testDecbufFactory(t, testContentSize, enc, true, true, func(t *testing.T, factory DecbufFactory) {
		d := factory.NewRawDecbuf(context.Background())
		t.Cleanup(func() {
			require.NoError(t, d.Close())
		})

		require.NoError(t, d.Err())
		require.Equal(t, 4+testContentSize+crc32.Size, d.Len())
	})
}

// TestDecbufFactory_NewDecbufInSection_HappyPath tests that creating a new Decbuf for a table section
// starts the reader in the correct place by placing a test byte at the expected start offset.
func TestDecbufFactory_NewDecbufInSection_HappyPath(t *testing.T) {
	testByte := byte(0x02)
	startOffset := 10
	endOffset := 25
	enc := createTestEncoderWithTestByte(testContentSize, startOffset-numLenBytes, testByte)
	enc.PutHash(crc32.New(table))

	testDisk := false // NewDecbufInSection is currently only implemented for BucketDecbufFactory
	testDecbufFactory(t, testContentSize, enc, testDisk, true, func(t *testing.T, factory DecbufFactory) {
		d := factory.NewDecbufInSection(context.Background(), 0, startOffset, endOffset)
		t.Cleanup(func() {
			require.NoError(t, d.Close())
		})
		require.NoError(t, d.Err())
		require.Equal(t, endOffset-startOffset, d.Len())
		// Make sure our start offset is placed where we expected by reading the first byte
		require.Equal(t, testByte, d.Byte())
	})
}

// TestDecbufFactory_NewDecbufInSection_SectionEndOffsetBeyondTableLength tests that calling NewDecbufInSection
// with an end offset beyond the end of the table produces a Decbuf that ends at the end of the table.
func TestDecbufFactory_NewDecbufInSection_SectionEndOffsetBeyondTableLength(t *testing.T) {
	testByte := byte(0x02)
	enc := createTestEncoderWithTestByte(testContentSize, 10-numLenBytes, testByte)
	enc.PutHash(crc32.New(table))

	testDisk := false // NewDecbufInSection is currently only implemented for BucketDecbufFactory
	testDecbufFactory(t, testContentSize, enc, testDisk, true, func(t *testing.T, factory DecbufFactory) {
		d := factory.NewDecbufInSection(context.Background(), 0, 10, testContentSize+1000)
		t.Cleanup(func() {
			require.NoError(t, d.Close())
		})
		require.NoError(t, d.Err())
		require.Equal(t, testContentSize+numLenBytes-10, d.Len())
		require.Equal(t, testByte, d.Byte())
	})
}

// TestDecbufFactory_NewDecbufInSection_SectionEndOffsetBeforeStartOffset tests that attempting to call NewDecbufInSection
// with an end offset before the start offset returns an error.
func TestDecbufFactory_NewDecbufInSection_SectionEndOffsetBeforeStartOffset(t *testing.T) {
	enc := createTestEncoder(testContentSize)
	enc.PutHash(crc32.New(table))

	testDisk := false // NewDecbufInSection is currently only implemented for BucketDecbufFactory
	testDecbufFactory(t, testContentSize, enc, testDisk, true, func(t *testing.T, factory DecbufFactory) {
		d := factory.NewDecbufInSection(context.Background(), 0, 2500, 30)
		t.Cleanup(func() {
			require.NoError(t, d.Close())
		})
		require.Error(t, d.Err())
	})
}

func TestDecbufFactory_Stop(t *testing.T) {
	enc := createTestEncoder(testContentSize)
	enc.PutHash(crc32.New(table))

	testBucket := false // bucket-based factory does not do anything on close and will not error
	testDecbufFactory(t, testContentSize, enc, true, testBucket, func(t *testing.T, factory DecbufFactory) {
		require.NoError(t, factory.Close())

		d := factory.NewRawDecbuf(context.Background())
		t.Cleanup(func() {
			require.NoError(t, d.Close())
		})

		require.ErrorIs(t, d.Err(), ErrFileDecbufFactoryClosed)
	})
}

func testDecbufFactory(
	t *testing.T,
	len int,
	enc promencoding.Encbuf,
	testDisk bool,
	testBucket bool,
	test func(t *testing.T, factory DecbufFactory),
) {
	if testDisk {
		t.Run("DecbufFactory=Disk", func(t *testing.T) {
			diskFactory, _ := createDecbufFactoriesWithBytes(t, len, enc)
			test(t, diskFactory)
		})
	}

	if testBucket {
		t.Run("DecbufFactory=Bucket", func(t *testing.T) {
			_, bucketFactory := createDecbufFactoriesWithBytes(t, len, enc)
			test(t, bucketFactory)
		})
	}

}

func createTestEncoderWithTestByte(numBytes, testByteOffset int, testByte byte) promencoding.Encbuf {
	enc := promencoding.Encbuf{}

	for i := 0; i < testByteOffset; i++ {
		enc.PutByte(0x01)
	}
	enc.PutByte(testByte)
	for i := testByteOffset + 1; i < numBytes; i++ {
		enc.PutByte(0x01)
	}
	return enc
}

func createTestEncoder(numBytes int) promencoding.Encbuf {
	enc := promencoding.Encbuf{}

	for i := 0; i < numBytes; i++ {
		enc.PutByte(0x01)
	}

	return enc
}

func createDecbufFactoriesWithBytes(t testing.TB, len int, enc promencoding.Encbuf) (*FileDecbufFactory, *BucketDecbufFactory) {
	// Prepend the contents of the buffer with the length of the content portion
	// which does not include the trailing 4 bytes for a CRC 32.
	lenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBytes, uint32(len))
	bytes := append(lenBytes, enc.Get()...)

	dir := t.TempDir()
	fileName := "test-file"
	filePath := path.Join(dir, fileName)
	require.NoError(t, os.WriteFile(filePath, bytes, 0700))

	diskFactory := NewFileDecbufFactory(filePath)
	t.Cleanup(func() {
		_ = diskFactory.Close()
	})

	bkt, err := filesystem.NewBucket(dir)
	require.NoError(t, err)
	instBkt := objstore.WithNoopInstr(bkt)
	t.Cleanup(func() {
		require.NoError(t, bkt.Close())
	})
	bucketFactory := NewBucketDecbufFactory(instBkt, fileName)

	return diskFactory, bucketFactory
}
