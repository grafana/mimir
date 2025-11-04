// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"slices"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	promtsdb "github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/pool"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestBucketChunkReader_refetchChunks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	newTestBucketBlock := prepareTestBlock(test.NewTB(t), appendTestSeries(1000))
	block := newTestBucketBlock()
	seriesRefsIterator, err := openBlockSeriesChunkRefsSetsIterator(
		ctx,
		5000,
		"tenant-1",
		block.indexReader(selectAllStrategy{}),
		noopCache{},
		block.meta,
		[]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "j", "foo")},
		nil,
		nil,
		defaultStrategy,
		block.meta.MinTime,
		block.meta.MaxTime,
		newSafeQueryStats(),
		log.NewNopLogger(),
		nil,
	)
	require.NoError(t, err)

	seriesRefs := readAllSeriesChunkRefs(newFlattenedSeriesChunkRefsIterator(seriesRefsIterator))
	require.NoError(t, seriesRefsIterator.Err())

	// Each func takes the estimated length and returns a new length.
	chunkLengthSkewingFuncs := map[string]func(uint32) uint32{
		"tsdb.EstimatedMaxChunkSize":    func(uint32) uint32 { return tsdb.EstimatedMaxChunkSize },
		"10xtsdb.EstimatedMaxChunkSize": func(uint32) uint32 { return 10 * tsdb.EstimatedMaxChunkSize },
		"size-1":                        func(chunkLength uint32) uint32 { return chunkLength - 1 },
		"size/2":                        func(chunkLength uint32) uint32 { return chunkLength / 2 },
		"1":                             func(uint32) uint32 { return 1 },
		"0":                             func(uint32) uint32 { return 0 },
	}

	for name, skewChunkLen := range chunkLengthSkewingFuncs {
		t.Run(name, func(t *testing.T) {
			// Start loading chunks in two readers - one with the estimations from the index and one with modified estimations.
			// We expect that the estimations from the index lead to no refetches and that the skewed lengths trigger refetches.
			loadedChunksCorrectLen := make([]seriesChunks, len(seriesRefs))
			chunkrCorrectLen := block.chunkReader(ctx)

			loadedChunksModifiedLen := make([]seriesChunks, len(seriesRefs))
			chunkrModifiedLen := block.chunkReader(ctx)

			for sIdx, seriesRef := range seriesRefs {
				loadedChunksCorrectLen[sIdx].chks = append(loadedChunksCorrectLen[sIdx].chks, make([]storepb.AggrChunk, len(seriesRef.refs))...)
				loadedChunksModifiedLen[sIdx].chks = append(loadedChunksModifiedLen[sIdx].chks, make([]storepb.AggrChunk, len(seriesRef.refs))...)
				for cIdx, r := range seriesRef.refs {
					assert.NoError(t, chunkrCorrectLen.addLoad(r.ref(), sIdx, cIdx, r.length))
					assert.NoError(t, chunkrModifiedLen.addLoad(r.ref(), sIdx, cIdx, skewChunkLen(r.length)))
				}
			}

			assert.NoError(t, chunkrModifiedLen.load(loadedChunksModifiedLen, pool.NewSafeSlabPool[byte](chunkBytesSlicePool, seriesChunksSlabSize), newSafeQueryStats()))
			assert.NoError(t, chunkrCorrectLen.load(loadedChunksCorrectLen, pool.NewSafeSlabPool[byte](chunkBytesSlicePool, seriesChunksSlabSize), newSafeQueryStats()))

			assert.Equal(t, loadedChunksCorrectLen, loadedChunksModifiedLen)
		})
	}
}

func TestBucketChunkReader_loadChunks_brokenChunkSize(t *testing.T) {
	brokenChunkSize := uint64(math.MaxInt32)

	// This is a mocked chunk, that only contains the len portion; it doesn't include any data or checksum. This is enough for this test.
	b := make([]byte, 20)
	n := binary.PutUvarint(b, brokenChunkSize)
	b = b[:n]
	bucketReader := mockObjectStoreBucketReader{b: b}

	block := &bucketBlock{
		bkt:          bucketReader,
		partitioners: blockPartitioners{naivePartitioner{}, naivePartitioner{}, naivePartitioner{}},
		chunkObjs:    []string{"test-chunk"},
	}

	reader := newBucketChunkReader(t.Context(), block)

	err := reader.addLoad(chunks.ChunkRef(0), 0, 0, 0)
	require.NoError(t, err)

	// The actual loadedChunks buffer isn't relevant for this test.
	loadedChunks := make([]seriesChunks, 1)
	loadedChunks[0].chks = make([]storepb.AggrChunk, 1)

	chunksPool := pool.NewSafeSlabPool[byte](chunkBytesSlicePool, seriesChunksSlabSize)

	err = reader.load(loadedChunks, chunksPool, newSafeQueryStats())
	require.Error(t, err)
}

// Helper to create chunk data with random content based on seed (data only, no encoding byte)
func createChunkData(chunkDataSize, seed int) []byte {
	data := make([]byte, chunkDataSize)
	// start from seed and loop around to avoid overflow
	for i := 0; i < chunkDataSize; i++ {
		data[i] = byte((seed + i) % 256)
	}
	return data
}

// Helper to calculate valid CRC32 for chunk (encoding + data)
func validCRC32(encoding chunkenc.Encoding, chunkData []byte) uint32 {
	// Create full chunk with encoding byte for CRC calculation
	fullChunk := make([]byte, 1+len(chunkData))
	fullChunk[0] = byte(encoding)
	copy(fullChunk[1:], chunkData)
	return crc32.Checksum(fullChunk, crc32.MakeTable(crc32.Castagnoli))
}

func createChunkBytes(encoding chunkenc.Encoding, chunkDataSize, seed int) []byte {
	chunkData := createChunkData(chunkDataSize, seed)
	// Create full chunk bytes: length (varint) + encoding byte + data + crc32
	buf := make([]byte, binary.MaxVarintLen64+len(chunkData)+4)
	dataLenUvarintLen := binary.PutUvarint(buf, uint64(len(chunkData)))
	buf[dataLenUvarintLen] = byte(encoding)
	copy(buf[dataLenUvarintLen+1:], chunkData)

	crc := validCRC32(encoding, chunkData)
	crcStart := dataLenUvarintLen + 1 + len(chunkData)
	binary.BigEndian.PutUint32(buf[crcStart:crcStart+crc32.Size], crc)

	return buf[:dataLenUvarintLen+1+len(chunkData)+crc32.Size]
}

func TestBucketChunkReader_loadChunks_verifiesCRC32(t *testing.T) {
	const chunkDataSize = 256

	invalidCRC32 := uint32(0xDEADBEEF)

	testCases := []struct {
		name        string
		encodings   []chunkenc.Encoding // encoding for each chunk
		chunks      [][]byte            // chunk data (data only, no encoding byte or CRC)
		crcs        []uint32            // CRC for each chunk
		expectError bool
	}{
		{
			name:        "1 chunk with valid CRC should pass",
			chunks:      [][]byte{createChunkData(chunkDataSize, 0)},
			encodings:   []chunkenc.Encoding{chunkenc.EncXOR},
			crcs:        []uint32{validCRC32(chunkenc.EncXOR, createChunkData(chunkDataSize, 0))},
			expectError: false,
		},
		{
			name:        "1 chunk with invalid CRC should fail (first chunk verified)",
			chunks:      [][]byte{createChunkData(chunkDataSize, 0)},
			encodings:   []chunkenc.Encoding{chunkenc.EncXOR},
			crcs:        []uint32{invalidCRC32},
			expectError: true,
		},
		{
			name: "127 chunks with all invalid CRC should fail (first chunk verified)",
			chunks: func() (chunks [][]byte) {
				for i := 0; i < 127; i++ {
					chunks = append(chunks, createChunkData(chunkDataSize, i))
				}
				return
			}(),
			encodings:   slices.Repeat([]chunkenc.Encoding{chunkenc.EncXOR}, 127),
			crcs:        slices.Repeat([]uint32{invalidCRC32}, 127),
			expectError: true,
		},
		{
			name: "128 chunks with all valid CRC should pass",
			chunks: func() (chunks [][]byte) {
				for i := 0; i < 128; i++ {
					chunks = append(chunks, createChunkData(chunkDataSize, i))
				}
				return
			}(),
			encodings: slices.Repeat([]chunkenc.Encoding{chunkenc.EncXOR}, 128),
			crcs: func() (crcs []uint32) {
				for i := 0; i < 128; i++ {
					crcs = append(crcs, validCRC32(chunkenc.EncXOR, createChunkData(chunkDataSize, i)))
				}
				return
			}(),
			expectError: false,
		},
		{
			name: "128 chunks with corrupted chunk at position 1 should pass (not verified)",
			chunks: func() (chunks [][]byte) {
				chunks = append(chunks, createChunkData(chunkDataSize, 0))
				chunks = append(chunks, createChunkData(chunkDataSize, 1))
				for i := 2; i < 128; i++ {
					chunks = append(chunks, createChunkData(chunkDataSize, i))
				}
				return
			}(),
			encodings: slices.Repeat([]chunkenc.Encoding{chunkenc.EncXOR}, 128),
			crcs: func() (crcs []uint32) {
				crcs = append(crcs, validCRC32(chunkenc.EncXOR, createChunkData(chunkDataSize, 0)))
				crcs = append(crcs, invalidCRC32)
				for i := 2; i < 128; i++ {
					crcs = append(crcs, validCRC32(chunkenc.EncXOR, createChunkData(chunkDataSize, i)))
				}
				return
			}(),
			expectError: false,
		},
		{
			name: "128 chunks with corrupted chunk at position 64 should pass (not verified)",
			chunks: func() (chunks [][]byte) {
				for i := 0; i < 64; i++ {
					chunks = append(chunks, createChunkData(chunkDataSize, i))
				}
				chunks = append(chunks, createChunkData(chunkDataSize, 64))
				for i := 65; i < 128; i++ {
					chunks = append(chunks, createChunkData(chunkDataSize, i))
				}
				return
			}(),
			encodings: slices.Repeat([]chunkenc.Encoding{chunkenc.EncXOR}, 128),
			crcs: func() (crcs []uint32) {
				for i := 0; i < 64; i++ {
					crcs = append(crcs, validCRC32(chunkenc.EncXOR, createChunkData(chunkDataSize, i)))
				}
				crcs = append(crcs, invalidCRC32)
				for i := 65; i < 128; i++ {
					crcs = append(crcs, validCRC32(chunkenc.EncXOR, createChunkData(chunkDataSize, i)))
				}
				return
			}(),
			expectError: false,
		},
		{
			name: "128 chunks with corrupted chunk at position 127 should pass (not verified)",
			chunks: func() (chunks [][]byte) {
				for i := 0; i < 127; i++ {
					chunks = append(chunks, createChunkData(chunkDataSize, i))
				}
				chunks = append(chunks, createChunkData(chunkDataSize, 127))
				return
			}(),
			encodings: slices.Repeat([]chunkenc.Encoding{chunkenc.EncXOR}, 128),
			crcs: func() (crcs []uint32) {
				for i := 0; i < 127; i++ {
					crcs = append(crcs, validCRC32(chunkenc.EncXOR, createChunkData(chunkDataSize, i)))
				}
				crcs = append(crcs, invalidCRC32)
				return
			}(),
			expectError: false,
		},
		{
			name: "256 chunks with corrupted chunk at position 1 should pass (not verified)",
			chunks: func() (chunks [][]byte) {
				chunks = append(chunks, createChunkData(chunkDataSize, 0))
				chunks = append(chunks, createChunkData(chunkDataSize, 1))
				for i := 2; i < 256; i++ {
					chunks = append(chunks, createChunkData(chunkDataSize, i))
				}
				return
			}(),
			encodings: slices.Repeat([]chunkenc.Encoding{chunkenc.EncXOR}, 256),
			crcs: func() (crcs []uint32) {
				crcs = append(crcs, validCRC32(chunkenc.EncXOR, createChunkData(chunkDataSize, 0)))
				crcs = append(crcs, invalidCRC32)
				for i := 2; i < 256; i++ {
					crcs = append(crcs, validCRC32(chunkenc.EncXOR, createChunkData(chunkDataSize, i)))
				}
				return
			}(),
			expectError: false,
		},
		{
			name: "256 chunks with corrupted chunk at position 129 should pass (not verified)",
			chunks: func() (chunks [][]byte) {
				for i := 0; i < 129; i++ {
					chunks = append(chunks, createChunkData(chunkDataSize, i))
				}
				chunks = append(chunks, createChunkData(chunkDataSize, 129))
				for i := 130; i < 256; i++ {
					chunks = append(chunks, createChunkData(chunkDataSize, i))
				}
				return
			}(),
			encodings: slices.Repeat([]chunkenc.Encoding{chunkenc.EncXOR}, 256),
			crcs: func() (crcs []uint32) {
				for i := 0; i < 129; i++ {
					crcs = append(crcs, validCRC32(chunkenc.EncXOR, createChunkData(chunkDataSize, i)))
				}
				crcs = append(crcs, invalidCRC32)
				for i := 130; i < 256; i++ {
					crcs = append(crcs, validCRC32(chunkenc.EncXOR, createChunkData(chunkDataSize, i)))
				}
				return
			}(),
			expectError: false,
		},
		{
			name: "256 chunks with corrupted chunk at position 255 should pass (not verified)",
			chunks: func() (chunks [][]byte) {
				for i := 0; i < 255; i++ {
					chunks = append(chunks, createChunkData(chunkDataSize, i))
				}
				chunks = append(chunks, createChunkData(chunkDataSize, 255))
				return
			}(),
			encodings: slices.Repeat([]chunkenc.Encoding{chunkenc.EncXOR}, 256),
			crcs: func() (crcs []uint32) {
				for i := 0; i < 255; i++ {
					crcs = append(crcs, validCRC32(chunkenc.EncXOR, createChunkData(chunkDataSize, i)))
				}
				crcs = append(crcs, invalidCRC32)
				return
			}(),
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			require.Equal(t, len(tc.chunks), len(tc.crcs), "chunks and crcs must have same length")
			require.Equal(t, len(tc.chunks), len(tc.encodings), "chunks and encodings must have same length")

			// Build buffer by alternating: varint + encoding + chunk data + CRC
			var buf bytes.Buffer
			chunkOffsets := make([]uint32, len(tc.chunks))

			for i, chunkData := range tc.chunks {
				chunkOffsets[i] = uint32(buf.Len())

				// Write varint length (data length without encoding byte)
				buf.Write(binary.AppendUvarint(nil, uint64(len(chunkData))))

				// Write encoding byte
				buf.WriteByte(byte(tc.encodings[i]))

				// Write chunk data
				buf.Write(chunkData)

				// Write CRC32
				buf.Write(binary.BigEndian.AppendUint32(nil, tc.crcs[i]))
			}

			bucketReader := mockObjectStoreBucketReader{b: buf.Bytes()}
			block := &bucketBlock{
				meta: &block.Meta{
					BlockMeta: promtsdb.BlockMeta{
						ULID: ulid.MustNew(10, nil),
					},
				},
				bkt:          bucketReader,
				partitioners: newGapBasedPartitioners(1_000_000, prometheus.NewRegistry()),
				chunkObjs:    []string{"segment-0"},
			}

			reader := newBucketChunkReader(ctx, block)

			// Add all chunks to load
			loadedChunks := make([]seriesChunks, 1)
			loadedChunks[0].chks = make([]storepb.AggrChunk, len(tc.chunks))
			for i := range tc.chunks {
				ref := chunkRef(0, chunkOffsets[i])
				err := reader.addLoad(ref, 0, i, binary.MaxVarintLen32+chunks.ChunkEncodingSize+chunkDataSize+crc32.Size)
				require.NoError(t, err)
			}

			chunksPool := pool.NewSafeSlabPool[byte](chunkBytesSlicePool, seriesChunksSlabSize)
			err := reader.load(loadedChunks, chunksPool, newSafeQueryStats())

			if tc.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "crc32 mismatch")
			} else {
				assert.NoError(t, err)
				// Verify we got all chunks back
				assert.Len(t, loadedChunks[0].chks, len(tc.chunks))

				// Verify chunk data matches expected
				for i, expectedData := range tc.chunks {
					loadedChunk := loadedChunks[0].chks[i]
					assert.NotNil(t, loadedChunk.Raw, "chunk %d should have data", i)

					assert.Greater(t, len(loadedChunk.Raw.Data), 0, "chunk %d should have data", i)
					assert.Equal(t, translateChunkEncoding(tc.encodings[i]), loadedChunk.Raw.Type, "chunk %d encoding mismatch", i)
					assert.EqualValues(t, expectedData, loadedChunk.Raw.Data, "chunk %d data mismatch", i)
				}
			}
		})
	}
}

func translateChunkEncoding(enc chunkenc.Encoding) storepb.Chunk_Encoding {
	switch enc {
	case chunkenc.EncXOR:
		return storepb.Chunk_XOR
	case chunkenc.EncHistogram:
		return storepb.Chunk_Histogram
	case chunkenc.EncFloatHistogram:
		return storepb.Chunk_FloatHistogram
	default:
		panic(fmt.Sprintf("unsupported chunk encoding: %v", enc))
	}
}

func BenchmarkBucketChunkReader_loadChunks(b *testing.B) {
	const chunkSize = 256

	benchCases := []int{
		1000,
		10_000,
		100_000,
		1_000_000,
	}
	for _, numChunks := range benchCases {
		b.Run(fmt.Sprintf("blocks-%d", numChunks), func(b *testing.B) {
			ctx := context.Background()
			block := &bucketBlock{
				bkt:          mockObjectStoreBucketReader{b: createChunkBytes(chunkenc.EncXOR, chunkSize, 0)},
				partitioners: blockPartitioners{naivePartitioner{}, naivePartitioner{}, naivePartitioner{}},
				chunkObjs:    []string{"00001"},
			}

			reader := newBucketChunkReader(ctx, block)

			// Prepare mock data for testing.
			loadIdxs := make([]loadIdx, numChunks)
			for i := 0; i < len(loadIdxs); i++ {
				loadIdxs[i] = loadIdx{
					offset:      0,
					length:      2 * chunkSize, // oversize to avoid refetches
					seriesEntry: 0,
					chunkEntry:  i,
				}
			}

			chunksPool := pool.NewSafeSlabPool[byte](chunkBytesSlicePool, seriesChunksSlabSize)

			// Run the benchmark.
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				reader.reset()

				for _, idx := range loadIdxs {
					if err := reader.addLoad(chunks.ChunkRef(0), idx.seriesEntry, idx.chunkEntry, idx.length); err != nil {
						b.Fatalf("error adding load: %v", err)
					}
				}
				var res = make([]seriesChunks, 1)
				res[0].chks = make([]storepb.AggrChunk, len(loadIdxs))

				err := reader.load(res, chunksPool, newSafeQueryStats())
				if err != nil {
					b.Fatalf("error loading chunks: %v", err)
				}
			}
		})
	}
}

type mockObjectStoreBucketReader struct {
	objstore.BucketReader
	b []byte
}

func (r mockObjectStoreBucketReader) GetRange(_ context.Context, _ string, off, length int64) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(r.b[off:min(off+length, int64(len(r.b)))])), nil
}
