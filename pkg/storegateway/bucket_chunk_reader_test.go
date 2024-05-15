// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb"
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
			chunkObjs := make([]string, numChunks)
			for i := 0; i < len(chunkObjs); i++ {
				chunkObjs[i] = fmt.Sprintf("chunk-%d", i)
			}

			ctx := context.Background()
			block := &bucketBlock{
				bkt:          newMockObjectStoreBucketReader(chunkSize),
				partitioners: blockPartitioners{naivePartitioner{}, naivePartitioner{}, naivePartitioner{}},
				chunkObjs:    chunkObjs,
			}

			reader := newBucketChunkReader(ctx, block)

			// Prepare mock data for testing.
			loadIdxs := make([]loadIdx, numChunks)
			for i := 0; i < len(loadIdxs); i++ {
				loadIdxs[i] = loadIdx{
					offset:      uint32(i * chunkSize),
					length:      chunkSize,
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

				for i, idx := range loadIdxs {
					if err := reader.addLoad(chunks.ChunkRef(i), idx.seriesEntry, idx.chunkEntry, idx.length); err != nil {
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

func newMockObjectStoreBucketReader(chunkSize int) mockObjectStoreBucketReader {
	b := make([]byte, chunkSize+4) // add extra 4 bytes (crc32)

	binary.PutUvarint(b, uint64(chunkSize)-chunks.ChunkEncodingSize)
	b[2] = byte(chunkenc.EncXOR) // chunk type

	return mockObjectStoreBucketReader{b: b}
}

func (r mockObjectStoreBucketReader) GetRange(_ context.Context, _ string, _, _ int64) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(r.b)), nil
}
