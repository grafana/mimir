// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/pool"
	"github.com/grafana/mimir/pkg/util/test"
)

func (g seriesChunkRefsRange) refAt(i int) chunks.ChunkRef {
	return chunkRef(g.segmentFile, g.refs[i].segFileOffset)
}

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
		nil,
		log.NewNopLogger(),
	)
	require.NoError(t, err)

	seriesRefs := readAllSeriesChunkRefs(newFlattenedSeriesChunkRefsIterator(seriesRefsIterator))
	require.NoError(t, seriesRefsIterator.Err())

	// Each func takes the estimated length and returns a new length.
	chunkLengthSkewingFuncs := map[string]func(uint32) uint32{
		"tsdb.EstimatedMaxChunkSize":    func(chunkLength uint32) uint32 { return tsdb.EstimatedMaxChunkSize },
		"10xtsdb.EstimatedMaxChunkSize": func(chunkLength uint32) uint32 { return 10 * tsdb.EstimatedMaxChunkSize },
		"size-1":                        func(chunkLength uint32) uint32 { return chunkLength - 1 },
		"size/2":                        func(chunkLength uint32) uint32 { return chunkLength / 2 },
		"1":                             func(chunkLength uint32) uint32 { return 1 },
		"0":                             func(chunkLength uint32) uint32 { return 0 },
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
				for _, r := range seriesRef.chunksRanges {
					existingChunksNum := len(loadedChunksCorrectLen[sIdx].chks)

					loadedChunksCorrectLen[sIdx].chks = append(loadedChunksCorrectLen[sIdx].chks, make([]storepb.AggrChunk, len(r.refs))...)
					loadedChunksModifiedLen[sIdx].chks = append(loadedChunksModifiedLen[sIdx].chks, make([]storepb.AggrChunk, len(r.refs))...)

					for cIdx, c := range r.refs {
						assert.NoError(t, chunkrCorrectLen.addLoad(r.refAt(cIdx), sIdx, existingChunksNum+cIdx, c.length))
						assert.NoError(t, chunkrModifiedLen.addLoad(r.refAt(cIdx), sIdx, existingChunksNum+cIdx, skewChunkLen(c.length)))
					}
				}
			}

			assert.NoError(t, chunkrModifiedLen.load(loadedChunksModifiedLen, pool.NewSafeSlabPool[byte](chunkBytesSlicePool, seriesChunksSlabSize), newSafeQueryStats()))
			assert.NoError(t, chunkrCorrectLen.load(loadedChunksCorrectLen, pool.NewSafeSlabPool[byte](chunkBytesSlicePool, seriesChunksSlabSize), newSafeQueryStats()))

			assert.Equal(t, loadedChunksCorrectLen, loadedChunksModifiedLen)
		})
	}
}
