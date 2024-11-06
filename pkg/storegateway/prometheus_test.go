// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"testing"

	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	promtsdb "github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
)

func openPromBlocks(t testing.TB, dir string) []promtsdb.BlockReader {
	promDB, err := promtsdb.OpenDBReadOnly(dir, "", promslog.NewNopLogger())
	require.NoError(t, err)
	promBlocks, err := promDB.Blocks()
	require.NoError(t, err)
	return promBlocks
}

func queryPromSeriesChunkMetas(t testing.TB, series labels.Labels, block promtsdb.BlockReader) []chunks.Meta {
	ctx := context.Background()

	promReader, err := block.Index()
	if err != nil {
		require.NoError(t, err)
	}
	defer promReader.Close()

	matchers := make([]*labels.Matcher, 0, series.Len())
	series.Range(func(l labels.Label) {
		matchers = append(matchers, labels.MustNewMatcher(labels.MatchEqual, l.Name, l.Value))
	})
	postings, err := promReader.PostingsForMatchers(ctx, false, matchers...)
	if err != nil {
		require.NoError(t, err)
	}

	if !postings.Next() {
		require.Truef(t, false, "selecting from prometheus returned no series for %s", util.MatchersStringer(matchers))
	}

	var metas []chunks.Meta
	err = promReader.Series(postings.At(), &labels.ScratchBuilder{}, &metas)
	if err != nil {
		require.NoError(t, err)
	}

	if postings.Next() {
		require.Falsef(t, true, "selecting %s returned more series than expected", util.MatchersStringer(matchers))
	}
	return metas
}

// loadPromChunks sets metas[].Chunk
func loadPromChunks(t testing.TB, metas []chunks.Meta, block promtsdb.BlockReader) {
	promReader, err := block.Chunks()
	require.NoError(t, err)
	defer promReader.Close()

	for i := range metas {
		chunk, iter, err := promReader.ChunkOrIterable(metas[i])
		require.NoError(t, err)
		require.Nil(t, iter)
		metas[i].Chunk = chunk
	}
}

func compareToPromChunks(t testing.TB, chks []storepb.AggrChunk, lbls labels.Labels, minT, maxT int64, promBlock promtsdb.BlockReader) {
	// Query the persisted block using prometheus
	promChunks := queryPromSeriesChunkMetas(t, lbls, promBlock)
	promChunks = filterPromChunksByTime(promChunks, minT, maxT)
	loadPromChunks(t, promChunks, promBlock)

	// Compare the chunks that prometheus returns with the chunks the store-gateway returns.
	// NB: Order of chunks isn't guaranteed by the store-gateway.
	actualChunkData := make([][]byte, 0, len(chks))
	for _, c := range chks {
		actualChunkData = append(actualChunkData, c.Raw.Data)
	}
	assert.Len(t, actualChunkData, len(promChunks))
	for _, c := range promChunks {
		assert.Contains(t, actualChunkData, c.Chunk.Bytes())
	}
}

// filterPromChunksByTime filters metas in place
func filterPromChunksByTime(metas []chunks.Meta, minT, maxT int64) []chunks.Meta {
	writeIdx := 0
	for _, c := range metas {
		if !c.OverlapsClosedInterval(minT, maxT) {
			continue
		}
		metas[writeIdx] = c
		writeIdx++
	}
	return metas[:writeIdx]
}
