// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/shipper/shipper.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package ingester

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
)

func createBlock(t *testing.T, blocksDir string, id ulid.ULID, m metadata.Meta) {
	// We need "chunks" dir and "index" files for upload to work correctly (it expects these to exist).
	require.NoError(t, os.MkdirAll(path.Join(blocksDir, id.String(), "chunks"), 0777))
	require.NoError(t, m.WriteToDir(log.NewNopLogger(), path.Join(blocksDir, id.String())))

	f, err := os.Create(path.Join(blocksDir, id.String(), "index"))
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func TestShipper(t *testing.T) {
	blocksDir := t.TempDir()
	bucketDir := t.TempDir()

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: bucketDir})
	require.NoError(t, err)

	logs := &concurrency.SyncBuffer{}
	logger := log.NewLogfmtLogger(logs)

	s := NewShipper(logger, nil, blocksDir, bkt, metadata.TestSource, metadata.NoneFunc)

	t.Run("no shipper file yet", func(t *testing.T) {
		// No shipper file = nothing is reported as shipped.
		shipped, err := readShippedBlocks(blocksDir)
		require.NoError(t, err)
		require.Empty(t, shipped)
	})

	id1 := ulid.MustNew(1, nil)

	t.Run("sync first block", func(t *testing.T) {
		createBlock(t, blocksDir, id1, metadata.Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID:    id1,
				MaxTime: 2000,
				MinTime: 1000,
				Version: 1,
				Stats: tsdb.BlockStats{
					NumSamples: 100, // Shipper checks if number of samples is greater than 0.
				},
			},
			Thanos: metadata.Thanos{Labels: map[string]string{"a": "b"}},
		})

		// Let shipper sync the blocks.
		uploaded, err := s.Sync(context.Background())
		require.NoError(t, err)
		require.Equal(t, 1, uploaded)

		// Verify that shipper has created a file for itself.
		shipped, err := readShippedBlocks(blocksDir)
		require.NoError(t, err)
		require.Len(t, shipped, 1)
		require.Contains(t, shipped, id1)
	})

	t.Run("sync without any new block", func(t *testing.T) {
		// Another sync doesn't do anything.
		uploaded, err := s.Sync(context.Background())
		require.NoError(t, err)
		require.Equal(t, 0, uploaded)
	})

	id2 := ulid.MustNew(2, nil)

	t.Run("sync block without external labels", func(t *testing.T) {
		createBlock(t, blocksDir, id2, metadata.Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID:    id2,
				MaxTime: 4000,
				MinTime: 2000,
				Version: 1,
				Stats: tsdb.BlockStats{
					NumSamples: 100,
				},
			},
			// No Thanos stuff, this will still work.
		})

		// Sync new block
		uploaded, err := s.Sync(context.Background())
		require.NoError(t, err)
		require.Equal(t, 1, uploaded)

		// Check content of shipper file
		shipped, err := readShippedBlocks(blocksDir)
		require.NoError(t, err)
		require.Len(t, shipped, 2)
		require.Contains(t, shipped, id1)
		require.Contains(t, shipped, id2)
	})

	id3 := ulid.MustNew(3, nil)

	t.Run("sync block with 0 samples", func(t *testing.T) {
		createBlock(t, blocksDir, id3, metadata.Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID:    id3,
				MaxTime: 4000,
				MinTime: 2000,
				Version: 1,
				Stats: tsdb.BlockStats{
					NumSamples: 0, // Blocks with 0 samples are not uploaded.
				},
			},
		})

		// Sync new block
		uploaded, err := s.Sync(context.Background())
		require.NoError(t, err)
		require.Equal(t, 0, uploaded)
	})

	t.Run("check if uploaded block has files set", func(t *testing.T) {
		meta, err := block.DownloadMeta(context.Background(), log.NewNopLogger(), bkt, id1)
		require.NoError(t, err)

		require.NotEmpty(t, meta.Thanos.Files)
	})

	t.Log(logs.String())
}
