// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestOffsetCatalogue(t *testing.T) {
	const userID = "user-1"

	t.Run("no existing catalogue file", func(t *testing.T) {
		dir := t.TempDir()
		id := ulid.MustNew(1, nil)
		createBlock(t, dir, id, block.Meta{BlockMeta: tsdb.BlockMeta{ULID: id}})

		c := newOffsetCatalogue(log.NewNopLogger(), newOffsetCatalogueMetrics(prometheus.NewRegistry()), dir, userID, 1)
		require.NoError(t, c.Sync(t.Context(), 100))

		data, err := readOffsetCatalogueFromFile(dir)
		require.NoError(t, err)
		require.Equal(t, offsetCatalogueVersion, data.Version)
		require.Equal(t, offsetWatermark{Partition: 1, Offset: 100}, data.Data[id.String()])
	})

	t.Run("preserves existing entries", func(t *testing.T) {
		dir := t.TempDir()
		id1, id2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)

		createBlock(t, dir, id1, block.Meta{BlockMeta: tsdb.BlockMeta{ULID: id1}})
		c := newOffsetCatalogue(log.NewNopLogger(), newOffsetCatalogueMetrics(prometheus.NewRegistry()), dir, userID, 1)
		require.NoError(t, c.Sync(t.Context(), 50))

		// New block appears; second Sync must not overwrite id1's watermark.
		createBlock(t, dir, id2, block.Meta{BlockMeta: tsdb.BlockMeta{ULID: id2}})
		require.NoError(t, c.Sync(t.Context(), 200))

		data, err := readOffsetCatalogueFromFile(dir)
		require.NoError(t, err)
		require.Equal(t, offsetWatermark{Partition: 1, Offset: 50}, data.Data[id1.String()])
		require.Equal(t, offsetWatermark{Partition: 1, Offset: 200}, data.Data[id2.String()])
	})

	t.Run("drops deleted blocks", func(t *testing.T) {
		dir := t.TempDir()
		id1, id2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)

		createBlock(t, dir, id1, block.Meta{BlockMeta: tsdb.BlockMeta{ULID: id1}})
		createBlock(t, dir, id2, block.Meta{BlockMeta: tsdb.BlockMeta{ULID: id2}})
		c := newOffsetCatalogue(log.NewNopLogger(), newOffsetCatalogueMetrics(prometheus.NewRegistry()), dir, userID, 1)
		require.NoError(t, c.Sync(t.Context(), 10))

		// Remove id1 (shipped / retention-deleted).
		require.NoError(t, os.RemoveAll(filepath.Join(dir, id1.String())))
		require.NoError(t, c.Sync(t.Context(), 20))

		data, err := readOffsetCatalogueFromFile(dir)
		require.NoError(t, err)
		require.NotContains(t, data.Data, id1.String())
		require.Contains(t, data.Data, id2.String())
	})

	t.Run("no blocks", func(t *testing.T) {
		dir := t.TempDir()
		c := newOffsetCatalogue(log.NewNopLogger(), newOffsetCatalogueMetrics(prometheus.NewRegistry()), dir, userID, 1)
		require.NoError(t, c.Sync(t.Context(), 42))

		data, err := readOffsetCatalogueFromFile(dir)
		require.NoError(t, err)
		require.Empty(t, data.Data)
	})

	t.Run("rejects version mismatch", func(t *testing.T) {
		dir := t.TempDir()
		content := `{"version":99,"updated_at":0,"data":{}}`
		require.NoError(t, os.WriteFile(filepath.Join(dir, offsetCatalogueFilename), []byte(content), 0o644))

		_, err := readOffsetCatalogueFromFile(dir)
		require.Error(t, err)
	})
}
