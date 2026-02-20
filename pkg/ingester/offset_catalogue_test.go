// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOffsetCatalogue_SaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)

	c1 := newOffsetCatalogue(log.NewNopLogger(), dir, "tenant-1")
	c1.Set(offsetCatalogueBlockHead, offsetWatermark{Topic: "ingest", Partition: 3, Offset: 500})
	c1.Set(block1.String(), offsetWatermark{Topic: "ingest", Partition: 3, Offset: 400})
	c1.Set(block2.String(), offsetWatermark{Topic: "ingest", Partition: 3, Offset: 450})
	require.NoError(t, c1.Save())

	// Verify file is at the expected path.
	_, err := os.Stat(filepath.Join(dir, offsetCatalogueFilename))
	require.NoError(t, err)

	c2 := newOffsetCatalogue(log.NewNopLogger(), dir, "tenant-1")
	require.NoError(t, c2.Load([]ulid.ULID{block1, block2}))

	got, ok := c2.Get(offsetCatalogueBlockHead)
	require.True(t, ok)
	assert.Equal(t, int64(500), got.Offset)

	got, ok = c2.Get(block1.String())
	require.True(t, ok)
	assert.Equal(t, int64(400), got.Offset)

	got, ok = c2.Get(block2.String())
	require.True(t, ok)
	assert.Equal(t, int64(450), got.Offset)
}

func TestOffsetCatalogue_LoadPrunes(t *testing.T) {
	dir := t.TempDir()
	block1 := ulid.MustNew(1, nil)
	stale := ulid.MustNew(2, nil)

	c1 := newOffsetCatalogue(log.NewNopLogger(), dir, "tenant-1")
	c1.Set(offsetCatalogueBlockHead, offsetWatermark{Offset: 500})
	c1.Set(block1.String(), offsetWatermark{Offset: 400})
	c1.Set(stale.String(), offsetWatermark{Offset: 450})
	require.NoError(t, c1.Save())

	c2 := newOffsetCatalogue(log.NewNopLogger(), dir, "tenant-1")
	require.NoError(t, c2.Load([]ulid.ULID{block1}))

	_, ok := c2.Get(offsetCatalogueBlockHead)
	assert.True(t, ok)

	_, ok = c2.Get(block1.String())
	assert.True(t, ok)

	_, ok = c2.Get(stale.String())
	assert.False(t, ok)
}

func TestOffsetCatalogue_LoadToleratesMissingAndCorruptFiles(t *testing.T) {
	t.Run("missing file", func(t *testing.T) {
		c := newOffsetCatalogue(log.NewNopLogger(), t.TempDir(), "tenant-1")
		require.NoError(t, c.Load(nil))
		_, ok := c.Get(offsetCatalogueBlockHead)
		assert.False(t, ok)
	})

	t.Run("corrupt file", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, offsetCatalogueFilename), []byte("not json"), 0644))

		c := newOffsetCatalogue(log.NewNopLogger(), dir, "tenant-1")
		require.NoError(t, c.Load(nil))
		_, ok := c.Get(offsetCatalogueBlockHead)
		assert.False(t, ok)
	})

	t.Run("unknown version", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.WriteFile(filepath.Join(dir, offsetCatalogueFilename), []byte(`{"version":99,"data":{}}`), 0644))

		c := newOffsetCatalogue(log.NewNopLogger(), dir, "tenant-1")
		require.NoError(t, c.Load(nil))
		_, ok := c.Get(offsetCatalogueBlockHead)
		assert.False(t, ok)
	})
}
