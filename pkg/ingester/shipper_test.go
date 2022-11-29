// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/shipper/shipper.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package ingester

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
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

// deceivingUploadBucket proxies the calls to the underlying bucket. On uploads and when
// the base name of the object matches objectBaseName, after proxying the call
// an error is returned regardless of what the underlying Bucket returned.
// Useful for when you want to simulate a particular file _appearing_ to fail uploading, but actually succeeding.
type deceivingUploadBucket struct {
	objstore.Bucket

	objectBaseName string
}

func (b deceivingUploadBucket) Upload(ctx context.Context, name string, r io.Reader) error {
	actualErr := b.Bucket.Upload(ctx, name, r)
	if actualErr != nil {
		return actualErr
	} else if path.Base(name) == b.objectBaseName {
		return fmt.Errorf("base name matches, will fail upload")
	}
	return nil
}

func TestShipper_DeceivingUploadErrors(t *testing.T) {
	blocksDir := t.TempDir()
	bucketDir := t.TempDir()

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: bucketDir})
	require.NoError(t, err)
	// Wrap bucket in a decorator so that meta.json file uploads always fail
	bkt = deceivingUploadBucket{Bucket: bkt, objectBaseName: block.MetaFilename}

	logger := log.NewLogfmtLogger(os.Stderr)
	s := NewShipper(logger, nil, blocksDir, bkt, metadata.TestSource, metadata.NoneFunc)

	// Create and upload a block
	id1 := ulid.MustNew(1, nil)
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

	// Let shipper sync the blocks, expecting the meta.json upload to fail.
	uploaded, err := s.Sync(context.Background())
	require.Error(t, err)
	require.Equal(t, 0, uploaded)

	// Sync again. This time the shipper should find the meta.json existing in the bucket and
	// should report an uploaded block without retrying to upload the whole block again.
	uploaded, err = s.Sync(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, uploaded)
}

func TestIterBlockMetas(t *testing.T) {
	dir := t.TempDir()

	id1 := ulid.MustNew(1, nil)
	require.NoError(t, os.Mkdir(path.Join(dir, id1.String()), os.ModePerm))
	require.NoError(t, metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    id1,
			MaxTime: 2000,
			MinTime: 1000,
			Version: 1,
		},
	}.WriteToDir(log.NewNopLogger(), path.Join(dir, id1.String())))

	id2 := ulid.MustNew(2, nil)
	require.NoError(t, os.Mkdir(path.Join(dir, id2.String()), os.ModePerm))
	require.NoError(t, metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    id2,
			MaxTime: 5000,
			MinTime: 4000,
			Version: 1,
		},
	}.WriteToDir(log.NewNopLogger(), path.Join(dir, id2.String())))

	id3 := ulid.MustNew(3, nil)
	require.NoError(t, os.Mkdir(path.Join(dir, id3.String()), os.ModePerm))
	require.NoError(t, metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    id3,
			MaxTime: 3000,
			MinTime: 2000,
			Version: 1,
		},
	}.WriteToDir(log.NewNopLogger(), path.Join(dir, id3.String())))

	shipper := NewShipper(nil, nil, dir, nil, metadata.TestSource, metadata.NoneFunc)
	metas, err := shipper.blockMetasFromOldest()
	require.NoError(t, err)
	require.Equal(t, sort.SliceIsSorted(metas, func(i, j int) bool {
		return metas[i].BlockMeta.MinTime < metas[j].BlockMeta.MinTime
	}), true)
}

func TestShipperAddsSegmentFiles(t *testing.T) {
	dir := t.TempDir()

	inmemory := objstore.NewInMemBucket()

	s := NewShipper(nil, nil, dir, inmemory, metadata.TestSource, metadata.NoneFunc)

	id := ulid.MustNew(1, nil)
	blockDir := path.Join(dir, id.String())
	chunksDir := path.Join(blockDir, block.ChunksDirname)
	require.NoError(t, os.MkdirAll(chunksDir, os.ModePerm))

	// Prepare minimal "block" for shipper (meta.json, index, one segment file).
	require.NoError(t, metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    id,
			MaxTime: 2000,
			MinTime: 1000,
			Version: 1,
			Stats: tsdb.BlockStats{
				NumSamples: 1000, // Not really, but shipper needs nonzero value.
			},
		},
	}.WriteToDir(log.NewNopLogger(), path.Join(dir, id.String())))
	require.NoError(t, os.WriteFile(filepath.Join(blockDir, "index"), []byte("index file"), 0666))
	segmentFile := "00001"
	require.NoError(t, os.WriteFile(filepath.Join(chunksDir, segmentFile), []byte("hello world"), 0666))

	uploaded, err := s.Sync(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, uploaded)

	meta, err := block.DownloadMeta(context.Background(), log.NewNopLogger(), inmemory, id)
	require.NoError(t, err)

	require.Equal(t, []string{segmentFile}, meta.Thanos.SegmentFiles)
}

func TestReadMetaFile(t *testing.T) {
	t.Run("Missing meta file", func(t *testing.T) {
		// Create TSDB directory without meta file
		dpath := t.TempDir()

		_, err := readShipperMetaFile(dpath)
		fpath := filepath.Join(dpath, shipperMetaFilename)
		require.Equal(t, fmt.Sprintf(`failed to read %s: open %s: no such file or directory`, fpath, fpath), err.Error())
	})

	t.Run("Non-JSON meta file", func(t *testing.T) {
		dpath := t.TempDir()
		fpath := filepath.Join(dpath, shipperMetaFilename)
		// Make an invalid JSON file
		require.NoError(t, os.WriteFile(fpath, []byte("{"), 0600))

		_, err := readShipperMetaFile(dpath)
		require.Equal(t, fmt.Sprintf(`failed to parse %s as JSON: "{": unexpected end of JSON input`, fpath), err.Error())
	})

	t.Run("Wrongly versioned meta file", func(t *testing.T) {
		dpath := t.TempDir()
		fpath := filepath.Join(dpath, shipperMetaFilename)
		require.NoError(t, os.WriteFile(fpath, []byte(`{"version": 2}`), 0600))

		_, err := readShipperMetaFile(dpath)
		require.Equal(t, "unexpected meta file version 2", err.Error())
	})
}
