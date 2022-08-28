// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/block_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package tsdb

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/block"

	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
	"github.com/grafana/mimir/pkg/storegateway/testhelper"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestUploadBlock(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()

	bkt := objstore.NewInMemBucket()
	b1, err := testhelper.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
		{{Name: "a", Value: "3"}},
		{{Name: "a", Value: "4"}},
		{{Name: "b", Value: "1"}},
	}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "val1"}}, 124, metadata.NoneFunc)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(path.Join(tmpDir, "test", b1.String()), os.ModePerm))

	t.Run("wrong dir", func(t *testing.T) {
		// Wrong dir.
		err := UploadBlock(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "not-existing"), nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "/not-existing: no such file or directory")
	})

	t.Run("wrong existing dir (not a block)", func(t *testing.T) {
		err := UploadBlock(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test"), nil)
		require.EqualError(t, err, "not a block dir: ulid: bad data size when unmarshaling")
	})

	t.Run("empty block dir", func(t *testing.T) {
		err := UploadBlock(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "/meta.json: no such file or directory")
	})

	t.Run("missing chunks", func(t *testing.T) {
		test.Copy(t, path.Join(tmpDir, b1.String(), block.MetaFilename), path.Join(tmpDir, "test", b1.String(), block.MetaFilename))

		err := UploadBlock(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "/chunks: no such file or directory")
	})

	t.Run("missing index file", func(t *testing.T) {
		require.NoError(t, os.MkdirAll(path.Join(tmpDir, "test", b1.String(), block.ChunksDirname), 0777))
		test.Copy(t, path.Join(tmpDir, b1.String(), block.ChunksDirname, "000001"), path.Join(tmpDir, "test", b1.String(), block.ChunksDirname, "000001"))

		err := UploadBlock(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "/index: no such file or directory")
	})

	t.Run("missing meta.json file", func(t *testing.T) {
		test.Copy(t, path.Join(tmpDir, b1.String(), block.IndexFilename), path.Join(tmpDir, "test", b1.String(), block.IndexFilename))
		require.NoError(t, os.Remove(path.Join(tmpDir, "test", b1.String(), block.MetaFilename)))

		// Missing meta.json file.
		err := UploadBlock(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "/meta.json: no such file or directory")
	})

	t.Run("missing meta.json file, but valid metadata supplied as argument", func(t *testing.T) {
		// Read meta.json from original block
		meta, err := metadata.ReadFromDir(path.Join(tmpDir, b1.String()))
		require.NoError(t, err)

		// Make sure meta.json doesn't exist in "test" block.
		require.NoError(t, os.RemoveAll(path.Join(tmpDir, "test", b1.String(), block.MetaFilename)))

		// Missing meta.json file.
		err = UploadBlock(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), meta)
		require.Error(t, err)
		require.Contains(t, err.Error(), "/meta.json: no such file or directory")
	})

	test.Copy(t, path.Join(tmpDir, b1.String(), block.MetaFilename), path.Join(tmpDir, "test", b1.String(), block.MetaFilename))

	t.Run("full block", func(t *testing.T) {
		// Full block.
		require.NoError(t, UploadBlock(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), nil))
		require.Equal(t, 3, len(bkt.Objects()))
		chunkFileSize := getFileSize(t, filepath.Join(tmpDir, b1.String(), block.ChunksDirname, "000001"))
		require.Equal(t, chunkFileSize, int64(len(bkt.Objects()[path.Join(b1.String(), block.ChunksDirname, "000001")])))
		require.Equal(t, 401, len(bkt.Objects()[path.Join(b1.String(), block.IndexFilename)]))
		require.Equal(t, 570, len(bkt.Objects()[path.Join(b1.String(), block.MetaFilename)]))

		origMeta, err := metadata.ReadFromDir(path.Join(tmpDir, "test", b1.String()))
		require.NoError(t, err)

		uploadedMeta, err := block.DownloadMeta(context.Background(), log.NewNopLogger(), bkt, b1)
		require.NoError(t, err)

		files := uploadedMeta.Thanos.Files
		require.Len(t, files, 3)
		require.Equal(t, metadata.File{RelPath: "chunks/000001", SizeBytes: chunkFileSize}, files[0])
		require.Equal(t, metadata.File{RelPath: "index", SizeBytes: 401}, files[1])
		require.Equal(t, metadata.File{RelPath: "meta.json", SizeBytes: 0}, files[2]) // meta.json is added to the files without its size.

		// clear files before comparing against original meta.json
		uploadedMeta.Thanos.Files = nil

		require.Equal(t, origMeta, &uploadedMeta)
	})

	t.Run("upload is idempotent", func(t *testing.T) {
		// Test Upload is idempotent.
		require.NoError(t, UploadBlock(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), nil))
		require.Equal(t, 3, len(bkt.Objects()))
		chunkFileSize := getFileSize(t, filepath.Join(tmpDir, b1.String(), block.ChunksDirname, "000001"))
		require.Equal(t, chunkFileSize, int64(len(bkt.Objects()[path.Join(b1.String(), block.ChunksDirname, "000001")])))
		require.Equal(t, 401, len(bkt.Objects()[path.Join(b1.String(), block.IndexFilename)]))
		require.Equal(t, 570, len(bkt.Objects()[path.Join(b1.String(), block.MetaFilename)]))
	})

	t.Run("upload with no external labels works just fine", func(t *testing.T) {
		// Upload with no external labels should be blocked.
		b2, err := testhelper.CreateBlock(ctx, tmpDir, []labels.Labels{
			{{Name: "a", Value: "1"}},
			{{Name: "a", Value: "2"}},
			{{Name: "a", Value: "3"}},
			{{Name: "a", Value: "4"}},
			{{Name: "b", Value: "1"}},
		}, 100, 0, 1000, nil, 124, metadata.NoneFunc)
		require.NoError(t, err)

		err = UploadBlock(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, b2.String()), nil)
		require.NoError(t, err)

		chunkFileSize := getFileSize(t, filepath.Join(tmpDir, b2.String(), block.ChunksDirname, "000001"))
		require.Equal(t, 6, len(bkt.Objects())) // 3 from b1, 3 from b2
		require.Equal(t, chunkFileSize, int64(len(bkt.Objects()[path.Join(b2.String(), block.ChunksDirname, "000001")])))
		require.Equal(t, 401, len(bkt.Objects()[path.Join(b2.String(), block.IndexFilename)]))
		require.Equal(t, 549, len(bkt.Objects()[path.Join(b2.String(), block.MetaFilename)]))

		origMeta, err := metadata.ReadFromDir(path.Join(tmpDir, b2.String()))
		require.NoError(t, err)

		uploadedMeta, err := block.DownloadMeta(context.Background(), log.NewNopLogger(), bkt, b2)
		require.NoError(t, err)

		// Files are not in the original meta.
		uploadedMeta.Thanos.Files = nil
		require.Equal(t, origMeta, &uploadedMeta)
	})

	t.Run("upload with supplied meta.json", func(t *testing.T) {
		// Upload with no external labels should be blocked.
		b3, err := testhelper.CreateBlock(ctx, tmpDir, []labels.Labels{
			{{Name: "a", Value: "1"}},
			{{Name: "a", Value: "2"}},
			{{Name: "a", Value: "3"}},
			{{Name: "a", Value: "4"}},
			{{Name: "b", Value: "1"}},
		}, 100, 0, 1000, nil, 124, metadata.NoneFunc)
		require.NoError(t, err)

		// Prepare metadata that will be uploaded to the bucket.
		updatedMeta, err := metadata.ReadFromDir(path.Join(tmpDir, b3.String()))
		require.NoError(t, err)
		require.Empty(t, updatedMeta.Thanos.Labels)
		require.Equal(t, metadata.TestSource, updatedMeta.Thanos.Source)
		updatedMeta.Thanos.Labels = map[string]string{"a": "b", "c": "d"}
		updatedMeta.Thanos.Source = "hello world"

		// Upload block with new metadata.
		err = UploadBlock(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, b3.String()), updatedMeta)
		require.NoError(t, err)

		// Verify that original (on-disk) meta.json is not changed
		origMeta, err := metadata.ReadFromDir(path.Join(tmpDir, b3.String()))
		require.NoError(t, err)
		require.Empty(t, origMeta.Thanos.Labels)
		require.Equal(t, metadata.TestSource, origMeta.Thanos.Source)

		// Verify that meta.json uploaded in the bucket has updated values.
		bucketMeta, err := block.DownloadMeta(context.Background(), log.NewNopLogger(), bkt, b3)
		require.NoError(t, err)
		require.Equal(t, updatedMeta.Thanos.Labels, bucketMeta.Thanos.Labels)
		require.Equal(t, updatedMeta.Thanos.Source, bucketMeta.Thanos.Source)
	})
}

func getFileSize(t *testing.T, filepath string) int64 {
	t.Helper()

	st, err := os.Stat(filepath)
	require.NoError(t, err)
	return st.Size()
}
