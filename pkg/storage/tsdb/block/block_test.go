// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/block_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package block

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
	"github.com/grafana/mimir/pkg/storegateway/testhelper"
	e2eutil "github.com/grafana/mimir/pkg/storegateway/testhelper"
	"github.com/grafana/mimir/pkg/util/test"
	testutil "github.com/grafana/mimir/pkg/util/test"
)

var (
	fiveLabels = []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("a", "3"),
		labels.FromStrings("a", "4"),
		labels.FromStrings("b", "1"),
	}
)

func TestIsBlockDir(t *testing.T) {
	for _, tc := range []struct {
		input string
		id    ulid.ULID
		bdir  bool
	}{
		{
			input: "",
			bdir:  false,
		},
		{
			input: "something",
			bdir:  false,
		},
		{
			id:    ulid.MustNew(1, nil),
			input: ulid.MustNew(1, nil).String(),
			bdir:  true,
		},
		{
			id:    ulid.MustNew(2, nil),
			input: "/" + ulid.MustNew(2, nil).String(),
			bdir:  true,
		},
		{
			id:    ulid.MustNew(3, nil),
			input: "some/path/" + ulid.MustNew(3, nil).String(),
			bdir:  true,
		},
		{
			input: ulid.MustNew(4, nil).String() + "/something",
			bdir:  false,
		},
	} {
		t.Run(tc.input, func(t *testing.T) {
			id, ok := IsBlockDir(tc.input)
			require.Equal(t, tc.bdir, ok)

			if id.Compare(tc.id) != 0 {
				t.Errorf("expected %s got %s", tc.id, id)
				t.FailNow()
			}
		})
	}
}

func TestDelete(t *testing.T) {
	testutil.VerifyNoLeak(t)
	ctx := context.Background()

	tmpDir := t.TempDir()

	bkt := objstore.NewInMemBucket()
	{
		b1, err := e2eutil.CreateBlock(ctx, tmpDir, fiveLabels,
			100, 0, 1000, labels.FromStrings("ext1", "val1"), 124, metadata.NoneFunc)
		require.NoError(t, err)
		require.NoError(t, Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, b1.String()), nil))
		require.Equal(t, 3, len(bkt.Objects()))

		markedForDeletion := promauto.With(prometheus.NewRegistry()).NewCounter(prometheus.CounterOpts{Name: "test"})
		require.NoError(t, MarkForDeletion(ctx, log.NewNopLogger(), bkt, b1, "", markedForDeletion))

		// Full delete.
		require.NoError(t, Delete(ctx, log.NewNopLogger(), bkt, b1))
		require.Equal(t, 0, len(bkt.Objects()))
	}
	{
		b2, err := e2eutil.CreateBlock(ctx, tmpDir, fiveLabels,
			100, 0, 1000, labels.FromStrings("ext1", "val1"), 124, metadata.NoneFunc)
		require.NoError(t, err)
		require.NoError(t, Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, b2.String()), nil))
		require.Equal(t, 3, len(bkt.Objects()))

		// Remove meta.json and check if delete can delete it.
		require.NoError(t, bkt.Delete(ctx, path.Join(b2.String(), MetaFilename)))
		require.NoError(t, Delete(ctx, log.NewNopLogger(), bkt, b2))
		require.Equal(t, 0, len(bkt.Objects()))
	}
}

func TestUpload(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()

	bkt := objstore.NewInMemBucket()
	b1, err := testhelper.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("a", "3"),
		labels.FromStrings("a", "4"),
		labels.FromStrings("b", "1"),
	}, 100, 0, 1000, labels.FromStrings("ext1", "val1"), 124, metadata.NoneFunc)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(path.Join(tmpDir, "test", b1.String()), os.ModePerm))

	t.Run("wrong dir", func(t *testing.T) {
		// Wrong dir.
		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "not-existing"), nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "/not-existing: no such file or directory")
	})

	t.Run("wrong existing dir (not a block)", func(t *testing.T) {
		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test"), nil)
		require.EqualError(t, err, "not a block dir: ulid: bad data size when unmarshaling")
	})

	t.Run("empty block dir", func(t *testing.T) {
		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "/meta.json: no such file or directory")
	})

	t.Run("missing chunks", func(t *testing.T) {
		test.Copy(t, path.Join(tmpDir, b1.String(), MetaFilename), path.Join(tmpDir, "test", b1.String(), MetaFilename))

		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "/chunks: no such file or directory")
	})

	t.Run("missing index file", func(t *testing.T) {
		require.NoError(t, os.MkdirAll(path.Join(tmpDir, "test", b1.String(), ChunksDirname), 0777))
		test.Copy(t, path.Join(tmpDir, b1.String(), ChunksDirname, "000001"), path.Join(tmpDir, "test", b1.String(), ChunksDirname, "000001"))

		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "/index: no such file or directory")
	})

	t.Run("missing meta.json file", func(t *testing.T) {
		test.Copy(t, path.Join(tmpDir, b1.String(), IndexFilename), path.Join(tmpDir, "test", b1.String(), IndexFilename))
		require.NoError(t, os.Remove(path.Join(tmpDir, "test", b1.String(), MetaFilename)))

		// Missing meta.json file.
		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "/meta.json: no such file or directory")
	})

	t.Run("missing meta.json file, but valid metadata supplied as argument", func(t *testing.T) {
		// Read meta.json from original block
		meta, err := metadata.ReadFromDir(path.Join(tmpDir, b1.String()))
		require.NoError(t, err)

		// Make sure meta.json doesn't exist in "test"
		require.NoError(t, os.RemoveAll(path.Join(tmpDir, "test", b1.String(), MetaFilename)))

		// Missing meta.json file.
		err = Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), meta)
		require.Error(t, err)
		require.Contains(t, err.Error(), "/meta.json: no such file or directory")
	})

	test.Copy(t, path.Join(tmpDir, b1.String(), MetaFilename), path.Join(tmpDir, "test", b1.String(), MetaFilename))

	t.Run("full block", func(t *testing.T) {
		// Full
		require.NoError(t, Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), nil))
		require.Equal(t, 3, len(bkt.Objects()))
		chunkFileSize := getFileSize(t, filepath.Join(tmpDir, b1.String(), ChunksDirname, "000001"))
		require.Equal(t, chunkFileSize, int64(len(bkt.Objects()[path.Join(b1.String(), ChunksDirname, "000001")])))
		require.Equal(t, 401, len(bkt.Objects()[path.Join(b1.String(), IndexFilename)]))
		require.Equal(t, 570, len(bkt.Objects()[path.Join(b1.String(), MetaFilename)]))

		origMeta, err := metadata.ReadFromDir(path.Join(tmpDir, "test", b1.String()))
		require.NoError(t, err)

		uploadedMeta, err := DownloadMeta(context.Background(), log.NewNopLogger(), bkt, b1)
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
		require.NoError(t, Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), nil))
		require.Equal(t, 3, len(bkt.Objects()))
		chunkFileSize := getFileSize(t, filepath.Join(tmpDir, b1.String(), ChunksDirname, "000001"))
		require.Equal(t, chunkFileSize, int64(len(bkt.Objects()[path.Join(b1.String(), ChunksDirname, "000001")])))
		require.Equal(t, 401, len(bkt.Objects()[path.Join(b1.String(), IndexFilename)]))
		require.Equal(t, 570, len(bkt.Objects()[path.Join(b1.String(), MetaFilename)]))
	})

	t.Run("upload with no external labels works just fine", func(t *testing.T) {
		// Upload with no external labels should be blocked.
		b2, err := testhelper.CreateBlock(ctx, tmpDir, []labels.Labels{
			labels.FromStrings("a", "1"),
			labels.FromStrings("a", "2"),
			labels.FromStrings("a", "3"),
			labels.FromStrings("a", "4"),
			labels.FromStrings("b", "1"),
		}, 100, 0, 1000, labels.EmptyLabels(), 124, metadata.NoneFunc)
		require.NoError(t, err)

		err = Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, b2.String()), nil)
		require.NoError(t, err)

		chunkFileSize := getFileSize(t, filepath.Join(tmpDir, b2.String(), ChunksDirname, "000001"))
		require.Equal(t, 6, len(bkt.Objects())) // 3 from b1, 3 from b2
		require.Equal(t, chunkFileSize, int64(len(bkt.Objects()[path.Join(b2.String(), ChunksDirname, "000001")])))
		require.Equal(t, 401, len(bkt.Objects()[path.Join(b2.String(), IndexFilename)]))
		require.Equal(t, 549, len(bkt.Objects()[path.Join(b2.String(), MetaFilename)]))

		origMeta, err := metadata.ReadFromDir(path.Join(tmpDir, b2.String()))
		require.NoError(t, err)

		uploadedMeta, err := DownloadMeta(context.Background(), log.NewNopLogger(), bkt, b2)
		require.NoError(t, err)

		// Files are not in the original meta.
		uploadedMeta.Thanos.Files = nil
		require.Equal(t, origMeta, &uploadedMeta)
	})

	t.Run("upload with supplied meta.json", func(t *testing.T) {
		// Upload with no external labels should be blocked.
		b3, err := testhelper.CreateBlock(ctx, tmpDir, []labels.Labels{
			labels.FromStrings("a", "1"),
			labels.FromStrings("a", "2"),
			labels.FromStrings("a", "3"),
			labels.FromStrings("a", "4"),
			labels.FromStrings("b", "1"),
		}, 100, 0, 1000, labels.EmptyLabels(), 124, metadata.NoneFunc)
		require.NoError(t, err)

		// Prepare metadata that will be uploaded to the bucket.
		updatedMeta, err := metadata.ReadFromDir(path.Join(tmpDir, b3.String()))
		require.NoError(t, err)
		require.Empty(t, updatedMeta.Thanos.Labels)
		require.Equal(t, metadata.TestSource, updatedMeta.Thanos.Source)
		updatedMeta.Thanos.Labels = map[string]string{"a": "b", "c": "d"}
		updatedMeta.Thanos.Source = "hello world"

		// Upload block with new metadata.
		err = Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, b3.String()), updatedMeta)
		require.NoError(t, err)

		// Verify that original (on-disk) meta.json is not changed
		origMeta, err := metadata.ReadFromDir(path.Join(tmpDir, b3.String()))
		require.NoError(t, err)
		require.Empty(t, origMeta.Thanos.Labels)
		require.Equal(t, metadata.TestSource, origMeta.Thanos.Source)

		// Verify that meta.json uploaded in the bucket has updated values.
		bucketMeta, err := DownloadMeta(context.Background(), log.NewNopLogger(), bkt, b3)
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

func TestMarkForDeletion(t *testing.T) {
	testutil.VerifyNoLeak(t)
	ctx := context.Background()

	tmpDir := t.TempDir()

	for _, tcase := range []struct {
		name      string
		preUpload func(t testing.TB, id ulid.ULID, bkt objstore.Bucket)

		blocksMarked int
	}{
		{
			name:         "block marked for deletion",
			preUpload:    func(t testing.TB, id ulid.ULID, bkt objstore.Bucket) {},
			blocksMarked: 1,
		},
		{
			name: "block with deletion mark already, expected log and no metric increment",
			preUpload: func(t testing.TB, id ulid.ULID, bkt objstore.Bucket) {
				deletionMark, err := json.Marshal(metadata.DeletionMark{
					ID:           id,
					DeletionTime: time.Now().Unix(),
					Version:      metadata.DeletionMarkVersion1,
				})
				require.NoError(t, err)
				require.NoError(t, bkt.Upload(ctx, path.Join(id.String(), metadata.DeletionMarkFilename), bytes.NewReader(deletionMark)))
			},
			blocksMarked: 0,
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			bkt := objstore.NewInMemBucket()
			id, err := e2eutil.CreateBlock(ctx, tmpDir, fiveLabels,
				100, 0, 1000, labels.FromStrings("ext1", "val1"), 124, metadata.NoneFunc)
			require.NoError(t, err)

			tcase.preUpload(t, id, bkt)

			require.NoError(t, Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, id.String()), nil))

			c := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
			err = MarkForDeletion(ctx, log.NewNopLogger(), bkt, id, "", c)
			require.NoError(t, err)
			require.Equal(t, float64(tcase.blocksMarked), promtest.ToFloat64(c))
		})
	}
}

func TestMarkForNoCompact(t *testing.T) {
	testutil.VerifyNoLeak(t)
	ctx := context.Background()

	tmpDir := t.TempDir()

	for _, tcase := range []struct {
		name      string
		preUpload func(t testing.TB, id ulid.ULID, bkt objstore.Bucket)

		blocksMarked int
	}{
		{
			name:         "block marked",
			preUpload:    func(t testing.TB, id ulid.ULID, bkt objstore.Bucket) {},
			blocksMarked: 1,
		},
		{
			name: "block with no-compact mark already, expected log and no metric increment",
			preUpload: func(t testing.TB, id ulid.ULID, bkt objstore.Bucket) {
				m, err := json.Marshal(metadata.NoCompactMark{
					ID:            id,
					NoCompactTime: time.Now().Unix(),
					Version:       metadata.NoCompactMarkVersion1,
				})
				require.NoError(t, err)
				require.NoError(t, bkt.Upload(ctx, path.Join(id.String(), metadata.NoCompactMarkFilename), bytes.NewReader(m)))
			},
			blocksMarked: 0,
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			bkt := objstore.NewInMemBucket()
			id, err := e2eutil.CreateBlock(ctx, tmpDir, fiveLabels,
				100, 0, 1000, labels.FromStrings("ext1", "val1"), 124, metadata.NoneFunc)
			require.NoError(t, err)

			tcase.preUpload(t, id, bkt)

			require.NoError(t, Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, id.String()), nil))

			c := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
			err = MarkForNoCompact(ctx, log.NewNopLogger(), bkt, id, metadata.ManualNoCompactReason, "", c)
			require.NoError(t, err)
			require.Equal(t, float64(tcase.blocksMarked), promtest.ToFloat64(c))
		})
	}
}

func TestUploadCleanup(t *testing.T) {
	testutil.VerifyNoLeak(t)

	ctx := context.Background()

	tmpDir := t.TempDir()

	bkt := objstore.NewInMemBucket()
	b1, err := e2eutil.CreateBlock(ctx, tmpDir, fiveLabels,
		100, 0, 1000, labels.FromStrings("ext1", "val1"), 124, metadata.NoneFunc)
	require.NoError(t, err)

	{
		errBkt := errBucket{Bucket: bkt, failSuffix: "/index"}

		uploadErr := Upload(ctx, log.NewNopLogger(), errBkt, path.Join(tmpDir, b1.String()), nil)
		require.ErrorIs(t, uploadErr, errUploadFailed)

		// If upload of index fails, block is deleted.
		require.Equal(t, 0, len(bkt.Objects()))
		require.Equal(t, 0, len(bkt.Objects()[path.Join(DebugMetas, fmt.Sprintf("%s.json", b1.String()))]))
	}

	{
		errBkt := errBucket{Bucket: bkt, failSuffix: "/meta.json"}

		uploadErr := Upload(ctx, log.NewNopLogger(), errBkt, path.Join(tmpDir, b1.String()), nil)
		require.ErrorIs(t, uploadErr, errUploadFailed)

		// If upload of meta.json fails, nothing is cleaned up.
		require.Equal(t, 3, len(bkt.Objects()))
		require.Greater(t, len(bkt.Objects()[path.Join(b1.String(), ChunksDirname, "000001")]), 0)
		require.Greater(t, len(bkt.Objects()[path.Join(b1.String(), IndexFilename)]), 0)
		require.Greater(t, len(bkt.Objects()[path.Join(b1.String(), MetaFilename)]), 0)
		require.Equal(t, 0, len(bkt.Objects()[path.Join(DebugMetas, fmt.Sprintf("%s.json", b1.String()))]))
	}
}

var errUploadFailed = errors.New("upload failed")

type errBucket struct {
	objstore.Bucket

	failSuffix string
}

func (eb errBucket) Upload(ctx context.Context, name string, r io.Reader) error {
	err := eb.Bucket.Upload(ctx, name, r)
	if err != nil {
		return err
	}

	if strings.HasSuffix(name, eb.failSuffix) {
		return errUploadFailed
	}
	return nil
}
