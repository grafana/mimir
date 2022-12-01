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
	"math/rand"
	"os"
	"path"
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
	e2eutil "github.com/grafana/mimir/pkg/storegateway/testhelper"
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

func TestUpload(t *testing.T) {
	testutil.VerifyNoLeak(t)

	ctx := context.Background()

	tmpDir := t.TempDir()

	bkt := objstore.NewInMemBucket()
	rand.Seed(1) // hard-coded sizes later in this test depend on values created "randomly" by CreateBlock.
	b1, err := e2eutil.CreateBlock(ctx, tmpDir, fiveLabels,
		100, 0, 1000, labels.FromStrings("ext1", "val1"), 124, metadata.NoneFunc)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(path.Join(tmpDir, "test", b1.String()), os.ModePerm))

	{
		// Wrong dir.
		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "not-existing"), metadata.NoneFunc)
		require.ErrorContains(t, err, "/not-existing: no such file or directory")
	}
	{
		// Wrong existing dir (not a block).
		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test"), metadata.NoneFunc)
		require.ErrorContains(t, err, "not a block dir: ulid: bad data size when unmarshaling")
	}
	{
		// Empty block dir.
		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), metadata.NoneFunc)
		require.ErrorContains(t, err, "/meta.json: no such file or directory")
	}
	testutil.Copy(t, path.Join(tmpDir, b1.String(), MetaFilename), path.Join(tmpDir, "test", b1.String(), MetaFilename))
	{
		// Missing chunks.
		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), metadata.NoneFunc)
		require.ErrorContains(t, err, "/chunks: no such file or directory")
	}
	require.NoError(t, os.MkdirAll(path.Join(tmpDir, "test", b1.String(), ChunksDirname), os.ModePerm))
	testutil.Copy(t, path.Join(tmpDir, b1.String(), ChunksDirname, "000001"), path.Join(tmpDir, "test", b1.String(), ChunksDirname, "000001"))
	{
		// Missing index file.
		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), metadata.NoneFunc)
		require.ErrorContains(t, err, "/index: no such file or directory")
	}
	testutil.Copy(t, path.Join(tmpDir, b1.String(), IndexFilename), path.Join(tmpDir, "test", b1.String(), IndexFilename))
	require.NoError(t, os.Remove(path.Join(tmpDir, "test", b1.String(), MetaFilename)))
	{
		// Missing meta.json file.
		err := Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), metadata.NoneFunc)
		require.ErrorContains(t, err, "/meta.json: no such file or directory")
	}
	testutil.Copy(t, path.Join(tmpDir, b1.String(), MetaFilename), path.Join(tmpDir, "test", b1.String(), MetaFilename))
	{
		// Full block.
		require.NoError(t, Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), metadata.NoneFunc))
		require.Equal(t, 3, len(bkt.Objects()))
		// Note this value 3751 depends on random numbers, so we fix the seed earlier.
		require.Equal(t, 3751, len(bkt.Objects()[path.Join(b1.String(), ChunksDirname, "000001")]))
		require.Equal(t, 401, len(bkt.Objects()[path.Join(b1.String(), IndexFilename)]))
		require.Equal(t, 570, len(bkt.Objects()[path.Join(b1.String(), MetaFilename)]))

		// File stats are gathered.
		require.Equal(t, fmt.Sprintf(`{
	"ulid": "%s",
	"minTime": 0,
	"maxTime": 1000,
	"stats": {
		"numSamples": 500,
		"numSeries": 5,
		"numChunks": 5
	},
	"compaction": {
		"level": 1,
		"sources": [
			"%s"
		]
	},
	"version": 1,
	"out_of_order": false,
	"thanos": {
		"labels": {
			"ext1": "val1"
		},
		"downsample": {
			"resolution": 124
		},
		"source": "test",
		"files": [
			{
				"rel_path": "chunks/000001",
				"size_bytes": 3751
			},
			{
				"rel_path": "index",
				"size_bytes": 401
			},
			{
				"rel_path": "meta.json"
			}
		]
	}
}
`, b1.String(), b1.String()), string(bkt.Objects()[path.Join(b1.String(), MetaFilename)]))
	}
	{
		// Test Upload is idempotent.
		require.NoError(t, Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, "test", b1.String()), metadata.NoneFunc))
		require.Equal(t, 3, len(bkt.Objects()))
		require.Equal(t, 3751, len(bkt.Objects()[path.Join(b1.String(), ChunksDirname, "000001")]))
		require.Equal(t, 401, len(bkt.Objects()[path.Join(b1.String(), IndexFilename)]))
		require.Equal(t, 570, len(bkt.Objects()[path.Join(b1.String(), MetaFilename)]))
	}
	{
		// Upload with no external labels should be blocked.
		b2, err := e2eutil.CreateBlock(ctx, tmpDir, fiveLabels,
			100, 0, 1000, labels.EmptyLabels(), 124, metadata.NoneFunc)
		require.NoError(t, err)
		err = Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, b2.String()), metadata.NoneFunc)
		require.Error(t, err)
		require.Equal(t, "empty external labels are not allowed for Thanos block", err.Error())
		require.Equal(t, 3, len(bkt.Objects()))
	}
	{
		// No external labels with UploadPromBlocks.
		b2, err := e2eutil.CreateBlock(ctx, tmpDir, fiveLabels,
			100, 0, 1000, labels.EmptyLabels(), 124, metadata.NoneFunc)
		require.NoError(t, err)
		err = UploadPromBlock(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, b2.String()), metadata.NoneFunc)
		require.NoError(t, err)
		require.Equal(t, 6, len(bkt.Objects()))
		require.Equal(t, 3736, len(bkt.Objects()[path.Join(b2.String(), ChunksDirname, "000001")]))
		require.Equal(t, 401, len(bkt.Objects()[path.Join(b2.String(), IndexFilename)]))
		require.Equal(t, 549, len(bkt.Objects()[path.Join(b2.String(), MetaFilename)]))
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
		require.NoError(t, Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, b1.String()), metadata.NoneFunc))
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
		require.NoError(t, Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, b2.String()), metadata.NoneFunc))
		require.Equal(t, 3, len(bkt.Objects()))

		// Remove meta.json and check if delete can delete it.
		require.NoError(t, bkt.Delete(ctx, path.Join(b2.String(), MetaFilename)))
		require.NoError(t, Delete(ctx, log.NewNopLogger(), bkt, b2))
		require.Equal(t, 0, len(bkt.Objects()))
	}
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

			require.NoError(t, Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, id.String()), metadata.NoneFunc))

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

			require.NoError(t, Upload(ctx, log.NewNopLogger(), bkt, path.Join(tmpDir, id.String()), metadata.NoneFunc))

			c := promauto.With(nil).NewCounter(prometheus.CounterOpts{})
			err = MarkForNoCompact(ctx, log.NewNopLogger(), bkt, id, metadata.ManualNoCompactReason, "", c)
			require.NoError(t, err)
			require.Equal(t, float64(tcase.blocksMarked), promtest.ToFloat64(c))
		})
	}
}

// TestHashDownload uploads an empty block to in-memory storage
// and tries to download it to the same dir. It should not try
// to download twice.
func TestHashDownload(t *testing.T) {
	testutil.VerifyNoLeak(t)

	ctx := context.Background()

	tmpDir := t.TempDir()

	bkt := objstore.NewInMemBucket()
	r := prometheus.NewRegistry()
	instrumentedBkt := objstore.BucketWithMetrics("test", bkt, r)

	b1, err := e2eutil.CreateBlockWithTombstone(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
	}, 100, 0, 1000, labels.FromStrings("ext1", "val1"), 42, metadata.SHA256Func)
	require.NoError(t, err)

	require.NoError(t, Upload(ctx, log.NewNopLogger(), instrumentedBkt, path.Join(tmpDir, b1.String()), metadata.SHA256Func))
	require.Equal(t, 3, len(bkt.Objects()))

	m, err := DownloadMeta(ctx, log.NewNopLogger(), bkt, b1)
	require.NoError(t, err)

	for _, fl := range m.Thanos.Files {
		if fl.RelPath == MetaFilename {
			continue
		}
		require.NotNilf(t, fl.Hash, "expected a hash for %s but got nil", fl.RelPath)
	}

	// Remove the hash from one file to check if we always download it.
	m.Thanos.Files[1].Hash = nil

	metaEncoded := strings.Builder{}
	require.NoError(t, m.Write(&metaEncoded))
	require.NoError(t, bkt.Upload(ctx, path.Join(b1.String(), MetaFilename), strings.NewReader(metaEncoded.String())))

	// Only downloads MetaFile and IndexFile.
	{
		err = Download(ctx, log.NewNopLogger(), instrumentedBkt, m.ULID, path.Join(tmpDir, b1.String()))
		require.NoError(t, err)
		require.NoError(t, promtest.GatherAndCompare(r, strings.NewReader(`
		# HELP thanos_objstore_bucket_operations_total Total number of all attempted operations against a bucket.
        # TYPE thanos_objstore_bucket_operations_total counter
        thanos_objstore_bucket_operations_total{bucket="test",operation="attributes"} 0
        thanos_objstore_bucket_operations_total{bucket="test",operation="delete"} 0
        thanos_objstore_bucket_operations_total{bucket="test",operation="exists"} 0
        thanos_objstore_bucket_operations_total{bucket="test",operation="get"} 2
        thanos_objstore_bucket_operations_total{bucket="test",operation="get_range"} 0
        thanos_objstore_bucket_operations_total{bucket="test",operation="iter"} 2
        thanos_objstore_bucket_operations_total{bucket="test",operation="upload"} 3
		`), `thanos_objstore_bucket_operations_total`))
	}

	// Ensures that we always download MetaFile.
	{
		require.NoError(t, os.Remove(path.Join(tmpDir, b1.String(), MetaFilename)))
		err = Download(ctx, log.NewNopLogger(), instrumentedBkt, m.ULID, path.Join(tmpDir, b1.String()))
		require.NoError(t, err)
		require.NoError(t, promtest.GatherAndCompare(r, strings.NewReader(`
		# HELP thanos_objstore_bucket_operations_total Total number of all attempted operations against a bucket.
        # TYPE thanos_objstore_bucket_operations_total counter
        thanos_objstore_bucket_operations_total{bucket="test",operation="attributes"} 0
        thanos_objstore_bucket_operations_total{bucket="test",operation="delete"} 0
        thanos_objstore_bucket_operations_total{bucket="test",operation="exists"} 0
        thanos_objstore_bucket_operations_total{bucket="test",operation="get"} 4
        thanos_objstore_bucket_operations_total{bucket="test",operation="get_range"} 0
        thanos_objstore_bucket_operations_total{bucket="test",operation="iter"} 4
        thanos_objstore_bucket_operations_total{bucket="test",operation="upload"} 3
		`), `thanos_objstore_bucket_operations_total`))
	}

	// Remove chunks => gets redownloaded.
	// Always downloads MetaFile.
	// Finally, downloads the IndexFile since we have removed its hash.
	{
		require.NoError(t, os.RemoveAll(path.Join(tmpDir, b1.String(), ChunksDirname)))
		err = Download(ctx, log.NewNopLogger(), instrumentedBkt, m.ULID, path.Join(tmpDir, b1.String()))
		require.NoError(t, err)
		require.NoError(t, promtest.GatherAndCompare(r, strings.NewReader(`
			# HELP thanos_objstore_bucket_operations_total Total number of all attempted operations against a bucket.
			# TYPE thanos_objstore_bucket_operations_total counter
			thanos_objstore_bucket_operations_total{bucket="test",operation="attributes"} 0
			thanos_objstore_bucket_operations_total{bucket="test",operation="delete"} 0
			thanos_objstore_bucket_operations_total{bucket="test",operation="exists"} 0
			thanos_objstore_bucket_operations_total{bucket="test",operation="get"} 7
			thanos_objstore_bucket_operations_total{bucket="test",operation="get_range"} 0
			thanos_objstore_bucket_operations_total{bucket="test",operation="iter"} 6
			thanos_objstore_bucket_operations_total{bucket="test",operation="upload"} 3
			`), `thanos_objstore_bucket_operations_total`))
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

		uploadErr := Upload(ctx, log.NewNopLogger(), errBkt, path.Join(tmpDir, b1.String()), metadata.NoneFunc)
		require.ErrorIs(t, uploadErr, errUploadFailed)

		// If upload of index fails, block is deleted.
		require.Equal(t, 0, len(bkt.Objects()))
		require.Equal(t, 0, len(bkt.Objects()[path.Join(DebugMetas, fmt.Sprintf("%s.json", b1.String()))]))
	}

	{
		errBkt := errBucket{Bucket: bkt, failSuffix: "/meta.json"}

		uploadErr := Upload(ctx, log.NewNopLogger(), errBkt, path.Join(tmpDir, b1.String()), metadata.NoneFunc)
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
