// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/header.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package block

import (
	"context"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/go-multierror"
<<<<<<< HEAD
	"github.com/oklog/ulid/v2"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
=======
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/thanos-io/objstore"
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
	"golang.org/x/sync/errgroup"
)

const (
	DefaultIndexHeaderLazyLoadingEnabled     = true
	DefaultIndexHeaderLazyLoadingIdleTimeout = 60 * time.Minute
)

// FirstShardIndex represents the default initial shard for a parquet block reader;
// TSDB blocks can be split into multiple shards when converted to our Parquet format,
// but this sharding is not in use yet; assume all blocks have a single Parquet shard for now.
const FirstShardIndex = 0

// Reader wraps access to a TSDB block's storage.ParquetShard interface.

type Reader interface {
<<<<<<< HEAD
	BlockID() ulid.ULID
=======
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
	storage.ParquetShard
	io.Closer
}

<<<<<<< HEAD
=======
type ParquetBucketOpener struct {
	bkt    objstore.BucketReader
	prefix string
}

func NewParquetBucketOpener(bkt objstore.BucketReader, prefix string) *ParquetBucketOpener {
	return &ParquetBucketOpener{
		bkt: bkt,
	}
}

func (o *ParquetBucketOpener) Open(
	ctx context.Context, name string, opts ...storage.FileOption,
) (*storage.ParquetFile, error) {
	return storage.OpenFromBucket(ctx, o.bkt, filepath.Join(o.prefix, name), opts...)
}

>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
type ParquetLocalFileOpener struct {
	dir string
}

func NewParquetLocalFileOpener(dir string) *ParquetLocalFileOpener {
	return &ParquetLocalFileOpener{
		dir: dir,
	}
}

func (o *ParquetLocalFileOpener) Open(
	ctx context.Context, name string, opts ...storage.FileOption,
) (*storage.ParquetFile, error) {
	return storage.OpenFromFile(ctx, filepath.Join(o.dir, name), opts...)
}

// BasicReader is a simple building-block implementation of the Reader interface.
// Parquet labels and chunks files are opened immediately in the constructor;
// lazy-loading or other lifecycle management can be implemented by wrapping this type.
type BasicReader struct {
<<<<<<< HEAD
	blockID                ulid.ULID
	labelsFile, chunksFile storage.ParquetFileView
=======
	labelsFile, chunksFile *storage.ParquetFile
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
	schema                 *schema.TSDBSchema
	o                      sync.Once
}

func NewBasicReader(
	ctx context.Context,
<<<<<<< HEAD
	blockID ulid.ULID,
=======
	blockID string,
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
	shard int,
	labelsFileOpener storage.ParquetOpener,
	chunksFileOpener storage.ParquetOpener,
	opts ...storage.FileOption,
) (*BasicReader, error) {
<<<<<<< HEAD
	labelsFileName := schema.LabelsPfileNameForShard(blockID.String(), shard)
	chunksFileName := schema.ChunksPfileNameForShard(blockID.String(), shard)
=======
	labelsFileName := schema.LabelsPfileNameForShard(blockID, shard)
	chunksFileName := schema.ChunksPfileNameForShard(blockID, shard)
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))

	errGroup := errgroup.Group{}

	var labelsFile, chunksFile *storage.ParquetFile

	errGroup.Go(func() (err error) {
		labelsFile, err = labelsFileOpener.Open(ctx, labelsFileName, opts...)
		return err
	})

	errGroup.Go(func() (err error) {
		chunksFile, err = chunksFileOpener.Open(ctx, chunksFileName, opts...)
		return err
	})

	if err := errGroup.Wait(); err != nil {
		return nil, err
	}

	return &BasicReader{
<<<<<<< HEAD
		blockID:    blockID,
=======
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
		labelsFile: labelsFile,
		chunksFile: chunksFile,
	}, nil
}

<<<<<<< HEAD
func (r *BasicReader) BlockID() ulid.ULID {
	return r.blockID
}

func (r *BasicReader) LabelsFile() storage.ParquetFileView {
	return r.labelsFile
}

func (r *BasicReader) ChunksFile() storage.ParquetFileView {
=======
func (r *BasicReader) LabelsFile() *storage.ParquetFile {
	return r.labelsFile
}

func (r *BasicReader) ChunksFile() *storage.ParquetFile {
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
	return r.chunksFile
}

func (r *BasicReader) TSDBSchema() (*schema.TSDBSchema, error) {
	var err error
	r.o.Do(func() {
<<<<<<< HEAD
		r.schema, err = schema.FromLabelsFile(r.labelsFile)
=======
		r.schema, err = schema.FromLabelsFile(r.labelsFile.File)
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
	})
	return r.schema, err
}

func (r *BasicReader) Close() error {
	err := &multierror.Error{}
	err = multierror.Append(err, r.labelsFile.Close())
	err = multierror.Append(err, r.chunksFile.Close())
	// TODO figure out if we need to do anything with the loaded schema here
	return err.ErrorOrNil()
}
