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
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/thanos-io/objstore"
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
	storage.ParquetShard
	io.Closer
}

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
	labelsFile, chunksFile *storage.ParquetFile
	schema                 *schema.TSDBSchema
	o                      sync.Once
}

func NewBasicReader(
	ctx context.Context,
	blockID string,
	shard int,
	labelsFileOpener storage.ParquetOpener,
	chunksFileOpener storage.ParquetOpener,
	opts ...storage.FileOption,
) (*BasicReader, error) {
	labelsFileName := schema.LabelsPfileNameForShard(blockID, shard)
	chunksFileName := schema.ChunksPfileNameForShard(blockID, shard)

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
		labelsFile: labelsFile,
		chunksFile: chunksFile,
	}, nil
}

func (r *BasicReader) LabelsFile() *storage.ParquetFile {
	return r.labelsFile
}

func (r *BasicReader) ChunksFile() *storage.ParquetFile {
	return r.chunksFile
}

func (r *BasicReader) TSDBSchema() (*schema.TSDBSchema, error) {
	var err error
	r.o.Do(func() {
		r.schema, err = schema.FromLabelsFile(r.labelsFile.File)
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
