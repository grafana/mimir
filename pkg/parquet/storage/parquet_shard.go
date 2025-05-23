// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus-community/parquet-common/blob/382b6ec8ae40fb5dcdcabd8019f69a4be1cd8869/storage/parquet_shard.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package storage

import (
	"context"
	"sync"

	"github.com/parquet-go/parquet-go"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/parquet/schema"
)

type ParquetFile struct {
	*parquet.File
	ReadAtWithContext
}

func (f *ParquetFile) GetPages(ctx context.Context, cc parquet.ColumnChunk) *parquet.FilePages {
	colChunk := cc.(*parquet.FileColumnChunk)
	pages := colChunk.PagesFrom(f.WithContext(ctx))
	return pages
}

func OpenFile(r ReadAtWithContext, size int64, options ...parquet.FileOption) (*ParquetFile, error) {
	file, err := parquet.OpenFile(r, size, options...)
	if err != nil {
		return nil, err
	}
	return &ParquetFile{
		File:              file,
		ReadAtWithContext: r,
	}, nil
}

type ParquetShard struct {
	labelsFile, chunksFile *ParquetFile
	schema                 *schema.TSDBSchema
	o                      sync.Once
}

// OpenParquetShard opens the sharded parquet block,
// using the options param.
func OpenParquetShard(ctx context.Context, bkt objstore.Bucket, name string, shard int, options ...parquet.FileOption) (*ParquetShard, error) {
	labelsFileName := schema.LabelsPfileNameForShard(name, shard)
	chunksFileName := schema.ChunksPfileNameForShard(name, shard)
	labelsAttr, err := bkt.Attributes(ctx, labelsFileName)
	if err != nil {
		return nil, err
	}
	labelsFile, err := OpenFile(NewBucketReadAt(ctx, labelsFileName, bkt), labelsAttr.Size, options...)
	if err != nil {
		return nil, err
	}

	chunksFileAttr, err := bkt.Attributes(ctx, chunksFileName)
	if err != nil {
		return nil, err
	}
	chunksFile, err := OpenFile(NewBucketReadAt(ctx, chunksFileName, bkt), chunksFileAttr.Size, options...)
	if err != nil {
		return nil, err
	}
	return &ParquetShard{
		labelsFile: labelsFile,
		chunksFile: chunksFile,
	}, nil
}

func (b *ParquetShard) LabelsFile() *ParquetFile {
	return b.labelsFile
}

func (b *ParquetShard) ChunksFile() *ParquetFile {
	return b.chunksFile
}

func (b *ParquetShard) TSDBSchema() (*schema.TSDBSchema, error) {
	var err error
	b.o.Do(func() {
		b.schema, err = schema.FromLabelsFile(b.labelsFile.File)
	})
	return b.schema, err
}
