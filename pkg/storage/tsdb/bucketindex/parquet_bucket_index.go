// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/26344c3ec7409713fcf52a9c41cd0dce537b3100/pkg/storage/tsdb/bucketindex/parquet_bucket_index.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package bucketindex

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/klauspost/compress/gzip"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
)

const (
	parquetIndex               = "bucket-index-parquet.json"
	parquetIndexCompressedName = parquetIndex + ".gz"
)

type ParquetIndex struct {
	// TODO(npazosmendez): add versionsing
	// TODO(npazosmendez): should we make this a slice?
	Blocks map[ulid.ULID]*Block `json:"blocks"`
}

func ReadParquetIndex(ctx context.Context, userBkt objstore.InstrumentedBucket, logger log.Logger) (*ParquetIndex, error) {
	reader, err := userBkt.WithExpectedErrs(func(err error) bool { return userBkt.IsAccessDeniedErr(err) && userBkt.IsObjNotFoundErr(err) }).Get(ctx, parquetIndexCompressedName)
	if err != nil {
		if userBkt.IsObjNotFoundErr(err) {
			return &ParquetIndex{Blocks: map[ulid.ULID]*Block{}}, nil
		}

		return nil, err
	}

	defer func() {
		err := reader.Close()
		if err != nil {
			level.Error(logger).Log("msg", "failed to close bucket index reader", "err", err)
		}
	}()

	gzipReader, err := gzip.NewReader(reader)
	if err != nil {
		return nil, ErrIndexCorrupted
	}

	defer func() {
		err := gzipReader.Close()
		if err != nil {
			level.Error(logger).Log("msg", "failed to close bucket index gzip reader", "err", err)
		}
	}()

	index := &ParquetIndex{}
	d := json.NewDecoder(gzipReader)
	if err := d.Decode(index); err != nil {
		return nil, ErrIndexCorrupted
	}
	return index, nil
}

func WriteParquetIndex(ctx context.Context, bkt objstore.Bucket, idx *ParquetIndex) error {
	content, err := json.Marshal(idx)
	if err != nil {
		return errors.Wrap(err, "marshal bucket index")
	}

	var gzipContent bytes.Buffer
	gzip := gzip.NewWriter(&gzipContent)
	gzip.Name = parquetIndex

	if _, err := gzip.Write(content); err != nil {
		return errors.Wrap(err, "gzip bucket index")
	}
	if err := gzip.Close(); err != nil {
		return errors.Wrap(err, "close gzip bucket index")
	}

	if err := bkt.Upload(ctx, parquetIndexCompressedName, bytes.NewReader(gzipContent.Bytes())); err != nil {
		return errors.Wrap(err, "upload bucket index")
	}

	return nil
}
