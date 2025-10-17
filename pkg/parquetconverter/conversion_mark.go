// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/26344c3ec7409713fcf52a9c41cd0dce537b3100/pkg/compactor/compation_marker.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package parquetconverter

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path"

	"github.com/efficientgo/core/errors"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid/v2"
	"github.com/thanos-io/objstore"
)

const (
	ParquetConversionMarkFileName = "parquet-conversion-mark.json"
	CurrentVersion                = 1
)

type ConversionMark struct {
	Version    int `json:"version"`
	ShardCount int `json:"shard_count"`
}

func ReadConversionMark(ctx context.Context, id ulid.ULID, userBkt objstore.InstrumentedBucket, logger log.Logger) (*ConversionMark, bool, error) {
	markPath := path.Join(id.String(), ParquetConversionMarkFileName)
	reader, err := userBkt.WithExpectedErrs(func(e error) bool {
		return userBkt.IsAccessDeniedErr(e) && userBkt.IsObjNotFoundErr(e)
	}).Get(ctx, markPath)
	if err != nil {
		if userBkt.IsObjNotFoundErr(err) {
			return nil, false, nil
		}

		return nil, false, err
	}

	defer runutil.CloseWithLogOnErr(logger, reader, "close bucket index reader")

	metaContent, err := io.ReadAll(reader)
	if err != nil {
		return nil, false, errors.Wrapf(err, "read file: %s", ParquetConversionMarkFileName)
	}

	mark := ConversionMark{
		// Default to 1 shard if not set, for retrocompatibility
		ShardCount: 1,
	}
	err = json.Unmarshal(metaContent, &mark)
	return &mark, err == nil, err
}

func WriteConversionMark(ctx context.Context, id ulid.ULID, userBkt objstore.InstrumentedBucket) error {
	mark := ConversionMark{
		Version: CurrentVersion,
		// TODO: replace once we add multi shard conversion
		ShardCount: 1,
	}
	markPath := path.Join(id.String(), ParquetConversionMarkFileName)
	b, err := json.Marshal(mark)
	if err != nil {
		return err
	}
	return userBkt.Upload(ctx, markPath, bytes.NewReader(b))
}
