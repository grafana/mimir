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
	"github.com/oklog/ulid/v2"
	"github.com/thanos-io/objstore"
)

const (
	ParquetCompactionMakerFileName = "parquet-compaction-mark.json"
	CurrentVersion                 = 6
)

type CompactionMark struct {
	Version int `json:"version"`
}

// TODO this function sets off the linter as it is not used yet
//func (m *CompactionMark) markerFilename() string { return ParquetCompactionMakerFileName }

func ReadCompactMark(ctx context.Context, id ulid.ULID, userBkt objstore.InstrumentedBucket, logger log.Logger) (*CompactionMark, error) {
	markerPath := path.Join(id.String(), ParquetCompactionMakerFileName)
	reader, err := userBkt.WithExpectedErrs(func(e error) bool {
		return userBkt.IsAccessDeniedErr(e) && userBkt.IsObjNotFoundErr(e)
	}).Get(ctx, markerPath)
	if err != nil {
		if userBkt.IsObjNotFoundErr(err) {
			return &CompactionMark{}, nil
		}

		return &CompactionMark{}, err
	}
	// TODO
	// defer runutil.CloseWithLogOnErr(logger, reader, "close bucket index reader")

	metaContent, err := io.ReadAll(reader)
	if err != nil {
		return nil, errors.Wrapf(err, "read file: %s", ParquetCompactionMakerFileName)
	}

	marker := CompactionMark{}
	err = json.Unmarshal(metaContent, &marker)
	return &marker, err
}

func WriteCompactMark(ctx context.Context, id ulid.ULID, uploader Uploader) error {
	marker := CompactionMark{
		Version: CurrentVersion,
	}
	markerPath := path.Join(id.String(), ParquetCompactionMakerFileName)
	b, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	return uploader.Upload(ctx, markerPath, bytes.NewReader(b))
}
