// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/bucketindex/storage.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package bucketindex

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/runutil"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
)

var (
	ErrIndexNotFound  = errors.New("bucket index not found")
	ErrIndexCorrupted = errors.New("bucket index corrupted")
)

// ReadIndex reads, parses and returns a bucket index from the bucket.
// ReadIndex has a one-minute timeout for completing the read against the bucket.
// One minute is hard-coded to a reasonably high value to protect against operations that can take unbounded time.
func ReadIndex(ctx context.Context, bkt objstore.Bucket, userID string, cfgProvider bucket.TenantConfigProvider, logger log.Logger) (*Index, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// wrapCorruptedError wraps err with ErrIndexCorrupted unless err is a context error.
	wrapCorruptedError := func(err error) error {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return fmt.Errorf("%w: %w", ErrIndexCorrupted, err)
	}

	userBkt := bucket.NewUserBucketClient(userID, bkt, cfgProvider)

	// Get the bucket index.
	reader, err := userBkt.WithExpectedErrs(userBkt.IsObjNotFoundErr).Get(ctx, IndexCompressedFilename)
	if err != nil {
		if userBkt.IsObjNotFoundErr(err) {
			return nil, ErrIndexNotFound
		}
		return nil, errors.Wrap(err, "read bucket index")
	}
	defer runutil.CloseWithLogOnErr(logger, reader, "close bucket index reader")

	// Read all the content.
	gzipReader, err := gzip.NewReader(reader)
	if err != nil {
		return nil, wrapCorruptedError(err)
	}
	defer runutil.CloseWithLogOnErr(logger, gzipReader, "close bucket index gzip reader")

	// Deserialize it.
	index := &Index{}
	d := json.NewDecoder(gzipReader)
	if err := d.Decode(index); err != nil {
		return nil, wrapCorruptedError(err)
	}

	return index, nil
}

// WriteIndex uploads the provided index to the storage.
func WriteIndex(ctx context.Context, bkt objstore.Bucket, userID string, cfgProvider bucket.TenantConfigProvider, idx *Index) error {
	bkt = bucket.NewUserBucketClient(userID, bkt, cfgProvider)

	// Marshal the index.
	content, err := json.Marshal(idx)
	if err != nil {
		return errors.Wrap(err, "marshal bucket index")
	}

	// Compress it.
	var gzipContent bytes.Buffer
	gzip := gzip.NewWriter(&gzipContent)
	gzip.Name = IndexFilename

	if _, err := gzip.Write(content); err != nil {
		return errors.Wrap(err, "gzip bucket index")
	}
	if err := gzip.Close(); err != nil {
		return errors.Wrap(err, "close gzip bucket index")
	}

	gzipReader := bytes.NewReader(gzipContent.Bytes())

	// Upload the index to the storage.
	if err := bkt.Upload(ctx, IndexCompressedFilename, gzipReader); err != nil {
		return errors.Wrap(err, "upload bucket index")
	}

	return nil
}

// DeleteIndex deletes the bucket index from the storage. No error is returned if the index
// does not exist.
func DeleteIndex(ctx context.Context, bkt objstore.Bucket, userID string, cfgProvider bucket.TenantConfigProvider) error {
	bkt = bucket.NewUserBucketClient(userID, bkt, cfgProvider)

	err := bkt.Delete(ctx, IndexCompressedFilename)
	if err != nil && !bkt.IsObjNotFoundErr(err) {
		return errors.Wrap(err, "delete bucket index")
	}
	return nil
}
