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
	"github.com/go-kit/log/level"
	"math/rand"
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

	const maxRetries = 5
	const baseDelay = 500 * time.Millisecond

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff with jitter: 500ms, 1s, 2s
			delay := baseDelay * time.Duration(1<<uint(attempt-1))
			jitter := time.Duration(rand.Int63n(int64(delay / 2)))

			level.Warn(logger).Log(
				"msg", "retrying bucket index read after corruption",
				"user", userID,
				"attempt", attempt+1,
				"max_retries", maxRetries,
				"delay", delay+jitter,
			)

			select {
			case <-time.After(delay + jitter):
				// Continue to retry
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		idx, err := readIndexAttempt(ctx, bkt, userID, cfgProvider, logger)
		if err == nil {
			if attempt > 0 {
				level.Info(logger).Log(
					"msg", "successfully read bucket index after retry",
					"user", userID,
					"attempt", attempt+1,
				)
			}
			return idx, nil
		}

		lastErr = err

		// Only retry on corruption errors (transient due to concurrent writes)
		if !errors.Is(err, ErrIndexCorrupted) {
			// Not corrupted, fail immediately (e.g., ErrIndexNotFound, network errors)
			return nil, err
		}

		// Log corruption and retry
		level.Warn(logger).Log(
			"msg", "bucket index corrupted, will retry",
			"user", userID,
			"attempt", attempt+1,
			"err", err,
		)
	}

	// All retries exhausted
	level.Error(logger).Log(
		"msg", "bucket index still corrupted after all retries",
		"user", userID,
		"max_retries", maxRetries,
		"err", lastErr,
	)
	return nil, lastErr
}

// readIndexAttempt performs a single attempt to read the bucket index
func readIndexAttempt(ctx context.Context, bkt objstore.Bucket, userID string, cfgProvider bucket.TenantConfigProvider, logger log.Logger) (*Index, error) {
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
		return nil, ErrIndexCorrupted
	}
	defer runutil.CloseWithLogOnErr(logger, gzipReader, "close bucket index gzip reader")

	// Deserialize it.
	index := &Index{}
	d := json.NewDecoder(gzipReader)
	if err := d.Decode(index); err != nil {
		return nil, ErrIndexCorrupted
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
