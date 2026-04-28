// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/bucket/gcs/bucket_client.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package gcs

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/gcs"
)

// NewBucketClient creates a new GCS bucket client.
// If cfg.EnableUploadRetries is true, all Upload operations will automatically be retried
// on transient errors using the GCS RetryAlways policy.
func NewBucketClient(ctx context.Context, cfg Config, name string, logger log.Logger) (objstore.Bucket, error) {
	bucketConfig := gcs.Config{
		Bucket:         cfg.BucketName,
		ServiceAccount: cfg.ServiceAccount.String(),
		HTTPConfig:     cfg.HTTP.ToExtHTTP(),
		MaxRetries:     cfg.MaxRetries,
	}
	gcsBucket, err := gcs.NewBucketWithConfig(ctx, logger, bucketConfig, name, nil)
	if err != nil {
		return nil, errors.Wrap(err, "NewBucketClient: create bucket")
	}

	if !cfg.EnableUploadRetries {
		return gcsBucket, nil
	}

	retryOpts := []storage.RetryOption{storage.WithPolicy(storage.RetryAlways)}
	if cfg.MaxRetries > 0 {
		retryOpts = append(retryOpts, storage.WithMaxAttempts(cfg.MaxRetries))
	}
	return &retryAlwaysBucket{
		Bucket:    gcsBucket,
		bkt:       gcsBucket.Handle().Retryer(retryOpts...),
		chunkSize: bucketConfig.ChunkSizeBytes,
	}, nil
}

// retryAlwaysBucket wraps a GCS bucket to automatically retry Upload operations
// using the RetryAlways policy. This retries all transient errors but does NOT
// guarantee idempotency - concurrent writes or retries may overwrite objects.
type retryAlwaysBucket struct {
	*gcs.Bucket
	bkt       *storage.BucketHandle
	chunkSize int
}

// Upload performs an upload using a GCS handle wrapped with RetryAlways policy.
// Uploads will be automatically retried on transient errors without any idempotency guarantees.
func (b *retryAlwaysBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	// Use the retry-wrapped handle for automatic retries.
	w := b.bkt.Object(name).NewWriter(ctx)

	uploadOpts := objstore.ApplyObjectUploadOptions(opts...)
	if b.chunkSize > 0 {
		w.ChunkSize = b.chunkSize
		w.ContentType = uploadOpts.ContentType
	}

	if _, err := io.Copy(w, r); err != nil {
		_ = w.Close()
		return errors.Wrap(err, "write object")
	}

	if err := w.Close(); err != nil {
		return errors.Wrap(err, "close writer")
	}

	return nil
}
