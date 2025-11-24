// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/bucket/gcs/bucket_client.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package gcs

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/gcs"
)

// NewBucketClient creates a new GCS bucket client.
// If cfg.EnableUploadRetries is true, all Upload operations will automatically be retried
// on transient errors using the GCS RetryAlways policy.
// If cfg.UploadRateLimitEnabled is true, uploads will be rate
// limited following Google Cloud Storage best practices for upload request rate ramping.
// If cfg.ReadRateLimitEnabled is true, reads will be rate
// limited following Google Cloud Storage best practices for read request rate ramping.
func NewBucketClient(ctx context.Context, cfg Config, name string, logger log.Logger, reg prometheus.Registerer) (objstore.Bucket, error) {
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

	var bucket objstore.Bucket = gcsBucket

	// Apply retry wrapper if enabled.
	if cfg.EnableUploadRetries {
		retryOpts := []storage.RetryOption{storage.WithPolicy(storage.RetryAlways)}
		if cfg.MaxRetries > 0 {
			retryOpts = append(retryOpts, storage.WithMaxAttempts(cfg.MaxRetries))
		}
		bucket = &retryAlwaysBucket{
			Bucket:    gcsBucket,
			bkt:       gcsBucket.Handle().Retryer(retryOpts...),
			chunkSize: bucketConfig.ChunkSizeBytes,
		}
	}

	// Apply rate limiting wrapper if enabled.
	if cfg.UploadRateLimitEnabled || cfg.ReadRateLimitEnabled {
		rlb := &rateLimitedBucket{Bucket: bucket}
		if cfg.UploadRateLimitEnabled {
			rlb.uploadLimiter = newRateLimiter(cfg.UploadInitialQPS, cfg.UploadMaxQPS, cfg.UploadRampPeriod, uploadRateLimiter, reg)
		}
		if cfg.ReadRateLimitEnabled {
			rlb.readLimiter = newRateLimiter(cfg.ReadInitialQPS, cfg.ReadMaxQPS, cfg.ReadRampPeriod, readRateLimiter, reg)
		}
		bucket = rlb
	}
	return bucket, nil
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

// rateLimitedBucket wraps a GCS bucket to rate limit uploads and/or reads.
// This implements exponential doubling of request rates following Google Cloud Storage
// best practices for ramping up request rates.
type rateLimitedBucket struct {
	objstore.Bucket
	uploadLimiter *rateLimiter
	readLimiter   *rateLimiter
}

// Upload performs a rate-limited upload. It waits for rate limiter approval before
// delegating to the underlying bucket's Upload method.
func (b *rateLimitedBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	if b.uploadLimiter != nil {
		if err := b.uploadLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("bucket upload rate limiter wait: %w", err)
		}
	}
	return b.Bucket.Upload(ctx, name, r, opts...)
}

// Get performs a rate-limited get. It waits for rate limiter approval before
// delegating to the underlying bucket's Get method.
func (b *rateLimitedBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if b.readLimiter != nil {
		if err := b.readLimiter.Wait(ctx); err != nil {
			return nil, fmt.Errorf("bucket read rate limiter wait: %w", err)
		}
	}
	return b.Bucket.Get(ctx, name)
}

// GetRange performs a rate-limited range get. It waits for rate limiter approval before
// delegating to the underlying bucket's GetRange method.
func (b *rateLimitedBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if b.readLimiter != nil {
		if err := b.readLimiter.Wait(ctx); err != nil {
			return nil, fmt.Errorf("bucket read rate limiter wait: %w", err)
		}
	}
	return b.Bucket.GetRange(ctx, name, off, length)
}

// Exists performs a rate-limited existence check. It waits for rate limiter approval before
// delegating to the underlying bucket's Exists method.
func (b *rateLimitedBucket) Exists(ctx context.Context, name string) (bool, error) {
	if b.readLimiter != nil {
		if err := b.readLimiter.Wait(ctx); err != nil {
			return false, fmt.Errorf("bucket read rate limiter wait: %w", err)
		}
	}
	return b.Bucket.Exists(ctx, name)
}

// Attributes performs a rate-limited attributes lookup. It waits for rate limiter approval before
// delegating to the underlying bucket's Attributes method.
func (b *rateLimitedBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if b.readLimiter != nil {
		if err := b.readLimiter.Wait(ctx); err != nil {
			return objstore.ObjectAttributes{}, fmt.Errorf("bucket read rate limiter wait: %w", err)
		}
	}
	return b.Bucket.Attributes(ctx, name)
}

// Iter performs a rate-limited iteration. It waits for rate limiter approval before
// delegating to the underlying bucket's Iter method.
func (b *rateLimitedBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	if b.readLimiter != nil {
		if err := b.readLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("bucket read rate limiter wait: %w", err)
		}
	}
	return b.Bucket.Iter(ctx, dir, f, options...)
}

// IterWithAttributes performs a rate-limited iteration with attributes. It waits for rate limiter approval before
// delegating to the underlying bucket's IterWithAttributes method.
func (b *rateLimitedBucket) IterWithAttributes(ctx context.Context, dir string, f func(objstore.IterObjectAttributes) error, options ...objstore.IterOption) error {
	if b.readLimiter != nil {
		if err := b.readLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("bucket read rate limiter wait: %w", err)
		}
	}
	return b.Bucket.IterWithAttributes(ctx, dir, f, options...)
}

// Delete performs a rate-limited delete. It waits for rate limiter approval before
// delegating to the underlying bucket's Delete method.
func (b *rateLimitedBucket) Delete(ctx context.Context, name string) error {
	if b.uploadLimiter != nil {
		if err := b.uploadLimiter.Wait(ctx); err != nil {
			return fmt.Errorf("bucket upload rate limiter wait: %w", err)
		}
	}
	return b.Bucket.Delete(ctx, name)
}
