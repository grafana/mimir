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
// If cfg.EnableIdempotentUploads is true, all Upload operations will automatically use GCS
// preconditions (GenerationMatch or DoesNotExist) to ensure idempotency, enabling automatic
// retries by the GCS SDK. This adds an extra Attrs() read before each upload.
func NewBucketClient(ctx context.Context, cfg Config, name string, logger log.Logger) (objstore.Bucket, error) {
	bucketConfig := gcs.Config{
		Bucket:         cfg.BucketName,
		ServiceAccount: cfg.ServiceAccount.String(),
		HTTPConfig:     cfg.HTTP.ToExtHTTP(),
	}
	gcsBucket, err := gcs.NewBucketWithConfig(ctx, logger, bucketConfig, name, nil)
	if err != nil {
		return nil, errors.Wrap(err, "NewBucketClient: create bucket")
	}

	if !cfg.EnableIdempotentUploads {
		return gcsBucket, nil
	}

	return &idempotentBucket{
		Bucket:    gcsBucket,
		bkt:       gcsBucket.Handle(),
		chunkSize: bucketConfig.ChunkSizeBytes,
	}, nil
}

// idempotentBucket wraps a GCS bucket to automatically add preconditions to Upload operations.
// This makes all uploads idempotent by using GenerationMatch (for existing objects) or
// DoesNotExist (for new objects), which enables GCS to safely retry them.
type idempotentBucket struct {
	*gcs.Bucket
	bkt       *storage.BucketHandle
	chunkSize int
}

// Upload performs an idempotent upload using GCS preconditions.
// This reads the current object generation first, then uploads with a precondition
// to ensure the upload is idempotent and safe to retry.
func (b *idempotentBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	obj := b.bkt.Object(name)

	attrs, err := obj.Attrs(ctx)
	var conds storage.Conditions
	if err != nil {
		if !b.IsObjNotFoundErr(err) {
			return errors.Wrap(err, "get object attrs for idempotent upload")
		}

		conds.DoesNotExist = true
	} else {
		conds.GenerationMatch = attrs.Generation
	}

	// Create writer with preconditions - this makes the upload idempotent.
	w := obj.If(conds).NewWriter(ctx)

	uploadOpts := objstore.ApplyObjectUploadOptions(opts...)
	if b.chunkSize > 0 {
		w.ChunkSize = b.chunkSize
		w.ContentType = uploadOpts.ContentType
	}

	if _, err := io.Copy(w, r); err != nil {
		_ = w.Close()
		return errors.Wrap(err, "write object with precondition")
	}

	if err := w.Close(); err != nil {
		return errors.Wrap(err, "close writer with precondition")
	}

	return nil
}
