// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/e2ecortex/storage.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package e2emimir

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/oklog/ulid"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket/s3"
)

type S3Client struct {
	writer objstore.Bucket
	reader objstore.BucketReader
}

func NewS3Client(cfg s3.Config) (*S3Client, error) {
	writer, err := s3.NewBucketClient(cfg, "test", log.NewNopLogger())
	if err != nil {
		return nil, err
	}

	reader, err := s3.NewBucketReaderClient(cfg, "test", log.NewNopLogger())
	if err != nil {
		return nil, err
	}

	return &S3Client{
		writer: writer,
		reader: reader,
	}, nil
}

func NewS3ClientForMinio(minio *e2e.HTTPService, bucketName string) (*S3Client, error) {
	return NewS3Client(s3.Config{
		Endpoint:        minio.HTTPEndpoint(),
		BucketName:      bucketName,
		SecretAccessKey: flagext.SecretWithValue(e2edb.MinioSecretKey),
		AccessKeyID:     e2edb.MinioAccessKey,
		Insecure:        true,
	})
}

// DeleteBlocks deletes all blocks for a tenant.
func (c *S3Client) DeleteBlocks(userID string) error {
	prefix := fmt.Sprintf("%s/", userID)

	return c.reader.Iter(context.Background(), prefix, func(entry string) error {
		if !strings.HasPrefix(entry, prefix) {
			return fmt.Errorf("unexpected key in the storage: %s", entry)
		}

		blockID := strings.TrimPrefix(entry, prefix)
		blockID = strings.TrimSuffix(blockID, "/")

		// Skip keys which are not block IDs
		if _, err := ulid.Parse(blockID); err != nil {
			return nil
		}

		return c.DeleteBlock(userID, blockID)
	})
}

// DeleteBlock deletes a single block.
func (c *S3Client) DeleteBlock(userID, blockID string) error {
	return c.Delete(fmt.Sprintf("%s/%s/", userID, blockID))
}

// Delete recursively deletes every object within the input prefix.
func (c *S3Client) Delete(prefix string) error {
	return c.reader.Iter(context.Background(), prefix, func(entry string) error {
		if !strings.HasPrefix(entry, prefix) {
			return fmt.Errorf("unexpected key in the storage: %s", entry)
		}

		// Recursively delete if it's a prefix.
		if strings.HasSuffix(entry, "/") {
			return c.Delete(entry)
		}

		return c.writer.Delete(context.Background(), entry)
	})
}
