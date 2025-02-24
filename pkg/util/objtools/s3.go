// SPDX-License-Identifier: AGPL-3.0-only

package objtools

import (
	"context"
	"flag"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
)

type S3ClientConfig struct {
	BucketName      string
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	Secure          bool
	PartSize        uint64
}

func (c *S3ClientConfig) RegisterFlags(prefix string, f *flag.FlagSet) {
	f.StringVar(&c.BucketName, prefix+"bucket-name", "", "The name of the bucket (not prefixed by a scheme).")
	f.StringVar(&c.Endpoint, prefix+"endpoint", "", "The endpoint to contact when accessing the bucket.")
	f.StringVar(&c.AccessKeyID, prefix+"access-key-id", "", "The access key ID used in AWS Signature Version 4 authentication.")
	f.StringVar(&c.SecretAccessKey, prefix+"secret-access-key", "", "The secret access key used in AWS Signature Version 4 authentication.")
	f.BoolVar(&c.Secure, prefix+"secure", true, "If true (default), use HTTPS when connecting to the bucket. If false, insecure HTTP is used.")
	f.Uint64Var(&c.PartSize, prefix+"part-size", 0, "If 0, and object's size is known and optimal for multipart upload, the default value is the minimum allowed size 16MiB.")
}

func (c *S3ClientConfig) Validate(prefix string) error {
	if c.BucketName == "" {
		return errors.New(prefix + "bucket-name is missing")
	}
	if c.Endpoint == "" {
		return errors.New(prefix + "endpoint is missing")
	}
	if c.AccessKeyID == "" {
		return errors.New(prefix + "access-key-id is missing")
	}
	if c.SecretAccessKey == "" {
		return errors.New(prefix + "secret-access-key is missing")
	}
	return nil
}

func (c *S3ClientConfig) ToBucket() (Bucket, error) {
	client, err := minio.New(c.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(c.AccessKeyID, c.SecretAccessKey, ""),
		Secure: c.Secure,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize S3 client")
	}
	return &s3Bucket{
		client:     client,
		bucketName: c.BucketName,
		partSize:   c.PartSize,
	}, nil
}

type s3Bucket struct {
	client     *minio.Client
	bucketName string
	partSize   uint64
}

func (bkt *s3Bucket) Get(ctx context.Context, objectName string, options GetOptions) (io.ReadCloser, error) {
	obj, err := bkt.client.GetObject(ctx, bkt.bucketName, objectName, minio.GetObjectOptions{
		VersionID: options.VersionID,
	})
	if err != nil {
		return nil, err
	}
	return obj, nil
}

const maxSingleCopySize int64 = 5 * (1024 * 1024 * 1024) // 5 GiB

func (bkt *s3Bucket) ServerSideCopy(ctx context.Context, objectName string, dstBucket Bucket, options CopyOptions) error {
	d, ok := dstBucket.(*s3Bucket)
	if !ok {
		return errors.New("destination Bucket wasn't an S3 Bucket")
	}

	stat, err := bkt.client.StatObject(ctx, bkt.bucketName, objectName, minio.StatObjectOptions{
		VersionID: options.SourceVersionID,
	})
	if err != nil {
		return err
	}

	dstOptions := minio.CopyDestOptions{
		Bucket: d.bucketName,
		Object: options.destinationObjectName(objectName),
	}

	srcOptions := minio.CopySrcOptions{
		Bucket:    bkt.bucketName,
		Object:    objectName,
		VersionID: options.SourceVersionID,
	}

	if stat.Size <= maxSingleCopySize {
		_, err := d.client.CopyObject(ctx, dstOptions, srcOptions)
		return err
	}

	// Uses a multi-part upload
	// Due to https://github.com/minio/minio-go/issues/1683 this likely does not work cross-region
	_, err = d.client.ComposeObject(ctx, dstOptions, srcOptions)
	return err
}

func (bkt *s3Bucket) ClientSideCopy(ctx context.Context, objectName string, dstBucket Bucket, options CopyOptions) error {
	obj, err := bkt.client.GetObject(ctx, bkt.bucketName, objectName, minio.GetObjectOptions{
		VersionID: options.SourceVersionID,
	})
	if err != nil {
		return errors.Wrap(err, "failed to get source object from S3")
	}
	objInfo, err := obj.Stat()
	if err != nil {
		return errors.Wrap(err, "failed to get source object information from S3")
	}
	if err := dstBucket.Upload(ctx, options.destinationObjectName(objectName), obj, objInfo.Size); err != nil {
		_ = obj.Close()
		return errors.Wrap(err, "failed to upload source object from S3 to destination")
	}
	return errors.Wrap(obj.Close(), "failed to close source object reader from S3")
}

func (bkt *s3Bucket) List(ctx context.Context, options ListOptions) (*ListResult, error) {
	prefix := ensureDelimiterSuffix(options.Prefix)

	listing := bkt.client.ListObjects(ctx, bkt.bucketName, minio.ListObjectsOptions{
		Prefix:       prefix,
		Recursive:    options.Recursive,
		WithVersions: options.Versioned,
	})

	objects := make([]ObjectAttributes, 0, 10)
	var prefixes []string
	if !options.Recursive {
		prefixes = make([]string, 0, 10)
	}

	for obj := range listing {
		if obj.Err != nil {
			return nil, obj.Err
		}
		if obj.LastModified.IsZero() { // prefixes only set the Key field
			prefixes = append(prefixes, obj.Key)
		} else {
			objects = append(objects, ObjectAttributes{
				Name:         obj.Key,
				Size:         obj.Size,
				LastModified: obj.LastModified,
				VersionInfo: VersionInfo{
					VersionID:      obj.VersionID,
					IsCurrent:      obj.IsLatest,
					IsDeleteMarker: obj.IsDeleteMarker,
				},
			})

		}
	}
	return &ListResult{objects, prefixes}, ctx.Err()
}

func (bkt *s3Bucket) RestoreVersion(ctx context.Context, objectName string, versionInfo VersionInfo) error {
	if versionInfo.IsDeleteMarker {
		return errors.New("delete markers are not restorable")
	} else if versionInfo.IsCurrent {
		// feasibly could return nil, but return an error instead to possibly catch logic bugs in callers
		return errors.New("a current version does not need to be restored")
	}

	// Docs: https://docs.aws.amazon.com/AmazonS3/latest/userguide/RestoringPreviousVersions.html
	return bkt.ServerSideCopy(ctx, objectName, bkt, CopyOptions{
		SourceVersionID: versionInfo.VersionID,
	})
}

func (bkt *s3Bucket) Upload(ctx context.Context, objectName string, reader io.Reader, contentLength int64) error {
	opts := minio.PutObjectOptions{
		PartSize: bkt.partSize,
	}
	_, err := bkt.client.PutObject(ctx, bkt.bucketName, objectName, reader, contentLength, opts)
	return err
}

func (bkt *s3Bucket) Delete(ctx context.Context, objectName string, options DeleteOptions) error {
	return bkt.client.RemoveObject(ctx, bkt.bucketName, objectName, minio.RemoveObjectOptions{
		VersionID: options.VersionID,
	})
}

func (bkt *s3Bucket) Name() string {
	return bkt.bucketName
}
