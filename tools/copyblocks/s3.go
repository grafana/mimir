// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"io"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
)

type s3Config struct {
	source      s3ClientConfig
	destination s3ClientConfig
}

func (c *s3Config) RegisterFlags(f *flag.FlagSet) {
	c.source.RegisterFlags("s3-source-", f)
	c.destination.RegisterFlags("s3-destination-", f)
}

func (c *s3Config) validate(source, destination string) error {
	if source == serviceS3 {
		if err := c.source.validate("s3-source-"); err != nil {
			return err
		}
	}
	if destination == serviceS3 {
		return c.destination.validate("s3-destination-")
	}
	return nil
}

type s3ClientConfig struct {
	endpoint  string
	accessKey string
	secretKey string
	secure    bool
}

func (c *s3ClientConfig) RegisterFlags(prefix string, f *flag.FlagSet) {
	f.StringVar(&c.endpoint, prefix+"endpoint", "", "The endpoint to contact when accessing the bucket.")
	f.StringVar(&c.accessKey, prefix+"access-key", "", "The access key used in AWSV4 Authorization.")
	f.StringVar(&c.secretKey, prefix+"secret-key", "", "The secret key used in AWSV4 Authorization.")
	f.BoolVar(&c.secure, prefix+"secure", true, "The default value true corresponds to using https, otherwise uses http.")
}

func (c *s3ClientConfig) validate(prefix string) error {
	if c.endpoint == "" {
		return errors.New(prefix + "endpoint is missing")
	}
	if c.accessKey == "" {
		return errors.New(prefix + "access-key is missing")
	}
	if c.secretKey == "" {
		return errors.New(prefix + "secret-key is missing")
	}
	return nil
}

type s3Bucket struct {
	*minio.Client
	bucketName string
}

func newS3Client(cfg s3ClientConfig, bucketName string) (bucket, error) {
	client, err := minio.New(cfg.endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.accessKey, cfg.secretKey, ""),
		Secure: cfg.secure,
	})
	if err != nil {
		return nil, err
	}
	return &s3Bucket{
		Client:     client,
		bucketName: bucketName,
	}, nil
}

func (bkt *s3Bucket) Get(ctx context.Context, objectName string) (io.ReadCloser, error) {
	obj, err := bkt.GetObject(ctx, bkt.bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func (bkt *s3Bucket) ServerSideCopy(ctx context.Context, objectName string, dstBucket bucket) error {
	d, ok := dstBucket.(*s3Bucket)
	if !ok {
		return errors.New("destination bucket wasn't an s3 bucket")
	}
	_, err := d.CopyObject(ctx,
		minio.CopyDestOptions{
			Bucket: d.bucketName,
			Object: objectName,
		},
		minio.CopySrcOptions{
			Bucket: bkt.bucketName,
			Object: objectName,
		},
	)
	return err
}

func (bkt *s3Bucket) ClientSideCopy(ctx context.Context, objectName string, dstBucket bucket) error {
	obj, err := bkt.GetObject(ctx, bkt.bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return errors.Wrap(err, "failed to get source object from s3")
	}
	objInfo, err := obj.Stat()
	if err != nil {
		return errors.Wrap(err, "failed to get source object information from s3")
	}
	if err := dstBucket.Upload(ctx, objectName, obj, objInfo.Size); err != nil {
		_ = obj.Close()
		return errors.Wrap(err, "failed to upload source object from s3 to destination")
	}
	return errors.Wrap(obj.Close(), "failed to close source object reader from s3")
}

func (bkt *s3Bucket) ListPrefix(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	if prefix != "" && !strings.HasSuffix(prefix, delim) {
		prefix = prefix + delim
	}
	options := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: recursive,
	}
	result := make([]string, 0, 10)
	objects := bkt.ListObjects(ctx, bkt.bucketName, options)
	for obj := range objects {
		if obj.Err != nil {
			return nil, obj.Err
		}
		result = append(result, obj.Key)
	}
	return result, ctx.Err()
}

func (bkt *s3Bucket) Upload(ctx context.Context, objectName string, reader io.Reader, contentLength int64) error {
	_, err := bkt.PutObject(ctx, bkt.bucketName, objectName, reader, contentLength, minio.PutObjectOptions{})
	return err
}

func (bkt *s3Bucket) Name() string {
	return bkt.bucketName
}
