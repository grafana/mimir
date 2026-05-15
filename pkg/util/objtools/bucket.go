// SPDX-License-Identifier: AGPL-3.0-only

package objtools

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/azure"
	"github.com/thanos-io/objstore/providers/gcs"
	"github.com/thanos-io/objstore/providers/s3"

	"github.com/grafana/mimir/pkg/storage/bucket"
)

const (
	Delim = "/" // Used by Mimir to delimit tenants and blocks, and objects within blocks.
)

// Bucket is an object storage interface intended to be used by tools that require functionality that isn't in objstore
type Bucket interface {
	Get(ctx context.Context, objectName string, options GetOptions) (io.ReadCloser, error)
	Exists(ctx context.Context, objectName string) (bool, error)
	ServerSideCopy(ctx context.Context, objectName string, dstBucket Bucket, options CopyOptions) error
	ClientSideCopy(ctx context.Context, objectName string, dstBucket Bucket, options CopyOptions) error
	List(ctx context.Context, options ListOptions) (*ListResult, error)
	RestoreVersion(ctx context.Context, name string, versionInfo VersionInfo) error
	Upload(ctx context.Context, objectName string, reader io.Reader, contentLength int64) error
	Delete(ctx context.Context, objectName string, options DeleteOptions) error
	Name() string
}

type CopyOptions struct {
	SourceVersionID       string
	DestinationObjectName string
}

func (options *CopyOptions) destinationObjectName(sourceObjectName string) string {
	if options.DestinationObjectName != "" {
		return options.DestinationObjectName
	}
	return sourceObjectName
}

type GetOptions struct {
	VersionID string
}

type DeleteOptions struct {
	VersionID string
}

type ListOptions struct {
	Prefix    string
	Recursive bool
	Versioned bool
}

type ListResult struct {
	Objects  []ObjectAttributes
	Prefixes []string
}

func (result *ListResult) ToNames() []string {
	r, _ := result.ToNamesWithoutPrefix("") // error is impossible with a blank prefix
	return r
}

func (result *ListResult) ToNamesWithoutPrefix(prefix string) ([]string, error) {
	if prefix != "" && !strings.HasSuffix(prefix, Delim) {
		prefix = prefix + Delim
	}
	names := make([]string, 0, len(result.Objects)+len(result.Prefixes))
	for _, attr := range result.Objects {
		name, hasPrefix := strings.CutPrefix(attr.Name, prefix)
		if !hasPrefix {
			return nil, fmt.Errorf("ToNames: object result has an invalid prefix: %v, expected prefix: %v", attr.Name, prefix)
		}
		names = append(names, name)
	}
	for _, p := range result.Prefixes {
		name, hasPrefix := strings.CutPrefix(p, prefix)
		if !hasPrefix {
			return nil, fmt.Errorf("ToNames: prefix result has an invalid prefix: %v, expected prefix: %v", p, prefix)
		}
		names = append(names, strings.TrimSuffix(name, Delim))
	}
	return names, nil
}

type ObjectAttributes struct {
	Name         string
	Size         int64
	LastModified time.Time
	VersionInfo  VersionInfo
}

type VersionInfo struct {
	VersionID        string // Identifier for a particular version
	IsCurrent        bool   // If this is the current version
	RequiresUndelete bool   // Azure specific, the "deleted" state of noncurrent versions that must be "undeleted" before being promoted
	IsDeleteMarker   bool   // S3 specific, version that is created on an unversioned delete in a versioned bucket
}

type BucketConfig struct {
	backend string
	azure   AzureClientConfig
	gcs     GCSClientConfig
	s3      S3ClientConfig
}

func (c *BucketConfig) RegisterFlags(f *flag.FlagSet) {
	c.registerFlags("", f, false)
}

func ifNotEmptySuffix(s, suffix string) string {
	if s == "" {
		return ""
	}
	return s + suffix
}

func (c *BucketConfig) registerFlags(descriptor string, f *flag.FlagSet, backfill bool) {
	descriptorFlagPrefix := ifNotEmptySuffix(descriptor, ".")
	acceptedBackends := fmt.Sprintf("%s, %s or %s.", bucket.Azure, bucket.GCS, bucket.S3)
	if backfill {
		acceptedBackends = fmt.Sprintf("%s, %s, %s or backfill.", bucket.Azure, bucket.GCS, bucket.S3)
	}
	f.StringVar(&c.backend, descriptorFlagPrefix+"backend", "",
		fmt.Sprintf("The %sobject storage backend. Accepted values are: %s", ifNotEmptySuffix(descriptor, " "), acceptedBackends))
	c.azure.RegisterFlags(bucket.Azure+"."+descriptorFlagPrefix, f)
	c.gcs.RegisterFlags(bucket.GCS+"."+descriptorFlagPrefix, f)
	c.s3.RegisterFlags(bucket.S3+"."+descriptorFlagPrefix, f)
}

func (c *BucketConfig) Validate() error {
	return c.validate("")
}

func (c *BucketConfig) validate(descriptor string) error {
	descriptorFlagPrefix := ifNotEmptySuffix(descriptor, ".")
	if c.backend == "" {
		return fmt.Errorf("--%sbackend is missing", descriptorFlagPrefix)
	}
	switch c.backend {
	case bucket.Azure:
		return c.azure.Validate(bucket.Azure + "." + descriptorFlagPrefix)
	case bucket.GCS:
		return c.gcs.Validate(bucket.GCS + "." + descriptorFlagPrefix)
	case bucket.S3:
		return c.s3.Validate(bucket.S3 + "." + descriptorFlagPrefix)
	default:
		return fmt.Errorf("unknown backend provided in --%sbackend", descriptorFlagPrefix)
	}
}

func (c *BucketConfig) ToBucket(ctx context.Context) (Bucket, error) {
	switch c.backend {
	case bucket.Azure:
		return c.azure.ToBucket()
	case bucket.GCS:
		return c.gcs.ToBucket(ctx)
	case bucket.S3:
		return c.s3.ToBucket()
	default:
		return nil, fmt.Errorf("unknown backend: %v", c.backend)
	}
}

// ToObjstoreBucket is an adapter to objstore
func (c *BucketConfig) ToObjstoreBucket(ctx context.Context, logger log.Logger) (objstore.Bucket, error) {
	switch c.backend {
	case bucket.S3:
		return s3.NewBucketWithConfig(logger, s3.Config{
			Bucket:    c.s3.BucketName,
			Endpoint:  c.s3.Endpoint,
			AccessKey: c.s3.AccessKeyID,
			SecretKey: c.s3.SecretAccessKey,
			Insecure:  !c.s3.Secure,
			PartSize:  c.s3.PartSize,
		}, "objtools-s3", nil)
	case bucket.GCS:
		return gcs.NewBucketWithConfig(ctx, logger, gcs.Config{
			Bucket: c.gcs.BucketName,
		}, "objtools-gcs", nil)
	case bucket.Azure:
		azCfg := azure.DefaultConfig
		azCfg.StorageAccountName = c.azure.AccountName
		azCfg.StorageAccountKey = c.azure.AccountKey
		azCfg.ContainerName = c.azure.ContainerName
		return azure.NewBucketWithConfig(logger, azCfg, "objtools-azure", nil)
	default:
		return nil, fmt.Errorf("unknown backend: %v", c.backend)
	}
}

func (c *BucketConfig) Backend() string {
	return c.backend
}

type CopyBucketConfig struct {
	clientSideCopy bool
	source         BucketConfig
	destination    BucketConfig
}

func (c *CopyBucketConfig) RegisterFlags(f *flag.FlagSet, includeBackfill bool) {
	f.BoolVar(&c.clientSideCopy, "client-side-copy", false, "Use client side copying. This option is only respected if copying between two buckets of the same backend service. Client side copying is always used when copying between different backend services.")
	c.source.registerFlags("source", f, false)
	c.destination.registerFlags("destination", f, includeBackfill)
}

func (c *CopyBucketConfig) Validate() error {
	err := c.source.validate("source")
	if err != nil {
		return err
	}
	return c.destination.validate("destination")
}

// SourceBucket creates and returns only the source bucket.
func (c *CopyBucketConfig) SourceBucket(ctx context.Context) (Bucket, error) {
	return c.source.ToBucket(ctx)
}

// SourceObjstoreBucket creates an objstore.Bucket from the source configuration.
func (c *CopyBucketConfig) SourceObjstoreBucket(ctx context.Context, logger log.Logger) (objstore.Bucket, error) {
	return c.source.ToObjstoreBucket(ctx, logger)
}

// ValidateSource validates only the source bucket configuration.
func (c *CopyBucketConfig) ValidateSource() error {
	return c.source.validate("source")
}

func (c *CopyBucketConfig) ToBuckets(ctx context.Context) (source Bucket, destination Bucket, copyFunc CopyFunc, err error) {
	source, err = c.source.ToBucket(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	destination, err = c.destination.ToBucket(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	return source, destination, c.toCopyFunc(source, destination), nil
}

// CopyFunc copies from the source to the destination either client-side or server-side depending on the configuration
type CopyFunc func(context.Context, string, CopyOptions) error

func (c *CopyBucketConfig) DestinationBackend() string {
	return c.destination.backend
}

func (c *CopyBucketConfig) toCopyFunc(source Bucket, destination Bucket) CopyFunc {
	if c.clientSideCopy || c.source.backend != c.destination.backend {
		return func(ctx context.Context, objectName string, options CopyOptions) error {
			return source.ClientSideCopy(ctx, objectName, destination, options)
		}
	}
	return func(ctx context.Context, objectName string, options CopyOptions) error {
		return source.ServerSideCopy(ctx, objectName, destination, options)
	}
}

func ensureDelimiterSuffix(prefix string) string {
	if prefix != "" && !strings.HasSuffix(prefix, Delim) {
		return prefix + Delim
	}
	return prefix
}
