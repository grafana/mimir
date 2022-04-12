// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/bucket/client.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package bucket

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket/azure"
	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
	"github.com/grafana/mimir/pkg/storage/bucket/gcs"
	"github.com/grafana/mimir/pkg/storage/bucket/s3"
	"github.com/grafana/mimir/pkg/storage/bucket/swift"
	"github.com/grafana/mimir/pkg/util"
)

const (
	// S3 is the value for the S3 storage backend.
	S3 = "s3"

	// GCS is the value for the GCS storage backend.
	GCS = "gcs"

	// Azure is the value for the Azure storage backend.
	Azure = "azure"

	// Swift is the value for the Openstack Swift storage backend.
	Swift = "swift"

	// Filesystem is the value for the filesystem storage backend.
	Filesystem = "filesystem"
)

var (
	SupportedBackends = []string{S3, GCS, Azure, Swift, Filesystem}

	ErrUnsupportedStorageBackend = errors.New("unsupported storage backend")
)

// Config holds configuration for accessing long-term storage.
type Config struct {
	Backend       string `yaml:"backend"`
	StoragePrefix string `yaml:"storage_prefix" category:"advanced"`
	// Backends
	S3         s3.Config         `yaml:"s3"`
	GCS        gcs.Config        `yaml:"gcs"`
	Azure      azure.Config      `yaml:"azure"`
	Swift      swift.Config      `yaml:"swift"`
	Filesystem filesystem.Config `yaml:"filesystem"`

	// Not used internally, meant to allow callers to wrap Buckets
	// created using this config
	Middlewares []func(objstore.Bucket) (objstore.Bucket, error) `yaml:"-"`

	// Used to inject additional backends into the config. Allows for this config to
	// be embedded in multiple contexts and support non-object storage based backends.
	ExtraBackends []string `yaml:"-"`
}

// Returns the supportedBackends for the package and any custom backends injected into the config.
func (cfg *Config) supportedBackends() []string {
	return append(SupportedBackends, cfg.ExtraBackends...)
}

// RegisterFlags registers the backend storage config.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

func (cfg *Config) RegisterFlagsWithPrefixAndDefaultDirectory(prefix, dir string, f *flag.FlagSet) {
	cfg.S3.RegisterFlagsWithPrefix(prefix, f)
	cfg.GCS.RegisterFlagsWithPrefix(prefix, f)
	cfg.Azure.RegisterFlagsWithPrefix(prefix, f)
	cfg.Swift.RegisterFlagsWithPrefix(prefix, f)
	cfg.Filesystem.RegisterFlagsWithPrefixAndDefaultDirectory(prefix, dir, f)

	f.StringVar(&cfg.Backend, prefix+"backend", Filesystem, fmt.Sprintf("Backend storage to use. Supported backends are: %s.", strings.Join(cfg.supportedBackends(), ", ")))
	f.StringVar(&cfg.StoragePrefix, prefix+"storage-prefix", "", "Prefix for all objects stored in the backend storage.")
}

func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefixAndDefaultDirectory(prefix, "", f)
}

func (cfg *Config) Validate() error {
	if !util.StringsContain(cfg.supportedBackends(), cfg.Backend) {
		return ErrUnsupportedStorageBackend
	}

	if cfg.Backend == S3 {
		if err := cfg.S3.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// NewClient creates a new bucket client based on the configured backend
func NewClient(ctx context.Context, cfg Config, name string, logger log.Logger, reg prometheus.Registerer) (client objstore.Bucket, err error) {
	switch cfg.Backend {
	case S3:
		client, err = s3.NewBucketClient(cfg.S3, name, logger)
	case GCS:
		client, err = gcs.NewBucketClient(ctx, cfg.GCS, name, logger)
	case Azure:
		client, err = azure.NewBucketClient(cfg.Azure, name, logger)
	case Swift:
		client, err = swift.NewBucketClient(cfg.Swift, name, logger)
	case Filesystem:
		client, err = filesystem.NewBucketClient(cfg.Filesystem)
	default:
		return nil, ErrUnsupportedStorageBackend
	}

	if err != nil {
		return nil, err
	}

	if cfg.StoragePrefix != "" {
		client = NewPrefixedBucketClient(client, cfg.StoragePrefix)
	}

	client = objstore.NewTracingBucket(bucketWithMetrics(client, name, reg))

	// Wrap the client with any provided middleware
	for _, wrap := range cfg.Middlewares {
		client, err = wrap(client)
		if err != nil {
			return nil, err
		}
	}

	return client, nil
}

func bucketWithMetrics(bucketClient objstore.Bucket, name string, reg prometheus.Registerer) objstore.Bucket {
	if reg == nil {
		return bucketClient
	}

	return objstore.BucketWithMetrics(
		"", // bucket label value
		bucketClient,
		prometheus.WrapRegistererWith(prometheus.Labels{"component": name}, reg))
}
