package alertstore

import (
	"flag"

	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/alertmanager/alertstore/local"
	"github.com/grafana/mimir/pkg/chunk/aws"
	"github.com/grafana/mimir/pkg/chunk/azure"
	"github.com/grafana/mimir/pkg/chunk/gcp"
	"github.com/grafana/mimir/pkg/storage/bucket"
)

// LegacyConfig configures the alertmanager storage backend using the legacy storage clients.
// TODO remove this legacy config in Mimir 1.11.
type LegacyConfig struct {
	Type string `yaml:"type"`

	// Object Storage Configs
	Azure azure.BlobStorageConfig `yaml:"azure"`
	GCS   gcp.GCSConfig           `yaml:"gcs"`
	S3    aws.S3Config            `yaml:"s3"`
	Local local.StoreConfig       `yaml:"local"`
}

// RegisterFlags registers flags.
func (cfg *LegacyConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Type, "alertmanager.storage.type", local.Name, "Type of backend to use to store alertmanager configs. Supported values are: \"gcs\", \"s3\", \"local\".")

	cfg.Azure.RegisterFlagsWithPrefix("alertmanager.storage.", f)
	cfg.GCS.RegisterFlagsWithPrefix("alertmanager.storage.", f)
	cfg.S3.RegisterFlagsWithPrefix("alertmanager.storage.", f)
	cfg.Local.RegisterFlagsWithPrefix("alertmanager.storage.", f)
}

// Validate config and returns error on failure
func (cfg *LegacyConfig) Validate() error {
	if err := cfg.Azure.Validate(); err != nil {
		return errors.Wrap(err, "invalid Azure Storage config")
	}
	if err := cfg.S3.Validate(); err != nil {
		return errors.Wrap(err, "invalid S3 Storage config")
	}
	return nil
}

// IsDefaults returns true if the storage options have not been set.
func (cfg *LegacyConfig) IsDefaults() bool {
	return cfg.Type == local.Name && cfg.Local.Path == ""
}

// Config configures a the alertmanager storage backend.
type Config struct {
	bucket.Config `yaml:",inline"`
	Local         local.StoreConfig `yaml:"local"`
}

// RegisterFlags registers the backend storage config.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	prefix := "alertmanager-storage."

	cfg.ExtraBackends = []string{local.Name}
	cfg.Local.RegisterFlagsWithPrefix(prefix, f)
	cfg.RegisterFlagsWithPrefix(prefix, f)
}

// IsFullStateSupported returns if the given configuration supports access to FullState objects.
func (cfg *Config) IsFullStateSupported() bool {
	for _, backend := range bucket.SupportedBackends {
		if cfg.Backend == backend {
			return true
		}
	}
	return false
}
