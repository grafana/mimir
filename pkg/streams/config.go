package streams

import (
	"flag"
	"time"

	"github.com/grafana/mimir/pkg/storage/bucket"
)

// TODO validate in validateBucketConfigs() and validateFilesystemPaths() in pkg/mimir/mimir.go,
// and also the checks done in pkg/mimir/sanity_check.go.
type Config struct {
	Enabled      bool          `yaml:"enabled"`
	BufferPeriod time.Duration `yaml:"buffer_period"`

	Bucket bucket.Config `yaml:",inline"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "ingest-storage.enabled", false, "True to enable the ingestion via object storage.")
	f.DurationVar(&cfg.BufferPeriod, "ingest-storage.buffer-period", 250*time.Millisecond, "How long to buffer incoming requests in the distributor before they're uploaded to the object storage.")

	cfg.Bucket.RegisterFlagsWithPrefixAndDefaultDirectory("ingest-storage.", "ingest", f)
}

// Validate the config.
func (cfg *Config) Validate() error {
	if err := cfg.Bucket.Validate(); err != nil {
		return err
	}

	return nil
}
