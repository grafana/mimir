package ingest

import (
	"flag"
)

type Config struct {
	Enabled bool `yaml:"enabled"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "ingest-storage.enabled", false, "True to enable the ingestion via object storage.")
}

// Validate the config.
func (cfg *Config) Validate() error {
	return nil
}
