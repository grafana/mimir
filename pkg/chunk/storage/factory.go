// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/storage/factory.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storage

import (
	"flag"

	"github.com/pkg/errors"
)

// Supported storage engines
const (
	StorageEngineChunks = "chunks"
	StorageEngineBlocks = "blocks"
)

// Config chooses which storage client to use.
type Config struct {
	Engine string `yaml:"engine"`
}

// RegisterFlags adds the flags required to configure this flag set.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Engine, "store.engine", "chunks", "The storage engine to use: chunks (deprecated) or blocks.")
}

// Validate config and returns error on failure
func (cfg *Config) Validate() error {
	if cfg.Engine != StorageEngineChunks && cfg.Engine != StorageEngineBlocks {
		return errors.New("unsupported storage engine")
	}
	return nil
}
