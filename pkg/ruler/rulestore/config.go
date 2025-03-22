// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/rulestore/config.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package rulestore

import (
	"flag"
	"fmt"
	"reflect"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/flagext"

	"github.com/grafana/mimir/pkg/storage/bucket"
)

const (
	BackendLocal = "local"
)

var supportedCacheBackends = []string{cache.BackendMemcached, cache.BackendRedis}

// Config configures a rule store.
type Config struct {
	bucket.Config `yaml:",inline"`
	Local         LocalStoreConfig `yaml:"local"`

	// Cache holds the configuration used for the ruler storage cache.
	Cache cache.BackendConfig `yaml:"cache"`
}

// RegisterFlags registers the backend storage config.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	prefix := "ruler-storage."

	cfg.StorageBackendConfig.ExtraBackends = []string{BackendLocal}
	cfg.Local.RegisterFlagsWithPrefix(prefix, f)
	cfg.RegisterFlagsWithPrefixAndDefaultDirectory(prefix, "ruler", f)

	f.StringVar(&cfg.Cache.Backend, prefix+"cache.backend", "", fmt.Sprintf("Backend for ruler storage cache, if not empty. The cache is supported for any storage backend except %q. Supported values: %s.", BackendLocal, strings.Join(supportedCacheBackends, ", ")))
	cfg.Cache.Memcached.RegisterFlagsWithPrefix(prefix+"cache.memcached.", f)
	cfg.Cache.Redis.RegisterFlagsWithPrefix(prefix+"cache.redis.", f)
}

func (cfg *Config) Validate() error {
	if err := cfg.Config.Validate(); err != nil {
		return err
	}

	return cfg.Cache.Validate()
}

// IsDefaults returns true if the storage options have not been set.
func (cfg *Config) IsDefaults() bool {
	defaults := Config{}
	flagext.DefaultValues(&defaults)

	// Note: cmp.Equal will panic if it encounters anything it cannot handle.
	return cmp.Equal(*cfg, defaults, cmp.FilterPath(filterNonYaml, cmp.Ignore()), cmp.Comparer(equalSecrets))
}

type LocalStoreConfig struct {
	Directory string `yaml:"directory"`
}

// RegisterFlagsWithPrefix registers flags with the input prefix.
func (cfg *LocalStoreConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Directory, prefix+"local.directory", "", "Directory to scan for rules")
}

// Return true if the path contains a struct field with tag `yaml:"-"`.
func filterNonYaml(path cmp.Path) bool {
	for i, step := range path {
		// If we're not looking at a struct, or next step not available, skip.
		if step.Type().Kind() != reflect.Struct || i >= len(path)-1 {
			continue
		}
		field := step.Type().Field((path[i+1].(cmp.StructField)).Index())
		if tag, ok := field.Tag.Lookup("yaml"); ok {
			if tag == "-" {
				return true
			}
		}
	}
	return false
}

// Helper for cmp.Equal to compare Secret values for equality, since it has unexported fields.
func equalSecrets(a, b flagext.Secret) bool {
	return a == b
}
