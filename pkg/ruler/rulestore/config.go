// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/rulestore/config.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package rulestore

import (
	"flag"
	"reflect"

	"github.com/go-kit/log"
	"github.com/google/go-cmp/cmp"
	"github.com/grafana/dskit/flagext"

	"github.com/grafana/mimir/pkg/ruler/rulestore/local"
	"github.com/grafana/mimir/pkg/storage/bucket"
)

// Config configures a rule store.
type Config struct {
	bucket.Config `yaml:",inline"`
	Local         local.Config `yaml:"local"`
}

// RegisterFlags registers the backend storage config.
func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	prefix := "ruler-storage."

	cfg.StorageBackendConfig.ExtraBackends = []string{local.Name}
	cfg.Local.RegisterFlagsWithPrefix(prefix, f)
	cfg.RegisterFlagsWithPrefixAndDefaultDirectory(prefix, "ruler", f, logger)
}

// IsDefaults returns true if the storage options have not been set.
func (cfg *Config) IsDefaults() bool {
	defaults := Config{}
	flagext.DefaultValues(&defaults)

	// Note: cmp.Equal will panic if it encounters anything it cannot handle.
	return cmp.Equal(*cfg, defaults, cmp.FilterPath(filterNonYaml, cmp.Ignore()), cmp.Comparer(equalSecrets))
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
