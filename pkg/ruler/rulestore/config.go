// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/rulestore/config.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package rulestore

import (
	"flag"
	"reflect"

	"github.com/grafana/mimir/pkg/ruler/rulestore/local"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/util/flagext"
)

// Config configures a rule store.
type Config struct {
	bucket.Config `yaml:",inline"`
	Local         local.Config `yaml:"local"`
}

// RegisterFlags registers the backend storage config.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	prefix := "ruler-storage."

	cfg.ExtraBackends = []string{local.Name}
	cfg.Local.RegisterFlagsWithPrefix(prefix, f)
	cfg.RegisterFlagsWithPrefix(prefix, f)
}

// IsDefaults returns true if the storage options have not been set.
func (cfg *Config) IsDefaults() bool {
	defaults := Config{}
	flagext.DefaultValues(&defaults)

	return reflect.DeepEqual(*cfg, defaults)
}
