// SPDX-License-Identifier: AGPL-3.0-only

package rulestore

import (
	"flag"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/configs/client"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/ruler/rulestore/configdb"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/ruler/rulestore/local"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/storage/bucket"
)

type Config struct {
	bucket.Config `yaml:",inline"`
	ConfigDB      client.Config `yaml:"configdb"`
	Local         local.Config  `yaml:"local"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	prefix := "ruler-storage."

	cfg.ExtraBackends = []string{configdb.Name, local.Name}
	cfg.ConfigDB.RegisterFlagsWithPrefix(prefix, f)
	cfg.Local.RegisterFlagsWithPrefix(prefix, f)
	cfg.RegisterFlagsWithPrefix(prefix, f)
}
