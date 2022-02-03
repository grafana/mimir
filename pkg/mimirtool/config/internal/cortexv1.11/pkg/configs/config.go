// SPDX-License-Identifier: AGPL-3.0-only

package configs

import (
	"flag"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/configs/api"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/configs/db"
)

type Config struct {
	DB  db.Config  `yaml:"database"`
	API api.Config `yaml:"api"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.DB.RegisterFlags(f)
	cfg.API.RegisterFlags(f)
}
