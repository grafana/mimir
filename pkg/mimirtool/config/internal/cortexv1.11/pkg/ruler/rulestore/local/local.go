// SPDX-License-Identifier: AGPL-3.0-only

package local

import (
	"flag"
)

type Config struct {
	Directory string `yaml:"directory"`
}

const (
	Name = "local"
)

func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Directory, prefix+"local.directory", "", "Directory to scan for rules")
}
