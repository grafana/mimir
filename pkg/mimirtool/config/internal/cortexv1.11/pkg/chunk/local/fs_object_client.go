// SPDX-License-Identifier: AGPL-3.0-only

package local

import (
	"flag"
)

type FSConfig struct {
	Directory string `yaml:"directory"`
}

func (cfg *FSConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}
func (cfg *FSConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Directory, prefix+"local.chunk-directory", "", "Directory to store chunks in.")
}
