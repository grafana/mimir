// SPDX-License-Identifier: AGPL-3.0-only

package filesystem

import "flag"

type Config struct {
	Directory string `yaml:"dir"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Directory, prefix+"filesystem.dir", "", "Local filesystem storage directory.")
}
