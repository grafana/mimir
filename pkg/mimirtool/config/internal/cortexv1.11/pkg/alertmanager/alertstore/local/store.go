// SPDX-License-Identifier: AGPL-3.0-only

package local

import (
	"flag"

	"github.com/pkg/errors"
)

type StoreConfig struct {
	Path string `yaml:"path"`
}

const (
	Name = "local"
)

var (
	errReadOnly = errors.New("local alertmanager config storage is read-only")
	errState    = errors.New("local alertmanager storage does not support state persistency")
)

func (cfg *StoreConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Path, prefix+"local.path", "", "Path at which alertmanager configurations are stored.")
}
