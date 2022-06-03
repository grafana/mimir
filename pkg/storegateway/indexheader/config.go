// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"flag"
)

type BinaryReaderConfig struct {
	MapPopulateEnabled bool `yaml:"map_populate_enabled" category:"experimental"`
}

func (cfg *BinaryReaderConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.BoolVar(&cfg.MapPopulateEnabled, prefix+"map-populate-enabled", false, "If enabled, the store-gateway will attempt to pre-populate the file system cache when memory-mapping index-header files. This flag has no effect on Windows platforms.")
}
