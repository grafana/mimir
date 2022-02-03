// SPDX-License-Identifier: AGPL-3.0-only

package openstack

import (
	"flag"

	cortex_swift "github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/storage/bucket/swift"
)

type SwiftConfig struct {
	cortex_swift.Config `yaml:",inline"`
}

func (cfg *SwiftConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}
func (cfg *SwiftConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.Config.RegisterFlagsWithPrefix(prefix, f)
}
