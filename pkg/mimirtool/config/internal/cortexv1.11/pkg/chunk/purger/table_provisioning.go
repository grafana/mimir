// SPDX-License-Identifier: AGPL-3.0-only

package purger

import (
	"flag"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/chunk"
)

type TableProvisioningConfig struct {
	chunk.ActiveTableProvisionConfig `yaml:",inline"`
	TableTags                        chunk.Tags `yaml:"tags"`
}

func (cfg *TableProvisioningConfig) RegisterFlags(argPrefix string, f *flag.FlagSet) {
	// default values ActiveTableProvisionConfig
	cfg.ProvisionedWriteThroughput = 1
	cfg.ProvisionedReadThroughput = 300
	cfg.ProvisionedThroughputOnDemandMode = false

	cfg.ActiveTableProvisionConfig.RegisterFlags(argPrefix, f)
	f.Var(&cfg.TableTags, argPrefix+".tags", "Tag (of the form key=value) to be added to the tables. Supported by DynamoDB")
}
