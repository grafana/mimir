// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"flag"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/util"
)

type Config struct {
	ShardingEnabled  bool       `yaml:"sharding_enabled"`
	ShardingRing     RingConfig `yaml:"sharding_ring" doc:"description=The hash ring configuration. This option is required only if blocks sharding is enabled."`
	ShardingStrategy string     `yaml:"sharding_strategy"`
}

const (
	syncReasonInitial    = "initial"
	syncReasonPeriodic   = "periodic"
	syncReasonRingChange = "ring-change"

	// sharedOptionWithQuerier is a message appended to all config options that should be also
	// set on the querier in order to work correct.
	sharedOptionWithQuerier = " This option needs be set both on the store-gateway and querier when running in microservices mode."

	// ringAutoForgetUnhealthyPeriods is how many consecutive timeout periods an unhealthy instance
	// in the ring will be automatically removed.
	ringAutoForgetUnhealthyPeriods = 10
)

var (
	supportedShardingStrategies = []string{util.ShardingStrategyDefault, util.ShardingStrategyShuffle}

	// Validation errors.
	errInvalidShardingStrategy = errors.New("invalid sharding strategy")
	errInvalidTenantShardSize  = errors.New("invalid tenant shard size, the value must be greater than 0")
)

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.ShardingRing.RegisterFlags(f)

	f.BoolVar(&cfg.ShardingEnabled, "store-gateway.sharding-enabled", false, "Shard blocks across multiple store gateway instances."+sharedOptionWithQuerier)
	f.StringVar(&cfg.ShardingStrategy, "store-gateway.sharding-strategy", util.ShardingStrategyDefault, fmt.Sprintf("The sharding strategy to use. Supported values are: %s.", strings.Join(supportedShardingStrategies, ", ")))
}
