// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/ruler_ring.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"flag"
	"fmt"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"

	"github.com/grafana/mimir/pkg/util"
)

const (
	// If a ruler is unable to heartbeat the ring, its better to quickly remove it and resume
	// the evaluation of all rules since the worst case scenario is that some rulers will
	// receive duplicate/out-of-order sample errors.
	ringAutoForgetUnhealthyPeriods = 2
)

// RingOp is the operation used for distributing rule groups between rulers.
var RingOp = ring.NewOp([]ring.InstanceState{ring.ACTIVE}, func(s ring.InstanceState) bool {
	// Only ACTIVE rulers get any rule groups. If instance is not ACTIVE, we need to find another ruler.
	return s != ring.ACTIVE
})

// RingConfig masks the ring lifecycler config which contains
// many options not really required by the rulers ring. This config
// is used to strip down the config to the minimum, and avoid confusion
// to the user.
type RingConfig struct {
	util.RingConfig `yaml:",inline"`

	NumTokens int `yaml:"num_tokens" category:"advanced"`

	// Used for testing
	SkipUnregister bool `yaml:"-"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *RingConfig) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	const flagNamePrefix = "ruler.ring."
	const kvStorePrefix = "rulers/"
	const componentPlural = "rulers"
	cfg.RingConfig.RegisterFlags(flagNamePrefix, kvStorePrefix, componentPlural, f, logger)
	
	f.IntVar(&cfg.NumTokens, flagNamePrefix+"num-tokens", 128, "Number of tokens for each ruler.")
}

// ToLifecyclerConfig returns a LifecyclerConfig based on the ruler
// ring config.
func (cfg *RingConfig) ToLifecyclerConfig(logger log.Logger) (ring.BasicLifecyclerConfig, error) {
	instanceAddr, err := ring.GetInstanceAddr(cfg.InstanceAddr, cfg.InstanceInterfaceNames, logger)
	if err != nil {
		return ring.BasicLifecyclerConfig{}, err
	}

	instancePort := ring.GetInstancePort(cfg.InstancePort, cfg.ListenPort)

	return ring.BasicLifecyclerConfig{
		ID:                  cfg.InstanceID,
		Addr:                fmt.Sprintf("%s:%d", instanceAddr, instancePort),
		HeartbeatPeriod:     cfg.HeartbeatPeriod,
		HeartbeatTimeout:    cfg.HeartbeatTimeout,
		TokensObservePeriod: 0,
		NumTokens:           cfg.NumTokens,
	}, nil
}

func (cfg *RingConfig) ringOptions() []util.Option {
	return []util.Option{
		util.WithSubringCacheDisabled(true),
		util.WithReplicationFactor(1),
	}
}
