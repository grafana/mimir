// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/distributor_ring.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"flag"
	"net"
	"strconv"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"

	"github.com/grafana/mimir/pkg/util"
)

const (
	// ringNumTokens is how many tokens each distributor should have in the ring.
	// Distributors use a ring because they need to know how many distributors there
	// are in total for rate limiting.
	ringNumTokens = 1
)

// RingConfig masks the ring lifecycler config which contains
// many options not really required by the distributors ring. This config
// is used to strip down the config to the minimum, and avoid confusion
// to the user.
type RingConfig struct {
	Common util.CommonRingConfig `yaml:",inline"`
}

func (cfg *RingConfig) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	cfg.Common.RegisterFlags("distributor.ring.", "collectors/", "distributors", f, logger)
}

func (cfg *RingConfig) ToBasicLifecyclerConfig(logger log.Logger) (ring.BasicLifecyclerConfig, error) {
	instanceAddr, err := ring.GetInstanceAddr(cfg.Common.InstanceAddr, cfg.Common.InstanceInterfaceNames, logger, cfg.Common.EnableIPv6)
	if err != nil {
		return ring.BasicLifecyclerConfig{}, err
	}

	instancePort := ring.GetInstancePort(cfg.Common.InstancePort, cfg.Common.ListenPort)

	return ring.BasicLifecyclerConfig{
		ID:                              cfg.Common.InstanceID,
		Addr:                            net.JoinHostPort(instanceAddr, strconv.Itoa(instancePort)),
		HeartbeatPeriod:                 cfg.Common.HeartbeatPeriod,
		HeartbeatTimeout:                cfg.Common.HeartbeatTimeout,
		TokensObservePeriod:             0,
		NumTokens:                       ringNumTokens,
		KeepInstanceInTheRingOnShutdown: false,
	}, nil
}

func (cfg *RingConfig) toRingConfig() ring.Config {
	rc := cfg.Common.ToRingConfig()
	rc.ReplicationFactor = 1

	return rc
}
