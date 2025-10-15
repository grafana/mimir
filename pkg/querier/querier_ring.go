// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"flag"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/util"
)

const (
	// ringNumTokens is how many tokens each querier should have in the ring.
	// Queriers use a ring because they need to share the maximum query plan version
	// they support with query-frontends.
	ringNumTokens = 1

	querierRingKey = "querier"

	MaximumSupportedQueryPlanVersion = 1
)

var querierRingComponentNames = map[uint64]string{
	MaximumSupportedQueryPlanVersion: "maximum supported query plan version",
}

// RingConfig strips the ring lifecycler configuration down to the minimum required for the querier ring.
// The ring lifecycler config contains many options not needed by the querier ring.
type RingConfig struct {
	Common util.CommonRingConfig `yaml:",inline"`

	AutoForgetUnhealthyPeriods int `yaml:"auto_forget_unhealthy_periods" category:"advanced"`
}

func (cfg *RingConfig) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	cfg.Common.RegisterFlags("querier.ring.", "collectors/", "queriers", f, logger)
	f.IntVar(&cfg.AutoForgetUnhealthyPeriods, "querier.ring.auto-forget-unhealthy-periods", 10, "Number of consecutive timeout periods an unhealthy instance in the ring is automatically removed after. Set to 0 to disable auto-forget.")
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
		HideTokensInStatusPage:          true,
		ShowVersionsInStatusPage:        true,
		ComponentNames:                  querierRingComponentNames,
		Versions: ring.InstanceVersions{
			MaximumSupportedQueryPlanVersion: uint64(planning.MaximumSupportedQueryPlanVersion),
		},
	}, nil
}

func (cfg *RingConfig) toRingConfig() ring.Config {
	rc := cfg.Common.ToRingConfig()
	rc.ReplicationFactor = 1
	rc.HideTokensInStatusPage = true
	rc.ShowVersionsInStatusPage = true
	rc.ComponentNames = querierRingComponentNames

	return rc
}

func NewLifecycler(cfg RingConfig, logger log.Logger, reg prometheus.Registerer) (*ring.BasicLifecycler, error) {
	reg = prometheus.WrapRegistererWithPrefix("cortex_", reg)
	kvStore, err := kv.NewClient(cfg.Common.KVStore, ring.GetCodec(), kv.RegistererWithKVName(reg, "querier-lifecycler"), logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize querier KV store: %w", err)
	}

	lifecyclerCfg, err := cfg.ToBasicLifecyclerConfig(logger)
	if err != nil {
		return nil, fmt.Errorf("failed to build querier lifecycler config: %w", err)
	}

	var delegate ring.BasicLifecyclerDelegate
	delegate = ring.NewInstanceRegisterDelegate(ring.ACTIVE, lifecyclerCfg.NumTokens)
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, logger)
	if cfg.AutoForgetUnhealthyPeriods > 0 {
		delegate = ring.NewAutoForgetDelegate(time.Duration(cfg.AutoForgetUnhealthyPeriods)*cfg.Common.HeartbeatTimeout, delegate, logger)
	}

	lifecycler, err := ring.NewBasicLifecycler(lifecyclerCfg, "querier", querierRingKey, kvStore, delegate, logger, reg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize querier lifecycler: %w", err)
	}

	return lifecycler, nil
}

func NewRing(cfg RingConfig, logger log.Logger, reg prometheus.Registerer) (*ring.Ring, error) {
	r, err := ring.New(cfg.toRingConfig(), "querier", querierRingKey, logger, reg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize querier ring client: %w", err)
	}

	return r, nil
}
