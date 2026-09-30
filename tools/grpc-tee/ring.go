// SPDX-License-Identifier: AGPL-3.0-only

package main

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

	"github.com/grafana/mimir/pkg/util"
)

const (
	// ringNumTokens is how many tokens each grpc-tee instance has in the ring.
	// Queriers pick a grpc-tee instance at random, so the tokens are not used for sharding.
	ringNumTokens = 1

	ringName = "grpc-tee"
	ringKey  = "grpc-tee"
)

var statusPageConfig = ring.StatusPageConfig{
	HideTokensUIElements: true,
}

// RingConfig is the configuration of the ring that grpc-tee instances join.
type RingConfig struct {
	Common util.CommonRingConfig `yaml:",inline"`

	AutoForgetUnhealthyPeriods int `yaml:"auto_forget_unhealthy_periods"`
}

func (cfg *RingConfig) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	cfg.Common.RegisterFlags("ring.", "collectors/", "grpc-tees", f, logger)
	f.IntVar(&cfg.AutoForgetUnhealthyPeriods, "ring.auto-forget-unhealthy-periods", 3, "Number of consecutive timeout periods after which an unhealthy instance is removed from the ring. Set to 0 to disable auto-forget.")
}

func (cfg *RingConfig) toBasicLifecyclerConfig(logger log.Logger) (ring.BasicLifecyclerConfig, error) {
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
		StatusPageConfig:                statusPageConfig,
	}, nil
}

func (cfg *RingConfig) toRingConfig() ring.Config {
	rc := cfg.Common.ToRingConfig()
	rc.ReplicationFactor = 1
	rc.StatusPageConfig = statusPageConfig

	return rc
}

func newLifecycler(cfg RingConfig, logger log.Logger, reg prometheus.Registerer) (*ring.BasicLifecycler, error) {
	kvStore, err := kv.NewClient(cfg.Common.KVStore, ring.GetCodec(), kv.RegistererWithKVName(reg, "grpc-tee-lifecycler"), logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize grpc-tee KV store: %w", err)
	}

	lifecyclerCfg, err := cfg.toBasicLifecyclerConfig(logger)
	if err != nil {
		return nil, fmt.Errorf("failed to build grpc-tee lifecycler config: %w", err)
	}

	var delegate ring.BasicLifecyclerDelegate
	delegate = ring.NewInstanceRegisterDelegate(ring.ACTIVE, lifecyclerCfg.NumTokens)
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, logger)
	if cfg.AutoForgetUnhealthyPeriods > 0 {
		delegate = ring.NewAutoForgetDelegate(time.Duration(cfg.AutoForgetUnhealthyPeriods)*cfg.Common.HeartbeatTimeout, delegate, logger)
	}

	lifecycler, err := ring.NewBasicLifecycler(lifecyclerCfg, ringName, ringKey, kvStore, delegate, logger, reg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize grpc-tee lifecycler: %w", err)
	}

	return lifecycler, nil
}

func newRing(cfg RingConfig, logger log.Logger, reg prometheus.Registerer) (*ring.Ring, error) {
	r, err := ring.New(cfg.toRingConfig(), ringName, ringKey, logger, reg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize grpc-tee ring client: %w", err)
	}

	return r, nil
}
