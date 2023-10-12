// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/alertmanager_ring.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertmanager

import (
	"flag"
	"net"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"

	"github.com/grafana/mimir/pkg/util"
)

const (
	// RingKey is the key under which we store the alertmanager ring in the KVStore.
	RingKey = "alertmanager"

	// RingNameForServer is the name of the ring used by the alertmanager server.
	RingNameForServer = "alertmanager"

	// RingNumTokens is a safe default instead of exposing to config option to the user
	// in order to simplify the config.
	RingNumTokens = 128
)

// RingOp is the operation used for reading/writing to the alertmanagers.
var RingOp = ring.NewOp([]ring.InstanceState{ring.ACTIVE}, func(s ring.InstanceState) bool {
	// Only ACTIVE Alertmanager get requests. If instance is not ACTIVE, we need to find another Alertmanager.
	return s != ring.ACTIVE
})

// SyncRingOp is the operation used for checking if a user is owned by an alertmanager.
var SyncRingOp = ring.NewOp([]ring.InstanceState{ring.ACTIVE, ring.JOINING}, func(s ring.InstanceState) bool {
	return s != ring.ACTIVE
})

// RingConfig masks the ring lifecycler config which contains
// many options not really required by the alertmanager ring. This config
// is used to strip down the config to the minimum, and avoid confusion
// to the user.
type RingConfig struct {
	// Common ring config used across components
	Common util.CommonRingConfig `yaml:",inline"`

	// Configuration specific to alertmanager rings
	ReplicationFactor    int    `yaml:"replication_factor" category:"advanced"`
	ZoneAwarenessEnabled bool   `yaml:"zone_awareness_enabled" category:"advanced"`
	InstanceZone         string `yaml:"instance_availability_zone" category:"advanced"`

	// Used for testing
	RingCheckPeriod time.Duration `yaml:"-"`
	SkipUnregister  bool          `yaml:"-"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *RingConfig) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	const flagNamePrefix = "alertmanager.sharding-ring."
	const kvStorePrefix = "alertmanagers/"
	const componentPlural = "alertmanagers"

	cfg.Common.RegisterFlags(flagNamePrefix, kvStorePrefix, componentPlural, f, logger)

	// Ring flags
	f.IntVar(&cfg.ReplicationFactor, flagNamePrefix+"replication-factor", 3, "The replication factor to use when sharding the alertmanager.")
	f.BoolVar(&cfg.ZoneAwarenessEnabled, flagNamePrefix+"zone-awareness-enabled", false, "True to enable zone-awareness and replicate alerts across different availability zones.")
	f.StringVar(&cfg.InstanceZone, flagNamePrefix+"instance-availability-zone", "", "The availability zone where this instance is running. Required if zone-awareness is enabled.")

	cfg.RingCheckPeriod = 5 * time.Second
}

// ToLifecyclerConfig returns a LifecyclerConfig based on the alertmanager
// ring config.
func (cfg *RingConfig) ToLifecyclerConfig(logger log.Logger) (ring.BasicLifecyclerConfig, error) {
	instanceAddr, err := ring.GetInstanceAddr(cfg.Common.InstanceAddr, cfg.Common.InstanceInterfaceNames, logger, cfg.Common.EnableIPv6)
	if err != nil {
		return ring.BasicLifecyclerConfig{}, err
	}

	instancePort := ring.GetInstancePort(cfg.Common.InstancePort, cfg.Common.ListenPort)

	return ring.BasicLifecyclerConfig{
		ID:                  cfg.Common.InstanceID,
		Addr:                net.JoinHostPort(instanceAddr, strconv.Itoa(instancePort)),
		HeartbeatPeriod:     cfg.Common.HeartbeatPeriod,
		HeartbeatTimeout:    cfg.Common.HeartbeatTimeout,
		TokensObservePeriod: 0,
		Zone:                cfg.InstanceZone,
		NumTokens:           RingNumTokens,
	}, nil
}

func (cfg *RingConfig) toRingConfig() ring.Config {
	rc := cfg.Common.ToRingConfig()
	rc.ReplicationFactor = cfg.ReplicationFactor
	rc.ZoneAwarenessEnabled = cfg.ZoneAwarenessEnabled

	return rc
}
