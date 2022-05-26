// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/alertmanager_ring.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertmanager

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/netutil"
	"github.com/grafana/dskit/ring"

	util_log "github.com/grafana/mimir/pkg/util/log"
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
	KVStore              kv.Config     `yaml:"kvstore" doc:"description=The key-value store used to share the hash ring across multiple instances."`
	HeartbeatPeriod      time.Duration `yaml:"heartbeat_period" category:"advanced"`
	HeartbeatTimeout     time.Duration `yaml:"heartbeat_timeout" category:"advanced"`
	ReplicationFactor    int           `yaml:"replication_factor" category:"advanced"`
	ZoneAwarenessEnabled bool          `yaml:"zone_awareness_enabled" category:"advanced"`

	// Instance details
	InstanceID             string   `yaml:"instance_id" doc:"default=<hostname>" category:"advanced"`
	InstanceInterfaceNames []string `yaml:"instance_interface_names" category:"advanced" doc:"default=[<private network interfaces>]"`
	InstancePort           int      `yaml:"instance_port" category:"advanced"`
	InstanceAddr           string   `yaml:"instance_addr" category:"advanced"`
	InstanceZone           string   `yaml:"instance_availability_zone" category:"advanced"`

	// Injected internally
	ListenPort      int           `yaml:"-"`
	RingCheckPeriod time.Duration `yaml:"-"`

	// Used for testing
	SkipUnregister bool `yaml:"-"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *RingConfig) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	hostname, err := os.Hostname()
	if err != nil {
		level.Error(util_log.Logger).Log("msg", "failed to get hostname", "err", err)
		os.Exit(1)
	}

	// Prefix used by all the ring flags
	rfprefix := "alertmanager.sharding-ring."

	// Ring flags
	cfg.KVStore.Store = "memberlist" // Override default value.
	cfg.KVStore.RegisterFlagsWithPrefix(rfprefix, "alertmanagers/", f)
	f.DurationVar(&cfg.HeartbeatPeriod, rfprefix+"heartbeat-period", 15*time.Second, "Period at which to heartbeat to the ring. 0 = disabled.")
	f.DurationVar(&cfg.HeartbeatTimeout, rfprefix+"heartbeat-timeout", time.Minute, "The heartbeat timeout after which alertmanagers are considered unhealthy within the ring. 0 = never (timeout disabled).")
	f.IntVar(&cfg.ReplicationFactor, rfprefix+"replication-factor", 3, "The replication factor to use when sharding the alertmanager.")
	f.BoolVar(&cfg.ZoneAwarenessEnabled, rfprefix+"zone-awareness-enabled", false, "True to enable zone-awareness and replicate alerts across different availability zones.")

	// Instance flags
	cfg.InstanceInterfaceNames = netutil.PrivateNetworkInterfacesWithFallback([]string{"eth0", "en0"}, logger)
	f.Var((*flagext.StringSlice)(&cfg.InstanceInterfaceNames), rfprefix+"instance-interface-names", "List of network interface names to look up when finding the instance IP address.")
	f.StringVar(&cfg.InstanceAddr, rfprefix+"instance-addr", "", "IP address to advertise in the ring. Default is auto-detected.")
	f.IntVar(&cfg.InstancePort, rfprefix+"instance-port", 0, "Port to advertise in the ring (defaults to -server.grpc-listen-port).")
	f.StringVar(&cfg.InstanceID, rfprefix+"instance-id", hostname, "Instance ID to register in the ring.")
	f.StringVar(&cfg.InstanceZone, rfprefix+"instance-availability-zone", "", "The availability zone where this instance is running. Required if zone-awareness is enabled.")

	cfg.RingCheckPeriod = 5 * time.Second
}

// ToLifecyclerConfig returns a LifecyclerConfig based on the alertmanager
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
		Zone:                cfg.InstanceZone,
		NumTokens:           RingNumTokens,
	}, nil
}

func (cfg *RingConfig) ToRingConfig() ring.Config {
	rc := ring.Config{}
	flagext.DefaultValues(&rc)

	rc.KVStore = cfg.KVStore
	rc.HeartbeatTimeout = cfg.HeartbeatTimeout
	rc.ReplicationFactor = cfg.ReplicationFactor
	rc.ZoneAwarenessEnabled = cfg.ZoneAwarenessEnabled

	return rc
}
