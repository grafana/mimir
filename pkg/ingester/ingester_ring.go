// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester_ring.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
)

const (
	RingNumTokens = 128
)

// Provide config options for basic layer for backwards
type LifecyclerConfig struct {
	RingConfig       RingConfig    `yaml:"ring"`
	NumTokens        int           `yaml:"num_tokens"`
	TokensFilePath   string        `yaml:"tokens_file_path"`
	HeartbeatPeriod  time.Duration `yaml:"heartbeat_period"`
	MinReadyDuration time.Duration `yaml:"min_ready_duration"`

	// Instance details
	ID             string   `yaml:"instance_id" doc:"hidden"`
	Port           int      `yaml:"instance_port" doc:"hidden"`
	Addr           string   `yaml:"instance_addr" doc:"hidden"`
	Zone           string   `yaml:"availability_zone"`
	InterfaceNames []string `yaml:"interface_names"`

	UnregisterOnShutdown bool `yaml:"unregister_on_shutdown"`

	// Injected internally
	ListenPort      int           `yaml:"-"`
	RingCheckPeriod time.Duration `yaml:"-"`
}

// RingConfig masks the ring lifecycler config which contains
// many options not really required by the store gateways ring. This config
// is used to strip down the config to the minimum, and avoid confusion
// to the user.
type RingConfig struct {
	KVStore kv.Config `yaml:"kvstore" doc:"description=The key-value store used to share the hash ring across multiple instances. This option needs be set both on the distributor and ingester when running in microservices mode."`

	HeartbeatTimeout     time.Duration `yaml:"heartbeat_timeout"`
	ReplicationFactor    int           `yaml:"replication_factor"`
	ZoneAwarenessEnabled bool          `yaml:"zone_awareness_enabled"`

	// Wait ring stability.
	// WaitStabilityMinDuration time.Duration `yaml:"wait_stability_min_duration"`
	// WaitStabilityMaxDuration time.Duration `yaml:"wait_stability_max_duration"`

}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *LifecyclerConfig) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	hostname, err := os.Hostname()
	if err != nil {
		level.Error(logger).Log("msg", "failed to get hostname", "err", err)
		os.Exit(1)
	}

	ringFlagsPrefix := "ingester."

	// Ring flags
	cfg.RingConfig.RegisterFlags(f)
	f.DurationVar(&cfg.HeartbeatPeriod, ringFlagsPrefix+"heartbeat-period", 15*time.Second, "Period at which to heartbeat to the ring. 0 = disabled.")
	f.IntVar(&cfg.NumTokens, ringFlagsPrefix+"num-tokens", RingNumTokens, "Number of tokens for each ingester.")
	f.StringVar(&cfg.TokensFilePath, ringFlagsPrefix+"tokens-file-path", "", "File path where tokens are stored. If empty, tokens are not stored at shutdown and restored at startup.")

	// Wait stability flags.
	// f.DurationVar(&cfg.WaitStabilityMinDuration, ringFlagsPrefix+"wait-stability-min-duration", time.Minute, "Minimum time to wait for ring stability at startup. 0 to disable.")
	// f.DurationVar(&cfg.WaitStabilityMaxDuration, ringFlagsPrefix+"wait-stability-max-duration", 5*time.Minute, "Maximum time to wait for ring stability at startup. If the store-gateway ring keeps changing after this period of time, the store-gateway will start anyway.")
	f.DurationVar(&cfg.MinReadyDuration, ringFlagsPrefix+"min-ready-duration", 15*time.Second, "Minimum duration to wait after the internal readiness checks have passed but before succeeding the readiness endpoint. This is used to slowdown deployment controllers (eg. Kubernetes) after an instance is ready and before they proceed with a rolling update, to give the rest of the cluster instances enough time to receive ring updates.")

	// Instance flags
	cfg.InterfaceNames = []string{"eth0", "en0"}
	f.Var((*flagext.StringSlice)(&cfg.InterfaceNames), ringFlagsPrefix+"interface", "Name of network interface to read address from.")
	f.StringVar(&cfg.Addr, ringFlagsPrefix+"addr", "", "IP address to advertise in the ring.")
	f.IntVar(&cfg.Port, ringFlagsPrefix+"port", 0, "Port to advertise in the ring (defaults to server.grpc-listen-port).")
	f.StringVar(&cfg.ID, ringFlagsPrefix+"ID", hostname, "Instance ID to register in the ring.")
	f.StringVar(&cfg.Zone, ringFlagsPrefix+"availability-zone", "", "The availability zone where this instance is running. Required if zone-awareness is enabled.")

	f.BoolVar(&cfg.UnregisterOnShutdown, ringFlagsPrefix+"unregister-on-shutdown", true, "Unregister from the ring upon clean shutdown.")

	// Defaults for internal settings.
	cfg.RingCheckPeriod = 5 * time.Second
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *RingConfig) RegisterFlags(f *flag.FlagSet) {
	ringFlagsPrefix := "ingester."

	// Ring flags
	cfg.KVStore.RegisterFlagsWithPrefix("", "collectors/", f)
	f.DurationVar(&cfg.HeartbeatTimeout, ringFlagsPrefix+"heartbeat-timeout", time.Minute, "The heartbeat timeout after which store gateways are considered unhealthy within the ring. 0 = never (timeout disabled).") // +sharedOptionWithQuerier)
	f.IntVar(&cfg.ReplicationFactor, "distributor.replication-factor", 3, "The replication factor to use when sharding blocks.")                                                                                       // +sharedOptionWithQuerier)
	f.BoolVar(&cfg.ZoneAwarenessEnabled, "distributor.zone-awareness-enabled", false, "True to enable zone-awareness and replicate blocks across different availability zones.")
}

func (cfg *RingConfig) ToRingConfig() ring.Config {
	rc := ring.Config{}
	flagext.DefaultValues(&rc)

	rc.KVStore = cfg.KVStore
	rc.HeartbeatTimeout = cfg.HeartbeatTimeout
	rc.ReplicationFactor = cfg.ReplicationFactor
	rc.ZoneAwarenessEnabled = cfg.ZoneAwarenessEnabled
	rc.SubringCacheDisabled = true

	return rc
}

func (cfg *LifecyclerConfig) ToLifecyclerConfig(logger log.Logger) (ring.BasicLifecyclerConfig, error) {
	instanceAddr, err := ring.GetInstanceAddr(cfg.Addr, cfg.InterfaceNames, logger)
	if err != nil {
		return ring.BasicLifecyclerConfig{}, err
	}

	instancePort := ring.GetInstancePort(cfg.Port, cfg.ListenPort)

	return ring.BasicLifecyclerConfig{
		ID:                              cfg.ID,
		Addr:                            fmt.Sprintf("%s:%d", instanceAddr, instancePort),
		Zone:                            cfg.Zone,
		HeartbeatPeriod:                 cfg.HeartbeatPeriod,
		HeartbeatTimeout:                cfg.RingConfig.HeartbeatTimeout,
		MinReadyDuration:                cfg.MinReadyDuration,
		TokensObservePeriod:             0,
		NumTokens:                       cfg.NumTokens,
		KeepInstanceInTheRingOnShutdown: !cfg.UnregisterOnShutdown,
	}, nil
}
