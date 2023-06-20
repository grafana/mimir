// SPDX-License-Identifier: AGPL-3.0-only

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
	"github.com/grafana/dskit/netutil"
	"github.com/grafana/dskit/ring"
)

const (
	// sharedOptionWithRingClient is a message appended to all config options that should be also
	// set on the components running the ingester ring client.
	sharedOptionWithRingClient      = " This option needs be set on ingesters, distributors, queriers and rulers when running in microservices mode."
	tokenGenerationStrategyFlag     = "token-generation-strategy"
	randomTokenGeneration           = "random-tokens"
	spreadMinimizingTokenGeneration = "spread-min-tokens"
)

type RingConfig struct {
	KVStore              kv.Config              `yaml:"kvstore" doc:"description=The key-value store used to share the hash ring across multiple instances. This option needs be set on ingesters, distributors, queriers and rulers when running in microservices mode."`
	HeartbeatPeriod      time.Duration          `yaml:"heartbeat_period" category:"advanced"`
	HeartbeatTimeout     time.Duration          `yaml:"heartbeat_timeout" category:"advanced"`
	ReplicationFactor    int                    `yaml:"replication_factor"`
	ZoneAwarenessEnabled bool                   `yaml:"zone_awareness_enabled"`
	ExcludedZones        flagext.StringSliceCSV `yaml:"excluded_zones" category:"advanced"`

	// Tokens
	TokensFilePath string `yaml:"tokens_file_path"`
	NumTokens      int    `yaml:"num_tokens" category:"advanced"`

	// Instance details
	InstanceID             string   `yaml:"instance_id" category:"advanced" doc:"default=<hostname>"`
	InstanceInterfaceNames []string `yaml:"instance_interface_names" category:"advanced" doc:"default=[<private network interfaces>]"`
	InstancePort           int      `yaml:"instance_port" category:"advanced"`
	InstanceAddr           string   `yaml:"instance_addr" category:"advanced"`
	EnableIPv6             bool     `yaml:"instance_enable_ipv6" category:"advanced"`
	InstanceZone           string   `yaml:"instance_availability_zone" category:"advanced"`

	UnregisterOnShutdown bool `yaml:"unregister_on_shutdown" category:"advanced"`

	// Config for the ingester lifecycle control
	ObservePeriod    time.Duration `yaml:"observe_period" category:"advanced"`
	MinReadyDuration time.Duration `yaml:"min_ready_duration" category:"advanced"`
	FinalSleep       time.Duration `yaml:"final_sleep" category:"advanced"`

	TokenGeneratorStrategy string                 `yaml:"token_generator_strategy" category:"experimental"`
	SpreadMinimizingZones  flagext.StringSliceCSV `yaml:"spread_minimizing_zones" category:"experimental"`

	// Injected internally
	ListenPort int `yaml:"-"`

	// Used only for testing.
	JoinAfter time.Duration `yaml:"-"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *RingConfig) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	hostname, err := os.Hostname()
	if err != nil {
		level.Error(logger).Log("msg", "failed to get hostname", "err", err)
		os.Exit(1)
	}

	prefix := "ingester.ring."

	// Ring flags
	cfg.KVStore.Store = "memberlist" // Override default value.
	cfg.KVStore.RegisterFlagsWithPrefix(prefix, "collectors/", f)

	f.DurationVar(&cfg.HeartbeatPeriod, prefix+"heartbeat-period", 15*time.Second, "Period at which to heartbeat to the ring. 0 = disabled.")
	f.DurationVar(&cfg.HeartbeatTimeout, prefix+"heartbeat-timeout", time.Minute, "The heartbeat timeout after which ingesters are skipped for reads/writes. 0 = never (timeout disabled)."+sharedOptionWithRingClient)
	f.IntVar(&cfg.ReplicationFactor, prefix+"replication-factor", 3, "Number of ingesters that each time series is replicated to."+sharedOptionWithRingClient)
	f.BoolVar(&cfg.ZoneAwarenessEnabled, prefix+"zone-awareness-enabled", false, "True to enable the zone-awareness and replicate ingested samples across different availability zones."+sharedOptionWithRingClient)
	f.Var(&cfg.ExcludedZones, prefix+"excluded-zones", "Comma-separated list of zones to exclude from the ring. Instances in excluded zones will be filtered out from the ring."+sharedOptionWithRingClient)

	f.StringVar(&cfg.TokensFilePath, prefix+"tokens-file-path", "", "File path where tokens are stored. If empty, tokens are not stored at shutdown and restored at startup.")
	f.IntVar(&cfg.NumTokens, prefix+"num-tokens", 128, "Number of tokens for each ingester.")

	// Instance flags
	f.StringVar(&cfg.InstanceID, prefix+"instance-id", hostname, "Instance ID to register in the ring.")
	cfg.InstanceInterfaceNames = netutil.PrivateNetworkInterfacesWithFallback([]string{"eth0", "en0"}, logger)
	f.Var((*flagext.StringSlice)(&cfg.InstanceInterfaceNames), prefix+"instance-interface-names", "List of network interface names to look up when finding the instance IP address.")
	f.IntVar(&cfg.InstancePort, prefix+"instance-port", 0, "Port to advertise in the ring (defaults to -server.grpc-listen-port).")
	f.StringVar(&cfg.InstanceAddr, prefix+"instance-addr", "", "IP address to advertise in the ring. Default is auto-detected.")
	f.BoolVar(&cfg.EnableIPv6, prefix+"instance-enable-ipv6", false, "Enable using a IPv6 instance address. (default false)")
	f.StringVar(&cfg.InstanceZone, prefix+"instance-availability-zone", "", "The availability zone where this instance is running.")

	f.BoolVar(&cfg.UnregisterOnShutdown, prefix+"unregister-on-shutdown", true, "Unregister from the ring upon clean shutdown. It can be useful to disable for rolling restarts with consistent naming.")

	// Lifecycler.
	f.DurationVar(&cfg.ObservePeriod, prefix+"observe-period", 0*time.Second, "Observe tokens after generating to resolve collisions. Useful when using gossiping ring.")
	flagext.DeprecatedFlag(f, prefix+"join-after", "Deprecated: this setting was used to set a period of time to wait before joining the hash ring. Mimir now behaves as this setting is always set to 0s.", logger)
	f.DurationVar(&cfg.MinReadyDuration, prefix+"min-ready-duration", 15*time.Second, "Minimum duration to wait after the internal readiness checks have passed but before succeeding the readiness endpoint. This is used to slowdown deployment controllers (eg. Kubernetes) after an instance is ready and before they proceed with a rolling update, to give the rest of the cluster instances enough time to receive ring updates.")
	f.DurationVar(&cfg.FinalSleep, prefix+"final-sleep", 0, "Duration to sleep for before exiting, to ensure metrics are scraped.")

	// TokenGenerator
	f.StringVar(&cfg.TokenGeneratorStrategy, prefix+tokenGenerationStrategyFlag, randomTokenGeneration, "Specifies the strategy used for generating tokens for ingesters. Possible values are \""+randomTokenGeneration+"\" (default) and \""+spreadMinimizingTokenGeneration+"\"")
	f.Var(&cfg.SpreadMinimizingZones, prefix+"spread-minimizing-zones", "Comma-separated list of zones in which SpreadMinimizingTokenGenerator is used for token generation. This configuration is used only when "+tokenGenerationStrategyFlag+" is set to \""+spreadMinimizingTokenGeneration+"\"")
}

// ToRingConfig returns a ring.Config based on the ingester
// ring config.
func (cfg *RingConfig) ToRingConfig() ring.Config {
	rc := ring.Config{}
	flagext.DefaultValues(&rc)

	// Configure ring
	rc.KVStore = cfg.KVStore
	rc.HeartbeatTimeout = cfg.HeartbeatTimeout
	rc.ReplicationFactor = cfg.ReplicationFactor
	rc.ZoneAwarenessEnabled = cfg.ZoneAwarenessEnabled
	rc.ExcludedZones = cfg.ExcludedZones
	rc.SubringCacheDisabled = false // Enable subring caching.

	return rc
}

// ToLifecyclerConfig returns a ring.LifecyclerConfig based on the ingester
// ring config.
func (cfg *RingConfig) ToLifecyclerConfig(logger log.Logger) ring.LifecyclerConfig {
	// Configure lifecycler
	lc := ring.LifecyclerConfig{}
	flagext.DefaultValues(&lc)

	lc.RingConfig = cfg.ToRingConfig()
	lc.NumTokens = cfg.NumTokens
	lc.HeartbeatPeriod = cfg.HeartbeatPeriod
	lc.HeartbeatTimeout = cfg.HeartbeatTimeout
	lc.ObservePeriod = cfg.ObservePeriod
	lc.JoinAfter = cfg.JoinAfter
	lc.MinReadyDuration = cfg.MinReadyDuration
	lc.InfNames = cfg.InstanceInterfaceNames
	lc.FinalSleep = cfg.FinalSleep
	lc.TokensFilePath = cfg.TokensFilePath
	lc.Zone = cfg.InstanceZone
	lc.UnregisterOnShutdown = cfg.UnregisterOnShutdown
	lc.ReadinessCheckRingHealth = false
	lc.Addr = cfg.InstanceAddr
	lc.Port = cfg.InstancePort
	lc.ID = cfg.InstanceID
	lc.ListenPort = cfg.ListenPort
	lc.EnableInet6 = cfg.EnableIPv6
	lc.RingTokenGenerator = cfg.customTokenGenerator(logger)

	return lc
}

// customTokenGenerator returns a token generator, which is an implementation of ring.TokenGenerator,
// according to this RingConfig's configuration. If "spread-min-tokens" token generation strategy is set,
// customTokenGenerator tries to build and return an instance of ring.SpreadMinimizingTokenGenerator.
// If it was impossible, if "random-tokens" or any other unsupported token generation strategy is set,
// an instance of ring.RandomTokenGenerator is returned.
func (cfg *RingConfig) customTokenGenerator(logger log.Logger) ring.TokenGenerator {
	switch cfg.TokenGeneratorStrategy {
	case spreadMinimizingTokenGeneration:
		tokenGenerator, err := ring.NewSpreadMinimizingTokenGenerator(cfg.InstanceID, cfg.InstanceZone, cfg.SpreadMinimizingZones, logger)
		if err != nil {
			level.Warn(logger).Log("msg", "It was impossible to generate an instance of SpreadMinimizingTokenGenerator. RandomTokenGenerator will be used instead.", "err", err)
			return ring.NewRandomTokenGenerator()
		}
		return tokenGenerator
	case randomTokenGeneration:
		return ring.NewRandomTokenGenerator()
	default:
		warn := fmt.Sprintf("Unsupported token generation strategy (\"%s\") has been chosen for %s. RandomTokenGenerator will be used instead.", cfg.TokenGeneratorStrategy, tokenGenerationStrategyFlag)
		level.Warn(logger).Log("msg", warn)
		return ring.NewRandomTokenGenerator()
	}
}
