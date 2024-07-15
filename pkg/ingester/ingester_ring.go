// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/netutil"
	"github.com/grafana/dskit/ring"
)

const (
	flagTokensFilePath                  = "tokens-file-path"
	flagTokenGenerationStrategy         = "token-generation-strategy"
	flagSpreadMinimizingJoinRingInOrder = "spread-minimizing-join-ring-in-order"
	// allowed values for token-generation-strategy
	tokenGenerationRandom           = "random"
	tokenGenerationSpreadMinimizing = "spread-minimizing"
)

// sharedOptionWithRingClient is a message appended to all config options that should be also
// set on the components running the ingester ring client.
const sharedOptionWithRingClient = " This option needs be set on ingesters, distributors, queriers, and rulers when running in microservices mode."

// ErrSpreadMinimizingValidation is a sentinel error that indicates a failure
// in the validation of spread minimizing token generation config.
var ErrSpreadMinimizingValidation = fmt.Errorf("%q token generation strategy is misconfigured", tokenGenerationSpreadMinimizing)

type RingConfig struct {
	KVStore              kv.Config              `yaml:"kvstore" doc:"description=The key-value store used to share the hash ring across multiple instances. This option needs be set on ingesters, distributors, queriers, and rulers when running in microservices mode."`
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

	TokenGenerationStrategy         string                 `yaml:"token_generation_strategy" category:"advanced"`
	SpreadMinimizingJoinRingInOrder bool                   `yaml:"spread_minimizing_join_ring_in_order" category:"advanced"`
	SpreadMinimizingZones           flagext.StringSliceCSV `yaml:"spread_minimizing_zones" category:"advanced"`

	// Injected internally
	ListenPort int `yaml:"-"`

	// Used only for testing.
	JoinAfter time.Duration `yaml:"-"`
}

func (cfg *RingConfig) Validate() error {
	if cfg.TokenGenerationStrategy != tokenGenerationRandom && cfg.TokenGenerationStrategy != tokenGenerationSpreadMinimizing {
		return fmt.Errorf("unsupported token generation strategy (%q) has been chosen for %s", cfg.TokenGenerationStrategy, flagTokenGenerationStrategy)
	}

	if cfg.TokenGenerationStrategy == tokenGenerationSpreadMinimizing {
		if cfg.TokensFilePath != "" {
			return fmt.Errorf("%w: strategy requires %q to be empty", ErrSpreadMinimizingValidation, flagTokensFilePath)
		}
		_, err := ring.NewSpreadMinimizingTokenGenerator(cfg.InstanceID, cfg.InstanceZone, cfg.SpreadMinimizingZones, cfg.SpreadMinimizingJoinRingInOrder)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrSpreadMinimizingValidation, err)
		}
		return nil
	}

	// at this point cfg.TokenGenerationStrategy is not tokenGenerationSpreadMinimizing
	if cfg.SpreadMinimizingJoinRingInOrder {
		return fmt.Errorf("%q must be false when using %q token generation strategy", flagSpreadMinimizingJoinRingInOrder, cfg.TokenGenerationStrategy)
	}

	return nil
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

	f.StringVar(&cfg.TokensFilePath, prefix+flagTokensFilePath, "", fmt.Sprintf("File path where tokens are stored. If empty, tokens are not stored at shutdown and restored at startup. Must be empty if -%s is set to %q.", prefix+flagTokenGenerationStrategy, tokenGenerationSpreadMinimizing))
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
	f.StringVar(&cfg.TokenGenerationStrategy, prefix+flagTokenGenerationStrategy, tokenGenerationRandom, fmt.Sprintf("Specifies the strategy used for generating tokens for ingesters. Supported values are: %s.", strings.Join([]string{tokenGenerationRandom, tokenGenerationSpreadMinimizing}, ",")))
	f.BoolVar(&cfg.SpreadMinimizingJoinRingInOrder, prefix+flagSpreadMinimizingJoinRingInOrder, false, fmt.Sprintf("True to allow this ingester registering tokens in the ring only after all previous ingesters (with ID lower than the current one) have already been registered. This configuration option is supported only when the token generation strategy is set to %q.", tokenGenerationSpreadMinimizing))
	f.Var(&cfg.SpreadMinimizingZones, prefix+"spread-minimizing-zones", fmt.Sprintf("Comma-separated list of zones in which spread minimizing strategy is used for token generation. This value must include all zones in which ingesters are deployed, and must not change over time. This configuration is used only when %q is set to %q.", flagTokenGenerationStrategy, tokenGenerationSpreadMinimizing))
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
func (cfg *RingConfig) ToLifecyclerConfig() ring.LifecyclerConfig {
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
	lc.RingTokenGenerator = cfg.customTokenGenerator()

	return lc
}

// customTokenGenerator returns a token generator, which is an implementation of ring.TokenGenerator,
// according to this RingConfig's configuration. If "spread-minimizing" token generation strategy is
// set, customTokenGenerator tries to build and return an instance of ring.SpreadMinimizingTokenGenerator.
// If it was impossible, or if "random" token generation strategy is set, customTokenGenerator returns
// an instance of ring.RandomTokenGenerator. Otherwise, nil is returned.
func (cfg *RingConfig) customTokenGenerator() ring.TokenGenerator {
	switch cfg.TokenGenerationStrategy {
	case tokenGenerationSpreadMinimizing:
		tokenGenerator, _ := ring.NewSpreadMinimizingTokenGenerator(cfg.InstanceID, cfg.InstanceZone, cfg.SpreadMinimizingZones, cfg.SpreadMinimizingJoinRingInOrder)
		return tokenGenerator
	case tokenGenerationRandom:
		return ring.NewRandomTokenGenerator()
	default:
		return nil
	}
}
