// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"flag"
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
	sharedOptionWithRingClient = " This option needs be set on ingesters, distributors, queriers and rulers when running in microservices mode."
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
	InstanceZone           string   `yaml:"instance_availability_zone" category:"advanced"`

	UnregisterOnShutdown bool `yaml:"unregister_on_shutdown" category:"advanced"`

	// Config for the ingester lifecycle control
	ObservePeriod            time.Duration `yaml:"observe_period" category:"advanced"`
	JoinAfter                time.Duration `yaml:"join_after" category:"advanced"`
	MinReadyDuration         time.Duration `yaml:"min_ready_duration" category:"advanced"`
	FinalSleep               time.Duration `yaml:"final_sleep" category:"advanced"`
	ReadinessCheckRingHealth bool          `yaml:"readiness_check_ring_health" category:"advanced"`

	// Injected internally
	ListenPort int `yaml:"-"`
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

	f.DurationVar(&cfg.HeartbeatPeriod, prefix+"heartbeat-period", 5*time.Second, "Period at which to heartbeat to the ring. 0 = disabled.")
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
	f.StringVar(&cfg.InstanceZone, prefix+"instance-availability-zone", "", "The availability zone where this instance is running.")

	f.BoolVar(&cfg.UnregisterOnShutdown, prefix+"unregister-on-shutdown", true, "Unregister from the ring upon clean shutdown. It can be useful to disable for rolling restarts with consistent naming in conjunction with -distributor.extend-writes=false.")

	/// Lifecycler.
	f.DurationVar(&cfg.ObservePeriod, prefix+"observe-period", 0*time.Second, "Observe tokens after generating to resolve collisions. Useful when using gossiping ring.")
	f.DurationVar(&cfg.JoinAfter, prefix+"join-after", 0*time.Second, "Period to wait for a claim from another member; will join automatically after this.")
	f.DurationVar(&cfg.MinReadyDuration, prefix+"min-ready-duration", 15*time.Second, "Minimum duration to wait after the internal readiness checks have passed but before succeeding the readiness endpoint. This is used to slowdown deployment controllers (eg. Kubernetes) after an instance is ready and before they proceed with a rolling update, to give the rest of the cluster instances enough time to receive ring updates.")
	f.DurationVar(&cfg.FinalSleep, prefix+"final-sleep", 0, "Duration to sleep for before exiting, to ensure metrics are scraped.")
	f.BoolVar(&cfg.ReadinessCheckRingHealth, prefix+"readiness-check-ring-health", true, "When enabled the readiness probe succeeds only after all instances are ACTIVE and healthy in the ring, otherwise only the instance itself is checked. This option should be disabled if in your cluster multiple instances can be rolled out simultaneously, otherwise rolling updates may be slowed down.")
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
	lc.ObservePeriod = cfg.ObservePeriod
	lc.JoinAfter = cfg.JoinAfter
	lc.MinReadyDuration = cfg.MinReadyDuration
	lc.InfNames = cfg.InstanceInterfaceNames
	lc.FinalSleep = cfg.FinalSleep
	lc.TokensFilePath = cfg.TokensFilePath
	lc.Zone = cfg.InstanceZone
	lc.UnregisterOnShutdown = cfg.UnregisterOnShutdown
	lc.ReadinessCheckRingHealth = cfg.ReadinessCheckRingHealth
	lc.Addr = cfg.InstanceAddr
	lc.Port = cfg.InstancePort
	lc.ID = cfg.InstanceID
	lc.ListenPort = cfg.ListenPort

	return lc
}
