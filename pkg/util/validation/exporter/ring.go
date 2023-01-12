// SPDX-License-Identifier: AGPL-3.0-only

package exporter

import (
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/netutil"
	"github.com/grafana/dskit/ring"

	util_log "github.com/grafana/mimir/pkg/util/log"
)

const (
	// ringKey is the key under which we store the overrides-exporter's ring in the KVStore.
	ringKey = "overrides-exporter"

	// ringNumTokens is how many tokens each overrides-exporter should have in the ring.
	// Overrides-exporters use a ring for tenant sharding and don't need perfect balancing.
	ringNumTokens = 64

	// ringAutoForgetUnhealthyPeriods is how many consecutive timeout periods an unhealthy instance
	// in the ring will be automatically removed after.
	ringAutoForgetUnhealthyPeriods = 4
)

// ringOp is used as an instace state filter when obtaining instances from the ring
var ringOp = ring.NewOp([]ring.InstanceState{ring.ACTIVE}, nil)

// RingConfig holds the configuration for the overrides-exporter ring
type RingConfig struct {
	Enabled bool `yaml:"enabled" category:"experimental"`

	// KV store details
	KVStore          kv.Config     `yaml:"kvstore" doc:"description=The key-value store used to share the hash ring across multiple instances."`
	HeartbeatPeriod  time.Duration `yaml:"heartbeat_period" category:"advanced"`
	HeartbeatTimeout time.Duration `yaml:"heartbeat_timeout" category:"advanced"`

	// Instance details
	InstanceID             string   `yaml:"instance_id" doc:"default=<hostname>" category:"advanced"`
	InstanceInterfaceNames []string `yaml:"instance_interface_names" doc:"default=[<private network interfaces>]"`
	InstancePort           int      `yaml:"instance_port" category:"advanced"`
	InstanceAddr           string   `yaml:"instance_addr" category:"advanced"`

	// Injected internally
	ListenPort int `yaml:"-"`
}

// RegisterFlags configures this RingConfig to the given flag set and sets defaults
func (c *RingConfig) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	hostname, err := os.Hostname()
	if err != nil {
		_ = level.Error(util_log.Logger).Log("msg", "failed to get hostname", "err", err)
		os.Exit(1)
	}
	f.BoolVar(&c.Enabled, "overrides-exporter.ring.enabled", false, "Enable the ring used by override-exporters to deduplicate exported limit metrics.")

	// Ring flags
	c.KVStore.Store = "memberlist" // Override default value.
	c.KVStore.RegisterFlagsWithPrefix("overrides-exporter.ring.", "collectors/", f)
	f.DurationVar(&c.HeartbeatPeriod, "overrides-exporter.ring.heartbeat-period", 15*time.Second, "Period at which to heartbeat to the ring. 0 = disabled.")
	f.DurationVar(&c.HeartbeatTimeout, "overrides-exporter.ring.heartbeat-timeout", time.Minute, "The heartbeat timeout after which overrides-exporters are considered unhealthy within the ring.")

	// Instance flags
	c.InstanceInterfaceNames = netutil.PrivateNetworkInterfacesWithFallback([]string{"eth0", "en0"}, logger)
	f.Var((*flagext.StringSlice)(&c.InstanceInterfaceNames), "overrides-exporter.ring.instance-interface-names", "List of network interface names to look up when finding the instance IP address.")
	f.StringVar(&c.InstanceAddr, "overrides-exporter.ring.instance-addr", "", "IP address to advertise in the ring. Default is auto-detected.")
	f.IntVar(&c.InstancePort, "overrides-exporter.ring.instance-port", 0, "Port to advertise in the ring (defaults to -server.grpc-listen-port).")
	f.StringVar(&c.InstanceID, "overrides-exporter.ring.instance-id", hostname, "Instance ID to register in the ring.")
}

// toBasicLifecyclerConfig transforms a RingConfig into configuration that can be used to create a BasicLifecycler
func (c *RingConfig) toBasicLifecyclerConfig(logger log.Logger) (ring.BasicLifecyclerConfig, error) {
	instanceAddr, err := ring.GetInstanceAddr(c.InstanceAddr, c.InstanceInterfaceNames, logger)
	if err != nil {
		return ring.BasicLifecyclerConfig{}, err
	}

	instancePort := ring.GetInstancePort(c.InstancePort, c.ListenPort)

	return ring.BasicLifecyclerConfig{
		ID:                              c.InstanceID,
		Addr:                            fmt.Sprintf("%s:%d", instanceAddr, instancePort),
		HeartbeatPeriod:                 c.HeartbeatPeriod,
		HeartbeatTimeout:                c.HeartbeatTimeout,
		TokensObservePeriod:             0,
		NumTokens:                       ringNumTokens,
		KeepInstanceInTheRingOnShutdown: false,
	}, nil
}

// toRingConfig transforms a RingConfig into a configuration that can be used to create a ring client
func (c *RingConfig) toRingConfig() ring.Config {
	rc := ring.Config{}
	flagext.DefaultValues(&rc)

	rc.KVStore = c.KVStore
	rc.HeartbeatTimeout = c.HeartbeatTimeout
	rc.ReplicationFactor = 1
	rc.SubringCacheDisabled = true

	return rc
}

// overridesExporterRing is a ring client that overrides-exporters can use to
// assume ownership of a tenant shard
type overridesExporterRing struct {
	config     RingConfig
	client     *ring.Ring
	lifecycler *ring.BasicLifecycler
}

// Owns looks up whether this ring member owns the given tenant.
func (o *overridesExporterRing) Owns(tenantID string) (bool, error) {
	return instanceOwnsIdentifier(o.client, o.lifecycler.GetInstanceAddr(), tenantID)
}

// newRing creates a new overridesExporterRing from the given configuration.
func newRing(config RingConfig, logger log.Logger, reg prometheus.Registerer) (*overridesExporterRing, error) {
	reg = prometheus.WrapRegistererWithPrefix("cortex_", reg)
	kvStore, err := kv.NewClient(
		config.KVStore,
		ring.GetCodec(),
		kv.RegistererWithKVName(reg, "overrides-exporter-lifecycler"),
		logger,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize overrides-exporter's KV store")
	}

	delegate := ring.BasicLifecyclerDelegate(ring.NewInstanceRegisterDelegate(ring.ACTIVE, ringNumTokens))
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, logger)
	delegate = ring.NewAutoForgetDelegate(ringAutoForgetUnhealthyPeriods*config.HeartbeatTimeout, delegate, logger)

	lifecyclerConfig, err := config.toBasicLifecyclerConfig(logger)
	if err != nil {
		return nil, err
	}

	const ringName = "overrides-exporter"
	lifecycler, err := ring.NewBasicLifecycler(lifecyclerConfig, ringName, ringKey, kvStore, delegate, logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize overrides-exporter's lifecycler")
	}

	ringClient, err := ring.New(config.toRingConfig(), ringName, ringKey, logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create a overrides-exporter ring client")
	}

	return &overridesExporterRing{
		config:     config,
		client:     ringClient,
		lifecycler: lifecycler,
	}, nil
}

// instanceOwnsIdentifier hashes the given key to a token, looks up the token
// owner in the ring and checks whether this instance is the token owner
func instanceOwnsIdentifier(r ring.ReadRing, instanceAddr string, key string) (bool, error) {
	// Hash the key.
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	hash := hasher.Sum32()

	// Check whether this instance owns the token.
	rs, err := r.Get(hash, ringOp, nil, nil, nil)
	if err != nil {
		return false, errors.Wrap(err, "failed to get instances from the ring")
	}

	if len(rs.Instances) != 1 {
		return false, fmt.Errorf(
			"unexpected number of overrides-exporters in the shard (expected 1, got %d)",
			len(rs.Instances),
		)
	}

	return rs.Instances[0].Addr == instanceAddr, nil
}
