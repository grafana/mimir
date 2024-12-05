// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"flag"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/netutil"
	"github.com/grafana/dskit/ring"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	InstanceRingKey  = "usage-tracker-instances"
	InstanceRingName = "usage-tracker-instances"

	// ringAutoForgetUnhealthyPeriods is how many consecutive timeout periods an unhealthy instance
	// in the ring will be automatically removed after.
	ringAutoForgetUnhealthyPeriods = 4
)

// InstanceRingConfig masks the ring lifecycler config which contains many options not really required by the usage-tracker ring.
// This config is used to strip down the config to the minimum, and avoid confusion to the user.
type InstanceRingConfig struct {
	KVStore          kv.Config     `yaml:"kvstore" doc:"description=The key-value store used to share the hash ring across multiple instances. When usage-tracker is enabled, this option needs be set on usage-trackers and distributors."`
	HeartbeatPeriod  time.Duration `yaml:"heartbeat_period" category:"advanced"`
	HeartbeatTimeout time.Duration `yaml:"heartbeat_timeout" category:"advanced"`

	// Instance details
	InstanceID             string   `yaml:"instance_id" doc:"default=<hostname>" category:"advanced"`
	InstanceInterfaceNames []string `yaml:"instance_interface_names" doc:"default=[<private network interfaces>]"`
	InstancePort           int      `yaml:"instance_port" category:"advanced"`
	InstanceAddr           string   `yaml:"instance_addr" category:"advanced"`
	InstanceZone           string   `yaml:"instance_availability_zone"`
	EnableIPv6             bool     `yaml:"instance_enable_ipv6" category:"advanced"`

	// Injected internally
	ListenPort int `yaml:"-"`
}

// RegisterFlags adds the flags required to config this to the given flag.FlagSet.
func (cfg *InstanceRingConfig) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	hostname, err := os.Hostname()
	if err != nil {
		level.Error(logger).Log("msg", "failed to get hostname", "err", err)
		os.Exit(1)
	}

	// Ring flags
	cfg.KVStore.Store = "memberlist" // Override default value.
	cfg.KVStore.RegisterFlagsWithPrefix("usage-tracker.instance-ring.", "collectors/", f)
	f.DurationVar(&cfg.HeartbeatPeriod, "usage-tracker.instance-ring.heartbeat-period", 15*time.Second, "Period at which to heartbeat to the ring. 0 = disabled.")
	f.DurationVar(&cfg.HeartbeatTimeout, "usage-tracker.instance-ring.heartbeat-timeout", time.Minute, "The heartbeat timeout after which usage-trackers are considered unhealthy within the ring.")

	// Instance flags
	cfg.InstanceInterfaceNames = netutil.PrivateNetworkInterfacesWithFallback([]string{"eth0", "en0"}, logger)
	f.Var((*flagext.StringSlice)(&cfg.InstanceInterfaceNames), "usage-tracker.instance-ring.instance-interface-names", "List of network interface names to look up when finding the instance IP address.")
	f.StringVar(&cfg.InstanceAddr, "usage-tracker.instance-ring.instance-addr", "", "IP address to advertise in the ring. Default is auto-detected.")
	f.IntVar(&cfg.InstancePort, "usage-tracker.instance-ring.instance-port", 0, "Port to advertise in the ring (defaults to -server.grpc-listen-port).")
	f.StringVar(&cfg.InstanceID, "usage-tracker.instance-ring.instance-id", hostname, "Instance ID to register in the ring.")
	f.StringVar(&cfg.InstanceZone, "usage-tracker.instance-ring.instance-availability-zone", "", "The availability zone where this instance is running.")
	f.BoolVar(&cfg.EnableIPv6, "usage-tracker.instance-ring.instance-enable-ipv6", false, "Enable using a IPv6 instance address. (default false)")
}

// ToBasicLifecyclerConfig returns a ring.BasicLifecyclerConfig based on the usage-tracker ring config.
func (cfg *InstanceRingConfig) ToBasicLifecyclerConfig(logger log.Logger) (ring.BasicLifecyclerConfig, error) {
	instanceAddr, err := ring.GetInstanceAddr(cfg.InstanceAddr, cfg.InstanceInterfaceNames, logger, cfg.EnableIPv6)
	if err != nil {
		return ring.BasicLifecyclerConfig{}, err
	}

	instancePort := ring.GetInstancePort(cfg.InstancePort, cfg.ListenPort)

	return ring.BasicLifecyclerConfig{
		ID:                              cfg.InstanceID,
		Addr:                            net.JoinHostPort(instanceAddr, strconv.Itoa(instancePort)),
		Zone:                            cfg.InstanceZone,
		HeartbeatPeriod:                 cfg.HeartbeatPeriod,
		HeartbeatTimeout:                cfg.HeartbeatTimeout,
		TokensObservePeriod:             0,
		NumTokens:                       1, // We just use the instance ring for service discovery.
		KeepInstanceInTheRingOnShutdown: false,
	}, nil
}

// ToRingConfig returns a ring.Config based on the usage-tracker ring config.
func (cfg *InstanceRingConfig) ToRingConfig() ring.Config {
	rc := ring.Config{}
	flagext.DefaultValues(&rc)

	rc.KVStore = cfg.KVStore
	rc.HeartbeatTimeout = cfg.HeartbeatTimeout
	rc.ReplicationFactor = 1
	rc.SubringCacheDisabled = true

	return rc
}

// NewInstanceRingLifecycler creates a new usage-tracker ring lifecycler with all required lifecycler delegates.
func NewInstanceRingLifecycler(cfg InstanceRingConfig, logger log.Logger, reg prometheus.Registerer) (*ring.BasicLifecycler, error) {
	reg = prometheus.WrapRegistererWithPrefix("cortex_", reg)
	kvStore, err := kv.NewClient(cfg.KVStore, ring.GetCodec(), kv.RegistererWithKVName(reg, "usage-tracker-lifecycler"), logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize usage-trackers' KV store")
	}

	lifecyclerCfg, err := cfg.ToBasicLifecyclerConfig(logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to build usage-trackers' lifecycler config")
	}

	var delegate ring.BasicLifecyclerDelegate
	delegate = ring.NewInstanceRegisterDelegate(ring.ACTIVE, 1)
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, logger)
	delegate = ring.NewAutoForgetDelegate(ringAutoForgetUnhealthyPeriods*cfg.HeartbeatTimeout, delegate, logger)

	lifecycler, err := ring.NewBasicLifecycler(lifecyclerCfg, InstanceRingName, InstanceRingKey, kvStore, delegate, logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize usage-trackers' lifecycler")
	}

	return lifecycler, nil
}

// NewInstanceRingClient creates a client for the usage-trackers instance ring.
func NewInstanceRingClient(cfg InstanceRingConfig, logger log.Logger, reg prometheus.Registerer) (*ring.Ring, error) {
	client, err := ring.New(cfg.ToRingConfig(), InstanceRingName, InstanceRingKey, logger, prometheus.WrapRegistererWithPrefix("cortex_", reg))
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize usage-trackers' ring client")
	}

	return client, err
}
