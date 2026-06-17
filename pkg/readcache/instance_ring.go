// SPDX-License-Identifier: AGPL-3.0-only

package readcache

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
	// InstanceRingKey is the KV key under which readcache lifecyclers
	// register their entries. The rebalancer and the distributor read
	// from this key to discover the live readcache fleet.
	InstanceRingKey = "readcache"
	// InstanceRingName is the human-readable ring name used in metrics
	// and log lines.
	InstanceRingName = "readcache"
)

// InstanceRingConfig is the read-side service-discovery ring for
// readcache pods. Modelled on pkg/usagetracker/InstanceRingConfig:
// the readcache doesn't use the ring for hash-based routing
// (partition ownership is supplied by the nautilus rebalancer log),
// only as the canonical list of live instances and their addresses.
type InstanceRingConfig struct {
	KVStore                    kv.Config     `yaml:"kvstore" doc:"description=The key-value store used to share the readcache hash ring across multiple instances. Required for the rebalancer's slicer and the distributor's read routing to discover readcache pods."`
	HeartbeatPeriod            time.Duration `yaml:"heartbeat_period" category:"advanced"`
	HeartbeatTimeout           time.Duration `yaml:"heartbeat_timeout" category:"advanced"`
	AutoForgetUnhealthyPeriods int           `yaml:"auto_forget_unhealthy_periods" category:"advanced"`

	// Instance details. Defaults mirror the ingester ring so a single
	// network-interface auto-detection rule covers both.
	InstanceID             string   `yaml:"instance_id" doc:"default=<hostname>" category:"advanced"`
	InstanceInterfaceNames []string `yaml:"instance_interface_names" doc:"default=[<private network interfaces>]"`
	InstancePort           int      `yaml:"instance_port" category:"advanced"`
	InstanceAddr           string   `yaml:"instance_addr" category:"advanced"`
	InstanceZone           string   `yaml:"instance_availability_zone"`
	EnableIPv6             bool     `yaml:"instance_enable_ipv6" category:"advanced"`

	// ListenPort is injected by pkg/mimir from -server.grpc-listen-port;
	// not user-settable via YAML.
	ListenPort int `yaml:"-"`
}

// RegisterFlags adds the readcache instance-ring flags on f.
func (cfg *InstanceRingConfig) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	hostname, err := os.Hostname()
	if err != nil {
		level.Error(logger).Log("msg", "failed to get hostname", "err", err)
		os.Exit(1)
	}

	// Ring flags. Default to memberlist to match the rest of the
	// nautilus stack; this is what the dev cell and Helm chart use.
	cfg.KVStore.Store = "memberlist"
	cfg.KVStore.RegisterFlagsWithPrefix("readcache.instance-ring.", "collectors/", f)
	f.DurationVar(&cfg.HeartbeatPeriod, "readcache.instance-ring.heartbeat-period", 15*time.Second, "Period at which to heartbeat to the readcache ring.")
	f.DurationVar(&cfg.HeartbeatTimeout, "readcache.instance-ring.heartbeat-timeout", time.Minute, "The heartbeat timeout after which readcache instances are considered unhealthy within the ring. Both the rebalancer and the distributor use this to filter the readcache instance set.")
	f.IntVar(&cfg.AutoForgetUnhealthyPeriods, "readcache.auto-forget-unhealthy-periods", 4, "Number of consecutive heartbeat-timeout periods after which Mimir automatically removes an unhealthy readcache instance from the ring. Set to 0 to disable auto-forget.")

	// Instance flags.
	cfg.InstanceInterfaceNames = netutil.PrivateNetworkInterfacesWithFallback([]string{"eth0", "en0"}, logger)
	f.Var((*flagext.StringSlice)(&cfg.InstanceInterfaceNames), "readcache.instance-ring.instance-interface-names", "List of network interface names to look up when finding the readcache instance IP address.")
	f.StringVar(&cfg.InstanceAddr, "readcache.instance-ring.instance-addr", "", "IP address to advertise in the ring. Default is auto-detected.")
	f.IntVar(&cfg.InstancePort, "readcache.instance-ring.instance-port", 0, "Port to advertise in the ring (defaults to -server.grpc-listen-port).")
	f.StringVar(&cfg.InstanceID, "readcache.instance-ring.instance-id", hostname, "Instance ID to register in the ring. Must match -readcache.instance-id.")
	f.StringVar(&cfg.InstanceZone, "readcache.instance-ring.instance-availability-zone", "", "The availability zone where this instance is running.")
	f.BoolVar(&cfg.EnableIPv6, "readcache.instance-ring.instance-enable-ipv6", false, "Enable using an IPv6 instance address. (default false)")
}

// ToBasicLifecyclerConfig returns a ring.BasicLifecyclerConfig based
// on the readcache instance-ring config. Modelled on the usagetracker
// equivalent but adapted for readcache's needs:
//
//   - KeepInstanceInTheRingOnShutdown is true so that a brief restart
//     doesn't trigger a slicer reshuffle. Operators that want clean
//     scale-down should call the prepare-downscale endpoint first.
//   - NumTokens is 1 because the ring is service discovery only;
//     partition ownership is supplied by the nautilus rebalancer log.
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
		NumTokens:                       1,
		KeepInstanceInTheRingOnShutdown: true,
	}, nil
}

// ToRingConfig returns a ring.Config used for read-only discovery
// (rebalancer + distributor side).
func (cfg *InstanceRingConfig) ToRingConfig() ring.Config {
	rc := ring.Config{}
	flagext.DefaultValues(&rc)

	rc.KVStore = cfg.KVStore
	rc.HeartbeatTimeout = cfg.HeartbeatTimeout
	// Replication factor is irrelevant: callers don't use the ring
	// for hash-based routing, they enumerate healthy members. Pin to
	// 1 so the ring's internal accounting stays sane.
	rc.ReplicationFactor = 1
	rc.SubringCacheDisabled = true
	return rc
}

// NewInstanceRingLifecycler constructs the lifecycler the readcache
// pod runs to register itself in the readcache ring.
func NewInstanceRingLifecycler(cfg InstanceRingConfig, logger log.Logger, reg prometheus.Registerer) (*ring.BasicLifecycler, error) {
	reg = prometheus.WrapRegistererWithPrefix("cortex_", reg)
	kvStore, err := kv.NewClient(cfg.KVStore, ring.GetCodec(), kv.RegistererWithKVName(reg, "readcache-lifecycler"), logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize readcache KV store")
	}

	lifecyclerCfg, err := cfg.ToBasicLifecyclerConfig(logger)
	if err != nil {
		return nil, errors.Wrap(err, "failed to build readcache lifecycler config")
	}

	// Register straight into ACTIVE: the rebalancer's slicer is the
	// only authority on what partitions a readcache pod consumes, and
	// the per-partition warm flag inside the readcache already gates
	// premature reads. Going through a JOINING state would just add
	// latency to scale-up without adding correctness.
	var delegate ring.BasicLifecyclerDelegate = ring.NewInstanceRegisterDelegate(ring.ACTIVE, 1)
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, logger)
	if cfg.AutoForgetUnhealthyPeriods > 0 {
		delegate = ring.NewAutoForgetDelegate(time.Duration(cfg.AutoForgetUnhealthyPeriods)*cfg.HeartbeatTimeout, delegate, logger)
	}

	lifecycler, err := ring.NewBasicLifecycler(lifecyclerCfg, InstanceRingName, InstanceRingKey, kvStore, delegate, logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize readcache lifecycler")
	}
	return lifecycler, nil
}

// NewInstanceRingClient constructs a read-only ring client used by
// the rebalancer (to discover the readcache instance set the slicer
// may assign partitions to) and the distributor (to resolve readcache
// instance IDs to dial-able addresses).
func NewInstanceRingClient(cfg InstanceRingConfig, logger log.Logger, reg prometheus.Registerer) (*ring.Ring, error) {
	client, err := ring.New(cfg.ToRingConfig(), InstanceRingName, InstanceRingKey, logger, prometheus.WrapRegistererWithPrefix("cortex_", reg))
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize readcache ring client")
	}
	return client, nil
}

// ReadcacheRingOp is the dskit ring operation readers pass to
// GetAllHealthy / GetReplicationSetForOperation. Since the readcache
// ring isn't used for hash-based routing, every healthy instance is
// considered eligible for every operation; we just need an op that
// trips the standard "ACTIVE + recently heartbeated" filter.
var ReadcacheRingOp = ring.NewOp([]ring.InstanceState{ring.ACTIVE}, nil)
