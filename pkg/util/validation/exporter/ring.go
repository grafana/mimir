// SPDX-License-Identifier: AGPL-3.0-only

package exporter

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/exp/slices"

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

	// ringNumTokens is how many tokens each overrides-exporter should have in the
	// ring. Overrides-exporter uses timestamps to establish a ring leader, therefore
	// no tokens are needed.
	ringNumTokens = 0

	// ringAutoForgetUnhealthyPeriods is how many consecutive timeout periods an
	// unhealthy instance in the ring will be automatically removed after.
	ringAutoForgetUnhealthyPeriods = 4

	// leaderToken is the special token that an instance must own to be considered
	// the leader in the ring.
	leaderToken = 0
)

// ringOp is used as an instance state filter when obtaining instances from the
// ring. Instances in the LEAVING state are included to help minimise the number
// of leader changes during rollout and scaling operations. These instances will
// be forgotten after ringAutoForgetUnhealthyPeriods (see
// `KeepInstanceInTheRingOnShutdown`).
var ringOp = ring.NewOp([]ring.InstanceState{ring.ACTIVE, ring.LEAVING}, nil)

// RingConfig holds the configuration for the overrides-exporter ring.
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

// RegisterFlags configures this RingConfig to the given flag set and sets defaults.
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

// toBasicLifecyclerConfig transforms a RingConfig into configuration that can be used to create a BasicLifecycler.
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
		KeepInstanceInTheRingOnShutdown: true,
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
// establish a leader replica that is the unique exporter of per-tenant limit metrics.
type overridesExporterRing struct {
	services.Service

	config RingConfig

	client     *ring.Ring
	lifecycler *ring.BasicLifecycler

	subserviceManager *services.Manager
	subserviceWatcher *services.FailureWatcher
	logger            log.Logger
}

// isLeader checks whether this instance is the leader replica that exports metrics for all tenants.
func (r *overridesExporterRing) isLeader(at time.Time) (bool, error) {
	// if this instance registered less than ringAutoForgetUnhealthyPeriods *
	// HeartbeatTimeout ago, it is not eligible for ring leadership on the grounds
	// that not all instances might have become aware of it yet.
	if r.lifecycler.GetState() == ring.LEAVING {
		return false, nil
	}

	t, err := r.registeredAt()
	if err != nil {
		return false, err
	}
	if t.Add(time.Duration(ringAutoForgetUnhealthyPeriods) * r.config.HeartbeatTimeout).After(at) {
		return false, nil
	}
	return instanceIsLeader(r.client, r.lifecycler.GetInstanceAddr())
}

func (r *overridesExporterRing) registeredAt() (time.Time, error) {
	rs, err := r.client.GetAllHealthy(ringOp)
	if err != nil {
		return time.Time{}, err
	}
	for _, instance := range rs.Instances {
		if instance.Addr == r.lifecycler.GetInstanceAddr() {
			return instance.GetRegisteredAt(), nil
		}
	}
	return time.Time{}, errors.New("instance not found in ring")
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

	manager, err := services.NewManager(lifecycler, ringClient)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create service manager")
	}

	r := &overridesExporterRing{
		config:            config,
		client:            ringClient,
		lifecycler:        lifecycler,
		subserviceManager: manager,
		subserviceWatcher: services.NewFailureWatcher(),
		logger:            logger,
	}
	r.Service = services.NewBasicService(r.starting, r.running, r.stopping)
	return r, nil
}

// instanceIsLeader checks whether the instance at `instanceAddr` is the leader.
// A replica is considered the leader if it is the oldest replica within the
// batch of most recent replicas (batch window 5 minutes, hardcoded for now).
// This is done to increase stability of the leader during rollouts/scaling.
func instanceIsLeader(r ring.ReadRing, instanceAddr string) (bool, error) {
	rs, err := r.GetAllHealthy(ringOp)
	if err != nil {
		return false, err
	}
	if len(rs.Instances) == 1 {
		return true, nil
	}
	// Sort instances by registered timestamp (descending order).
	slices.SortStableFunc(rs.Instances, func(a ring.InstanceDesc, b ring.InstanceDesc) bool {
		return a.RegisteredTimestamp > b.RegisteredTimestamp
	})

	leader := rs.Instances[0]
	// Find the oldest of the new instances within the given lookback time window.
	lookback := 5 * time.Minute // TODO: this is just a random magic value for now, make this something that covers typical rollout/scaling periods.
	for _, instance := range rs.Instances[1:] {
		if time.Unix(rs.Instances[0].RegisteredTimestamp, 0).Add(-lookback).Before(time.Unix(instance.RegisteredTimestamp, 0)) {
			leader = instance
		} else {
			break
		}
	}

	return leader.Addr == instanceAddr, nil
}

func (r *overridesExporterRing) starting(ctx context.Context) error {
	r.subserviceWatcher.WatchManager(r.subserviceManager)
	if err := services.StartManagerAndAwaitHealthy(ctx, r.subserviceManager); err != nil {
		return errors.Wrap(err, "unable to start overrides-exporter ring subservice manager")
	}

	_ = level.Info(r.logger).Log("msg", "waiting until overrides-exporter is ACTIVE in the ring")
	if err := ring.WaitInstanceState(ctx, r.client, r.lifecycler.GetInstanceID(), ring.ACTIVE); err != nil {
		return errors.Wrap(err, "overrides-exporter failed to become ACTIVE in the ring")
	}
	_ = level.Info(r.logger).Log("msg", "overrides-exporter is ACTIVE in the r")

	return nil
}

func (r *overridesExporterRing) running(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-r.subserviceWatcher.Chan():
		return errors.Wrap(err, "a subservice of overrides-exporter ring has failed")
	}
}

func (r *overridesExporterRing) stopping(_ error) error {
	return errors.Wrap(
		services.StopManagerAndAwaitStopped(context.Background(), r.subserviceManager),
		"failed to stop overrides-exporter's ring subservice manager",
	)
}
