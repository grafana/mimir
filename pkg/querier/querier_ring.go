// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"flag"
	"fmt"
	"math"
	"net"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	// ringNumTokens is how many tokens each querier should have in the ring.
	// Queriers use a ring because they need to share the maximum query plan version
	// they support with query-frontends.
	ringNumTokens = 1

	querierRingKey = "querier"

	MaximumSupportedQueryPlanVersion = 1
)

var statusPageConfig = ring.StatusPageConfig{
	HideTokensUIElements: true,
	ShowVersions:         true,
	ComponentNames: map[uint64]string{
		MaximumSupportedQueryPlanVersion: "maximum supported query plan version",
	},
}

// RingConfig strips the ring lifecycler configuration down to the minimum required for the querier ring.
// The ring lifecycler config contains many options not needed by the querier ring.
type RingConfig struct {
	Common util.CommonRingConfig `yaml:",inline"`

	AutoForgetUnhealthyPeriods int `yaml:"auto_forget_unhealthy_periods" category:"advanced"`
}

func (cfg *RingConfig) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	cfg.Common.RegisterFlags("querier.ring.", "collectors/", "queriers", f, logger)
	f.IntVar(&cfg.AutoForgetUnhealthyPeriods, "querier.ring.auto-forget-unhealthy-periods", 10, "Number of consecutive timeout periods after which Mimir automatically removes an unhealthy instance in the ring. Set to 0 to disable auto-forget.")
}

func (cfg *RingConfig) ToBasicLifecyclerConfig(logger log.Logger) (ring.BasicLifecyclerConfig, error) {
	instanceAddr, err := ring.GetInstanceAddr(cfg.Common.InstanceAddr, cfg.Common.InstanceInterfaceNames, logger, cfg.Common.EnableIPv6)
	if err != nil {
		return ring.BasicLifecyclerConfig{}, err
	}

	instancePort := ring.GetInstancePort(cfg.Common.InstancePort, cfg.Common.ListenPort)

	return ring.BasicLifecyclerConfig{
		ID:                              cfg.Common.InstanceID,
		Addr:                            net.JoinHostPort(instanceAddr, strconv.Itoa(instancePort)),
		HeartbeatPeriod:                 cfg.Common.HeartbeatPeriod,
		HeartbeatTimeout:                cfg.Common.HeartbeatTimeout,
		TokensObservePeriod:             0,
		NumTokens:                       ringNumTokens,
		KeepInstanceInTheRingOnShutdown: false,
		StatusPageConfig:                statusPageConfig,
		Versions: ring.InstanceVersions{
			MaximumSupportedQueryPlanVersion: uint64(planning.MaximumSupportedQueryPlanVersion),
		},
	}, nil
}

func (cfg *RingConfig) toRingConfig() ring.Config {
	rc := cfg.Common.ToRingConfig()
	rc.ReplicationFactor = 1
	rc.StatusPageConfig = statusPageConfig

	return rc
}

func NewLifecycler(cfg RingConfig, logger log.Logger, reg prometheus.Registerer) (*ring.BasicLifecycler, error) {
	reg = prometheus.WrapRegistererWithPrefix("cortex_", reg)
	kvStore, err := kv.NewClient(cfg.Common.KVStore, ring.GetCodec(), kv.RegistererWithKVName(reg, "querier-lifecycler"), logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize querier KV store: %w", err)
	}

	lifecyclerCfg, err := cfg.ToBasicLifecyclerConfig(logger)
	if err != nil {
		return nil, fmt.Errorf("failed to build querier lifecycler config: %w", err)
	}

	var delegate ring.BasicLifecyclerDelegate
	delegate = ring.NewInstanceRegisterDelegate(ring.ACTIVE, lifecyclerCfg.NumTokens)
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, logger)
	if cfg.AutoForgetUnhealthyPeriods > 0 {
		delegate = ring.NewAutoForgetDelegate(time.Duration(cfg.AutoForgetUnhealthyPeriods)*cfg.Common.HeartbeatTimeout, delegate, logger)
	}

	lifecycler, err := ring.NewBasicLifecycler(lifecyclerCfg, "querier", querierRingKey, kvStore, delegate, logger, reg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize querier lifecycler: %w", err)
	}

	promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "querier_maximum_supported_query_plan_version",
		Help: "The maximum supported query plan version this process was compiled to support.",
	}).Set(float64(planning.MaximumSupportedQueryPlanVersion))

	return lifecycler, nil
}

func NewRing(cfg RingConfig, logger log.Logger, reg prometheus.Registerer) (*ring.Ring, error) {
	reg = prometheus.WrapRegistererWithPrefix("cortex_", reg)
	r, err := ring.New(cfg.toRingConfig(), "querier", querierRingKey, logger, reg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize querier ring client: %w", err)
	}

	return r, nil
}

type RingQueryPlanVersionProvider struct {
	ring   ring.ReadRing
	logger log.Logger
}

func NewRingQueryPlanVersionProvider(ring ring.ReadRing, reg prometheus.Registerer, logger log.Logger) streamingpromql.QueryPlanVersionProvider {
	provider := &RingQueryPlanVersionProvider{
		ring:   ring,
		logger: logger,
	}

	// The metrics below are only expected to be exposed by query-frontends, hence the cortex_query_frontend_ prefix.
	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_query_frontend_querier_ring_calculated_maximum_supported_query_plan_version",
		Help: "The maximum supported query plan version calculated from the querier ring.",
	}, func() float64 {
		version, err := provider.GetMaximumSupportedQueryPlanVersion(context.Background())
		if err != nil {
			level.Warn(logger).Log("msg", "failed to compute maximum supported query plan version", "err", err)
			return -1
		}

		return float64(version)
	})

	promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_query_frontend_querier_ring_expected_maximum_supported_query_plan_version",
		Help: "The maximum supported query plan version this process was compiled to support.",
	}).Set(float64(planning.MaximumSupportedQueryPlanVersion))

	return provider
}

var queryPlanVersioningOp = ring.NewOp([]ring.InstanceState{ring.ACTIVE, ring.PENDING, ring.JOINING, ring.LEAVING}, nil)
var errQuerierHasNoSupportedQueryPlanVersion = fmt.Errorf("at least one querier in the ring is not reporting a supported query plan version")

func (r *RingQueryPlanVersionProvider) GetMaximumSupportedQueryPlanVersion(ctx context.Context) (planning.QueryPlanVersion, error) {
	logger := spanlogger.FromContext(ctx, r.logger)

	instances, err := r.ring.GetAllHealthy(queryPlanVersioningOp)
	if err != nil {
		return 0, fmt.Errorf("could not compute maximum supported query plan version: could not get all queriers from the ring: %w", err)
	}

	lowestVersionSeen := uint64(math.MaxUint64)

	for _, instance := range instances.Instances {
		version, ok := instance.Versions[MaximumSupportedQueryPlanVersion]
		if !ok {
			level.Warn(logger).Log(
				"msg", "could not compute maximum supported query plan version because at least one querier is not reporting a supported query plan version",
				"instance", instance.Addr,
				"instance_state", instance.State,
				"instance_last_heartbeat", time.Unix(instance.Timestamp, 0).UTC(),
			)

			return 0, fmt.Errorf("could not compute maximum supported query plan version: %w", errQuerierHasNoSupportedQueryPlanVersion)
		}

		lowestVersionSeen = min(lowestVersionSeen, version)
	}

	return planning.QueryPlanVersion(lowestVersionSeen), nil
}
