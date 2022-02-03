// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/auxiliary/cortexpb"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/util"
)

type Config struct {
	PoolConfig PoolConfig `yaml:"pool"`

	HATrackerConfig HATrackerConfig `yaml:"ha_tracker"`

	MaxRecvMsgSize  int           `yaml:"max_recv_msg_size"`
	RemoteTimeout   time.Duration `yaml:"remote_timeout"`
	ExtraQueryDelay time.Duration `yaml:"extra_queue_delay"`

	ShardingStrategy string `yaml:"sharding_strategy"`
	ShardByAllLabels bool   `yaml:"shard_by_all_labels"`
	ExtendWrites     bool   `yaml:"extend_writes"`

	// Distributors ring
	DistributorRing RingConfig `yaml:"ring"`

	// for testing and for extending the ingester by adding calls to the client

	// when true the distributor does not validate the label name, Cortex doesn't directly use
	// this (and should never use it) but this feature is used by other projects built on top of it

	// This config is dynamically injected because defined in the querier config.

	// Limits for distributor
	InstanceLimits InstanceLimits `yaml:"instance_limits"`
}

var (
	emptyPreallocSeries = cortexpb.PreallocTimeseries{}

	supportedShardingStrategies = []string{util.ShardingStrategyDefault, util.ShardingStrategyShuffle}

	// Validation errors.
	errInvalidShardingStrategy = errors.New("invalid sharding strategy")
	errInvalidTenantShardSize  = errors.New("invalid tenant shard size, the value must be greater than 0")

	// Distributor instance limits errors.
	errTooManyInflightPushRequests    = errors.New("too many inflight push requests in distributor")
	errMaxSamplesPushRateLimitReached = errors.New("distributor's samples push rate limit reached")
)

const (
	typeSamples  = "samples"
	typeMetadata = "metadata"

	instanceIngestionRateTickInterval = time.Second
)

const (
	instanceLimitsMetric     = "cortex_distributor_instance_limits"
	instanceLimitsMetricHelp = "Instance limits used by this distributor." // Must be same for all registrations.
	limitLabel               = "limit"
)

type InstanceLimits struct {
	MaxIngestionRate        float64 `yaml:"max_ingestion_rate"`
	MaxInflightPushRequests int     `yaml:"max_inflight_push_requests"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.PoolConfig.RegisterFlags(f)
	cfg.HATrackerConfig.RegisterFlags(f)
	cfg.DistributorRing.RegisterFlags(f)

	f.IntVar(&cfg.MaxRecvMsgSize, "distributor.max-recv-msg-size", 100<<20, "remote_write API max receive message size (bytes).")
	f.DurationVar(&cfg.RemoteTimeout, "distributor.remote-timeout", 2*time.Second, "Timeout for downstream ingesters.")
	f.DurationVar(&cfg.ExtraQueryDelay, "distributor.extra-query-delay", 0, "Time to wait before sending more than the minimum successful query requests.")
	f.BoolVar(&cfg.ShardByAllLabels, "distributor.shard-by-all-labels", false, "Distribute samples based on all labels, as opposed to solely by user and metric name.")
	f.StringVar(&cfg.ShardingStrategy, "distributor.sharding-strategy", util.ShardingStrategyDefault, fmt.Sprintf("The sharding strategy to use. Supported values are: %s.", strings.Join(supportedShardingStrategies, ", ")))
	f.BoolVar(&cfg.ExtendWrites, "distributor.extend-writes", true, "Try writing to an additional ingester in the presence of an ingester not in the ACTIVE state. It is useful to disable this along with -ingester.unregister-on-shutdown=false in order to not spread samples to extra ingesters during rolling restarts with consistent naming.")

	f.Float64Var(&cfg.InstanceLimits.MaxIngestionRate, "distributor.instance-limits.max-ingestion-rate", 0, "Max ingestion rate (samples/sec) that this distributor will accept. This limit is per-distributor, not per-tenant. Additional push requests will be rejected. Current ingestion rate is computed as exponentially weighted moving average, updated every second. 0 = unlimited.")
	f.IntVar(&cfg.InstanceLimits.MaxInflightPushRequests, "distributor.instance-limits.max-inflight-push-requests", 0, "Max inflight push requests that this distributor can handle. This limit is per-distributor, not per-tenant. Additional requests will be rejected. 0 = unlimited.")
}
