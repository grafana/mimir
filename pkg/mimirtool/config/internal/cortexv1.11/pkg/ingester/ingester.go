// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"flag"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/ring"
)

type Config struct {
	WALConfig        WALConfig             `yaml:"walconfig" doc:"description=Configures the Write-Ahead Log (WAL) for the Cortex chunks storage. This config is ignored when running the Cortex blocks storage."`
	LifecyclerConfig ring.LifecyclerConfig `yaml:"lifecycler"`

	// Config for transferring chunks. Zero or negative = no retries.
	MaxTransferRetries int `yaml:"max_transfer_retries"`

	// Config for chunk flushing.
	FlushCheckPeriod  time.Duration `yaml:"flush_period"`
	RetainPeriod      time.Duration `yaml:"retain_period"`
	MaxChunkIdle      time.Duration `yaml:"max_chunk_idle_time"`
	MaxStaleChunkIdle time.Duration `yaml:"max_stale_chunk_idle_time"`
	FlushOpTimeout    time.Duration `yaml:"flush_op_timeout"`
	MaxChunkAge       time.Duration `yaml:"max_chunk_age"`
	ChunkAgeJitter    time.Duration `yaml:"chunk_age_jitter"`
	ConcurrentFlushes int           `yaml:"concurrent_flushes"`
	SpreadFlushes     bool          `yaml:"spread_flushes"`

	// Config for metadata purging.
	MetadataRetainPeriod time.Duration `yaml:"metadata_retain_period"`

	RateUpdatePeriod time.Duration `yaml:"rate_update_period"`

	ActiveSeriesMetricsEnabled      bool          `yaml:"active_series_metrics_enabled"`
	ActiveSeriesMetricsUpdatePeriod time.Duration `yaml:"active_series_metrics_update_period"`
	ActiveSeriesMetricsIdleTimeout  time.Duration `yaml:"active_series_metrics_idle_timeout"`

	// Use blocks storage.
	StreamChunksWhenUsingBlocks bool `yaml:"-"`
	// Runtime-override for type of streaming query to use (chunks or samples).

	// Injected at runtime and read from the distributor config, required
	// to accurately apply global limits.

	DefaultLimits InstanceLimits `yaml:"instance_limits"`

	IgnoreSeriesLimitForMetricNames string `yaml:"ignore_series_limit_for_metric_names"`

	// For testing, you can override the address and ID of this ingester.
}

const (
	// Number of timeseries to return in each batch of a QueryStream.
	queryStreamBatchSize = 128

	// Discarded Metadata metric labels.
	perUserMetadataLimit   = "per_user_metadata_limit"
	perMetricMetadataLimit = "per_metric_metadata_limit"

	// Period at which to attempt purging metadata from memory.
	metadataPurgePeriod = 5 * time.Minute
)

var (
	// This is initialised if the WAL is enabled and the records are fetched from this pool.
	recordPool sync.Pool

	errIngesterStopping = errors.New("ingester stopping")
)

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.LifecyclerConfig.RegisterFlags(f)
	cfg.WALConfig.RegisterFlags(f)

	f.IntVar(&cfg.MaxTransferRetries, "ingester.max-transfer-retries", 10, "Number of times to try and transfer chunks before falling back to flushing. Negative value or zero disables hand-over. This feature is supported only by the chunks storage.")

	f.DurationVar(&cfg.FlushCheckPeriod, "ingester.flush-period", 1*time.Minute, "Period with which to attempt to flush chunks.")
	f.DurationVar(&cfg.RetainPeriod, "ingester.retain-period", 5*time.Minute, "Period chunks will remain in memory after flushing.")
	f.DurationVar(&cfg.MaxChunkIdle, "ingester.max-chunk-idle", 5*time.Minute, "Maximum chunk idle time before flushing.")
	f.DurationVar(&cfg.MaxStaleChunkIdle, "ingester.max-stale-chunk-idle", 2*time.Minute, "Maximum chunk idle time for chunks terminating in stale markers before flushing. 0 disables it and a stale series is not flushed until the max-chunk-idle timeout is reached.")
	f.DurationVar(&cfg.FlushOpTimeout, "ingester.flush-op-timeout", 1*time.Minute, "Timeout for individual flush operations.")
	f.DurationVar(&cfg.MaxChunkAge, "ingester.max-chunk-age", 12*time.Hour, "Maximum chunk age before flushing.")
	f.DurationVar(&cfg.ChunkAgeJitter, "ingester.chunk-age-jitter", 0, "Range of time to subtract from -ingester.max-chunk-age to spread out flushes")
	f.IntVar(&cfg.ConcurrentFlushes, "ingester.concurrent-flushes", 50, "Number of concurrent goroutines flushing to dynamodb.")
	f.BoolVar(&cfg.SpreadFlushes, "ingester.spread-flushes", true, "If true, spread series flushes across the whole period of -ingester.max-chunk-age.")

	f.DurationVar(&cfg.MetadataRetainPeriod, "ingester.metadata-retain-period", 10*time.Minute, "Period at which metadata we have not seen will remain in memory before being deleted.")

	f.DurationVar(&cfg.RateUpdatePeriod, "ingester.rate-update-period", 15*time.Second, "Period with which to update the per-user ingestion rates.")
	f.BoolVar(&cfg.ActiveSeriesMetricsEnabled, "ingester.active-series-metrics-enabled", true, "Enable tracking of active series and export them as metrics.")
	f.DurationVar(&cfg.ActiveSeriesMetricsUpdatePeriod, "ingester.active-series-metrics-update-period", 1*time.Minute, "How often to update active series metrics.")
	f.DurationVar(&cfg.ActiveSeriesMetricsIdleTimeout, "ingester.active-series-metrics-idle-timeout", 10*time.Minute, "After what time a series is considered to be inactive.")
	f.BoolVar(&cfg.StreamChunksWhenUsingBlocks, "ingester.stream-chunks-when-using-blocks", false, "Stream chunks when using blocks. This is experimental feature and not yet tested. Once ready, it will be made default and this config option removed.")

	f.Float64Var(&cfg.DefaultLimits.MaxIngestionRate, "ingester.instance-limits.max-ingestion-rate", 0, "Max ingestion rate (samples/sec) that ingester will accept. This limit is per-ingester, not per-tenant. Additional push requests will be rejected. Current ingestion rate is computed as exponentially weighted moving average, updated every second. This limit only works when using blocks engine. 0 = unlimited.")
	f.Int64Var(&cfg.DefaultLimits.MaxInMemoryTenants, "ingester.instance-limits.max-tenants", 0, "Max users that this ingester can hold. Requests from additional users will be rejected. This limit only works when using blocks engine. 0 = unlimited.")
	f.Int64Var(&cfg.DefaultLimits.MaxInMemorySeries, "ingester.instance-limits.max-series", 0, "Max series that this ingester can hold (across all tenants). Requests to create additional series will be rejected. This limit only works when using blocks engine. 0 = unlimited.")
	f.Int64Var(&cfg.DefaultLimits.MaxInflightPushRequests, "ingester.instance-limits.max-inflight-push-requests", 0, "Max inflight push requests that this ingester can handle (across all tenants). Additional requests will be rejected. 0 = unlimited.")

	f.StringVar(&cfg.IgnoreSeriesLimitForMetricNames, "ingester.ignore-series-limit-for-metric-names", "", "Comma-separated list of metric names, for which -ingester.max-series-per-metric and -ingester.max-global-series-per-metric limits will be ignored. Does not affect max-series-per-user or max-global-series-per-metric limits.")
}
