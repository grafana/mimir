// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/26344c3ec7409713fcf52a9c41cd0dce537b3100/pkg/compactor/parquet_compactor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package parquetconverter

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/runutil"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

// blockConverter defines the interface for converting blocks to parquet format.
type blockConverter interface {
	ConvertBlock(ctx context.Context, meta *block.Meta, localBlockDir string, bkt objstore.Bucket, logger log.Logger, opts []convert.ConvertOption) error
}

type defaultBlockConverter struct{}

func (d defaultBlockConverter) ConvertBlock(ctx context.Context, meta *block.Meta, localBlockDir string, bkt objstore.Bucket, logger log.Logger, opts []convert.ConvertOption) error {
	tsdbBlock, err := tsdb.OpenBlock(
		util_log.SlogFromGoKit(logger), localBlockDir, nil, tsdb.DefaultPostingsDecoderFactory,
	)
	if err != nil {
		return err
	}
	defer runutil.CloseWithErrCapture(&err, tsdbBlock, "close tsdb block")

	_, err = convert.ConvertTSDBBlock(
		ctx,
		bkt,
		meta.MinTime,
		meta.MaxTime,
		[]convert.Convertible{tsdbBlock},
		opts...,
	)
	return err
}

type Config struct {
	EnabledTenants  flagext.StringSliceCSV `yaml:"enabled_tenants" category:"advanced"`
	DisabledTenants flagext.StringSliceCSV `yaml:"disabled_tenants" category:"advanced"`
	allowedTenants  *util.AllowList

	DataDir string `yaml:"data_dir"`

	ConversionInterval time.Duration `yaml:"conversion_interval"`
	DiscoveryInterval  time.Duration `yaml:"discovery_interval"`
	MaxBlockAge        time.Duration `yaml:"max_block_age"`
	MinBlockTimestamp  uint64        `yaml:"min_block_timestamp"`
	MinDataAge         time.Duration `yaml:"min_data_age" category:"advanced"`

	SortingLabels      flagext.StringSliceCSV `yaml:"sorting_labels" category:"advanced"`
	ColDuration        time.Duration          `yaml:"col_duration" category:"advanced"`
	MaxRowsPerGroup    int                    `yaml:"max_rows_per_group" category:"advanced"`
	MinCompactionLevel int                    `yaml:"min_compaction_level" category:"advanced"`
	MinBlockDuration   time.Duration          `yaml:"min_block_duration" category:"advanced"`

	CompressionEnabled bool `yaml:"compression_enabled" category:"advanced"`

	LoadBalancingStrategy string              `yaml:"load_balancing_strategy"`
	ShardingRing          RingConfig          `yaml:"sharding_ring"`
	MemcachedLock         cache.BackendConfig `yaml:"locking"`
}

// RegisterFlags registers the ParquetConverter flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	f.StringVar(&cfg.LoadBalancingStrategy, "parquet-converter.load-balancing-strategy", "ring", "The strategy to use to balance blocks to convert across multiple parquet-converter instances. Supported values: ring, locking.")
	cfg.ShardingRing.RegisterFlags(f, logger)
	f.StringVar(&cfg.MemcachedLock.Backend, "parquet-converter.locking.backend", "", "Backend for parquet-converter locking cache (used when load-balancing-strategy is 'locking'). Supported values: memcached.")
	cfg.MemcachedLock.Memcached.RegisterFlagsWithPrefix("parquet-converter.locking.memcached.", f)
	f.Var(&cfg.EnabledTenants, "parquet-converter.enabled-tenants", "Comma-separated list of tenants that can have their TSDB blocks converted into Parquet. If specified, the Parquet-converter only converts these tenants. Otherwise, it converts all tenants. Subject to sharding.")
	f.Var(&cfg.DisabledTenants, "parquet-converter.disabled-tenants", "Comma-separated list of tenants that cannot have their TSDB blocks converted into Parquet. If specified, and the Parquet-converter would normally pick a given tenant to convert the blocks to Parquet (via -parquet-converter.enabled-tenants or sharding), it is ignored instead.")
	f.DurationVar(&cfg.ConversionInterval, "parquet-converter.conversion-interval", time.Minute, "The frequency at which the conversion runs.")
	f.DurationVar(&cfg.DiscoveryInterval, "parquet-converter.discovery-interval", 5*time.Minute, "The frequency at which user and block discovery runs.")
	f.DurationVar(&cfg.MaxBlockAge, "parquet-converter.max-block-age", 0, "Maximum age of blocks to convert. Blocks older than this will be skipped. Set to 0 to disable age filtering.")
	f.Uint64Var(&cfg.MinBlockTimestamp, "parquet-converter.min-block-timestamp", 0, "Minimum block timestamp (based on ULID) to convert. Set to 0 to disable timestamp filtering.")
	f.DurationVar(&cfg.MinDataAge, "parquet-converter.min-data-age", 0, "Minimum age of data in blocks to convert. Only convert blocks containing data older than this duration from now, based on their MinTime. Set to 0 to disable age filtering.")
	f.StringVar(&cfg.DataDir, "parquet-converter.data-dir", "./data-parquet-converter/", "Directory to temporarily store blocks during conversion. This directory is not required to persist between restarts.")
	f.Var(&cfg.SortingLabels, "parquet-converter.sorting-labels", "Comma-separated list of labels to sort by when converting to Parquet format. If not the file will be sorted by '__name__'.")
	f.DurationVar(&cfg.ColDuration, "parquet-converter.col-duration", 8*time.Hour, "Duration for column chunks in Parquet files.")
	f.IntVar(&cfg.MaxRowsPerGroup, "parquet-converter.max-rows-per-group", 1e6, "Maximum number of rows per row group in Parquet files.")
	f.IntVar(&cfg.MinCompactionLevel, "parquet-converter.min-compaction-level", 2, "Minimum compaction level required for blocks to be converted to Parquet. Blocks equal or greater than this level will be converted.")
	f.DurationVar(&cfg.MinBlockDuration, "parquet-converter.min-block-duration", 0, "Minimum duration of blocks to convert. Blocks with a duration shorter than this will be skipped. Set to 0 to disable duration filtering.")
	f.BoolVar(&cfg.CompressionEnabled, "parquet-converter.compression-enabled", true, "Whether compression is enabled for labels and chunks parquet files. When disabled, parquet files will be converted and stored uncompressed.")
}

type ParquetConverter struct {
	services.Service

	Cfg                 Config
	registerer          prometheus.Registerer
	logger              log.Logger
	limits              *validation.Overrides
	bucketClientFactory func(ctx context.Context) (objstore.Bucket, error)

	bucketClient        objstore.Bucket
	loadBalancer        loadBalancer
	loadBalancerWatcher *services.FailureWatcher

	blockConverter       blockConverter
	baseConverterOptions []convert.ConvertOption
	metrics              parquetConverterMetrics

	conversionQueue *priorityQueue

	queuedBlocks sync.Map // map[ulid.ULID]time.Time - persistent cache of blocks that have been queued
}

func buildBaseConverterOptions(cfg Config) []convert.ConvertOption {
	var baseConverterOptions []convert.ConvertOption

	if len(cfg.SortingLabels) > 0 {
		baseConverterOptions = append(baseConverterOptions, convert.WithSortBy(cfg.SortingLabels...))
	} else {
		baseConverterOptions = append(baseConverterOptions, convert.WithSortBy(labels.MetricName))
	}

	if cfg.ColDuration > 0 {
		baseConverterOptions = append(baseConverterOptions, convert.WithColDuration(cfg.ColDuration))
	}

	if cfg.MaxRowsPerGroup > 0 {
		baseConverterOptions = append(baseConverterOptions, convert.WithRowGroupSize(cfg.MaxRowsPerGroup))
	}

	baseConverterOptions = append(baseConverterOptions, convert.WithCompression(schema.WithCompressionEnabled(cfg.CompressionEnabled)))

	return baseConverterOptions
}

func NewParquetConverter(cfg Config, storageCfg mimir_tsdb.BlocksStorageConfig, logger log.Logger, registerer prometheus.Registerer, limits *validation.Overrides) (*ParquetConverter, error) {
	bucketClientFactory := func(ctx context.Context) (objstore.Bucket, error) {
		return bucket.NewClient(ctx, storageCfg.Bucket, "parquet-converter", logger, registerer)
	}

	var loadBalancer loadBalancer
	switch cfg.LoadBalancingStrategy {
	case "ring":
		ringLB, err := newRingLoadBalancer(cfg.ShardingRing, logger, registerer)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create ring load balancer")
		}
		loadBalancer = ringLB
	case "locking":
		if cfg.MemcachedLock.Backend != "memcached" {
			return nil, errors.New("locking backend must be 'memcached'")
		}
		lockingCache, err := cache.CreateClient("parquet-converter-lock", cfg.MemcachedLock, logger, registerer)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create locking cache client")
		}
		loadBalancer = newCacheLockLoadBalancer(lockingCache)
	default:
		return nil, fmt.Errorf("unsupported load balancing strategy: %s (supported: ring, locking)", cfg.LoadBalancingStrategy)
	}

	return newParquetConverter(cfg, logger, registerer, bucketClientFactory, limits, defaultBlockConverter{}, loadBalancer)
}
func newParquetConverter(
	cfg Config,
	logger log.Logger,
	registerer prometheus.Registerer,
	bucketClientFactory func(ctx context.Context) (objstore.Bucket, error),
	limits *validation.Overrides,
	blockConverter blockConverter,
	loadBalancer loadBalancer,
) (*ParquetConverter, error) {
	cfg.allowedTenants = util.NewAllowList(cfg.EnabledTenants, cfg.DisabledTenants)

	c := &ParquetConverter{
		Cfg:                  cfg,
		bucketClientFactory:  bucketClientFactory,
		logger:               log.With(logger, "component", "parquet-converter"),
		registerer:           registerer,
		limits:               limits,
		loadBalancer:         loadBalancer,
		blockConverter:       blockConverter,
		baseConverterOptions: buildBaseConverterOptions(cfg),
		metrics:              newParquetConverterMetrics(registerer),
		conversionQueue:      newPriorityQueue(),
	}

	c.Service = services.NewBasicService(c.starting, c.running, c.stopping).WithName("parquet-converter")
	return c, nil
}

func (c *ParquetConverter) starting(ctx context.Context) error {
	var err error
	c.bucketClient, err = c.bucketClientFactory(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create bucket client")
	}

	c.loadBalancerWatcher = services.NewFailureWatcher()
	c.loadBalancerWatcher.WatchService(c.loadBalancer)

	if err := services.StartAndAwaitRunning(ctx, c.loadBalancer); err != nil {
		return errors.Wrap(err, "unable to start load balancer")
	}

	level.Info(c.logger).Log("msg", "parquet-converter started", "strategy", c.Cfg.LoadBalancingStrategy)
	return nil
}

func (c *ParquetConverter) running(ctx context.Context) error {
	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer func() {
			if r := recover(); r != nil {
				level.Error(c.logger).Log("msg", "parquet-converter discovery goroutine panicked", "panic", r)
				panic(r) // Re-panic to force service restart
			}
		}()

		c.discoverAndEnqueueBlocks(gCtx)

		discoveryTicker := time.NewTicker(c.Cfg.DiscoveryInterval)
		defer discoveryTicker.Stop()

		for {
			select {
			case <-gCtx.Done():
				return nil
			case <-discoveryTicker.C:
				func() {
					defer func() {
						if r := recover(); r != nil {
							level.Error(c.logger).Log("msg", "parquet-converter discoverAndEnqueueBlocks panicked", "panic", r)
						}
					}()
					c.discoverAndEnqueueBlocks(gCtx)
				}()
			}
		}
	})

	g.Go(func() error {
		defer func() {
			if r := recover(); r != nil {
				level.Error(c.logger).Log("msg", "parquet-converter processing goroutine panicked", "panic", r)
				panic(r) // Re-panic to force service restart
			}
		}()

		for {
			select {
			case <-gCtx.Done():
				return nil
			default:
				task, ok := c.conversionQueue.Pop()
				if !ok {
					select {
					case <-gCtx.Done():
						return nil
					case <-time.After(1 * time.Second):
						continue
					}
				}

				func() {
					defer func() {
						if r := recover(); r != nil {
							level.Error(c.logger).Log("msg", "parquet-converter processBlock panicked", "panic", r, "block", task.Meta.ULID.String())
						}
					}()

					c.metrics.queueSize.WithLabelValues(c.Cfg.LoadBalancingStrategy).Set(float64(c.conversionQueue.Size()))
					c.metrics.queueItemsProcessed.WithLabelValues(task.UserID).Inc()

					waitTime := time.Since(task.EnqueuedAt)
					c.metrics.queueWaitTime.WithLabelValues(task.UserID).Observe(waitTime.Seconds())

					ulogger := util_log.WithUserID(task.UserID, c.logger)
					c.processBlock(gCtx, task.UserID, task.Meta, task.Bucket, ulogger)
				}()
			}
		}
	})

	for {
		select {
		case <-ctx.Done():
			return g.Wait()
		case err := <-c.loadBalancerWatcher.Chan():
			return errors.Wrap(err, "parquet-converter load balancer failed")
		case <-gCtx.Done():
			return g.Wait()
		}
	}
}

// discoverAndEnqueueBlocks discovers blocks and adds them to the priority queue
func (c *ParquetConverter) discoverAndEnqueueBlocks(ctx context.Context) {
	level.Info(c.logger).Log("msg", "starting block discovery")
	users, err := c.discoverUsers(ctx)
	if err != nil {
		level.Error(c.logger).Log("msg", "error scanning users", "err", err)
		return
	}

	for _, u := range users {
		if ctx.Err() != nil {
			return
		}

		ulogger := util_log.WithUserID(u, c.logger)
		uBucket := bucket.NewUserBucketClient(u, c.bucketClient, c.limits)

		fetcher, err := block.NewMetaFetcher(
			ulogger,
			20,
			uBucket,
			c.metaSyncDirForUser(u),
			prometheus.NewRegistry(), // TODO: we are discarding metrics for now
			[]block.MetadataFilter{},
			nil, // TODO: No Meta cache for now
			0,   // No lookback limit, we take all blocks
		)
		if err != nil {
			level.Error(ulogger).Log("msg", "error creating meta fetcher", "err", err)
			continue
		}

		metas, _, err := fetcher.FetchWithoutMarkedForDeletion(ctx)
		if err != nil {
			level.Error(ulogger).Log("msg", "error fetching metas", "err", err)
			continue
		}
		level.Info(ulogger).Log("msg", "discovered blocks", "count", len(metas))

		for _, m := range metas {
			if ctx.Err() != nil {
				return
			}
			blogger := log.With(ulogger, "block", m.ULID.String(), "ulid_timestamp_ms", m.ULID.Time())

			skipReason, err := c.shouldProcessBlock(ctx, m, uBucket)
			if err != nil {
				level.Error(blogger).Log("msg", "error checking if block should be processed", "err", err)
				continue
			}
			if skipReason != "" {
				level.Debug(blogger).Log("msg", "skipping block", "reason", skipReason)
				continue
			}

			level.Debug(blogger).Log("msg", "block selected for processing")

			task := newConversionTask(u, m, uBucket)

			if c.conversionQueue.Push(task) {
				c.metrics.queueItemsEnqueued.WithLabelValues(u).Inc()
				c.queuedBlocks.Store(m.ULID, time.Now())

				level.Info(blogger).Log(
					"msg", "enqueued block for conversion",
					"block", m.ULID.String(),
					"queue_size", c.conversionQueue.Size(),
				)
			} else {
				c.metrics.queueItemsDropped.WithLabelValues(u).Inc()
				level.Warn(ulogger).Log("msg", "failed to enqueue block, queue closed", "block", m.ULID.String())
			}

			c.metrics.queueSize.WithLabelValues(c.Cfg.LoadBalancingStrategy).Set(float64(c.conversionQueue.Size()))
		}
	}

}

// processBlock handles the conversion of a single block with proper metrics tracking.
func (c *ParquetConverter) processBlock(ctx context.Context, userID string, meta *block.Meta, uBucket objstore.InstrumentedBucket, logger log.Logger) {
	start := time.Now()
	var err error
	var skipped bool
	defer func() {
		c.queuedBlocks.Delete(meta.ULID)

		duration := time.Since(start)
		if err != nil {
			c.metrics.blocksConvertedFailed.WithLabelValues(userID).Inc()
		} else if !skipped {
			c.metrics.blocksConverted.WithLabelValues(userID).Inc()
			c.metrics.conversionDuration.WithLabelValues(userID).Observe(duration.Seconds())
		}
	}()

	var ok bool
	ok, err = c.loadBalancer.lock(ctx, meta.ULID.String())
	if err != nil {
		level.Error(logger).Log("msg", "failed to acquire block processing rights", "block", meta.ULID.String(), "err", err)
		return
	}
	if !ok {
		skipped = true
		level.Debug(logger).Log("msg", "skipped block, already being processed by another instance", "block", meta.ULID.String())
		return
	}
	defer func() {
		if err := c.loadBalancer.unlock(context.Background(), meta.ULID.String()); err != nil {
			level.Warn(logger).Log("msg", "failed to notify block processing completion", "block", meta.ULID.String(), "err", err)
		}
	}()

	var mark *ConversionMark
	mark, err = ReadConversionMark(ctx, meta.ULID, uBucket, logger)
	if err != nil {
		level.Error(logger).Log("msg", "failed to read conversion mark, skipping", "err", err, "block", meta.ULID.String())
		return
	}

	if mark.Version == CurrentVersion {
		skipped = true
		ulidTime := time.UnixMilli(int64(meta.ULID.Time()))
		level.Info(logger).Log(
			"msg", "skipped block, already converted",
			"block", meta.ULID.String(),
			"ulid_timestamp_ms", meta.ULID.Time(),
			"ulid_time_human", ulidTime.UTC().Format(time.RFC3339),
		)
		return
	}

	if err := os.RemoveAll(c.rootDir()); err != nil {
		level.Error(logger).Log("msg", "failed to remove work directory", "path", c.rootDir(), "err", err)
	}

	localBlockDir := filepath.Join(c.dirForUser(userID), meta.ULID.String())
	level.Info(logger).Log("msg", "downloading block", "block", meta.ULID.String(), "maxTime", meta.MaxTime)
	if err = block.Download(ctx, logger, uBucket, meta.ULID, localBlockDir, objstore.WithFetchConcurrency(10)); err != nil {
		level.Error(logger).Log("msg", "error downloading block", "err", err)
		return
	}

	convertOpts := make([]convert.ConvertOption, len(c.baseConverterOptions)+1)
	copy(convertOpts, c.baseConverterOptions)
	convertOpts[len(c.baseConverterOptions)] = convert.WithName(meta.ULID.String())

	err = c.blockConverter.ConvertBlock(ctx, meta, localBlockDir, uBucket, logger, convertOpts)
	if err != nil {
		level.Error(logger).Log("msg", "failed to convert block", "block", meta.ULID.String(), "err", err)
		return
	}

	ulidTime := time.UnixMilli(int64(meta.ULID.Time()))
	level.Info(logger).Log(
		"msg", "converted block",
		"block", meta.ULID.String(),
		"ulid_timestamp_ms", meta.ULID.Time(),
		"ulid_time_human", ulidTime.UTC().Format(time.RFC3339),
		"duration_seconds", time.Since(start).Seconds(),
		"block_size_bytes", meta.BlockBytes(),
	)

	err = WriteConversionMark(ctx, meta.ULID, uBucket)
	if err != nil {
		level.Error(logger).Log("msg", "failed to write conversion mark", "block", meta.ULID.String(), "err", err)
	}
}

func (c *ParquetConverter) stopping(_ error) error {
	c.conversionQueue.Close()

	return services.StopAndAwaitTerminated(context.Background(), c.loadBalancer)
}

// discoverUsers scans the bucket for users and returns a list of user IDs that are allowed to be converted.
func (c *ParquetConverter) discoverUsers(ctx context.Context) ([]string, error) {
	var users []string

	err := c.bucketClient.Iter(ctx, "", func(entry string) error {
		u := strings.TrimSuffix(entry, "/")
		users = append(users, u)
		return nil
	})

	if err != nil {
		return nil, err
	}

	filtered := make([]string, 0, len(users))
	for _, user := range users {
		if c.Cfg.allowedTenants.IsAllowed(user) {
			filtered = append(filtered, user)
		}
	}

	c.metrics.tenantsDiscovered.Set(float64(len(filtered)))

	return filtered, nil
}

// dirForUser returns the directory to be used to download and convert the blocks for a user
func (c *ParquetConverter) dirForUser(userID string) string {
	return filepath.Join(c.rootDir(), userID)
}

func (c *ParquetConverter) rootDir() string {
	return filepath.Join(c.Cfg.DataDir, "convert")
}

const metaPrefix = "parq-conv-meta-"

// metaSyncDirForUser returns directory to store cached meta files. The fetcher
// stores cached metas in the "meta-syncer/" sub directory, but we prefix it
// with "parq-conv-meta-" in order to guarantee no clashing with the
// directory used by the Thanos Syncer or Compactor, whatever is the user ID.
func (c *ParquetConverter) metaSyncDirForUser(userID string) string {
	return filepath.Join(c.Cfg.DataDir, metaPrefix+userID)

}

// shouldProcessBlock determines whether a block should be processed for conversion to parquet.
// It returns a reason string if the block should NOT be processed, an empty string means should process.
func (c *ParquetConverter) shouldProcessBlock(ctx context.Context, meta *block.Meta, uBucket objstore.InstrumentedBucket) (string, error) {
	owned, err := c.loadBalancer.shouldEnqueue(ctx, meta.ULID.String())
	if err != nil {
		return "", fmt.Errorf("error checking block ownership: %w", err)
	}
	if !owned {
		return "not owned", nil
	}
	if _, alreadyQueued := c.queuedBlocks.Load(meta.ULID); alreadyQueued {
		return "already queued", nil
	}

	if meta.Compaction.Level < c.Cfg.MinCompactionLevel {
		return fmt.Sprintf("compaction level %d below minimum %d", meta.Compaction.Level, c.Cfg.MinCompactionLevel), nil
	}

	if c.Cfg.MinBlockTimestamp > 0 && meta.ULID.Time() < c.Cfg.MinBlockTimestamp {
		return fmt.Sprintf("ULID timestamp %d below minimum %d", meta.ULID.Time(), c.Cfg.MinBlockTimestamp), nil
	}

	if blockAge := time.Since(time.UnixMilli(meta.MinTime)); c.Cfg.MaxBlockAge > 0 && blockAge > c.Cfg.MaxBlockAge {
		return fmt.Sprintf("block age %s exceeds maximum %s", blockAge.String(), c.Cfg.MaxBlockAge.String()), nil
	}

	if dataAge := time.Since(time.UnixMilli(meta.MinTime)); c.Cfg.MinDataAge > 0 && dataAge < c.Cfg.MinDataAge {
		return fmt.Sprintf("data age %s below minimum %s", dataAge.String(), c.Cfg.MinDataAge.String()), nil
	}

	if c.Cfg.MinBlockDuration > 0 {
		blockDurationMs := meta.MaxTime - meta.MinTime
		minDurationMs := c.Cfg.MinBlockDuration.Milliseconds()
		if blockDurationMs < minDurationMs {
			return fmt.Sprintf("block duration %d below minimum %d", blockDurationMs, minDurationMs), nil
		}
	}

	mark, err := ReadConversionMark(ctx, meta.ULID, uBucket, nil)
	if err != nil {
		return "", fmt.Errorf("error reading conversion mark: %w", err)
	} else if mark.Version == CurrentVersion {
		return "conversion mark found", nil
	}

	return "", nil
}
