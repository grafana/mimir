// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/26344c3ec7409713fcf52a9c41cd0dce537b3100/pkg/compactor/parquet_compactor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package parquetconverter

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/runutil"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	ringKey = "parquet-converter"

	// ringAutoForgetUnhealthyPeriods is how many consecutive timeout periods an unhealthy instance
	// in the ring will be automatically removed after.
	ringAutoForgetUnhealthyPeriods = 10
)

var (
	RingOp = ring.NewOp([]ring.InstanceState{ring.ACTIVE}, nil)
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

	SortingLabels      flagext.StringSliceCSV `yaml:"sorting_labels" category:"advanced"`
	ColDuration        time.Duration          `yaml:"col_duration" category:"advanced"`
	MaxRowsPerGroup    int                    `yaml:"max_rows_per_group" category:"advanced"`
	MinCompactionLevel int                    `yaml:"min_compaction_level" category:"advanced"`

	CompressionEnabled bool `yaml:"compression_enabled" category:"advanced"`
	MaxQueueSize       int  `yaml:"max_queue_size" category:"advanced"`

	ShardingRing RingConfig `yaml:"sharding_ring"`
}

// RegisterFlags registers the ParquetConverter flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	cfg.ShardingRing.RegisterFlags(f, logger)
	f.Var(&cfg.EnabledTenants, "parquet-converter.enabled-tenants", "Comma-separated list of tenants that can have their TSDB blocks converted into Parquet. If specified, the Parquet-converter only converts these tenants. Otherwise, it converts all tenants. Subject to sharding.")
	f.Var(&cfg.DisabledTenants, "parquet-converter.disabled-tenants", "Comma-separated list of tenants that cannot have their TSDB blocks converted into Parquet. If specified, and the Parquet-converter would normally pick a given tenant to convert the blocks to Parquet (via -parquet-converter.enabled-tenants or sharding), it is ignored instead.")
	f.DurationVar(&cfg.ConversionInterval, "parquet-converter.conversion-interval", time.Minute, "The frequency at which the conversion runs.")
	f.DurationVar(&cfg.DiscoveryInterval, "parquet-converter.discovery-interval", 5*time.Minute, "The frequency at which user and block discovery runs.")
	f.DurationVar(&cfg.MaxBlockAge, "parquet-converter.max-block-age", 0, "Maximum age of blocks to convert. Blocks older than this will be skipped. Set to 0 to disable age filtering.")
	f.StringVar(&cfg.DataDir, "parquet-converter.data-dir", "./data-parquet-converter/", "Directory to temporarily store blocks during conversion. This directory is not required to persist between restarts.")
	f.Var(&cfg.SortingLabels, "parquet-converter.sorting-labels", "Comma-separated list of labels to sort by when converting to Parquet format. If not the file will be sorted by '__name__'.")
	f.DurationVar(&cfg.ColDuration, "parquet-converter.col-duration", 8*time.Hour, "Duration for column chunks in Parquet files.")
	f.IntVar(&cfg.MaxRowsPerGroup, "parquet-converter.max-rows-per-group", 1e6, "Maximum number of rows per row group in Parquet files.")
	f.IntVar(&cfg.MinCompactionLevel, "parquet-converter.min-compaction-level", 2, "Minimum compaction level required for blocks to be converted to Parquet. Blocks equal or greater than this level will be converted.")
	f.BoolVar(&cfg.CompressionEnabled, "parquet-converter.compression-enabled", true, "Whether compression is enabled for labels and chunks parquet files. When disabled, parquet files will be converted and stored uncompressed.")
	f.IntVar(&cfg.MaxQueueSize, "parquet-converter.max-queue-size", 5, "Maximum number of blocks that can be queued for conversion at once. This helps distribute work evenly across replicas.")
}

type ParquetConverter struct {
	services.Service

	Cfg                 Config
	registerer          prometheus.Registerer
	logger              log.Logger
	limits              *validation.Overrides
	bucketClientFactory func(ctx context.Context) (objstore.Bucket, error)

	bucketClient objstore.Bucket

	ringLifecycler         *ring.BasicLifecycler
	ring                   *ring.Ring
	ringSubservices        *services.Manager
	ringSubservicesWatcher *services.FailureWatcher

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
	return newParquetConverter(cfg, logger, registerer, bucketClientFactory, limits, defaultBlockConverter{})
}
func newParquetConverter(
	cfg Config,
	logger log.Logger,
	registerer prometheus.Registerer,
	bucketClientFactory func(ctx context.Context) (objstore.Bucket, error),
	limits *validation.Overrides,
	blockConverter blockConverter,
) (*ParquetConverter, error) {
	cfg.allowedTenants = util.NewAllowList(cfg.EnabledTenants, cfg.DisabledTenants)

	c := &ParquetConverter{
		Cfg:                  cfg,
		bucketClientFactory:  bucketClientFactory,
		logger:               log.With(logger, "component", "parquet-converter"),
		registerer:           registerer,
		limits:               limits,
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

	// Initialize the parquet-converters ring if sharding is enabled.
	c.ring, c.ringLifecycler, err = newRingAndLifecycler(c.Cfg.ShardingRing, c.logger, c.registerer)
	if err != nil {
		return err
	}

	c.ringSubservices, err = services.NewManager(c.ringLifecycler, c.ring)
	if err != nil {
		return errors.Wrap(err, "unable to create parquet-converter ring dependencies")
	}

	c.ringSubservicesWatcher = services.NewFailureWatcher()
	c.ringSubservicesWatcher.WatchManager(c.ringSubservices)
	if err = c.ringSubservices.StartAsync(ctx); err != nil {
		return errors.Wrap(err, "unable to start parquet-converter ring dependencies")
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, c.Cfg.ShardingRing.WaitActiveInstanceTimeout)
	defer cancel()
	if err = c.ringSubservices.AwaitHealthy(ctxTimeout); err != nil {
		return errors.Wrap(err, "unable to start parquet-converter ring dependencies")
	}

	// If sharding is enabled we should wait until this instance is ACTIVE within the ring. This
	// MUST be done before starting any other component depending on the users scanner, because
	// the users scanner depends on the ring (to check whether a user belongs to this shard or not).
	level.Info(c.logger).Log("msg", "waiting until parquet-converter is ACTIVE in the ring")
	if err = ring.WaitInstanceState(ctxTimeout, c.ring, c.ringLifecycler.GetInstanceID(), ring.ACTIVE); err != nil {
		return errors.Wrap(err, "parquet-converter failed to become ACTIVE in the ring")
	}

	level.Info(c.logger).Log("msg", "parquet-converter is ACTIVE in the ring")

	// In the event of a cluster cold start or scale up of 2+ parquet-converter instances at the same
	// time, we may end up in a situation where each new parquet-converter instance starts at a slightly
	// different time and thus each one starts with a different state of the ring. It's better
	// to just wait a short time for ring stability.
	if c.Cfg.ShardingRing.WaitStabilityMinDuration > 0 {
		minWaiting := c.Cfg.ShardingRing.WaitStabilityMinDuration
		maxWaiting := c.Cfg.ShardingRing.WaitStabilityMaxDuration

		level.Info(c.logger).Log("msg", "waiting until parquet-converter ring topology is stable", "min_waiting", minWaiting.String(), "max_waiting", maxWaiting.String())
		if err := ring.WaitRingStability(ctx, c.ring, RingOp, minWaiting, maxWaiting); err != nil {
			level.Warn(c.logger).Log("msg", "parquet-converter ring topology is not stable after the max waiting time, proceeding anyway")
		} else {
			level.Info(c.logger).Log("msg", "parquet-converter ring topology is stable")
		}
	}

	return nil
}

func newRingAndLifecycler(cfg RingConfig, logger log.Logger, reg prometheus.Registerer) (*ring.Ring, *ring.BasicLifecycler, error) {
	reg = prometheus.WrapRegistererWithPrefix("cortex_", reg)
	kvStore, err := kv.NewClient(cfg.Common.KVStore, ring.GetCodec(), kv.RegistererWithKVName(reg, "parquet-converter-lifecycler"), logger)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialize parquet-converters' KV store")
	}

	lifecyclerCfg, err := cfg.ToBasicLifecyclerConfig(logger)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to build parquet-converters' lifecycler config")
	}

	var delegate ring.BasicLifecyclerDelegate
	delegate = ring.NewInstanceRegisterDelegate(ring.ACTIVE, lifecyclerCfg.NumTokens)
	delegate = ring.NewLeaveOnStoppingDelegate(delegate, logger)
	delegate = ring.NewAutoForgetDelegate(ringAutoForgetUnhealthyPeriods*lifecyclerCfg.HeartbeatTimeout, delegate, logger)

	parquetConvertersLifecycler, err := ring.NewBasicLifecycler(lifecyclerCfg, "parquet-converter", ringKey, kvStore, delegate, logger, reg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialize parquet-converter' lifecycler")
	}

	parquetConvertersRing, err := ring.New(cfg.toRingConfig(), "parquet-converter", ringKey, logger, reg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialize parquet-converter' parquetConvertersRing client")
	}

	return parquetConvertersRing, parquetConvertersLifecycler, nil
}

func (c *ParquetConverter) running(ctx context.Context) error {
	go c.runBlockDiscovery(ctx)
	go c.runBlockProcessing(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-c.ringSubservicesWatcher.Chan():
			return errors.Wrap(err, "parquet-converter ring subservice failed")
		}
	}
}

// runBlockDiscovery discovers blocks and enqueues them for processing
func (c *ParquetConverter) runBlockDiscovery(ctx context.Context) {
	discoveryTicker := time.NewTicker(c.Cfg.DiscoveryInterval)
	defer discoveryTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-discoveryTicker.C:
			c.discoverAndEnqueueBlocks(ctx)
		}
	}
}

// runBlockProcessing processes blocks from the priority queue
func (c *ParquetConverter) runBlockProcessing(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			task, ok := c.conversionQueue.Pop()
			if !ok {
				select {
				case <-ctx.Done():
					return
				case <-time.After(1 * time.Second):
					continue
				}
			}

			c.metrics.queueSize.Set(float64(c.conversionQueue.Size()))
			c.metrics.queueItemsProcessed.WithLabelValues(task.UserID).Inc()

			waitTime := time.Since(task.EnqueuedAt)
			c.metrics.queueWaitTime.WithLabelValues(task.UserID).Observe(waitTime.Seconds())

			ulogger := util_log.WithUserID(task.UserID, c.logger)
			c.processBlock(ctx, task.UserID, task.Meta, task.Bucket, ulogger)
		}
	}
}

// discoverAndEnqueueBlocks discovers blocks and adds them to the priority queue
func (c *ParquetConverter) discoverAndEnqueueBlocks(ctx context.Context) {
	if c.conversionQueue.Size() >= c.Cfg.MaxQueueSize {
		return
	}

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

		for _, m := range metas {
			if ctx.Err() != nil {
				return
			}
			level.Debug(c.logger).Log("msg", "syncing block", "id", m.ULID, "meta", m)

			if m.Compaction.Level < c.Cfg.MinCompactionLevel {
				level.Debug(c.logger).Log("msg", "skipping block below min compaction level", "id", m.ULID, "meta", m)
				continue
			}

			ok, err := c.ownBlock(m.ULID.String())
			if err != nil {
				level.Error(ulogger).Log("msg", "error checking if block is owned", "err", err)
				continue
			}
			if !ok {
				continue
			}

			if c.Cfg.MaxBlockAge > 0 {
				blockAge := time.Since(time.UnixMilli(m.MinTime))
				if blockAge > c.Cfg.MaxBlockAge {
					continue
				}
			}

			if _, alreadyQueued := c.queuedBlocks.Load(m.ULID); alreadyQueued {
				continue
			}

			mark, err := ReadConversionMark(ctx, m.ULID, uBucket, ulogger)
			if err != nil {
				level.Debug(ulogger).Log("msg", "failed to read conversion mark, assuming not converted", "block", m.ULID.String(), "err", err)
			} else if mark.Version == CurrentVersion {
				continue
			}

			task := newConversionTask(u, m, uBucket)

			if c.conversionQueue.Push(task) {
				c.metrics.queueItemsEnqueued.WithLabelValues(u).Inc()
				// Mark block as queued to prevent duplicate queuing
				c.queuedBlocks.Store(m.ULID, time.Now())

				ulidTime := time.UnixMilli(int64(m.ULID.Time()))
				level.Info(ulogger).Log(
					"msg", "enqueued block for conversion",
					"block", m.ULID.String(),
					"ulid_timestamp_ms", m.ULID.Time(),
					"ulid_time_human", ulidTime.UTC().Format(time.RFC3339),
					"queue_size", c.conversionQueue.Size(),
				)
			} else {
				c.metrics.queueItemsDropped.WithLabelValues(u).Inc()
				level.Warn(ulogger).Log("msg", "failed to enqueue block, queue closed", "block", m.ULID.String())
			}

			c.metrics.queueSize.Set(float64(c.conversionQueue.Size()))
		}
	}

}

// processBlock handles the conversion of a single block with proper metrics tracking.
func (c *ParquetConverter) processBlock(ctx context.Context, userID string, meta *block.Meta, uBucket objstore.InstrumentedBucket, logger log.Logger) {
	start := time.Now()
	var err error
	var skipped bool
	defer func() {
		// Remove block from queued blocks map when processing completes (success or failure)
		c.queuedBlocks.Delete(meta.ULID)

		duration := time.Since(start)
		c.metrics.conversionDuration.WithLabelValues(userID).Observe(duration.Seconds())
		if err != nil {
			c.metrics.blocksConvertedFailed.WithLabelValues(userID).Inc()
		} else if !skipped {
			c.metrics.blocksConverted.WithLabelValues(userID).Inc()
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
	ctx := context.Background()

	c.conversionQueue.Close()

	if c.ringSubservices != nil {
		return services.StopManagerAndAwaitStopped(ctx, c.ringSubservices)
	}
	return nil
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

func (c *ParquetConverter) ownBlock(id string) (bool, error) {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(id))
	rs, err := c.ring.Get(hasher.Sum32(), RingOp, nil, nil, nil)
	if err != nil {
		return false, err
	}

	if len(rs.Instances) != 1 {
		return false, fmt.Errorf("unexpected number of parquet-converter in the shard (expected 1, got %d)", len(rs.Instances))
	}

	return rs.Instances[0].Addr == c.ringLifecycler.GetInstanceAddr(), nil
}
