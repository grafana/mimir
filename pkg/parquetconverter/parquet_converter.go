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
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/runutil"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/parquet/convert"
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
	ConvertBlock(ctx context.Context, meta *block.Meta, localBlockDir string, bkt objstore.Bucket, logger log.Logger) error
}

type defaultBlockConverter struct{}

func (d defaultBlockConverter) ConvertBlock(ctx context.Context, meta *block.Meta, localBlockDir string, bkt objstore.Bucket, logger log.Logger) error {
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
		convert.WithName(meta.ULID.String()),
	)
	return err
}

type Config struct {
	EnabledTenants  flagext.StringSliceCSV `yaml:"enabled_tenants" category:"advanced"`
	DisabledTenants flagext.StringSliceCSV `yaml:"disabled_tenants" category:"advanced"`
	allowedTenants  *util.AllowList

	DataDir string `yaml:"data_dir"`

	ConversionInterval time.Duration `yaml:"conversion_interval"`

	ShardingRing RingConfig `yaml:"sharding_ring"`
}

// RegisterFlags registers the ParquetConverter flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {

	cfg.ShardingRing.RegisterFlags(f, logger)
	f.Var(&cfg.EnabledTenants, "parquet-converter.enabled-tenants", "Comma-separated list of tenants that can have their TSDB blocks converted into Parquet. If specified, the Parquet-converter only converts these tenants. Otherwise, it converts all tenants. Subject to sharding.")
	f.Var(&cfg.DisabledTenants, "parquet-converter.disabled-tenants", "Comma-separated list of tenants that cannot have their TSDB blocks converted into Parquet. If specified, and the Parquet-converter would normally pick a given tenant to convert the blocks to Parquet (via -parquet-converter.enabled-tenants or sharding), it is ignored instead.")
	f.DurationVar(&cfg.ConversionInterval, "parquet-converter.conversion-interval", time.Minute, "The frequency at which the conversion runs.")
	f.StringVar(&cfg.DataDir, "parquet-converter.data-dir", "./data-parquet-converter/", "Directory to temporarily store blocks during conversion. This directory is not required to persist between restarts.")
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

	blockConverter blockConverter
	metrics        parquetConverterMetrics
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
		Cfg:                 cfg,
		bucketClientFactory: bucketClientFactory,
		logger:              log.With(logger, "component", "parquet-converter"),
		registerer:          registerer,
		limits:              limits,
		blockConverter:      blockConverter,
		metrics:             newParquetConverterMetrics(registerer),
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
	convertBlocksTicker := time.NewTicker(c.Cfg.ConversionInterval)
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-c.ringSubservicesWatcher.Chan():
			return errors.Wrap(err, "parquet-converter ring subservice failed")

		case <-convertBlocksTicker.C:
			users, err := c.discoverUsers(ctx)
			if err != nil {
				level.Error(c.logger).Log("msg", "error scanning users", "err", err)
				return err
			}

			for _, u := range users {
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

				metas, _, err := fetcher.Fetch(ctx)
				if err != nil {
					level.Error(ulogger).Log("msg", "error fetching metas", "err", err)
					continue
				}

				for _, m := range metas {
					if ctx.Err() != nil {
						return ctx.Err()
					}

					ok, err := c.ownBlock(m.ULID.String())
					if err != nil {
						level.Error(ulogger).Log("msg", "error checking if block is owned", "err", err)
						continue
					}
					if !ok {
						continue
					}

					c.processBlock(ctx, u, m, uBucket, ulogger)
				}
			}
		}
	}
}

// processBlock handles the conversion of a single block with proper metrics tracking.
func (c *ParquetConverter) processBlock(ctx context.Context, userID string, meta *block.Meta, uBucket objstore.InstrumentedBucket, logger log.Logger) {
	start := time.Now()
	var err error
	var skipped bool
	defer func() {
		duration := time.Since(start)
		c.metrics.conversionDuration.Observe(duration.Seconds())
		if err != nil {
			c.metrics.blocksConvertedFailed.Inc()
		} else if !skipped {
			c.metrics.blocksConverted.Inc()
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

	err = c.blockConverter.ConvertBlock(ctx, meta, localBlockDir, uBucket, logger)
	if err != nil {
		level.Error(logger).Log("msg", "failed to convert block", "block", meta.ULID.String(), "err", err)
		return
	}

	level.Info(logger).Log("msg", "converted block", "block", meta.ULID.String())

	err = WriteConversionMark(ctx, meta.ULID, uBucket)
	if err != nil {
		level.Error(logger).Log("msg", "failed to write conversion mark", "block", meta.ULID.String(), "err", err)
	}
}

func (c *ParquetConverter) stopping(_ error) error {
	ctx := context.Background()

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
