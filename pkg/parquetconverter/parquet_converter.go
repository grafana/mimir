// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/26344c3ec7409713fcf52a9c41cd0dce537b3100/pkg/compactor/parquet_compactor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package parquetconverter

import (
	"context"
	"flag"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/compactor"
	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	batchSize                = 50000
	batchStreamBufferSize    = 10
	parquetFileName          = "block.parquet"
	compactorRingKey         = "parquet-converter"
	maxParquetIndexSizeLimit = 100
)

type Config struct {
	EnabledTenants  flagext.StringSliceCSV `yaml:"enabled_tenants" category:"advanced"`
	DisabledTenants flagext.StringSliceCSV `yaml:"disabled_tenants" category:"advanced"`
	allowedTenants  *util.AllowList
}

// RegisterFlags registers the MultitenantCompactor flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	f.Var(&cfg.EnabledTenants, "parquet-converter.enabled-tenants", "Comma separated list of tenants that can have their TSDB blocks converted into parquet. If specified, only these tenants will be converted by the parquet-converter, otherwise all tenants can be compacted. Subject to sharding.")
	f.Var(&cfg.DisabledTenants, "parquet-converter.disabled-tenants", "Comma separated list of tenants that cannot have their TSDB blocks converted into parquet. If specified, and the parquet-converter would normally pick a given tenant to convert the blocks to parquet (via -parquet-converter.enabled-tenants or sharding), it will be ignored instead.")
}

type ParquetConverter struct {
	services.Service

	bucket objstore.InstrumentedBucket // TODO (jesus.vazquez) Compactor is using objstore.Bucket instead

	loader       *bucketindex.Loader
	Cfg          Config
	CompactorCfg compactor.Config
	registerer   prometheus.Registerer
	logger       log.Logger
	limits       *validation.Overrides
	blockRanges  []int64

	ringLifecycler         *ring.BasicLifecycler
	ring                   *ring.Ring
	ringSubservices        *services.Manager
	ringSubservicesWatcher *services.FailureWatcher
}

func NewParquetConverter(cfg Config, compactorCfg compactor.Config, storageCfg mimir_tsdb.BlocksStorageConfig, logger log.Logger, registerer prometheus.Registerer, limits *validation.Overrides) (*ParquetConverter, error) {
	bucketClient, err := bucket.NewClient(context.Background(), storageCfg.Bucket, "parquet-converter", logger, registerer)
	cfg.allowedTenants = util.NewAllowList(cfg.EnabledTenants, cfg.DisabledTenants)

	if err != nil {
		return nil, err
	}
	indexLoaderConfig := bucketindex.LoaderConfig{
		CheckInterval:         time.Minute,
		UpdateOnStaleInterval: storageCfg.BucketStore.SyncInterval,
		UpdateOnErrorInterval: storageCfg.BucketStore.BucketIndex.UpdateOnErrorInterval,
		IdleTimeout:           storageCfg.BucketStore.BucketIndex.IdleTimeout,
	}

	loader := bucketindex.NewLoader(indexLoaderConfig, bucketClient, limits, util_log.Logger, registerer)

	c := &ParquetConverter{
		Cfg:          cfg,
		CompactorCfg: compactorCfg,
		bucket:       bucketClient,
		loader:       loader,
		logger:       log.With(logger, "component", "parquet-converter"),
		registerer:   registerer,
		blockRanges:  compactorCfg.BlockRanges.ToMilliseconds(),
		limits:       limits,
	}

	c.Service = services.NewBasicService(c.starting, c.run, c.stopping)
	return c, nil
}

func (c *ParquetConverter) stopping(err error) error {
	return nil
}

func (c *ParquetConverter) starting(ctx context.Context) error {
	if true /*c.Cfg.ShardingEnabled*/ { // TODO
		kvStore, err := kv.NewClient(c.CompactorCfg.ShardingRing.Common.KVStore, ring.GetCodec(), kv.RegistererWithKVName(c.registerer, "parquet-converter-lifecycler"), c.logger)
		if err != nil {
			return errors.Wrap(err, "failed to initialize parquet-converter' KV store")
		}
		lifecyclerCfg, err := c.CompactorCfg.ShardingRing.ToBasicLifecyclerConfig(c.logger)
		if err != nil {
			return errors.Wrap(err, "unable to create parquet-converter ring lifecycler config")
		}
		var delegate ring.BasicLifecyclerDelegate
		delegate = ring.NewInstanceRegisterDelegate(ring.ACTIVE, lifecyclerCfg.NumTokens)
		delegate = ring.NewLeaveOnStoppingDelegate(delegate, c.logger)
		delegate = ring.NewAutoForgetDelegate(compactor.RingAutoForgetUnhealthyPeriods*lifecyclerCfg.HeartbeatTimeout, delegate, c.logger)

		c.ringLifecycler, err = ring.NewBasicLifecycler(lifecyclerCfg,
			"parquet-converter", compactorRingKey, kvStore, delegate, c.logger, prometheus.WrapRegistererWithPrefix("cortex_", c.registerer))
		if err != nil {
			return errors.Wrap(err, "unable to initialize parquet-converter ring lifecycler")
		}

		c.ring, err = ring.New(c.CompactorCfg.ShardingRing.ToRingConfig(), "parquet-converter", compactorRingKey, c.logger, prometheus.WrapRegistererWithPrefix("cortex_", c.registerer))
		if err != nil {
			return errors.Wrap(err, "unable to initialize parquet-converter ring")
		}

		c.ringSubservices, err = services.NewManager(c.ringLifecycler, c.ring)
		if err == nil {
			c.ringSubservicesWatcher = services.NewFailureWatcher()
			c.ringSubservicesWatcher.WatchManager(c.ringSubservices)

			if err = services.StartManagerAndAwaitHealthy(ctx, c.ringSubservices); err != nil {
				return errors.Wrap(err, "unable to start parquet-converter subservices")
			}
		}
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, c.CompactorCfg.ShardingRing.WaitActiveInstanceTimeout)
	defer cancel()
	if err := ring.WaitInstanceState(ctxWithTimeout, c.ring, c.ringLifecycler.GetInstanceID(), ring.ACTIVE); err != nil {
		level.Error(c.logger).Log("msg", "parquet-converter failed to become ACTIVE in the ring", "err", err)
		return err
	}
	level.Info(c.logger).Log("msg", "parquet-converter is ACTIVE in the ring")
	if err := c.loader.StartAsync(context.Background()); err != nil {
		return errors.Wrap(err, "failed to start loader")
	}

	if err := c.loader.AwaitRunning(ctx); err != nil {
		return errors.Wrap(err, "failed to start loader")
	}
	return nil
}

func (c *ParquetConverter) run(ctx context.Context) error {

	go func() {
		updateIndexTicker := time.NewTicker(time.Second * 60)
		for {
			select {
			case <-ctx.Done():
				return
			case <-updateIndexTicker.C:
			}
		}
	}()

	t := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

type Uploader interface {
	Upload(ctx context.Context, path string, r io.Reader) error
}

type InDiskUploader struct {
	Dir string
}

func (i *InDiskUploader) Upload(ctx context.Context, path string, r io.Reader) error {
	if err := os.MkdirAll(filepath.Join(i.Dir, filepath.Dir(path)), os.ModePerm); err != nil {
		return err
	}

	f, err := os.Create(filepath.Join(i.Dir, path))
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, r)
	return err
}
