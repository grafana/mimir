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
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/compactor"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/parquet"
	"github.com/grafana/mimir/pkg/storage/parquet/tsdbcodec"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
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
	registerer = prometheus.WrapRegistererWithPrefix("parquet_", registerer)
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

			err = services.StartManagerAndAwaitHealthy(ctx, c.ringSubservices)
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
				users, err := c.discoverUsers(ctx)
				if err != nil {
					level.Error(util_log.Logger).Log("msg", "Error scanning users", "err", err)
					break
				}
				for _, u := range users {
					if c.Cfg.allowedTenants.IsAllowed(u) {
						if ok, _ := c.ownBlock(u); ok {
							err := c.updateParquetIndex(ctx, u)
							if err != nil {
								level.Error(util_log.Logger).Log("msg", "Error updating index", "err", err)
							}
						}
					}
				}
			}
		}
	}()

	t := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			u, err := c.discoverUsers(ctx)
			if err != nil {
				level.Error(util_log.Logger).Log("msg", "Error scanning users", "err", err)
				return err
			}

			for _, u := range u {
				if !c.Cfg.allowedTenants.IsAllowed(u) {
					continue
				}
				uBucket := bucket.NewUserBucketClient(u, c.bucket, c.limits)

				pIdx, err := bucketindex.ReadParquetIndex(ctx, uBucket, util_log.Logger)
				if err != nil {
					level.Error(util_log.Logger).Log("msg", "Error loading index", "err", err)
					break
				}

				for {
					if ctx.Err() != nil {
						return ctx.Err()
					}

					level.Info(util_log.Logger).Log("msg", "Scanning User", "user", u)

					idx, err := c.loader.GetIndex(ctx, u)
					if err != nil {
						level.Error(util_log.Logger).Log("msg", "Error loading index", "err", err)
						break
					}

					level.Info(util_log.Logger).Log("msg", "Loaded index", "user", u, "totalBlocks", len(idx.Blocks), "deleteBlocks", len(idx.BlockDeletionMarks))

					level.Info(util_log.Logger).Log("msg", "Loaded Parquet index", "user", u, "totalBlocks", len(pIdx.Blocks))

					ownedBlocks, totalBlocks := c.findNextBlockToCompact(ctx, uBucket, idx, pIdx)
					if len(ownedBlocks) == 0 {
						level.Info(util_log.Logger).Log("msg", "No owned blocks to compact", "numBlocks", len(pIdx.Blocks), "totalBlocks", totalBlocks)
						break
					}

					b := ownedBlocks[0]

					if err := os.RemoveAll(c.compactRootDir()); err != nil {
						level.Error(util_log.Logger).Log("msg", "failed to remove compaction work directory", "path", c.compactRootDir(), "err", err)
					}
					bdir := filepath.Join(c.compactDirForUser(u), b.ID.String())
					level.Info(util_log.Logger).Log("msg", "Downloading block", "block", b.ID.String(), "maxTime", b.MaxTime, "dir", bdir, "ownedBlocks", len(ownedBlocks), "totalBlocks", totalBlocks)
					if err := block.Download(ctx, util_log.Logger, uBucket, b.ID, bdir, objstore.WithFetchConcurrency(10)); err != nil {
						level.Error(util_log.Logger).Log("msg", "Error downloading block", "err", err)
						continue
					}
					err = TSDBBlockToParquet(ctx, b.ID, uBucket, bdir, c.logger)
					if err != nil {
						level.Error(util_log.Logger).Log("msg", "failed to import block", "block", b.String(), "err", err)
					}
					// Add the blocks
					pIdx.Blocks[b.ID] = bucketindex.BlockWithExtension{Block: b}
				}
			}
		}
	}
}

func (c *ParquetConverter) updateParquetIndex(ctx context.Context, u string) error {
	level.Info(util_log.Logger).Log("msg", "Updating index", "user", u)
	uBucket := bucket.NewUserBucketClient(u, c.bucket, c.limits)
	deleted := map[ulid.ULID]struct{}{}
	idx, err := c.loader.GetIndex(ctx, u)

	if err != nil {
		return err
	}

	for _, b := range idx.BlockDeletionMarks {
		deleted[b.ID] = struct{}{}
	}

	pIdx, err := bucketindex.ReadParquetIndex(ctx, uBucket, util_log.Logger)
	if err != nil {
		return errors.Wrap(err, "failed to read parquet index")
	}

	for _, b := range idx.Blocks {
		if _, ok := deleted[b.ID]; ok {
			continue
		}

		if _, ok := pIdx.Blocks[b.ID]; ok {
			continue
		}

		marker, err := ReadCompactMark(ctx, b.ID, uBucket, c.logger)
		if err != nil {
			level.Error(util_log.Logger).Log("msg", "failed to check if file exists", "err", err)
			continue
		}

		if marker.Version == CurrentVersion {
			// m, err := block.DownloadMeta(ctx, util_log.Logger, uBucket, b.ID)
			// if err != nil {
			// 	level.Error(util_log.Logger).Log("msg", "failed to download block", "block", b.ID, "err", err)
			// }
			extensions := bucketindex.Extensions{}
			// TODO
			// _, err = metadata.ConvertExtensions(m.Thanos.Extensions, &extensions)
			// if err != nil {
			// 	level.Error(util_log.Logger).Log("msg", "failed to convert extensions", "err", err)
			// }
			pIdx.Blocks[b.ID] = bucketindex.BlockWithExtension{Block: b, Extensions: extensions}
		}
	}
	c.removeDeletedBlocks(idx, pIdx)
	//// Remove block from bucket index if marker version is outdated.
	//c.removeOutdatedBlocks(ctx, uBucket, pIdx)
	return bucketindex.WriteParquetIndex(ctx, uBucket, pIdx)
}

func (c *ParquetConverter) removeDeletedBlocks(idx *bucketindex.Index, pIdx *bucketindex.ParquetIndex) {
	blocks := map[ulid.ULID]struct{}{}
	deleted := map[ulid.ULID]struct{}{}

	for _, b := range idx.BlockDeletionMarks {
		deleted[b.ID] = struct{}{}
	}

	for _, b := range idx.Blocks {
		if _, ok := deleted[b.ID]; !ok {
			blocks[b.ID] = struct{}{}
		}
	}

	for _, b := range pIdx.Blocks {
		if _, ok := blocks[b.ID]; !ok {
			delete(pIdx.Blocks, b.ID)
		}
	}
}

func (c *ParquetConverter) removeOutdatedBlocks(ctx context.Context, uBucket objstore.InstrumentedBucket, pIdx *bucketindex.ParquetIndex) {
	for _, b := range pIdx.Blocks {
		marker, err := ReadCompactMark(ctx, b.ID, uBucket, c.logger)
		if err != nil {
			level.Error(util_log.Logger).Log("msg", "failed to check if file exists", "err", err)
			continue
		}

		if marker.Version < CurrentVersion {
			delete(pIdx.Blocks, b.ID)
		}
	}
}

func (c *ParquetConverter) findNextBlockToCompact(ctx context.Context, uBucket objstore.InstrumentedBucket, idx *bucketindex.Index, pIdx *bucketindex.ParquetIndex) ([]*bucketindex.Block, int) {
	deleted := map[ulid.ULID]struct{}{}
	result := make([]*bucketindex.Block, 0, len(idx.Blocks))
	totalBlocks := 0

	for _, b := range idx.BlockDeletionMarks {
		deleted[b.ID] = struct{}{}
	}

	for _, b := range idx.Blocks {
		// Do not compact if deleted
		if _, ok := deleted[b.ID]; ok {
			continue
		}

		// Do not compact if is already compacted
		if _, ok := pIdx.Blocks[b.ID]; ok {
			continue
		}

		// Don't try to compact 2h block. Compact 12h and 24h.
		if getBlockTimeRange(b, c.blockRanges) == c.blockRanges[0] {
			continue
		}

		totalBlocks++

		// Do not compact block if is not owned
		if ok, err := c.ownBlock(b.ID.String()); err != nil || !ok {
			continue
		}

		marker, err := ReadCompactMark(ctx, b.ID, uBucket, c.logger)

		if err == nil && marker.Version == CurrentVersion {
			continue
		}

		result = append(result, b)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].MinTime > result[j].MinTime
	})

	return result, totalBlocks
}

func (c *ParquetConverter) discoverUsers(ctx context.Context) ([]string, error) {
	var users []string

	err := c.bucket.Iter(ctx, "", func(entry string) error {
		u := strings.TrimSuffix(entry, "/")
		users = append(users, u)
		return nil
	})

	return users, err
}

// compactDirForUser returns the directory to be used to download and compact the blocks for a user
func (c *ParquetConverter) compactDirForUser(userID string) string {
	return filepath.Join(c.compactRootDir(), userID)
}

func (c *ParquetConverter) compactRootDir() string {
	return filepath.Join(c.CompactorCfg.DataDir, "compact")
}

func (c *ParquetConverter) ownBlock(userID string) (bool, error) {
	// Hash the user ID.
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(userID))
	userHash := hasher.Sum32()

	rs, err := c.ring.Get(userHash, compactor.RingOp, nil, nil, nil)
	if err != nil {
		return false, err
	}

	if len(rs.Instances) != 1 {
		return false, fmt.Errorf("unexpected number of parquet-converter in the shard (expected 1, got %d)", len(rs.Instances))
	}

	return rs.Instances[0].Addr == c.ringLifecycler.GetInstanceAddr(), nil
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

func TSDBBlockToParquet(ctx context.Context, id ulid.ULID, uploader Uploader, bDir string, logger log.Logger) (err error) {
	r, w := io.Pipe()

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		defer r.Close()
		err = uploader.Upload(context.Background(), path.Join(id.String(), parquetFileName), r)
		if err != nil {
			level.Error(util_log.Logger).Log("msg", "failed to upload file", "err", err)
		}
		if err == nil {
			err = WriteCompactMark(ctx, id, uploader)
		}
	}()

	batchedRowsStream, ln, totalMetrics, err := tsdbcodec.BlockToParquetRowsStream(
		ctx, bDir, maxParquetIndexSizeLimit, batchSize, batchStreamBufferSize, logger,
	)
	if err != nil {
		return err
	}

	writer := parquet.NewParquetWriter(w, 1e6, maxParquetIndexSizeLimit, parquet.ChunkColumnsPerDay, ln, labels.MetricName)
	if err != nil {
		return err
	}

	total := 0
	for rows := range batchedRowsStream {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		fmt.Printf("Writing Metrics [%v] [%v]\n", 100*(float64(total)/float64(totalMetrics)), rows[0].Columns[labels.MetricName])
		writer.WriteRows(rows)
		total += len(rows)
	}

	if e := writer.Close(); err == nil {
		err = e
	}

	if e := w.Close(); err == nil {
		err = e
	}

	wg.Wait()
	return err
}

func getBlockTimeRange(b *bucketindex.Block, timeRanges []int64) int64 {
	timeRange := int64(0)
	// fallback logic to guess block time range based
	// on MaxTime and MinTime
	blockRange := b.MaxTime - b.MinTime
	for _, tr := range timeRanges {
		rangeStart := getStart(b.MinTime, tr)
		rangeEnd := rangeStart + tr
		if tr >= blockRange && rangeEnd >= b.MaxTime {
			timeRange = tr
			break
		}
	}
	return timeRange
}

func getStart(mint int64, tr int64) int64 {
	// Compute start of aligned time range of size tr closest to the current block's start.
	// This code has been copied from TSDB.
	if mint >= 0 {
		return tr * (mint / tr)
	}

	return tr * ((mint - tr + 1) / tr)
}
