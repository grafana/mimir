// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"context"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/tenant"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	promcfg "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/ingester/activeseries"
	asmodel "github.com/grafana/mimir/pkg/ingester/activeseries/model"
	"github.com/grafana/mimir/pkg/ingester/lookupplan"
	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	util_log "github.com/grafana/mimir/pkg/util/log"
	util_math "github.com/grafana/mimir/pkg/util/math"
)

// applyTSDBSettings goes through all tenants and applies
// * The current max-exemplars setting. If it changed, tsdb will resize the buffer; if it didn't change tsdb will return quickly.
// * The current out-of-order time window. If it changes from 0 to >0, then a new Write-Behind-Log gets created for that tenant.
func (i *Ingester) applyTSDBSettings() {
	for _, userID := range i.getTSDBUsers() {
		oooTW := i.limits.OutOfOrderTimeWindow(userID)
		if oooTW < 0 {
			oooTW = 0
		}

		// We populate a Config struct with just TSDB related config, which is OK
		// because DB.ApplyConfig only looks at the specified config.
		// The other fields in Config are things like Rules, Scrape
		// settings, which don't apply to Head.
		cfg := promcfg.Config{
			StorageConfig: promcfg.StorageConfig{
				ExemplarsConfig: &promcfg.ExemplarsConfig{
					MaxExemplars: int64(i.limiter.maxExemplarsPerUser(userID)),
				},
				TSDBConfig: &promcfg.TSDBConfig{
					OutOfOrderTimeWindow: oooTW.Milliseconds(),
				},
			},
		}
		db := i.getTSDB(userID)
		if db == nil {
			continue
		}
		if err := db.db.ApplyConfig(&cfg); err != nil {
			level.Error(i.logger).Log("msg", "failed to apply config to TSDB", "user", userID, "err", err)
		}
	}
}

func (i *Ingester) getTSDB(userID string) *userTSDB {
	i.tsdbsMtx.RLock()
	defer i.tsdbsMtx.RUnlock()
	db := i.tsdbs[userID]
	return db
}

// List all users for which we have a TSDB. We do it here in order
// to keep the mutex locked for the shortest time possible.
func (i *Ingester) getTSDBUsers() []string {
	i.tsdbsMtx.RLock()
	defer i.tsdbsMtx.RUnlock()

	ids := make([]string, 0, len(i.tsdbs))
	for userID := range i.tsdbs {
		ids = append(ids, userID)
	}

	return ids
}

func (i *Ingester) getOrCreateTSDB(userID string) (*userTSDB, error) {
	db := i.getTSDB(userID)
	if db != nil {
		return db, nil
	}

	i.tsdbsMtx.Lock()
	defer i.tsdbsMtx.Unlock()

	// Check again for DB in the event it was created in-between locks
	var ok bool
	db, ok = i.tsdbs[userID]
	if ok {
		return db, nil
	}

	gl := i.getInstanceLimits()
	if gl != nil && gl.MaxInMemoryTenants > 0 {
		if users := int64(len(i.tsdbs)); users >= gl.MaxInMemoryTenants {
			i.metrics.rejected.WithLabelValues(reasonIngesterMaxTenants).Inc()
			return nil, errMaxTenantsReached
		}
	}

	// Create the database and a shipper for a user
	db, err := i.createTSDB(userID, 0)
	if err != nil {
		return nil, err
	}

	// Add the db to list of user databases
	i.tsdbs[userID] = db
	i.metrics.memUsers.Inc()

	return db, nil
}

// createTSDB creates a TSDB for a given userID, and returns the created db.
func (i *Ingester) createTSDB(userID string, walReplayConcurrency int) (*userTSDB, error) {
	tsdbPromReg := prometheus.NewRegistry()
	udir := i.cfg.BlocksStorageConfig.TSDB.BlocksDir(userID)
	userLogger := util_log.WithUserID(userID, i.logger)

	blockRanges := i.cfg.BlocksStorageConfig.TSDB.BlockRanges.ToMilliseconds()
	matchersConfig := i.limits.ActiveSeriesCustomTrackersConfig(userID)

	initialLocalLimit := 0
	if i.limiter != nil {
		initialLocalLimit = i.limiter.maxSeriesPerUser(userID, 0)
	}
	ownedSeriedStateShardSize := 0
	if i.ownedSeriesService != nil {
		ownedSeriedStateShardSize = i.ownedSeriesService.ringStrategy.shardSizeForUser(userID)
	}

	userDB := &userTSDB{
		cfg:                     &i.cfg,
		userID:                  userID,
		activeSeries:            activeseries.NewActiveSeries(asmodel.NewMatchers(matchersConfig), i.cfg.ActiveSeriesMetrics.IdleTimeout, i.costAttributionMgr.ActiveSeriesTracker(userID)),
		seriesInMetric:          newMetricCounter(i.limiter, i.cfg.getIgnoreSeriesLimitForMetricNamesMap()),
		ingestedAPISamples:      util_math.NewEWMARate(0.2, i.cfg.RateUpdatePeriod),
		ingestedRuleSamples:     util_math.NewEWMARate(0.2, i.cfg.RateUpdatePeriod),
		instanceLimitsFn:        i.getInstanceLimits,
		instanceSeriesCount:     &i.seriesCount,
		instanceErrors:          i.metrics.rejected,
		blockMinRetention:       i.cfg.BlocksStorageConfig.TSDB.Retention,
		useOwnedSeriesForLimits: i.cfg.UseIngesterOwnedSeriesForLimits,

		ownedState: ownedSeriesState{
			shardSize:        ownedSeriedStateShardSize, // initialize series shard size so that it's correct even before we update ownedSeries for the first time
			localSeriesLimit: initialLocalLimit,
		},
	}
	userDB.triggerRecomputeOwnedSeries(recomputeOwnedSeriesReasonNewUser)

	if i.cfg.BlocksStorageConfig.TSDB.IndexLookupPlanning.Enabled {
		plannerFactory := lookupplan.NewPlannerFactory(i.lookupPlanMetrics.ForUser(userID), userLogger, lookupplan.NewStatisticsGenerator(userLogger), i.cfg.BlocksStorageConfig.TSDB.IndexLookupPlanning.CostConfig)
		userDB.plannerProvider = newPlannerProvider(plannerFactory)
	}

	userDBHasDB := atomic.NewBool(false)
	blocksToDelete := func(blocks []*tsdb.Block) map[ulid.ULID]struct{} {
		if !userDBHasDB.Load() {
			return nil
		}
		return userDB.blocksToDelete(blocks)
	}
	blockGeneration := blockGenerationCalculator(userDB, blockRanges[0])

	if i.cfg.BlocksStorageConfig.TSDB.OffsetCatalogue.Enabled {
		userDB.offsetCatalogue = newOffsetCatalogue(userLogger, i.offsetCatalogueMetrics, udir, userID, i.ingestPartitionID)
	}

	oooTW := i.limits.OutOfOrderTimeWindow(userID)
	// Create a new user database
	db, err := tsdb.Open(udir, util_log.SlogFromGoKit(userLogger), tsdbPromReg, &tsdb.Options{
		RetentionDuration:                    i.cfg.BlocksStorageConfig.TSDB.Retention.Milliseconds(),
		MinBlockDuration:                     blockRanges[0],
		MaxBlockDuration:                     blockRanges[len(blockRanges)-1],
		NoLockfile:                           true,
		StripeSize:                           i.cfg.BlocksStorageConfig.TSDB.StripeSize,
		HeadChunksWriteBufferSize:            i.cfg.BlocksStorageConfig.TSDB.HeadChunksWriteBufferSize,
		HeadChunksEndTimeVariance:            i.cfg.BlocksStorageConfig.TSDB.HeadChunksEndTimeVariance,
		WALCompression:                       i.cfg.BlocksStorageConfig.TSDB.WALCompressionType(),
		WALSegmentSize:                       i.cfg.BlocksStorageConfig.TSDB.WALSegmentSizeBytes,
		WALReplayConcurrency:                 walReplayConcurrency,
		SeriesLifecycleCallback:              userDB,
		BlocksToDelete:                       blocksToDelete,
		EnableExemplarStorage:                true, // enable for everyone so we can raise the limit later
		MaxExemplars:                         int64(i.limiter.maxExemplarsPerUser(userID)),
		SeriesHashCache:                      i.seriesHashCache,
		EnableMemorySnapshotOnShutdown:       i.cfg.BlocksStorageConfig.TSDB.MemorySnapshotOnShutdown,
		EnableBiggerOOOBlockForOldSamples:    i.cfg.BlocksStorageConfig.TSDB.BiggerOutOfOrderBlocksForOldSamples,
		IsolationDisabled:                    true,
		HeadChunksWriteQueueSize:             i.cfg.BlocksStorageConfig.TSDB.HeadChunksWriteQueueSize,
		EnableOverlappingCompaction:          false,                // always false since Mimir only uploads lvl 1 compacted blocks
		EnableSharding:                       true,                 // Always enable query sharding support.
		OutOfOrderTimeWindow:                 oooTW.Milliseconds(), // The unit must be same as our timestamps.
		OutOfOrderCapMax:                     int64(i.cfg.BlocksStorageConfig.TSDB.OutOfOrderCapacityMax),
		TimelyCompaction:                     i.cfg.BlocksStorageConfig.TSDB.TimelyHeadCompaction,
		SharedPostingsForMatchersCache:       i.cfg.BlocksStorageConfig.TSDB.SharedPostingsForMatchersCache,
		PostingsForMatchersCacheKeyFunc:      tenant.TenantID,
		HeadPostingsForMatchersCacheFactory:  i.headPostingsForMatchersCacheFactory,
		BlockPostingsForMatchersCacheFactory: i.blockPostingsForMatchersCacheFactory,
		PostingsClonerFactory:                lookupplan.ActualSelectedPostingsClonerFactory{},
		SecondaryHashFunction:                secondaryTSDBHashFunctionForUser(userID),
		IndexLookupPlannerFunc:               userDB.getIndexLookupPlannerFunc(),
		BlockChunkQuerierFunc: func(b tsdb.BlockReader, mint, maxt int64) (storage.ChunkQuerier, error) {
			i.metrics.queriedBlocks.WithLabelValues(blockGeneration(b)).Inc()
			return i.createBlockChunkQuerier(userID, b, mint, maxt)
		},
	}, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open TSDB: %s", udir)
	}
	db.DisableCompactions() // we will compact on our own schedule

	// Run compaction before using this TSDB. If there is data in head that needs to be put into blocks,
	// this will actually create the blocks. If there is no data (empty TSDB), this is a no-op, although
	// local blocks compaction may still take place if configured.
	level.Info(userLogger).Log("msg", "Running compaction after WAL replay")
	// Note that we want to let TSDB creation finish without being interrupted by eventual context cancellation,
	// so passing an independent context here
	err = db.Compact(context.Background())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to compact TSDB: %s", udir)
	}

	userDB.db = db
	userDBHasDB.Store(true)
	// We set the limiter here because we don't want to limit
	// series during WAL replay.
	userDB.limiter = i.limiter

	// Set a reference the head's postings for matchers cache, so that ingesters can invalidate entries
	if i.cfg.BlocksStorageConfig.TSDB.SharedPostingsForMatchersCache && i.cfg.BlocksStorageConfig.TSDB.HeadPostingsForMatchersCacheInvalidation {
		userDB.postingsCache = db.Head().PostingsForMatchersCache()
	}

	// If head is empty (eg. new TSDB), don't close it right after.
	lastUpdateTime := time.Now()
	if db.Head().NumSeries() > 0 {
		// If there are series in the head, use max time from head. If this time is too old,
		// TSDB will be eligible for flushing and closing sooner, unless more data is pushed to it quickly.
		//
		// If TSDB's maxTime is in the future, ignore it. If we set "lastUpdate" to very distant future, it would prevent
		// us from detecting TSDB as idle for a very long time.
		headMaxTime := time.UnixMilli(db.Head().MaxTime())
		if headMaxTime.Before(lastUpdateTime) {
			lastUpdateTime = headMaxTime
		}
	}
	userDB.setLastUpdate(lastUpdateTime)

	// Create a new shipper for this database
	if i.cfg.BlocksStorageConfig.TSDB.IsBlocksShippingEnabled() {
		userDB.shipper = newShipper(
			userLogger,
			i.limits,
			userID,
			i.shipperMetrics,
			udir,
			bucket.NewUserBucketClient(userID, i.bucket, i.limits),
			block.ReceiveSource,
		)

		// Initialise the shipper blocks cache.
		if err := userDB.updateCachedShippedBlocks(); err != nil {
			level.Error(userLogger).Log("msg", "failed to update cached shipped blocks after shipper initialisation", "err", err)
		}
	}

	i.tsdbMetrics.SetRegistryForTenant(userID, tsdbPromReg)

	if i.cfg.BlocksStorageConfig.TSDB.IndexLookupPlanning.Enabled {
		// Generate initial statistics only after the TSDB has been opened and initialized.
		if err := userDB.generateHeadStatistics(); err != nil {
			level.Error(userLogger).Log("msg", "failed to generate initial TSDB head statistics", "err", err)
		}
	}

	return userDB, nil
}

// createBlockChunkQuerier creates a BlockChunkQuerier that wraps the default querier with stats tracking
// and optionally with mirroring for comparison.
func (i *Ingester) createBlockChunkQuerier(userID string, b tsdb.BlockReader, mint, maxt int64) (storage.ChunkQuerier, error) {
	defaultQuerier, err := tsdb.NewBlockChunkQuerier(b, mint, maxt)
	if err != nil {
		return nil, err
	}

	lookupStatsQuerier := newStatsTrackingChunkQuerier(b.Meta().ULID, defaultQuerier, i.metrics, i.lookupPlanMetrics.ForUser(userID))

	if rand.Float64() > i.cfg.BlocksStorageConfig.TSDB.IndexLookupPlanning.ComparisonPortion {
		return lookupStatsQuerier, nil
	}

	// If mirroring is enabled, wrap the stats querier with mirrored querier
	mirroredQuerier := newMirroredChunkQuerierWithMeta(
		userID,
		i.metrics.indexLookupComparisonOutcomes,
		mint, maxt,
		b.Meta(),
		i.logger,
		lookupStatsQuerier,
	)

	return mirroredQuerier, nil
}

// returns a function that computes the generation label for a block relative to the TSDB head.
// Generation 0 is the head block; persisted blocks get generation 1, 2, etc counting back in block-range units
// from the head's MinTime.
func blockGenerationCalculator(db *userTSDB, blockRange int64) func(b tsdb.BlockReader) string {
	return func(b tsdb.BlockReader) string {
		if _, ok := b.(*tsdb.RangeHead); ok {
			return "0" // Special case: querying head block
		}
		headMinTime := db.Head().MinTime()
		if headMinTime == math.MaxInt64 {
			return "unknown" // Edge case: head is empty, and the query touches block generation >=1
		}
		gen := max(1, (headMinTime-b.Meta().MinTime)/blockRange)
		if gen > 100 {
			return "100+" // Bound generation's cardinality. A tenant with very large OOO window can query a very old block, but we don't need to be precise.
		}
		return strconv.FormatInt(gen, 10)
	}
}

func (i *Ingester) closeAllTSDB() {
	i.tsdbsMtx.Lock()

	// First, mark all TSDBs as closing to prevent new appends from starting.
	// We try to transition from any active state to closing.
	for _, userDB := range i.tsdbs {
		userDB.setClosingState()
	}

	// Now wait for all in-flight appends to complete before closing.
	// This prevents closing TSDBs while appenders are still writing to them.
	for _, userDB := range i.tsdbs {
		userDB.inFlightAppends.Wait()
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(i.tsdbs))

	// Concurrently close all users TSDB
	for userID, userDB := range i.tsdbs {
		go func(db *userTSDB) {
			defer wg.Done()

			if err := db.Close(); err != nil {
				level.Warn(i.logger).Log("msg", "unable to close TSDB", "err", err, "user", userID)
				return
			}

			// Now that the TSDB has been closed, we should remove it from the
			// set of open ones. This lock acquisition doesn't deadlock with the
			// outer one, because the outer one is released as soon as all go
			// routines are started.
			i.tsdbsMtx.Lock()
			delete(i.tsdbs, userID)
			i.tsdbsMtx.Unlock()

			i.metrics.memUsers.Dec()
			i.metrics.deletePerUserCustomTrackerMetrics(userID, db.activeSeries.CurrentMatcherNames())
		}(userDB)
	}

	// Wait until all Close() completed
	i.tsdbsMtx.Unlock()
	wg.Wait()
}

// openExistingTSDB walks the user tsdb dir, and opens a tsdb for each user. This may start a WAL replay, so we limit the number of
// concurrently opening TSDB.
func (i *Ingester) openExistingTSDB(ctx context.Context) error {
	level.Info(i.logger).Log("msg", "opening existing TSDBs")
	startTime := time.Now()

	queue := make(chan string)
	group, groupCtx := errgroup.WithContext(ctx)

	userIDs, err := i.findUserIDsWithTSDBOnFilesystem()
	if err != nil {
		level.Error(i.logger).Log("msg", "error while finding existing TSDBs", "err", err)
		return err
	}

	if len(userIDs) == 0 {
		return nil
	}

	tsdbOpenConcurrency, tsdbWALReplayConcurrency := getOpenTSDBsConcurrencyConfig(i.cfg.BlocksStorageConfig.TSDB, len(userIDs))

	// Create a pool of workers which will open existing TSDBs.
	for n := 0; n < tsdbOpenConcurrency; n++ {
		group.Go(func() error {
			for userID := range queue {
				db, err := i.createTSDB(userID, tsdbWALReplayConcurrency)
				if err != nil {
					level.Error(i.logger).Log("msg", "unable to open TSDB", "err", err, "user", userID)
					return errors.Wrapf(err, "unable to open TSDB for user %s", userID)
				}

				// Add the database to the map of user databases
				i.tsdbsMtx.Lock()
				i.tsdbs[userID] = db
				i.tsdbsMtx.Unlock()
				i.metrics.memUsers.Inc()
			}

			return nil
		})
	}

	// Spawn a goroutine to place all users with a TSDB found on the filesystem in the queue.
	group.Go(func() error {
		defer close(queue)

		for _, userID := range userIDs {
			// Enqueue the user to be processed.
			select {
			case queue <- userID:
				// Nothing to do.
			case <-groupCtx.Done():
				// Interrupt in case a failure occurred in another goroutine.
				return nil
			}
		}
		return nil
	})

	// Wait for all workers to complete.
	err = group.Wait()
	if err != nil {
		level.Error(i.logger).Log("msg", "error while opening existing TSDBs", "err", err)
		return err
	}

	// Update the usage statistics once all TSDBs have been opened.
	i.updateUsageStats()

	i.metrics.openExistingTSDB.Add(time.Since(startTime).Seconds())
	level.Info(i.logger).Log("msg", "successfully opened existing TSDBs")
	return nil
}

func getOpenTSDBsConcurrencyConfig(tsdbConfig mimir_tsdb.TSDBConfig, userCount int) (tsdbOpenConcurrency, tsdbWALReplayConcurrency int) {
	tsdbOpenConcurrency = mimir_tsdb.DefaultMaxTSDBOpeningConcurrencyOnStartup
	tsdbWALReplayConcurrency = 0
	// When WALReplayConcurrency is enabled, we want to ensure the WAL replay at ingester startup
	// doesn't use more than the configured number of CPU cores. In order to optimize performance
	// both on single tenant and multi tenant Mimir clusters, we use a heuristic to decide whether
	// it's better to parallelize the WAL replay of each single TSDB (low number of tenants) or
	// the WAL replay of multiple TSDBs at the same time (high number of tenants).
	if tsdbConfig.WALReplayConcurrency > 0 {
		if userCount <= maxTSDBOpenWithoutConcurrency {
			tsdbOpenConcurrency = 1
			tsdbWALReplayConcurrency = tsdbConfig.WALReplayConcurrency
		} else {
			tsdbOpenConcurrency = tsdbConfig.WALReplayConcurrency
			tsdbWALReplayConcurrency = 1
		}
	}
	return
}

// findUserIDsWithTSDBOnFilesystem finds all users with a TSDB on the filesystem.
func (i *Ingester) findUserIDsWithTSDBOnFilesystem() ([]string, error) {
	var userIDs []string
	walkErr := filepath.Walk(i.cfg.BlocksStorageConfig.TSDB.Dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// If the root directory doesn't exist, we're OK (not needed to be created upfront).
			if os.IsNotExist(err) && path == i.cfg.BlocksStorageConfig.TSDB.Dir {
				return filepath.SkipDir
			}

			level.Error(i.logger).Log("msg", "an error occurred while iterating the filesystem storing TSDBs", "path", path, "err", err)
			return errors.Wrapf(err, "an error occurred while iterating the filesystem storing TSDBs at %s", path)
		}

		// Skip root dir and all other files
		if path == i.cfg.BlocksStorageConfig.TSDB.Dir || !info.IsDir() {
			return nil
		}

		// Top level directories are assumed to be user TSDBs
		userID := info.Name()
		f, err := os.Open(path)
		if err != nil {
			level.Error(i.logger).Log("msg", "unable to open TSDB dir", "err", err, "user", userID, "path", path)
			return errors.Wrapf(err, "unable to open TSDB dir %s for user %s", path, userID)
		}
		defer f.Close()

		// If the dir is empty skip it
		if _, err := f.Readdirnames(1); err != nil {
			if errors.Is(err, io.EOF) {
				return filepath.SkipDir
			}

			level.Error(i.logger).Log("msg", "unable to read TSDB dir", "err", err, "user", userID, "path", path)
			return errors.Wrapf(err, "unable to read TSDB dir %s for user %s", path, userID)
		}

		// Save userId.
		userIDs = append(userIDs, userID)

		// Don't descend into subdirectories.
		return filepath.SkipDir
	})

	return userIDs, errors.Wrapf(walkErr, "unable to walk directory %s containing existing TSDBs", i.cfg.BlocksStorageConfig.TSDB.Dir)
}

// getOldestUnshippedBlockMetric returns the unix timestamp of the oldest unshipped block or
// 0 if all blocks have been shipped.
func (i *Ingester) getOldestUnshippedBlockMetric() float64 {
	i.tsdbsMtx.RLock()
	defer i.tsdbsMtx.RUnlock()

	oldest := uint64(0)
	for _, db := range i.tsdbs {
		if ts := db.getOldestUnshippedBlockTime(); oldest == 0 || ts < oldest {
			oldest = ts
		}
	}

	return float64(oldest / 1000)
}

func (i *Ingester) minTsdbHeadTimestamp() float64 {
	i.tsdbsMtx.RLock()
	defer i.tsdbsMtx.RUnlock()

	minTime := int64(math.MaxInt64)
	for _, db := range i.tsdbs {
		minTime = min(minTime, db.db.Head().MinTime())
	}

	if minTime == math.MaxInt64 {
		return 0
	}
	// convert to seconds
	return float64(minTime) / 1000
}

func (i *Ingester) maxTsdbHeadTimestamp() float64 {
	i.tsdbsMtx.RLock()
	defer i.tsdbsMtx.RUnlock()

	maxTime := int64(math.MinInt64)
	for _, db := range i.tsdbs {
		maxTime = max(maxTime, db.db.Head().MaxTime())
	}

	if maxTime == math.MinInt64 {
		return 0
	}
	// convert to seconds
	return float64(maxTime) / 1000
}

func (i *Ingester) closeAndDeleteIdleUserTSDBs(ctx context.Context) error {
	for _, userID := range i.getTSDBUsers() {
		if ctx.Err() != nil {
			return nil
		}

		result := i.closeAndDeleteUserTSDBIfIdle(userID)

		i.metrics.idleTsdbChecks.WithLabelValues(string(result)).Inc()
	}

	return nil
}

func (i *Ingester) closeAndDeleteUserTSDBIfIdle(userID string) tsdbCloseCheckResult {
	userDB := i.getTSDB(userID)
	if userDB == nil {
		return tsdbShippingDisabled
	}

	if userDB.shipper == nil && !i.cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBWhenShippingDisabled {
		// We will not delete local data when not using shipping to storage unless explicitly configured.
		return tsdbShippingDisabled
	}

	if result := userDB.shouldCloseTSDB(i.cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout); !result.shouldClose() {
		return result
	}

	// This disables pushes and force-compactions. Not allowed to close while shipping is in progress.
	if ok, _ := userDB.changeState(active, closing); !ok {
		return tsdbNotActive
	}

	// If TSDB is fully closed, we will set state to 'closed', which will prevent this defered closing -> active transition.
	defer userDB.changeState(closing, active)

	// Make sure we don't ignore any possible inflight pushes.
	userDB.inFlightAppends.Wait()

	// Verify again, things may have changed during the checks and pushes.
	tenantDeleted := false
	if result := userDB.shouldCloseTSDB(i.cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout); !result.shouldClose() {
		// This will also change TSDB state back to active (via defer above).
		return result
	} else if result == tsdbTenantMarkedForDeletion {
		tenantDeleted = true
	}

	// At this point there are no more pushes to TSDB, and no possible compaction. Normally TSDB is empty,
	// but if we're closing TSDB because of tenant deletion mark, then it may still contain some series.
	// We need to remove these series from series count.
	i.seriesCount.Sub(int64(userDB.Head().NumSeries()))

	dir := userDB.db.Dir()

	if err := userDB.Close(); err != nil {
		level.Error(i.logger).Log("msg", "failed to close idle TSDB", "user", userID, "err", err)
		return tsdbCloseFailed
	}

	level.Info(i.logger).Log("msg", "closed idle TSDB", "user", userID)

	// This will prevent going back to "active" state in deferred statement.
	userDB.changeState(closing, closed)

	// Only remove user from TSDBState when everything is cleaned up
	// This will prevent concurrency problems when cortex are trying to open new TSDB - Ie: New request for a given tenant
	// came in - while closing the tsdb for the same tenant.
	// If this happens now, the request will get reject as the push will not be able to acquire the lock as the tsdb will be
	// in closed state
	defer func() {
		i.tsdbsMtx.Lock()
		delete(i.tsdbs, userID)
		i.tsdbsMtx.Unlock()
	}()

	i.metrics.memUsers.Dec()
	i.tsdbMetrics.RemoveRegistryForTenant(userID)

	i.deleteUserMetadata(userID)
	i.metrics.deletePerUserMetrics(userID)
	i.metrics.deletePerUserCustomTrackerMetrics(userID, userDB.activeSeries.CurrentMatcherNames())

	// And delete local data.
	if err := os.RemoveAll(dir); err != nil {
		level.Error(i.logger).Log("msg", "failed to delete local TSDB", "user", userID, "err", err)
		return tsdbDataRemovalFailed
	}

	if tenantDeleted {
		level.Info(i.logger).Log("msg", "deleted local TSDB, user marked for deletion", "user", userID, "dir", dir)
		return tsdbTenantMarkedForDeletion
	}

	level.Info(i.logger).Log("msg", "deleted local TSDB, due to being idle", "user", userID, "dir", dir)
	return tsdbIdleClosed
}

func (i *Ingester) RemoveGroupMetricsForUser(userID, group string) {
	i.metrics.deletePerGroupMetricsForUser(userID, group)
}

func (i *Ingester) NotifyPreCommit(ctx context.Context) error {
	level.Debug(i.logger).Log("msg", "fsyncing TSDBs", "concurrency", i.cfg.IngestStorageConfig.WriteLogsFsyncBeforeKafkaCommitConcurrency)

	return concurrency.ForEachUser(ctx, i.getTSDBUsers(), i.cfg.IngestStorageConfig.WriteLogsFsyncBeforeKafkaCommitConcurrency, func(ctx context.Context, userID string) error {
		db := i.getTSDB(userID)
		if db == nil {
			return nil
		}
		return db.Head().FsyncWLSegments()
	})
}
