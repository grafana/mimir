// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"context"
	"math"
	"math/rand"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/timeutil"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util"
)

func (i *Ingester) shipBlocksLoop(ctx context.Context) error {
	// We add a slight jitter to make sure that if the head compaction interval and ship interval are set to the same
	// value they don't clash (if they both continuously run at the same exact time, the head compaction may not run
	// because can't successfully change the state).
	shipTicker := time.NewTicker(util.DurationWithJitter(i.cfg.BlocksStorageConfig.TSDB.ShipInterval, 0.01))
	defer shipTicker.Stop()

	for {
		select {
		case <-shipTicker.C:
			i.shipBlocks(ctx, nil)

		case req := <-i.shipTrigger:
			i.shipBlocks(ctx, req.users)
			close(req.callback) // Notify back.

		case <-ctx.Done():
			return nil
		}
	}
}

// shipBlocks runs shipping for all users.
func (i *Ingester) shipBlocks(ctx context.Context, allowed *util.AllowList) {
	// Number of concurrent workers is limited in order to avoid to concurrently sync a lot
	// of tenants in a large cluster.
	_ = concurrency.ForEachUser(ctx, i.getTSDBUsers(), i.cfg.BlocksStorageConfig.TSDB.ShipConcurrency, func(ctx context.Context, userID string) error {
		if !allowed.IsAllowed(userID) {
			return nil
		}

		// Get the user's DB. If the user doesn't exist, we skip it.
		userDB := i.getTSDB(userID)
		if userDB == nil || userDB.shipper == nil {
			return nil
		}

		if userDB.deletionMarkFound.Load() {
			return nil
		}

		if time.Since(time.Unix(userDB.lastDeletionMarkCheck.Load(), 0)) > mimir_tsdb.DeletionMarkCheckInterval {
			// Even if check fails with error, we don't want to repeat it too often.
			userDB.lastDeletionMarkCheck.Store(time.Now().Unix())

			deletionMarkExists, err := mimir_tsdb.TenantDeletionMarkExists(ctx, i.bucket, userID)
			if err != nil {
				// If we cannot check for deletion mark, we continue anyway, even though in production shipper will likely fail too.
				// This however simplifies unit tests, where tenant deletion check is enabled by default, but tests don't setup bucket.
				level.Warn(i.logger).Log("msg", "failed to check for tenant deletion mark before shipping blocks", "user", userID, "err", err)
			} else if deletionMarkExists {
				userDB.deletionMarkFound.Store(true)

				level.Info(i.logger).Log("msg", "tenant deletion mark exists, not shipping blocks", "user", userID)
				return nil
			}
		}

		// Run the shipper's Sync() to upload unshipped blocks. Make sure the TSDB state is active, in order to
		// avoid any race condition with closing idle TSDBs.
		if ok, s := userDB.changeState(active, activeShipping); !ok {
			level.Info(i.logger).Log("msg", "shipper skipped because the TSDB is not active", "user", userID, "state", s.String())
			return nil
		}
		defer userDB.changeState(activeShipping, active)

		uploaded, err := userDB.shipper.Sync(ctx)
		if err != nil {
			level.Warn(i.logger).Log("msg", "shipper failed to synchronize TSDB blocks with the storage", "user", userID, "uploaded", uploaded, "err", err)
		} else {
			level.Debug(i.logger).Log("msg", "shipper successfully synchronized TSDB blocks with storage", "user", userID, "uploaded", uploaded)
		}

		// The shipper meta file could be updated even if the Sync() returned an error,
		// so it's safer to update it each time at least a block has been uploaded.
		// Moreover, the shipper meta file could be updated even if no blocks are uploaded
		// (eg. blocks removed due to retention) but doesn't cause any harm not updating
		// the cached list of blocks in such case, so we're not handling it.
		if uploaded > 0 {
			if err := userDB.updateCachedShippedBlocks(); err != nil {
				level.Error(i.logger).Log("msg", "failed to update cached shipped blocks after shipper synchronisation", "user", userID, "err", err)
			}
		}

		return nil
	})
}

// compactionServiceRunning is the running function of internal service responsible to periodically
// compact TSDB Head.
func (i *Ingester) compactionServiceRunning(ctx context.Context) error {
	// At ingester startup, spread the first compaction over the configured compaction
	// interval. Then, the next compactions will happen at a regular interval. This logic
	// helps to have different ingesters running the compaction at a different time,
	// effectively spreading the compactions over the configured interval.
	firstInterval, standardInterval := i.compactionServiceInterval(time.Now(), i.instanceRing.Zones())

	// After the first interval, we want the compaction to run at a specified interval for the zone if we have multiple zones,
	// before we switch to running the compaction at the standard configured `HeadCompactionInterval`.
	// If the criteria to have staggered compactions are not met, standardInterval and i.cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval are the same.
	stopTicker, tickerChan := timeutil.NewVariableTicker(firstInterval, standardInterval)
	defer func() {
		// We call stopTicker() from an anonymous function because the stopTicker()
		// reference may change during the lifetime of compactionServiceRunning().
		stopTicker()
	}()

	for ctx.Err() == nil {
		select {
		case <-tickerChan:
			// Count the number of compactions in progress to keep the downscale handler from
			// clearing the read-only mode. See [Ingester.PrepareInstanceRingDownscaleHandler]
			i.numCompactionsInProgress.Inc()

			// The forcedCompactionMaxTime has no meaning because force=false.
			i.compactBlocks(ctx, false, 0, nil)

			// Check if any TSDB Head should be compacted to reduce the number of in-memory series.
			i.compactBlocksToReduceInMemorySeries(ctx, time.Now())

			// Check if any TSDB Head should be compacted based on per-tenant owned series thresholds.
			i.compactBlocksToReducePerTenantOwnedSeries(ctx, time.Now())

			// Decrement the counter after compaction is complete
			i.numCompactionsInProgress.Dec()

			// If the ingester state is no longer "Starting", we switch to a different interval.
			// We only compare the standard interval because the first interval may be random due to jittering.
			if newFirstInterval, newStandardInterval := i.compactionServiceInterval(time.Now(), i.instanceRing.Zones()); standardInterval != newStandardInterval {
				// Stop the previous ticker before creating a new one.
				stopTicker()

				standardInterval = newStandardInterval
				stopTicker, tickerChan = timeutil.NewVariableTicker(newFirstInterval, newStandardInterval)
			}

		case req := <-i.forceCompactTrigger:
			// Note:
			// Inc/Dec numCompactionsInProgress is not done here but before the force compaction is triggered.
			// This is because we want to track the number of compactions accurately before the
			// downscale handler is called. This ensures that the ingester will never leave the
			// read-only state. (See [Ingester.FlushHandler])

			// Always pass math.MaxInt64 as forcedCompactionMaxTime because we want to compact the whole TSDB head.
			i.compactBlocks(ctx, true, math.MaxInt64, req.users)
			close(req.callback) // Notify back.

		case <-ctx.Done():
			return nil
		}

		// Sync user DB's offset catalogue in the end of this compaction cycle.
		i.offsetCataloguesSync(ctx)
	}
	return nil
}

func (i *Ingester) offsetCataloguesSync(ctx context.Context) {
	if !i.cfg.BlocksStorageConfig.TSDB.OffsetCatalogue.Enabled {
		return
	}

	// If any block was cut from the head and discovered in this sync tick,
	// all series in the block are guaranteed to come from below this lastSeenOffset.
	// Note: for normal compaction cycle, that cuts head at "chunkRange * 3/2",
	// this offset overshoots by ~1h. This is technically correct, but very conservative.
	offsetHW := i.ingestReader.LastSeenOffset()

	level.Info(i.logger).Log("msg", "syncing offset catalogues for tenants", "last_seen_offset", offsetHW)

	_ = concurrency.ForEachUser(ctx, i.getTSDBUsers(), i.cfg.BlocksStorageConfig.TSDB.OffsetCatalogue.SyncConcurrency, func(ctx context.Context, userID string) error {
		// Get the user's DB. If the user doesn't exist, we skip it.
		db := i.getTSDB(userID)
		if db == nil || db.offsetCatalogue == nil {
			return nil
		}

		err := db.offsetCatalogue.Sync(ctx, offsetHW)
		if err != nil {
			level.Warn(i.logger).Log("msg", "offset catalogue sync failed", "user", userID, "offset", offsetHW, "err", err)
		} else {
			level.Debug(i.logger).Log("msg", "successfully sync offset catalogue", "user", userID, "offset", offsetHW)
		}

		return nil
	})
}

// compactionServiceInterval returns how frequently the TSDB Head should be checked for compaction.
// The returned standardInterval is guaranteed to have no jittering or staggering per zone applied.
// The returned intervals may change over time, depending on the ingester service state.
func (i *Ingester) compactionServiceInterval(now time.Time, zones []string) (firstInterval, standardInterval time.Duration) {
	startingInterval := i.cfg.BlocksStorageConfig.TSDB.HeadCompactionIntervalWhileStarting
	interval := i.cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval

	// Trigger TSDB Head compaction frequently when starting up, because we may replay data from the partition
	// if ingest storage is enabled.
	if i.State() == services.Starting {
		standardInterval = min(startingInterval, interval)

		if i.cfg.BlocksStorageConfig.TSDB.HeadCompactionIntervalJitterEnabled {
			firstInterval = util.DurationWithNegativeJitter(standardInterval, 1)
		} else {
			firstInterval = standardInterval
		}

		return firstInterval, standardInterval
	}

	zoneAwareInterval := i.timeToNextZoneAwareCompaction(now, zones)

	// If we don't have jittering enabled, we return the standard interval as-is.
	if !i.cfg.BlocksStorageConfig.TSDB.HeadCompactionIntervalJitterEnabled {
		return zoneAwareInterval, interval
	}

	// With jittering enabled, we want to apply a positive jitter to the staggered interval.
	// We estimate that roughly 50% of the interval window is a good enough heuristic than when the 50% jitter is applied,
	// we get enough variability to not get overlap between zones and ingesters.
	var jitter time.Duration
	if len(zones) > 0 {
		jitter = time.Duration(rand.Int63n((interval.Nanoseconds() / int64(len(zones))) / 2))
	}

	return zoneAwareInterval + jitter, interval
}

// timeToNextZoneAwareCompaction calculates when the next compaction should occur,
// staggering compactions across zones to distribute load over time.
//
// With zone awareness enabled, each zone gets a unique offset within the compaction interval.
// Example with 15min interval and 2 zones:
// - Zone 'a' compacts at 0:00, 0:15, 0:30, 0:45
// - Zone 'b' compacts at 0:07, 0:22, 0:37, 0:52
func (i *Ingester) timeToNextZoneAwareCompaction(now time.Time, zones []string) time.Duration {
	// To make the computed offset deterministic, zones must be sorted.
	slices.Sort(zones)

	interval := i.cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval

	// If we don't have zone awareness enabled, we return the configured head compaction interval as-is,
	// because we don't need to adjust it based on the number of zones.
	if !i.cfg.IngesterRing.ZoneAwarenessEnabled || i.cfg.IngesterRing.InstanceZone == "" {
		return interval
	}

	// No more than 1 zone, we don't need to adjust the interval.
	if len(zones) <= 1 {
		return interval
	}

	// Find the index of the current zone in the zones list to determine its offset
	current := i.cfg.IngesterRing.InstanceZone
	zoneIndex := slices.Index(zones, current)

	// Zone not found in the list - this shouldn't happen but handle gracefully
	if zoneIndex == -1 {
		level.Warn(i.logger).Log("msg", "could not compute when the next zone-aware TSDB head compaction should occur, current zone not found in zones list, using default interval", "current_zone", current, "available_zones", strings.Join(zones, ","))
		return interval
	}

	// Calculate this zone's offset and the next compaction interval.
	offsetStep := interval / time.Duration(len(zones))
	zoneOffset := time.Duration(zoneIndex) * offsetStep
	result := timeUntilCompaction(now, interval, zoneOffset)

	level.Debug(i.logger).Log("msg", "computed timing for the next TSDB head zone-aware compaction",
		"zone", current,
		"zone_index", zoneIndex,
		"total_zones", len(zones),
		"configured_interval", interval,
		"zone_offset", zoneOffset,
		"next_compaction_in", result)

	return result
}

// Compacts all compactable blocks. Force flag will force compaction even if head is not compactable yet.
func (i *Ingester) compactBlocks(ctx context.Context, force bool, forcedCompactionMaxTime int64, allowed *util.AllowList) {
	defer i.metrics.resetForcedCompactions()

	_ = concurrency.ForEachUser(ctx, i.getTSDBUsers(), i.cfg.BlocksStorageConfig.TSDB.HeadCompactionConcurrency, func(_ context.Context, userID string) error {
		if !allowed.IsAllowed(userID) {
			return nil
		}

		userDB := i.getTSDB(userID)
		if userDB == nil {
			return nil
		}

		// Don't do anything, if there is nothing to compact.
		h := userDB.Head()
		if h.NumSeries() == 0 {
			return nil
		}

		var err error

		i.metrics.compactionsTriggered.Inc()

		minTimeBefore := userDB.Head().MinTime()

		reason := ""
		switch {
		case force:
			reason = "forced"
			i.metrics.increaseForcedCompactions()
			err = userDB.compactHead(i.cfg.BlocksStorageConfig.TSDB.BlockRanges[0].Milliseconds(), forcedCompactionMaxTime)
			i.metrics.decreaseForcedCompactions()

		case i.compactionIdleTimeout > 0 && userDB.isIdle(time.Now(), i.compactionIdleTimeout):
			reason = "idle"
			level.Info(i.logger).Log("msg", "TSDB is idle, forcing compaction", "user", userID)

			// We want to compact the entire TSDB head.
			//
			// However, when a partition switches from INACTIVE back to ACTIVE while the ingester
			// is compacting idle TSDBs, consumption from Kafka can be paused (effectively fail
			// and the gets retried). This happens if incoming samples have timestamps that overlap
			// with the forced compaction interval.
			//
			// To reduce the likelihood of this, we force compaction only up to the maximum sample
			// timestamp currently in the TSDB head, instead of using a higher value (for example,
			// math.MaxInt64). This still compacts all samples in the head, but in practice the
			// maximum compacted timestamp is roughly HeadCompactionIdleTimeout old (1h by default).
			// As a result, it’s unlikely that Kafka consumption will be paused, since newly
			// ingested samples are expected to be much newer.
			userMaxTime := max(userDB.db.Head().MaxTime(), userDB.db.Head().MaxOOOTime())
			if userMaxTime > math.MinInt64 {
				i.metrics.increaseForcedCompactions()
				err = userDB.compactHead(i.cfg.BlocksStorageConfig.TSDB.BlockRanges[0].Milliseconds(), userMaxTime)
				i.metrics.decreaseForcedCompactions()
			}

		default:
			reason = "regular"
			err = userDB.Compact()
		}

		if err != nil {
			i.metrics.compactionsFailed.Inc()
			level.Warn(i.logger).Log("msg", "TSDB blocks compaction for user has failed", "user", userID, "err", err, "compactReason", reason)
		} else {
			level.Debug(i.logger).Log("msg", "TSDB blocks compaction completed successfully", "user", userID, "compactReason", reason)
		}

		minTimeAfter := userDB.Head().MinTime()

		// If head was compacted, its MinTime has changed. We need to recalculate series owned by this ingester,
		// because in-memory series are removed during compaction.
		if minTimeBefore != minTimeAfter {
			r := recomputeOwnedSeriesReasonCompaction
			if force && forcedCompactionMaxTime != math.MaxInt64 {
				r = recomputeOwnedSeriesReasonEarlyCompaction
			}
			userDB.triggerRecomputeOwnedSeries(r)
		}

		return nil
	})
}

// compactBlocksToReduceInMemorySeries compacts the TSDB Head of the eligible tenants to reduce the in-memory series.
func (i *Ingester) compactBlocksToReduceInMemorySeries(ctx context.Context, now time.Time) {
	// Skip if disabled.
	if i.cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinInMemorySeries <= 0 || !i.cfg.ActiveSeriesMetrics.Enabled {
		return
	}

	// No need to prematurely compact TSDB heads if the number of in-memory series is below a critical threshold.
	totalMemorySeries := i.seriesCount.Load()
	if totalMemorySeries < i.cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinInMemorySeries {
		return
	}

	level.Info(i.logger).Log("msg", "the number of in-memory series is higher than the configured early compaction threshold", "in_memory_series", totalMemorySeries, "early_compaction_threshold", i.cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinInMemorySeries)

	// Estimates the series reduction opportunity for each tenant.
	var (
		userIDs     = i.getTSDBUsers()
		estimations = make([]seriesReductionEstimation, 0, len(userIDs))
	)

	for _, userID := range userIDs {
		db := i.getTSDB(userID)
		if db == nil {
			continue
		}

		userMemorySeries := db.Head().NumSeries()
		if userMemorySeries == 0 {
			continue
		}

		// Purge the active series so that the next call to Active() will return the up-to-date count.
		idx := db.Head().MustIndex()
		db.activeSeries.Purge(now, idx)
		idx.Close()

		// Estimate the number of series that would be dropped from the TSDB Head if we would
		// compact the head up until "now - active series idle timeout".
		totalActiveSeries, _, _, _ := db.activeSeries.Active()
		estimatedSeriesReduction := max(0, int64(userMemorySeries)-int64(totalActiveSeries))
		estimations = append(estimations, seriesReductionEstimation{
			userID:              userID,
			estimatedCount:      estimatedSeriesReduction,
			estimatedPercentage: int((uint64(estimatedSeriesReduction) * 100) / userMemorySeries),
		})
	}

	usersToCompact := filterUsersToCompactToReduceInMemorySeries(totalMemorySeries, i.cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinInMemorySeries, i.cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinEstimatedSeriesReductionPercentage, estimations)
	if len(usersToCompact) == 0 {
		level.Info(i.logger).Log("msg", "no viable per-tenant TSDB found to early compact in order to reduce in-memory series")
		return
	}

	level.Info(i.logger).Log("msg", "running TSDB head compaction to reduce the number of in-memory series", "users", strings.Join(usersToCompact, " "))
	forcedCompactionMaxTime := now.Add(-i.cfg.ActiveSeriesMetrics.IdleTimeout).UnixMilli()
	i.compactBlocks(ctx, true, forcedCompactionMaxTime, util.NewAllowList(usersToCompact, nil))

	// Update last compaction time for all compacted users
	for _, userID := range usersToCompact {
		if db := i.getTSDB(userID); db != nil {
			db.setLastEarlyCompaction(now)
		}
	}

	level.Info(i.logger).Log("msg", "run TSDB head compaction to reduce the number of in-memory series", "before_in_memory_series", totalMemorySeries, "after_in_memory_series", i.seriesCount.Load())
}

func (i *Ingester) anyUserHasEarlyHeadCompactionEnabled() bool {
	for _, userID := range i.getTSDBUsers() {
		if i.limits.EarlyHeadCompactionOwnedSeriesThreshold(userID) > 0 {
			return true
		}
	}
	return false
}

// compactBlocksToReducePerTenantOwnedSeries compacts the TSDB Head for tenants that exceed their per-tenant early compaction threshold.
func (i *Ingester) compactBlocksToReducePerTenantOwnedSeries(ctx context.Context, now time.Time) {
	// Early return if active series metrics are not enabled (required for series reduction estimation) or if owned series are not used for limits.
	if !i.cfg.ActiveSeriesMetrics.Enabled || !i.cfg.UseIngesterOwnedSeriesForLimits {
		if i.anyUserHasEarlyHeadCompactionEnabled() {
			level.Warn(i.logger).Log("msg", "per-tenant early head compaction is enabled, but active series metrics are not enabled or owned series are not used for limits", "active_series_metrics_enabled", i.cfg.ActiveSeriesMetrics.Enabled, "use_ingester_owned_series_for_limits", i.cfg.UseIngesterOwnedSeriesForLimits)
		}
		return
	}

	idleTimeout := i.cfg.ActiveSeriesMetrics.IdleTimeout
	forcedCompactionMaxTime := now.Add(-idleTimeout).UnixMilli()

	for _, userID := range i.getTSDBUsers() {
		if ctx.Err() != nil {
			return
		}

		// Get per-tenant limits
		threshold := i.limits.EarlyHeadCompactionOwnedSeriesThreshold(userID)
		if threshold <= 0 {
			continue // Per-tenant early compaction disabled for this tenant
		}

		db := i.getTSDB(userID)
		if db == nil {
			continue
		}

		minReductionPercentage := i.limits.EarlyHeadCompactionMinEstimatedSeriesReductionPercentage(userID)

		// Check cooldown - don't trigger if compacted less than IdleTimeout ago
		if lastCompaction := db.getLastEarlyCompaction(); !lastCompaction.IsZero() && now.Sub(lastCompaction) < idleTimeout {
			continue
		}

		// Get owned series count
		ownedState := db.ownedSeriesState()
		ownedSeriesCount := ownedState.ownedSeriesCount

		// Check if owned series exceeds threshold
		localThreshold := i.limiter.ringStrategy.convertGlobalToLocalLimit(userID, threshold)
		if localThreshold <= 0 || ownedSeriesCount < localThreshold {
			continue
		}

		// Estimate series reduction
		userMemorySeries := db.Head().NumSeries()
		if userMemorySeries == 0 {
			continue
		}

		// Purge active series to get accurate count
		idx := db.Head().MustIndex()
		db.activeSeries.Purge(now, idx)
		_ = idx.Close()

		totalActiveSeries, _, _, _ := db.activeSeries.Active()
		estimatedSeriesReduction := max(0, int64(userMemorySeries)-int64(totalActiveSeries))
		estimatedPercentage := int((uint64(estimatedSeriesReduction) * 100) / userMemorySeries)

		// Check if estimated reduction meets threshold
		if estimatedPercentage < minReductionPercentage {
			continue
		}

		level.Info(i.logger).Log(
			"msg", "triggering per-tenant early head compaction",
			"user", userID,
			"owned_series", ownedSeriesCount,
			"global_threshold", threshold,
			"estimated_local_threshold", localThreshold,
			"estimated_series_reduction", estimatedSeriesReduction,
			"estimated_reduction_percentage", estimatedPercentage,
		)

		// Trigger compaction for this user
		i.compactBlocks(ctx, true, forcedCompactionMaxTime, util.NewAllowList([]string{userID}, nil))

		// Update metrics and last compaction time
		i.metrics.perTenantEarlyCompactionsTriggered.Inc()
		db.setLastEarlyCompaction(now)

		level.Info(i.logger).Log(
			"msg", "per-tenant early head compaction completed",
			"user", userID,
			"before_in_memory_series", userMemorySeries,
			"after_in_memory_series", db.Head().NumSeries(),
		)
	}
}

type seriesReductionEstimation struct {
	userID              string
	estimatedCount      int64
	estimatedPercentage int
}

func filterUsersToCompactToReduceInMemorySeries(numMemorySeries, earlyCompactionMinSeries int64, earlyCompactionMinPercentage int, estimations []seriesReductionEstimation) []string {
	var (
		usersToCompact        []string
		seriesReductionSum    = int64(0)
		seriesReductionTarget = numMemorySeries - earlyCompactionMinSeries
	)

	// Skip if the estimated series reduction is too low (there would be no big benefit).
	totalEstimatedSeriesReduction := int64(0)
	for _, entry := range estimations {
		totalEstimatedSeriesReduction += entry.estimatedCount
	}

	if (totalEstimatedSeriesReduction*100)/numMemorySeries < int64(earlyCompactionMinPercentage) {
		return nil
	}

	// Compact all TSDBs blocks required to get the number of in-memory series below the threshold and, in addition,
	// all TSDBs where the estimated series reduction is greater than the minimum reduction percentage.
	slices.SortFunc(estimations, func(a, b seriesReductionEstimation) int {
		switch {
		case b.estimatedCount < a.estimatedCount:
			return -1
		case b.estimatedCount > a.estimatedCount:
			return 1
		default:
			return 0
		}
	})

	for _, entry := range estimations {
		if seriesReductionSum < seriesReductionTarget || entry.estimatedPercentage >= earlyCompactionMinPercentage {
			usersToCompact = append(usersToCompact, entry.userID)
			seriesReductionSum += entry.estimatedCount
		}
	}

	return usersToCompact
}

// timeUntilCompaction calculates the precise time until the next compaction for a specific zone
// based on the current time, the configured compaction interval, and the computed zone's offset.
//
// This creates a predictable, clock-aligned schedule where each zone compacts at fixed times
// (e.g., zone 'a' at :00, :15, :30, :45 and zone 'b' at :07, :22, :37, :52 for a 15-minute interval).
//
// Returns the interval until the next scheduled compaction for the zone as a time.Duration.
func timeUntilCompaction(now time.Time, compactionInterval, zoneOffset time.Duration) time.Duration {
	// Calculate how much time has elapsed since the start of the current hour.
	elapsed := now.Sub(now.Truncate(time.Hour))

	// Calculate how long elapsed since the last time compaction should have run for this zone.
	// compactionInterval is guaranteed to be more than 0 and less than 15m.
	timeSinceLastCompaction := (elapsed - zoneOffset) % compactionInterval
	if timeSinceLastCompaction < 0 {
		timeSinceLastCompaction += compactionInterval
	}

	return compactionInterval - timeSinceLastCompaction
}
