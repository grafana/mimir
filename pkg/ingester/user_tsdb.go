// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester_v2.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/ingester/activeseries"
	"github.com/grafana/mimir/pkg/util/extract"
	"github.com/grafana/mimir/pkg/util/globalerror"
	util_math "github.com/grafana/mimir/pkg/util/math"
)

type tsdbState int

const (
	active          tsdbState = iota // Pushes are allowed.
	activeShipping                   // Pushes are allowed. Blocks shipping is in progress.
	forceCompacting                  // TSDB is being force-compacted.
	closing                          // Used while closing idle TSDB.
	closed                           // Used to avoid setting closing back to active in closeAndDeleteIdleUsers method.
)

func (s tsdbState) String() string {
	switch s {
	case active:
		return "active"
	case activeShipping:
		return "activeShipping"
	case forceCompacting:
		return "forceCompacting"
	case closing:
		return "closing"
	case closed:
		return "closed"
	default:
		return "unknown"
	}
}

// Describes result of TSDB-close check. String is used as metric label.
type tsdbCloseCheckResult string

const (
	tsdbIdle                    tsdbCloseCheckResult = "idle" // Not reported via metrics. Metrics use tsdbIdleClosed on success.
	tsdbShippingDisabled        tsdbCloseCheckResult = "shipping_disabled"
	tsdbNotIdle                 tsdbCloseCheckResult = "not_idle"
	tsdbNotCompacted            tsdbCloseCheckResult = "not_compacted"
	tsdbNotShipped              tsdbCloseCheckResult = "not_shipped"
	tsdbCheckFailed             tsdbCloseCheckResult = "check_failed"
	tsdbCloseFailed             tsdbCloseCheckResult = "close_failed"
	tsdbNotActive               tsdbCloseCheckResult = "not_active"
	tsdbDataRemovalFailed       tsdbCloseCheckResult = "data_removal_failed"
	tsdbTenantMarkedForDeletion tsdbCloseCheckResult = "tenant_marked_for_deletion"
	tsdbIdleClosed              tsdbCloseCheckResult = "idle_closed" // Success.
)

func (r tsdbCloseCheckResult) shouldClose() bool {
	return r == tsdbIdle || r == tsdbTenantMarkedForDeletion
}

var (
	errTSDBForcedCompaction = newTSDBUnavailableError("TSDB Head forced compaction in progress and no write request is currently allowed")
	errTSDBEarlyCompaction  = newTSDBUnavailableError("TSDB Head early compaction in progress and the write request contains samples overlapping with it")
	errTSDBClosing          = newTSDBUnavailableError("TSDB is closing")
	errTSDBNotActive        = newTSDBUnavailableError("TSDB is not active")
)

type ownedSeriesState struct {
	ownedSeriesCount int // Number of "owned" series, based on current ring.
	shardSize        int // Tenant shard size when "owned" series was last updated due to ring or shard size changes. Used to detect shard size changes.
	localSeriesLimit int // Local series limit when "owned" series was last updated due to ring or shard size changes. Used as a minimum when calculating series limits.
}

type userTSDB struct {
	db             *tsdb.DB
	userID         string
	activeSeries   *activeseries.ActiveSeries
	seriesInMetric *metricCounter
	limiter        *Limiter

	instanceSeriesCount *atomic.Int64 // Shared across all userTSDB instances created by ingester.
	instanceLimitsFn    func() *InstanceLimits
	instanceErrors      *prometheus.CounterVec

	stateMtx                                     sync.RWMutex
	state                                        tsdbState
	inFlightAppends                              sync.WaitGroup // Increased with stateMtx read lock held.
	inFlightAppendsStartedBeforeForcedCompaction sync.WaitGroup // Increased with stateMtx read lock held.
	forcedCompactionMaxTime                      int64          // Max timestamp of samples that will be compacted from the TSDB head during a forced o early compaction.

	// Used to detect idle TSDBs.
	lastUpdate atomic.Int64

	// Thanos shipper used to upload blocks to the storage.
	shipper BlocksUploader

	// When deletion marker is found for the tenant (checked before shipping),
	// shipping stops and TSDB is closed before reaching idle timeout time (if enabled).
	deletionMarkFound atomic.Bool

	// Unix timestamp of last deletion mark check.
	lastDeletionMarkCheck atomic.Int64

	// for statistics
	ingestedAPISamples  *util_math.EwmaRate
	ingestedRuleSamples *util_math.EwmaRate

	// Block min retention
	blockMinRetention time.Duration

	// Cached shipped blocks.
	shippedBlocksMtx sync.Mutex
	shippedBlocks    map[ulid.ULID]time.Time

	useOwnedSeriesForLimits bool

	// We use a mutex so that we can update count, shard size, and local limit at the same time (when updating owned series count).
	ownedStateMtx sync.Mutex
	ownedState    ownedSeriesState

	// Only accessed by ownedSeries service, no need to synchronization.
	ownedTokenRanges ring.TokenRanges

	requiresOwnedSeriesUpdate atomic.String // Non-empty string means that we need to recompute "owned series" for the user. Value will be used in the log message.
}

func (u *userTSDB) Appender(ctx context.Context) storage.Appender {
	return u.db.Appender(ctx)
}

// Querier returns a new querier over the data partition for the given time range.
func (u *userTSDB) Querier(mint, maxt int64) (storage.Querier, error) {
	return u.db.Querier(mint, maxt)
}

func (u *userTSDB) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	return u.db.ChunkQuerier(mint, maxt)
}

func (u *userTSDB) UnorderedChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	return u.db.UnorderedChunkQuerier(mint, maxt)
}

func (u *userTSDB) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return u.db.ExemplarQuerier(ctx)
}

func (u *userTSDB) Head() *tsdb.Head {
	return u.db.Head()
}

func (u *userTSDB) Blocks() []*tsdb.Block {
	return u.db.Blocks()
}

func (u *userTSDB) Close() error {
	return u.db.Close()
}

func (u *userTSDB) Compact() error {
	return u.db.Compact(context.Background())
}

func (u *userTSDB) StartTime() (int64, error) {
	return u.db.StartTime()
}

// changeState atomically compare-and-swap the current state, and returns state after the operation.
func (u *userTSDB) changeState(from, to tsdbState, updates ...func()) (bool, tsdbState) {
	u.stateMtx.Lock()
	defer u.stateMtx.Unlock()

	if u.state != from {
		return false, u.state
	}
	u.state = to

	// Run any custom update while the lock is held.
	for _, update := range updates {
		update()
	}

	return true, u.state
}

// changeStateToForcedCompaction atomically compare-and-swap the current state to forceCompacting,
// setting the forcedCompactionMaxTime too.
func (u *userTSDB) changeStateToForcedCompaction(from tsdbState, forcedCompactionMaxTime int64) (bool, tsdbState) {
	return u.changeState(from, forceCompacting, func() {
		u.forcedCompactionMaxTime = forcedCompactionMaxTime
	})
}

// compactHead triggers a forced compaction of the TSDB Head. This function compacts the in-order Head
// block with the specified block duration and the OOO Head block at the chunk range duration, to avoid
// having huge blocks.
//
// The input forcedMaxTime allows to specify the maximum timestamp of samples compacted from the
// in-order Head. You can pass math.MaxInt64 to compact the entire in-order Head.
func (u *userTSDB) compactHead(blockDuration, forcedCompactionMaxTime int64) error {
	if ok, s := u.changeStateToForcedCompaction(active, forcedCompactionMaxTime); !ok {
		return fmt.Errorf("TSDB head cannot be compacted because it is not in active state (possibly being closed or blocks shipping in progress): %s", s.String())
	}

	defer u.changeState(forceCompacting, active)

	// Ingestion of samples with a time range overlapping with forced compaction can lead to overlapping blocks.
	// For this reason, we wait for existing in-flight requests to finish, except the ones that have been intentionally
	// allowed while forced compaction was in progress because they append samples newer than forcedMaxTime
	// (requests appending samples older than forcedMaxTime will fail until forced compaction is completed).
	u.inFlightAppendsStartedBeforeForcedCompaction.Wait()

	// Compact the TSDB head.
	h := u.Head()
	for {
		blockMinTime, blockMaxTime, isValid, isLast := nextForcedHeadCompactionRange(blockDuration, h.MinTime(), h.MaxTime(), forcedCompactionMaxTime)
		if !isValid {
			break
		}

		if err := u.db.CompactHead(tsdb.NewRangeHead(h, blockMinTime, blockMaxTime)); err != nil {
			return err
		}

		// Do not check again if it was the last range.
		if isLast {
			break
		}
	}

	return u.db.CompactOOOHead(context.Background())
}

// nextForcedHeadCompactionRange computes the next TSDB head range to compact when a forced compaction
// is triggered. If the returned isValid is false, then the returned range should not be compacted.
func nextForcedHeadCompactionRange(blockDuration, headMinTime, headMaxTime, forcedMaxTime int64) (minTime, maxTime int64, isValid, isLast bool) {
	// Nothing to compact if the head is empty.
	if headMinTime == math.MaxInt64 || headMaxTime == math.MinInt64 {
		return 0, 0, false, true
	}

	// By default we try to compact the whole head, honoring the forcedMaxTime.
	minTime = headMinTime
	maxTime = min(headMaxTime, forcedMaxTime)

	// Due to the forcedMaxTime, the range may be empty. In that case we just skip it.
	if maxTime < minTime {
		return 0, 0, false, true
	}

	// Check whether the head compaction range would span across multiple block ranges.
	// If so, we break it to honor the block range period.
	if (minTime/blockDuration)*blockDuration != (maxTime/blockDuration)*blockDuration {
		// Block max time is exclusive, so we do a -1 here.
		maxTime = ((minTime/blockDuration)+1)*blockDuration - 1
		return minTime, maxTime, true, false
	}

	return minTime, maxTime, true, true
}

func (u *userTSDB) PreCreation(metric labels.Labels) error {
	if u.limiter == nil {
		return nil
	}

	// Verify ingester's global limit
	gl := u.instanceLimitsFn()
	if gl != nil && gl.MaxInMemorySeries > 0 {
		if series := u.instanceSeriesCount.Load(); series >= gl.MaxInMemorySeries {
			u.instanceErrors.WithLabelValues(reasonIngesterMaxInMemorySeries).Inc()
			return errMaxInMemorySeriesReached
		}
	}

	// Total series limit.
	series, minLocalLimit := u.getSeriesCountAndMinLocalLimit()
	if !u.limiter.IsWithinMaxSeriesPerUser(u.userID, series, minLocalLimit) {
		return globalerror.MaxSeriesPerUser
	}

	// Series per metric name limit.
	metricName, err := extract.MetricNameFromLabels(metric)
	if err != nil {
		return err
	}
	if !u.seriesInMetric.canAddSeriesFor(u.userID, metricName) {
		return globalerror.MaxSeriesPerMetric
	}

	return nil
}

// getSeriesCountAndMinLocalLimit returns current number of series and minimum local limit that should be used for computing
// series limit.
func (u *userTSDB) getSeriesCountAndMinLocalLimit() (int, int) {
	if u.useOwnedSeriesForLimits {
		os := u.ownedSeriesState()
		return os.ownedSeriesCount, os.localSeriesLimit
	}

	count := int(u.Head().NumSeries())
	minLocalLimit := 0
	return count, minLocalLimit
}

func (u *userTSDB) PostCreation(metric labels.Labels) {
	u.instanceSeriesCount.Inc()

	// If series was just created, it must belong to this ingester. (Unless it was created while replaying WAL,
	// but we will recompute owned series when ingester joins the ring.)
	u.ownedStateMtx.Lock()
	u.ownedState.ownedSeriesCount++
	u.ownedStateMtx.Unlock()

	metricName, err := extract.MetricNameFromLabels(metric)
	if err != nil {
		// This should never happen because it has already been checked in PreCreation().
		return
	}
	u.seriesInMetric.increaseSeriesForMetric(metricName)
}

func (u *userTSDB) PostDeletion(metrics map[chunks.HeadSeriesRef]labels.Labels) {
	u.instanceSeriesCount.Sub(int64(len(metrics)))

	for _, lbls := range metrics {
		metricName, err := extract.MetricNameFromLabels(lbls)
		if err != nil {
			// This should never happen because it has already been checked in PreCreation().
			continue
		}
		u.seriesInMetric.decreaseSeriesForMetric(metricName)
	}

	// We cannot update ownedSeriesCount here, as we don't know whether deleted series were owned by this ingester or not.
	// Instead, we recompute owned series after each compaction.

	u.activeSeries.PostDeletion(metrics)
}

// blocksToDelete filters the input blocks and returns the blocks which are safe to be deleted from the ingester.
func (u *userTSDB) blocksToDelete(blocks []*tsdb.Block) map[ulid.ULID]struct{} {
	if u.db == nil {
		return nil
	}

	deletable := tsdb.DefaultBlocksToDelete(u.db)(blocks)
	result := map[ulid.ULID]struct{}{}
	deadline := time.Now().Add(-u.blockMinRetention)

	// The shipper enabled case goes first because its common in the way we run the ingesters
	if u.shipper != nil {
		shippedBlocks := u.getCachedShippedBlocks()

		for blockID := range deletable {
			shippedBlockTime, ok := shippedBlocks[blockID]
			if ok && shippedBlockTime.Before(deadline) {
				result[blockID] = struct{}{}
			}
		}
		return result
	}

	for blockID := range deletable {
		blockCreationTime := time.UnixMilli(int64(blockID.Time()))
		if blockCreationTime.Before(deadline) {
			result[blockID] = struct{}{}
		}
	}

	return result
}

// updateCachedShippedBlocks reads the shipper meta file and updates the cached shipped blocks.
func (u *userTSDB) updateCachedShippedBlocks() error {
	shippedBlocks, err := readShippedBlocks(u.db.Dir())
	if err != nil {
		return err
	}

	// Cache it.
	u.shippedBlocksMtx.Lock()
	u.shippedBlocks = shippedBlocks
	u.shippedBlocksMtx.Unlock()

	return nil
}

// getCachedShippedBlocks returns the cached shipped blocks.
func (u *userTSDB) getCachedShippedBlocks() map[ulid.ULID]time.Time {
	u.shippedBlocksMtx.Lock()
	defer u.shippedBlocksMtx.Unlock()

	// It's safe to directly return the map because it's never updated in-place.
	return u.shippedBlocks
}

// getOldestUnshippedBlockTime returns the unix timestamp with milliseconds precision of the oldest
// TSDB block not shipped to the storage yet, or 0 if all blocks have been shipped.
func (u *userTSDB) getOldestUnshippedBlockTime() uint64 {
	shippedBlocks := u.getCachedShippedBlocks()
	oldestTs := uint64(0)

	for _, b := range u.Blocks() {
		if _, ok := shippedBlocks[b.Meta().ULID]; ok {
			continue
		}

		if oldestTs == 0 || b.Meta().ULID.Time() < oldestTs {
			oldestTs = b.Meta().ULID.Time()
		}
	}

	return oldestTs
}

func (u *userTSDB) isIdle(now time.Time, idle time.Duration) bool {
	return u.getLastUpdate().Add(idle).Before(now)
}

func (u *userTSDB) setLastUpdate(t time.Time) {
	u.lastUpdate.Store(t.UnixMilli())
}

func (u *userTSDB) getLastUpdate() time.Time {
	return time.UnixMilli(u.lastUpdate.Load())
}

// Checks if TSDB can be closed.
func (u *userTSDB) shouldCloseTSDB(idleTimeout time.Duration) tsdbCloseCheckResult {
	if u.deletionMarkFound.Load() {
		return tsdbTenantMarkedForDeletion
	}

	if !u.isIdle(time.Now(), idleTimeout) {
		return tsdbNotIdle
	}

	// If head is not compacted, we cannot close this yet.
	if u.Head().NumSeries() > 0 {
		return tsdbNotCompacted
	}

	// Ensure that all blocks have been shipped.
	if oldest := u.getOldestUnshippedBlockTime(); oldest > 0 {
		return tsdbNotShipped
	}

	return tsdbIdle
}

// acquireAppendLock acquires a lock to append to the per-tenant TSDB. The minTimestamp
// parameter must specify the lowest timestamp value that is going to be appended to
// TSDB while the lock is held.
func (u *userTSDB) acquireAppendLock(minTimestamp int64) (tsdbState, error) {
	u.stateMtx.RLock()
	defer u.stateMtx.RUnlock()

	switch u.state {
	case active:
	case activeShipping:
		// Pushes are allowed.
	case forceCompacting:
		if u.forcedCompactionMaxTime == math.MaxInt64 {
			return u.state, errTSDBForcedCompaction
		}
		if minTimestamp <= u.forcedCompactionMaxTime {
			return u.state, errors.Wrapf(errTSDBEarlyCompaction, "request_min_timestamp: %s allowed_min_timestamp: %s", time.UnixMilli(minTimestamp).String(), time.UnixMilli(u.forcedCompactionMaxTime+1).String())
		}
	case closing:
		return u.state, errTSDBClosing
	default:
		return u.state, errTSDBNotActive
	}

	u.inFlightAppends.Add(1)
	if u.state != forceCompacting {
		u.inFlightAppendsStartedBeforeForcedCompaction.Add(1)
	}

	return u.state, nil
}

// releaseAppendLock releases the lock acquired calling acquireAppendLock().
// The input acquireState MUST be the state returned by acquireAppendLock().
func (u *userTSDB) releaseAppendLock(acquireState tsdbState) {
	u.inFlightAppends.Done()
	if acquireState != forceCompacting {
		u.inFlightAppendsStartedBeforeForcedCompaction.Done()
	}
}

// ownedSeriesState returns a copy of the current state
func (u *userTSDB) ownedSeriesState() ownedSeriesState {
	u.ownedStateMtx.Lock()
	defer u.ownedStateMtx.Unlock()

	return u.ownedState
}

func (u *userTSDB) getAndClearReasonForRecomputeOwnedSeries() string {
	return u.requiresOwnedSeriesUpdate.Swap("")
}

func (u *userTSDB) triggerRecomputeOwnedSeries(reason string) {
	u.requiresOwnedSeriesUpdate.CompareAndSwap("", reason)
}

// recomputeOwnedSeries recomputes owned series for current token ranges, and updates both owned series and shard size.
//
// This method returns false, if recomputation of owned series failed multiple times due to too
// many new series being added during the computation. If no such problem happened, this method returns true.
//
// This method and updateTokenRanges should be only called from the same goroutine. (ownedSeries service)
func (u *userTSDB) recomputeOwnedSeries(shardSize int, reason string, logger log.Logger) (success bool) {
	success, _ = u.recomputeOwnedSeriesWithComputeFn(shardSize, reason, logger, u.computeOwnedSeries)
	return success
}

const (
	recomputeOwnedSeriesMaxAttempts   = 3
	recomputeOwnedSeriesMaxSeriesDiff = 1000
)

func (u *userTSDB) recomputeOwnedSeriesWithComputeFn(shardSize int, reason string, logger log.Logger, compute func() int) (success bool, _ int) {
	start := time.Now()

	var ownedSeriesNew, ownedSeriesBefore, shardSizeBefore, localLimitBefore, localLimitNew int

	success = false
	attempts := 0
	for !success && attempts < recomputeOwnedSeriesMaxAttempts {
		attempts++

		os := u.ownedSeriesState()
		ownedSeriesBefore = os.ownedSeriesCount
		shardSizeBefore = os.shardSize
		localLimitBefore = os.localSeriesLimit

		localLimitNew = u.limiter.maxSeriesPerUser(u.userID, 0)
		ownedSeriesNew = compute()

		u.ownedStateMtx.Lock()

		// Check how many new series were added while we were computing owned series.
		// If too many series were created in the meantime, our new number of owned series may be wrong
		// (it may or may not include the new series, we don't know).
		// In that case, just run the computation again -- if there are more attempts left.
		seriesDiff := u.ownedState.ownedSeriesCount - ownedSeriesBefore
		if seriesDiff >= 0 && seriesDiff <= recomputeOwnedSeriesMaxSeriesDiff {
			success = true
		}

		// Even if we run computation again, we can start using our (possibly incorrect) values already.
		u.ownedState.ownedSeriesCount = ownedSeriesNew
		u.ownedState.shardSize = shardSize
		u.ownedState.localSeriesLimit = localLimitNew

		u.ownedStateMtx.Unlock()
	}

	var l log.Logger
	if success {
		l = level.Info(logger)
	} else {
		l = level.Warn(logger)
	}
	l.Log("msg", "owned series: recomputed owned series for user",
		"user", u.userID,
		"reason", reason,
		"ownedSeriesBefore", ownedSeriesBefore,
		"ownedSeriesNew", ownedSeriesNew,
		"shardSizeBefore", shardSizeBefore,
		"shardSizeNew", shardSize,
		"localLimitBefore", localLimitBefore,
		"localLimitNew", localLimitNew,
		"duration", time.Since(start),
		"attempts", attempts,
		"success", success)
	return success, attempts
}

// updateTokenRanges sets owned token ranges to supplied value, and returns true, if token ranges have changed.
//
// This method and recomputeOwnedSeries should be only called from the same goroutine. (ownedSeries service)
func (u *userTSDB) updateTokenRanges(newTokenRanges []uint32) bool {
	prev := u.ownedTokenRanges
	u.ownedTokenRanges = newTokenRanges

	return !prev.Equal(newTokenRanges)
}

func (u *userTSDB) computeOwnedSeries() int {
	// This can happen if ingester doesn't own this tenant anymore.
	if len(u.ownedTokenRanges) == 0 {
		u.activeSeries.Clear()
		return 0
	}

	count := 0
	idx := u.Head().MustIndex()
	defer idx.Close()

	u.Head().ForEachSecondaryHash(func(refs []chunks.HeadSeriesRef, secondaryHashes []uint32) {
		for i, sh := range secondaryHashes {
			if u.ownedTokenRanges.IncludesKey(sh) {
				count++
			} else {
				u.activeSeries.Delete(refs[i], idx)
			}
		}
	})
	return count
}
