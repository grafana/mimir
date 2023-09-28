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
	errTSDBForcedCompaction = errors.New("TSDB Head forced compaction in progress and no write request is currently allowed")
	errTSDBEarlyCompaction  = errors.New("TSDB Head early compaction in progress and the write request contains samples overlapping with it")
	errTSDBClosing          = errors.New("TSDB is closing")
	errTSDBNotActive        = errors.New("TSDB is not active")
)

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

func (u *userTSDB) UnorderedChunkQuerier(ctx context.Context, mint, maxt int64) (storage.ChunkQuerier, error) {
	return u.db.UnorderedChunkQuerier(ctx, mint, maxt)
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
	maxTime = util_math.Min(headMaxTime, forcedMaxTime)

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
	if !u.limiter.IsWithinMaxSeriesPerUser(u.userID, int(u.Head().NumSeries())) {
		return errMaxSeriesPerUserLimitExceeded
	}

	// Series per metric name limit.
	metricName, err := extract.MetricNameFromLabels(metric)
	if err != nil {
		return err
	}
	if !u.seriesInMetric.canAddSeriesFor(u.userID, metricName) {
		return errMaxSeriesPerMetricLimitExceeded
	}

	return nil
}

func (u *userTSDB) PostCreation(metric labels.Labels) {
	u.instanceSeriesCount.Inc()

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
