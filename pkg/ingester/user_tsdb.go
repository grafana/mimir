// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester_v2.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/grafana/dskit/multierror"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
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

type userTSDB struct {
	db             *tsdb.DB
	ephemeral      *tsdb.Head // storage for temporary samples
	userID         string
	activeSeries   *activeseries.ActiveSeries
	seriesInMetric *metricCounter
	limiter        *Limiter

	instanceSeriesCount *atomic.Int64 // Shared across all userTSDB instances created by ingester.
	instanceLimitsFn    func() *InstanceLimits

	stateMtx       sync.RWMutex
	state          tsdbState
	pushesInFlight sync.WaitGroup // Increased with stateMtx read lock held, only if state == active or activeShipping.

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

	// Cached shipped blocks.
	shippedBlocksMtx sync.Mutex
	shippedBlocks    map[ulid.ULID]struct{}
}

// Explicitly wrapping the tsdb.DB functions that we use.

func (u *userTSDB) Appender(ctx context.Context) storage.Appender {
	return u.db.Appender(ctx)
}

func (u *userTSDB) EphemeralAppender(ctx context.Context) storage.Appender {
	if u.ephemeral == nil {
		return nil
	}

	return u.ephemeral.Appender(ctx)
}

func getQuerier[T any](wantEq, wantPq bool, getEq, getPq func() (T, error), merge func(T, T) T) (T, error) {
	var eq, pq T
	var err error

	if !wantEq && !wantPq {
		return eq, fmt.Errorf("no querier wanted")
	}

	if wantEq {
		eq, err = getEq()
		if err != nil {
			return eq, err
		}

		if !wantPq {
			return eq, nil
		}
	}

	pq, err = getPq()
	if err != nil {
		return pq, err
	}

	if wantEq {
		return merge(eq, pq), nil
	}

	return pq, nil
}

// Querier returns a new querier over the data partition for the given time range.
func (u *userTSDB) Querier(ctx context.Context, mint, maxt int64, ephemeral, persistent bool) (storage.Querier, error) {
	return getQuerier(
		ephemeral,
		persistent,
		func() (storage.Querier, error) {
			if u.ephemeral == nil {
				return nil, fmt.Errorf("ephemeral data not available")
			}

			return tsdb.NewBlockQuerier(u.ephemeral, mint, maxt)
		},
		func() (storage.Querier, error) {
			return u.db.Querier(ctx, mint, maxt)
		},
		func(q1, q2 storage.Querier) storage.Querier {
			return storage.NewMergeQuerier([]storage.Querier{q1, q2}, nil, storage.ChainedSeriesMerge)
		},
	)
}

func (u *userTSDB) ChunkQuerier(ctx context.Context, mint, maxt int64, ephemeral bool, persistent bool) (storage.ChunkQuerier, error) {
	return getQuerier(
		ephemeral,
		persistent,
		func() (storage.ChunkQuerier, error) {
			if u.ephemeral == nil {
				return nil, fmt.Errorf("ephemeral data not available")
			}

			return tsdb.NewBlockChunkQuerier(u.ephemeral, mint, maxt)
		},
		func() (storage.ChunkQuerier, error) {
			return u.db.ChunkQuerier(ctx, mint, maxt)
		},
		func(q1, q2 storage.ChunkQuerier) storage.ChunkQuerier {
			return storage.NewMergeChunkQuerier([]storage.ChunkQuerier{q1, q2}, nil, storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge))
		},
	)
}

func (u *userTSDB) UnorderedChunkQuerier(ctx context.Context, mint, maxt int64, ephemeral, persistent bool) (storage.ChunkQuerier, error) {
	return getQuerier(
		ephemeral,
		persistent,
		func() (storage.ChunkQuerier, error) {
			if u.ephemeral == nil {
				return nil, fmt.Errorf("ephemeral data not available")
			}

			return tsdb.NewBlockChunkQuerier(u.ephemeral, mint, maxt)
		},
		func() (storage.ChunkQuerier, error) {
			return u.db.UnorderedChunkQuerier(ctx, mint, maxt)
		},
		func(q1, q2 storage.ChunkQuerier) storage.ChunkQuerier {
			return storage.NewMergeChunkQuerier([]storage.ChunkQuerier{q1, q2}, nil, storage.NewCompactingChunkSeriesMerger(storage.ChainedSeriesMerge))
		},
	)
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
	var merr multierror.MultiError
	if u.ephemeral != nil {
		merr.Add(u.ephemeral.Truncate(time.Now().Add(-10 * time.Minute).UnixMilli()))
	}

	merr.Add(u.db.Compact())
	return merr.Err()
}

func (u *userTSDB) StartTime() (int64, error) {
	return u.db.StartTime()
}

func (u *userTSDB) casState(from, to tsdbState) bool {
	u.stateMtx.Lock()
	defer u.stateMtx.Unlock()

	if u.state != from {
		return false
	}
	u.state = to
	return true
}

// compactHead compacts the Head block at specified block durations avoiding a single huge block.
func (u *userTSDB) compactHead(blockDuration int64) error {
	if !u.casState(active, forceCompacting) {
		return errors.New("TSDB head cannot be compacted because it is not in active state (possibly being closed or blocks shipping in progress)")
	}

	defer u.casState(forceCompacting, active)

	// Ingestion of samples in parallel with forced compaction can lead to overlapping blocks,
	// and possible invalidation of the references returned from Appender.GetRef().
	// So we wait for existing in-flight requests to finish. Future push requests would fail until compaction is over.
	u.pushesInFlight.Wait()

	h := u.Head()

	minTime, maxTime := h.MinTime(), h.MaxTime()

	for (minTime/blockDuration)*blockDuration != (maxTime/blockDuration)*blockDuration {
		// Data in Head spans across multiple block ranges, so we break it into blocks here.
		// Block max time is exclusive, so we do a -1 here.
		blockMaxTime := ((minTime/blockDuration)+1)*blockDuration - 1
		if err := u.db.CompactHead(tsdb.NewRangeHead(h, minTime, blockMaxTime)); err != nil {
			return err
		}

		// Get current min/max times after compaction.
		minTime, maxTime = h.MinTime(), h.MaxTime()
	}

	return u.db.CompactHead(tsdb.NewRangeHead(h, minTime, maxTime))
}

// PreCreation implements SeriesLifecycleCallback interface.
func (u *userTSDB) PreCreation(metric labels.Labels) error {
	if u.limiter == nil {
		return nil
	}

	// Verify ingester's global limit
	gl := u.instanceLimitsFn()
	if gl != nil && gl.MaxInMemorySeries > 0 {
		if series := u.instanceSeriesCount.Load(); series >= gl.MaxInMemorySeries {
			return errMaxInMemorySeriesReached
		}
	}

	// Total series limit.
	if err := u.limiter.AssertMaxSeriesPerUser(u.userID, int(u.Head().NumSeries())); err != nil {
		return err
	}

	// Series per metric name limit.
	metricName, err := extract.MetricNameFromLabels(metric)
	if err != nil {
		return err
	}
	if err := u.seriesInMetric.canAddSeriesFor(u.userID, metricName); err != nil {
		return err
	}

	return nil
}

// PostCreation implements SeriesLifecycleCallback interface.
func (u *userTSDB) PostCreation(metric labels.Labels) {
	u.instanceSeriesCount.Inc()

	metricName, err := extract.MetricNameFromLabels(metric)
	if err != nil {
		// This should never happen because it has already been checked in PreCreation().
		return
	}
	u.seriesInMetric.increaseSeriesForMetric(metricName)
}

// PostDeletion implements SeriesLifecycleCallback interface.
func (u *userTSDB) PostDeletion(metrics ...labels.Labels) {
	u.instanceSeriesCount.Sub(int64(len(metrics)))

	for _, metric := range metrics {
		metricName, err := extract.MetricNameFromLabels(metric)
		if err != nil {
			// This should never happen because it has already been checked in PreCreation().
			continue
		}
		u.seriesInMetric.decreaseSeriesForMetric(metricName)
	}
}

// blocksToDelete filters the input blocks and returns the blocks which are safe to be deleted from the ingester.
func (u *userTSDB) blocksToDelete(blocks []*tsdb.Block) map[ulid.ULID]struct{} {
	if u.db == nil {
		return nil
	}
	deletable := tsdb.DefaultBlocksToDelete(u.db)(blocks)
	if u.shipper == nil {
		return deletable
	}

	shippedBlocks := u.getCachedShippedBlocks()

	result := map[ulid.ULID]struct{}{}
	for shippedID := range shippedBlocks {
		if _, ok := deletable[shippedID]; ok {
			result[shippedID] = struct{}{}
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
func (u *userTSDB) getCachedShippedBlocks() map[ulid.ULID]struct{} {
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
	lu := u.lastUpdate.Load()

	return time.Unix(lu, 0).Add(idle).Before(now)
}

func (u *userTSDB) setLastUpdate(t time.Time) {
	u.lastUpdate.Store(t.Unix())
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

func (u *userTSDB) acquireAppendLock() error {
	u.stateMtx.RLock()
	defer u.stateMtx.RUnlock()

	switch u.state {
	case active:
	case activeShipping:
		// Pushes are allowed.
	case forceCompacting:
		return errors.New("forced compaction in progress")
	case closing:
		return errors.New("TSDB is closing")
	default:
		return errors.New("TSDB is not active")
	}

	u.pushesInFlight.Add(1)
	return nil
}

func (u *userTSDB) releaseAppendLock() {
	u.pushesInFlight.Done()
}
