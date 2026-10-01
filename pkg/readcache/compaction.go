// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"math"
	"math/rand"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/timeutil"

	"github.com/grafana/mimir/pkg/util"
)

// compactionIdleTimeoutJitter matches the ingester: idle flush waits
// HeadCompactionIdleTimeout plus up to 25%.
const compactionIdleTimeoutJitter = 0.25

func (r *Readcache) initCompactionSchedule() {
	r.compactionIdleTimeout = util.DurationWithPositiveJitter(r.cfg.BlocksStorage.TSDB.HeadCompactionIdleTimeout, compactionIdleTimeoutJitter)
}

func (r *Readcache) startCompactionTicker() (func(), <-chan time.Time) {
	if !r.cfg.IngesterScheduleCompaction {
		interval := r.cfg.HeadCompactionInterval
		if interval <= 0 {
			interval = time.Hour
		}
		t := time.NewTicker(interval)
		level.Info(r.logger).Log(
			"msg", "readcache compaction schedule",
			"mode", "legacy",
			"interval", interval,
		)
		return t.Stop, t.C
	}
	first, standard := r.compactionSchedule(time.Now())
	level.Info(r.logger).Log(
		"msg", "readcache compaction schedule",
		"mode", "ingester",
		"first_in", first,
		"interval", standard,
		"zone", r.cfg.InstanceRing.InstanceZone,
		"idle_timeout", r.compactionIdleTimeout,
	)
	return timeutil.NewVariableTicker(first, standard)
}

// headCompactionInterval is the ingester check interval. Cells set
// -blocks-storage.tsdb.head-compaction-interval (15m). The readcache
// flag default of 1h is only a fallback for tests that never register
// the blocks-storage flags.
func (r *Readcache) headCompactionInterval() time.Duration {
	interval := r.cfg.BlocksStorage.TSDB.HeadCompactionInterval
	if interval <= 0 {
		interval = r.cfg.HeadCompactionInterval
	}
	if interval <= 0 {
		return time.Minute
	}
	return interval
}

func (r *Readcache) compactionSchedule(now time.Time) (first, standard time.Duration) {
	interval := r.headCompactionInterval()
	var zones []string
	if r.instanceRing != nil {
		zones = r.instanceRing.Zones()
	}
	next := timeToNextReadcacheCompaction(now, interval, r.cfg.InstanceRing.InstanceZone, zones, r.logger)
	if !r.cfg.BlocksStorage.TSDB.HeadCompactionIntervalJitterEnabled || len(zones) == 0 {
		return next, interval
	}
	// Positive jitter of up to half a zone slot, so pods in one zone
	// do not share the tick. The variable ticker keeps this phase.
	halfSlot := (interval.Nanoseconds() / int64(len(zones))) / 2
	if halfSlot <= 0 {
		return next, interval
	}
	return next + time.Duration(rand.Int63n(halfSlot)), interval
}

// timeToNextReadcacheCompaction is the ingester's zone offset:
// interval/numZones, clock-aligned. A single zone, or a pod with no
// zone, waits the full interval.
func timeToNextReadcacheCompaction(now time.Time, interval time.Duration, zone string, zones []string, logger log.Logger) time.Duration {
	if zone == "" || len(zones) <= 1 {
		return interval
	}
	zones = slices.Clone(zones)
	slices.Sort(zones)

	zoneIndex := slices.Index(zones, zone)
	if zoneIndex == -1 {
		level.Warn(logger).Log(
			"msg", "readcache compaction zone not in the instance ring, using the full interval",
			"zone", zone,
			"available_zones", strings.Join(zones, ","),
		)
		return interval
	}

	offsetStep := interval / time.Duration(len(zones))
	zoneOffset := time.Duration(zoneIndex) * offsetStep
	return timeUntilReadcacheCompaction(now, interval, zoneOffset)
}

// timeUntilReadcacheCompaction is the ingester's clock-aligned wait.
// interval is the blocks-storage head compaction interval (at most 15m
// on a cell).
func timeUntilReadcacheCompaction(now time.Time, compactionInterval, zoneOffset time.Duration) time.Duration {
	elapsed := now.Sub(now.Truncate(time.Hour))
	timeSinceLast := (elapsed - zoneOffset) % compactionInterval
	if timeSinceLast < 0 {
		timeSinceLast += compactionInterval
	}
	return compactionInterval - timeSinceLast
}

func (r *Readcache) blockDurationMs() int64 {
	ranges := r.cfg.BlocksStorage.TSDB.BlockRanges.ToMilliseconds()
	if len(ranges) == 0 {
		return int64(2 * time.Hour / time.Millisecond)
	}
	return ranges[0]
}

func (r *Readcache) compactOne(db *partitionTSDB) {
	if db.IsClosed() {
		return
	}
	now := time.Now()
	if r.shouldCloseIdleTSDB(db, now) {
		r.closeIdleTSDB(db)
		return
	}
	if db.Head() == nil || db.Head().NumSeries() == 0 {
		return
	}

	var err error
	if r.compactionIdleTimeout > 0 && db.lastAppendTime().Add(r.compactionIdleTimeout).Before(now) {
		maxTime := max(db.Head().MaxTime(), db.Head().MaxOOOTime())
		if maxTime == math.MinInt64 {
			return
		}
		level.Info(r.logger).Log("msg", "readcache TSDB is idle, forcing compaction",
			"user", db.tenantID, "partition", db.partitionID)
		err = db.compactHeadForced(r.blockDurationMs(), maxTime)
	} else {
		err = db.compactRegular()
	}
	if err != nil {
		level.Warn(r.logger).Log("msg", "readcache compaction failed",
			"user", db.tenantID, "partition", db.partitionID, "err", err)
	}
}

// shouldCloseIdleTSDB reports whether this TSDB has nothing a querier
// still reads from the readcache. The head is empty, the last push is
// older than local retention, and so is the newest stored sample.
// Retention of 0 disables the close. BeyondTimeRetention never deletes
// the newest block, so the directory has to go explicitly.
func (r *Readcache) shouldCloseIdleTSDB(db *partitionTSDB, now time.Time) bool {
	retention := r.cfg.LocalBlockRetention
	if retention <= 0 || db.Head() == nil || db.Head().NumSeries() > 0 {
		return false
	}
	if db.lastAppendTime().Add(retention).After(now) {
		return false
	}
	_, maxT := db.sampleBounds()
	if maxT < 0 {
		return true
	}
	return maxT < now.Add(-retention).UnixMilli()
}

func (r *Readcache) closeIdleTSDB(db *partitionTSDB) {
	if !db.beginIdleClose() {
		return
	}
	if !r.shouldCloseIdleTSDB(db, time.Now()) {
		db.abortIdleClose()
		return
	}

	r.partitionMu.RLock()
	p := r.partitions[db.partitionID]
	r.partitionMu.RUnlock()
	if p == nil {
		db.abortIdleClose()
		return
	}

	// compactMu is still held. freezePartition takes it before detaching
	// tenants, so a TSDB missing here has already moved to a frozen epoch.
	p.tenantsMu.Lock()
	stillLive := p.tenants[db.tenantID] == db
	p.tenantsMu.Unlock()
	if !stillLive {
		db.abortIdleClose()
		return
	}

	if err := db.finishIdleClose(); err != nil {
		level.Warn(r.logger).Log("msg", "failed to close idle readcache TSDB",
			"user", db.tenantID, "partition", db.partitionID, "err", err)
		// DB.Close has stopped the TSDB. Leaving the dead object in the
		// map makes every later push re-resolve to it and retry forever.
	}

	dir := db.Dir()
	if err := os.RemoveAll(dir); err != nil {
		level.Warn(r.logger).Log("msg", "failed to delete idle readcache TSDB",
			"user", db.tenantID, "partition", db.partitionID, "dir", dir, "err", err)
	}

	p.tenantsMu.Lock()
	if p.tenants[db.tenantID] == db {
		delete(p.tenants, db.tenantID)
	}
	p.tenantsMu.Unlock()

	if r.tsdbMetrics != nil {
		r.tsdbMetrics.RemoveRegistryForTenant(tsdbMetricsTenantID(db.tenantID, db.partitionID))
	}
	level.Info(r.logger).Log("msg", "closed idle readcache TSDB",
		"user", db.tenantID, "partition", db.partitionID, "dir", dir)
}

// flushPartitionHead forces the head out to blocks before a partition
// is detached. The Kafka reader has already stopped.
func (r *Readcache) flushPartitionHead(db *partitionTSDB) error {
	if !r.cfg.IngesterScheduleCompaction {
		return nil
	}
	if db.IsClosed() || db.Head() == nil || db.Head().NumSeries() == 0 {
		return nil
	}
	maxTime := max(db.Head().MaxTime(), db.Head().MaxOOOTime())
	if maxTime == math.MinInt64 {
		return nil
	}
	return db.compactHeadForced(r.blockDurationMs(), maxTime)
}
