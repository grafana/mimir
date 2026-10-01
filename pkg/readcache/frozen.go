// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// frozenEpochReapInterval is how often the reaper scans frozen
	// epochs for ones whose data has aged out.
	frozenEpochReapInterval = 5 * time.Minute

	// frozenEpochReapGrace is added to LocalBlockRetention before a
	// frozen epoch is reaped, so an epoch is dropped only once its
	// newest sample is comfortably older than the readcache serving
	// horizon. This keeps the previous owner queryable for the whole
	// window the distributor may still route to it (the distributor
	// clamps queries to now-QueryIngestersWithin and the rebalancer
	// retains the lease over the same horizon), avoiding a gap where
	// the log still names this pod as a past owner but its slice is
	// already gone.
	frozenEpochReapGrace = 30 * time.Minute

	// frozenMarkerFilename is the JSON marker written into each
	// per-tenant TSDB directory when its epoch is frozen. It persists
	// the state the reaper needs (most importantly the epoch's maxT)
	// so a restarted pod can still delete aged-out frozen directories:
	// the reaper's bookkeeping is otherwise in-memory only and a
	// restart would orphan the directories on disk forever. The file
	// lives inside the TSDB dir so it is created and deleted
	// atomically with the data it describes (RemoveAll on the dir
	// takes the marker with it).
	frozenMarkerFilename = "readcache-frozen.json"
)

func parsePartitionEpochDirName(name string) (partitionID int32, epoch int, ok bool) {
	rest, ok := strings.CutPrefix(name, "partition-")
	if !ok {
		return 0, 0, false
	}

	partitionText, epochText, hasEpoch := strings.Cut(rest, "-epoch-")
	partition, err := strconv.ParseInt(partitionText, 10, 32)
	if err != nil || partition < 0 {
		return 0, 0, false
	}
	if !hasEpoch {
		return int32(partition), 0, true
	}

	parsedEpoch, err := strconv.Atoi(epochText)
	if err != nil || parsedEpoch <= 0 {
		return 0, 0, false
	}
	return int32(partition), parsedEpoch, true
}

// frozenEpoch is a read-only TSDB set retained after this pod stopped
// actively owning a partition. Each epoch corresponds to one ownership
// stint ({partition, epoch}); its per-tenant TSDBs keep serving the
// slice ingested during that stint until the reaper drops it.
//
// minT/maxT are the sample-time bounds captured at freeze time across
// all tenants in the epoch. The reaper uses maxT (absolute wallclock)
// to decide when the epoch has aged out of the readcache serving
// horizon — Prometheus's relative block retention never fires on a
// frozen DB because no new, newer blocks arrive to push the old ones
// past the retention window.
type frozenEpoch struct {
	partitionID int32
	epoch       int
	tenants     map[string]*partitionTSDB
	minT        int64
	maxT        int64

	// startOffset/endOffset are the Kafka offset span this epoch
	// consumed before it was frozen: startOffset is where the reader
	// joined the partition, endOffset is the last offset it saw. Both
	// are captured at freeze time (the reader is gone afterwards) and
	// surfaced on the admin page. -1 when unknown (e.g. the reader
	// never started).
	startOffset int64
	endOffset   int64

	// startedConsumingAt/stoppedConsumingAt bracket the wallclock
	// window during which this pod consumed the partition for this
	// epoch (both UnixMilli): startedConsumingAt is carried over from
	// the live partitionState, stoppedConsumingAt is the freeze time.
	// startedConsumingAt is 0 when the reader never started.
	startedConsumingAt int64
	stoppedConsumingAt int64
}

// frozenMarker is the on-disk (JSON) record of a frozen epoch's state,
// written into every per-tenant TSDB directory of the epoch at freeze
// time. MaxT is what the restart-time sweep needs to decide whether
// the directory has aged out; the remaining fields snapshot the rest
// of the frozenEpoch so future restores (e.g. reopening the epoch for
// querying after a restart) don't need to re-derive them.
//
// All epoch-level fields carry the EPOCH's values (identical across
// every tenant dir of the same epoch), not per-tenant ones, so the
// whole epoch expires as one unit — the same grouping the in-memory
// reaper uses. MinT/MaxT are the math.MaxInt64/math.MinInt64 sentinels
// when the epoch held no data; such directories are already reapable.
type frozenMarker struct {
	PartitionID        int32  `json:"partition_id"`
	Epoch              int    `json:"epoch"`
	MinT               int64  `json:"min_t"`
	MaxT               int64  `json:"max_t"`
	StartOffset        int64  `json:"start_offset"`
	EndOffset          int64  `json:"end_offset"`
	StartedConsumingAt int64  `json:"started_consuming_at"`
	StoppedConsumingAt int64  `json:"stopped_consuming_at"`
	Tenant             string `json:"tenant"`
}

// freezePartition stops the partition's Kafka reader and moves its
// per-tenant TSDBs into a read-only frozen epoch instead of closing
// them, so the slice this pod ingested stays queryable after the
// partition moved to another readcache. The caller must guarantee p
// is no longer reachable through r.partitions before invoking this
// (removePartition deletes from the map under partitionMu first), so
// this runs WITHOUT holding partitionMu — required to avoid
// deadlocking the Kafka reader's stop path against in-flight pushers
// (see removePartition for the full rationale).
func (r *Readcache) freezePartition(partitionID int32, p *partitionState) error {
	return r.freezePartitionWithOffsetPolicy(partitionID, p, true)
}

func (r *Readcache) freezePartitionWithOffsetPolicy(partitionID int32, p *partitionState, removeOffsetFile bool) error {
	if hook := r.stopPartitionHook; hook != nil {
		hook(partitionID)
	}
	p.cancelWarmup()

	// Capture the Kafka offset span before tearing down the reader:
	// stopKafkaReaderLocked nils out p.reader, after which the
	// last-seen offset is no longer reachable.
	startOffset := p.startOffset.Load()
	endOffset := int64(-1)
	if p.reader != nil {
		endOffset = p.reader.LastSeenOffsets().ForKafkaCluster(0)
	}
	startedConsumingAt := p.startedConsumingAt.Load()
	stoppedConsumingAt := time.Now().UnixMilli()

	var firstErr error
	if err := r.stopKafkaReaderLocked(p); err != nil {
		firstErr = err
		level.Warn(r.logger).Log("msg", "stopping partition reader before freeze", "partition", partitionID, "err", err)
	}

	// Ownership is gone: drop the stored offset file so a future
	// re-acquisition of this partition joins at the live edge instead
	// of resuming. Resuming across another pod's ownership stint would
	// replay records the intermediate owner already ingested (and still
	// serves from its frozen epoch), double-counting them at query
	// time. The resume-from-offset path in startKafkaReader is only
	// for process restarts within a single ownership stint.
	// closeLiveForRestart leaves the file in place and does not write
	// a frozen marker, so the next process reopens the same TSDB.
	if removeOffsetFile {
		if err := os.Remove(r.partitionOffsetFilePath(partitionID)); err != nil && !os.IsNotExist(err) {
			level.Warn(r.logger).Log("msg", "removing partition offset file on freeze", "partition", partitionID, "err", err)
		}
	}

	// The reader has stopped, so nothing appends. Remember the inclusive
	// sample bounds first: compaction's block max is one millisecond
	// past the last sample, and the marker's maxT is the reap key.
	// Then flush each head so the frozen epoch does not keep a live
	// head for the rest of local retention.
	p.tenantsMu.RLock()
	toFlush := make([]*partitionTSDB, 0, len(p.tenants))
	preFlushBounds := make(map[*partitionTSDB][2]int64, len(p.tenants))
	for _, db := range p.tenants {
		mn, mx := db.sampleBounds()
		preFlushBounds[db] = [2]int64{mn, mx}
		toFlush = append(toFlush, db)
	}
	p.tenantsMu.RUnlock()
	for _, db := range toFlush {
		if err := r.flushPartitionHead(db); err != nil {
			level.Warn(r.logger).Log("msg", "flushing readcache head before freeze",
				"user", db.tenantID, "partition", partitionID, "err", err)
		}
	}

	// Detach tenants idle close does not own. An empty head is flushed
	// above without taking compactMu, so idle close can be past its
	// liveness check and about to Close and RemoveAll this directory.
	tenants := detachLiveTenants(p, toFlush)

	if len(tenants) == 0 {
		level.Info(r.logger).Log("msg", "readcache: partition removed (nothing to freeze)", "partition", partitionID)
		return firstErr
	}

	ep := &frozenEpoch{
		partitionID:        partitionID,
		epoch:              p.epoch,
		tenants:            tenants,
		minT:               math.MaxInt64,
		maxT:               math.MinInt64,
		startOffset:        startOffset,
		endOffset:          endOffset,
		startedConsumingAt: startedConsumingAt,
		stoppedConsumingAt: stoppedConsumingAt,
	}
	for tenant, db := range tenants {
		bounds, ok := preFlushBounds[db]
		mn, mx := int64(0), int64(-1)
		if ok {
			mn, mx = bounds[0], bounds[1]
		}
		if mx < mn {
			continue // empty TSDB; bounds left at the sentinel
		}
		if mn < ep.minT {
			ep.minT = mn
		}
		if mx > ep.maxT {
			ep.maxT = mx
		}
		// Release the (tenant, partition) metrics key so the next live
		// epoch of this partition can register cleanly; the metrics
		// key does not encode the epoch.
		if r.tsdbMetrics != nil {
			r.tsdbMetrics.RemoveRegistryForTenant(tsdbMetricsTenantID(tenant, partitionID))
		}
	}
	// Persist the freeze on disk so the reaper survives restarts:
	// without a marker, a restart would forget these directories and
	// leak them forever (the in-memory frozen map starts empty and
	// nothing else re-discovers the dirs). Best-effort per tenant — a
	// failed write only degrades that one dir back to the pre-marker
	// behavior.
	for tenant, db := range tenants {
		if err := writeFrozenMarker(db.dir, ep, tenant); err != nil {
			level.Warn(r.logger).Log("msg", "writing frozen epoch marker",
				"user", tenant, "partition", partitionID, "epoch", p.epoch, "dir", db.dir, "err", err)
		}
	}

	r.frozenMu.Lock()
	r.frozen[partitionID] = append(r.frozen[partitionID], ep)
	r.frozenMu.Unlock()

	level.Info(r.logger).Log("msg", "readcache: partition frozen",
		"partition", partitionID, "epoch", p.epoch, "minT", ep.minT, "maxT", ep.maxT)
	return firstErr
}

// detachLiveTenants moves tenant TSDBs that idle close does not own out
// of p.tenants. Idle close holds compactMu from beginIdleClose until
// the TSDB is closed, so taking that lock waits the close out; a TSDB
// already marked closed stays in the map for closeIdleTSDB to delete
// after RemoveAll. Publishing it would let the close delete a directory
// the frozen epoch still serves.
//
// candidates are the TSDBs snapshotted before the flush. A TSDB opened
// after that snapshot is taken too, unless it is already closed.
// The caller must not hold tenantsMu or any compactMu.
func detachLiveTenants(p *partitionState, candidates []*partitionTSDB) map[string]*partitionTSDB {
	locked := make([]*partitionTSDB, 0, len(candidates))
	seen := make(map[*partitionTSDB]struct{}, len(candidates))
	for _, db := range candidates {
		if db == nil {
			continue
		}
		if _, ok := seen[db]; ok {
			continue
		}
		seen[db] = struct{}{}
		db.compactMu.Lock()
		locked = append(locked, db)
	}
	defer func() {
		for _, db := range locked {
			db.compactMu.Unlock()
		}
	}()

	p.tenantsMu.Lock()
	defer p.tenantsMu.Unlock()

	moving := make(map[string]*partitionTSDB, len(p.tenants))
	for id, db := range p.tenants {
		if _, ok := seen[db]; ok {
			if db.idleCloseClaimed() {
				continue
			}
		} else if db.IsClosed() {
			continue
		}
		moving[id] = db
		delete(p.tenants, id)
	}
	return moving
}

// writeFrozenMarker serializes the epoch's state into
// <dir>/readcache-frozen.json. The Prometheus TSDB only interprets
// ULID-named subdirectories and its own well-known files, so an extra
// file in the DB root is inert if the directory is ever reopened.
func writeFrozenMarker(dir string, ep *frozenEpoch, tenant string) error {
	return writeFrozenMarkerData(dir, frozenMarker{
		PartitionID:        ep.partitionID,
		Epoch:              ep.epoch,
		MinT:               ep.minT,
		MaxT:               ep.maxT,
		StartOffset:        ep.startOffset,
		EndOffset:          ep.endOffset,
		StartedConsumingAt: ep.startedConsumingAt,
		StoppedConsumingAt: ep.stoppedConsumingAt,
		Tenant:             tenant,
	})
}

func writeFrozenMarkerData(dir string, marker frozenMarker) error {
	data, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, frozenMarkerFilename), data, 0o644)
}

// reapFrozenEpochs closes and deletes frozen epochs whose newest
// sample is older than the readcache serving horizon
// (LocalBlockRetention + grace) relative to now. Reaping is by
// absolute wallclock against the epoch's captured maxT: a frozen TSDB
// receives no new samples, so Prometheus's relative block retention
// would never delete its blocks on its own.
func (r *Readcache) reapFrozenEpochs(now time.Time) {
	cutoff := now.Add(-r.cfg.LocalBlockRetention - frozenEpochReapGrace).UnixMilli()

	var toClose []*frozenEpoch
	r.frozenMu.Lock()
	for partitionID, eps := range r.frozen {
		kept := eps[:0]
		for _, ep := range eps {
			if ep.maxT < cutoff {
				toClose = append(toClose, ep)
				continue
			}
			kept = append(kept, ep)
		}
		if len(kept) == 0 {
			delete(r.frozen, partitionID)
		} else {
			r.frozen[partitionID] = kept
		}
	}
	r.frozenMu.Unlock()

	for _, ep := range toClose {
		for tenant, db := range ep.tenants {
			if err := db.Close(); err != nil {
				level.Warn(r.logger).Log("msg", "closing reaped frozen epoch TSDB",
					"user", tenant, "partition", ep.partitionID, "epoch", ep.epoch, "err", err)
			}
			// Reclaim the on-disk directory: a reaped epoch is never
			// reopened (re-acquisition gets a fresh epoch number and
			// directory), so leaving it would leak disk on churn.
			if db.dir != "" {
				if err := os.RemoveAll(db.dir); err != nil {
					level.Warn(r.logger).Log("msg", "removing reaped frozen epoch dir",
						"user", tenant, "partition", ep.partitionID, "epoch", ep.epoch, "dir", db.dir, "err", err)
				}
			}
		}
		level.Info(r.logger).Log("msg", "readcache: frozen epoch reaped",
			"partition", ep.partitionID, "epoch", ep.epoch, "maxT", ep.maxT)
	}
}

// restoreFrozenEpochsOnStartup rebuilds the frozen-epoch state left
// behind by a previous process: freezing writes a marker file into
// each per-tenant TSDB dir (see writeFrozenMarker), but the frozen map
// itself is in-memory, so without this a restart would both leak the
// directories on disk forever AND silently stop serving slices the
// distributor still routes to this pod for (the readcache assignment
// log names this pod as a past owner for the whole serving horizon,
// regardless of restarts).
//
// For every <DataDir>/<tenant>/partition-* directory carrying a
// marker:
//
//   - already past the serving horizon: the directory is deleted
//     immediately;
//   - still within the horizon: the TSDB is reopened and grouped back
//     into its (partition, epoch) frozenEpoch in r.frozen. Freeze
//     flushes the head first, so a clean shutdown leaves blocks rather
//     than a WAL to replay. From there it is queryable and reaped
//     exactly like an epoch frozen by this process;
//   - unopenable (e.g. unrepairable corruption): the directory is
//     deleted. Readcache is a cache — the canonical copy of the data
//     lives with the ingester/blockbuilder — and a dir we cannot open
//     can never be served, so keeping it would only re-create the
//     disk leak this restore exists to prevent.
//
// epochSeq is seeded past every surviving marker's epoch so that
// re-acquiring the partition can never hand out an epoch whose
// directory is still in use by a restored frozen epoch.
//
// Directories without a marker are live epochs left by a restart or an
// abrupt crash. The newest such epoch is remembered in resumeEpoch and
// reopened as the live TSDB when the first assignment still owns the
// partition. An older unmarked epoch, or one that already has a frozen
// sibling, is not resumed: the first assignment freezes it instead.
// A partition the first assignment does not return is frozen too, and
// its resumeEpoch entry is dropped so a later add opens the next epoch.
//
// Must run during starting(), before this pod registers in the ring
// and starts receiving assignments: distributors may route to us the
// moment we are visible, and epochSeq seeding must win any race with
// addPartition.
func (r *Readcache) restoreFrozenEpochsOnStartup(now time.Time) {
	cutoff := now.Add(-r.cfg.LocalBlockRetention - frozenEpochReapGrace).UnixMilli()

	tenantEntries, err := os.ReadDir(r.cfg.DataDir)
	if err != nil {
		level.Warn(r.logger).Log("msg", "reading readcache data-dir for frozen epoch restore", "dir", r.cfg.DataDir, "err", err)
		return
	}

	type epochKey struct {
		partitionID int32
		epoch       int
	}
	restored := map[epochKey]*frozenEpoch{}
	var restoredDBs, deleted int
	addRestored := func(marker frozenMarker, tenant string, db *partitionTSDB) {
		key := epochKey{partitionID: marker.PartitionID, epoch: marker.Epoch}
		ep := restored[key]
		if ep == nil {
			ep = &frozenEpoch{
				partitionID:        marker.PartitionID,
				epoch:              marker.Epoch,
				tenants:            map[string]*partitionTSDB{},
				minT:               marker.MinT,
				maxT:               marker.MaxT,
				startOffset:        marker.StartOffset,
				endOffset:          marker.EndOffset,
				startedConsumingAt: marker.StartedConsumingAt,
				stoppedConsumingAt: marker.StoppedConsumingAt,
			}
			restored[key] = ep
		}
		ep.tenants[tenant] = db
		restoredDBs++

		r.partitionMu.Lock()
		if next := marker.Epoch + 1; next > r.epochSeq[marker.PartitionID] {
			r.epochSeq[marker.PartitionID] = next
		}
		r.partitionMu.Unlock()
	}

	for _, tenantEntry := range tenantEntries {
		if !tenantEntry.IsDir() {
			continue // e.g. partition-<id>.offset.json files at the root
		}
		tenantDir := filepath.Join(r.cfg.DataDir, tenantEntry.Name())
		dbEntries, err := os.ReadDir(tenantDir)
		if err != nil {
			level.Warn(r.logger).Log("msg", "reading tenant dir for frozen epoch restore", "dir", tenantDir, "err", err)
			continue
		}

		for _, dbEntry := range dbEntries {
			if !dbEntry.IsDir() {
				continue
			}
			dir := filepath.Join(tenantDir, dbEntry.Name())
			data, err := os.ReadFile(filepath.Join(dir, frozenMarkerFilename))
			if os.IsNotExist(err) {
				partitionID, epoch, ok := parsePartitionEpochDirName(dbEntry.Name())
				if !ok {
					continue
				}
				// No marker: the previous process closed this TSDB in
				// place (or crashed). Remember it. The first assignment
				// reopens it as the live epoch when this pod still owns
				// the partition, and freezes it when ownership moved.
				if r.resumeEpoch == nil {
					r.resumeEpoch = map[int32]int{}
				}
				if epoch >= r.resumeEpoch[partitionID] {
					r.resumeEpoch[partitionID] = epoch
				}
				r.partitionMu.Lock()
				if next := epoch + 1; next > r.epochSeq[partitionID] {
					r.epochSeq[partitionID] = next
				}
				r.partitionMu.Unlock()
				r.unmarked = append(r.unmarked, unmarkedTSDBDir{
					tenant:      tenantEntry.Name(),
					partitionID: partitionID,
					epoch:       epoch,
					dir:         dir,
				})
				continue
			}
			if err != nil {
				level.Warn(r.logger).Log("msg", "reading frozen epoch marker", "dir", dir, "err", err)
				continue
			}
			var marker frozenMarker
			if err := json.Unmarshal(data, &marker); err != nil {
				level.Warn(r.logger).Log("msg", "parsing frozen epoch marker", "dir", dir, "err", err)
				continue
			}

			if marker.MaxT < cutoff {
				// Aged out (or the epoch never held data — the maxT
				// sentinel is math.MinInt64): delete now rather than
				// paying to reopen a DB the reaper would drop anyway.
				if err := os.RemoveAll(dir); err != nil {
					level.Warn(r.logger).Log("msg", "removing expired frozen epoch dir on startup",
						"partition", marker.PartitionID, "epoch", marker.Epoch, "dir", dir, "err", err)
					continue
				}
				deleted++
				level.Info(r.logger).Log("msg", "readcache: expired frozen epoch dir removed on startup",
					"partition", marker.PartitionID, "epoch", marker.Epoch, "dir", dir, "maxT", marker.MaxT)
				continue
			}

			// Reopen the TSDB so the slice is queryable again. No
			// tsdbMetrics registration: freezing released the
			// (tenant, partition) metrics key so the next live epoch
			// can claim it, and a restored frozen epoch must not
			// steal it back.
			tenant := tenantEntry.Name()
			db, err := openPartitionTSDB(
				tenant,
				marker.PartitionID,
				marker.Epoch,
				r.cfg.DataDir,
				r.cfg.BlocksStorage.TSDB,
				r.cfg.LocalBlockRetention,
				r.limits,
				r.cfg.MaxExemplarsPerPartitionTSDB,
				r.seriesHashCache,
				r.headPostingsForMatchersCacheFactory,
				r.blockPostingsForMatchersCacheFactory,
				r.lookupPlanMetrics,
				prometheus.NewRegistry(),
				r.logger,
			)
			if err != nil {
				level.Warn(r.logger).Log("msg", "reopening frozen epoch TSDB on startup failed; deleting dir",
					"user", tenant, "partition", marker.PartitionID, "epoch", marker.Epoch, "dir", dir, "err", err)
				if err := os.RemoveAll(dir); err != nil {
					level.Warn(r.logger).Log("msg", "removing unopenable frozen epoch dir",
						"user", tenant, "partition", marker.PartitionID, "epoch", marker.Epoch, "dir", dir, "err", err)
				}
				continue
			}
			r.instrumentTSDB(db)

			addRestored(marker, tenant, db)
		}

		// Best-effort: drop the tenant dir if the sweep emptied it.
		// os.Remove fails on non-empty directories, which is exactly
		// the behavior we want.
		_ = os.Remove(tenantDir)
	}

	if len(restored) > 0 {
		r.frozenMu.Lock()
		for _, ep := range restored {
			r.frozen[ep.partitionID] = append(r.frozen[ep.partitionID], ep)
		}
		r.frozenMu.Unlock()
	}

	if restoredDBs > 0 || deleted > 0 {
		level.Info(r.logger).Log("msg", "readcache: frozen epoch restore finished",
			"restored_epochs", len(restored), "restored_tsdbs", restoredDBs, "deleted_expired_dirs", deleted)
	}
	// A resume epoch has to be the newest epoch of its partition and
	// entirely unmarked. A newer frozen epoch, or a marker on some
	// tenant of the same epoch, means this directory is history: living
	// in it would overlap the frozen copy or reopen a dir it already holds.
	r.forgetUnusableResumeEpochs()
}

// unmarkedTSDBDir is a tenant TSDB directory with no frozen marker.
type unmarkedTSDBDir struct {
	tenant      string
	partitionID int32
	epoch       int
	dir         string
}

// takeLiveEpoch returns the epoch addPartition should open. A restart
// that still owns the partition reuses the closed-in-place epoch.
// partitionMu is held by the caller.
func (r *Readcache) takeLiveEpoch(partitionID int32) (epoch int, resumed bool) {
	if r.resumeEpoch != nil {
		if epoch, ok := r.resumeEpoch[partitionID]; ok {
			delete(r.resumeEpoch, partitionID)
			if next := epoch + 1; next > r.epochSeq[partitionID] {
				r.epochSeq[partitionID] = next
			}
			return epoch, true
		}
	}
	epoch = r.epochSeq[partitionID]
	r.epochSeq[partitionID]++
	return epoch, false
}

// promoteUnownedResumeEpochs freezes unmarked directories for partitions
// the first assignment did not give back to this pod, and for epochs
// that are not the live one. Directories for an owned partition at its
// resume epoch stay unmarked and are opened into the live tenant map
// so a tenant with no post-restart append is still queryable.
// Adopting an epoch drops its resumeEpoch entry: a later addPartition
// must open the next epoch, not the directory now held frozen.
func (r *Readcache) promoteUnownedResumeEpochs(owned map[int32]struct{}) {
	if len(r.unmarked) == 0 {
		return
	}
	now := time.Now()
	cutoff := now.Add(-r.cfg.LocalBlockRetention - frozenEpochReapGrace).UnixMilli()
	kept := r.unmarked[:0]
	for _, dir := range r.unmarked {
		if _, ok := owned[dir.partitionID]; ok && r.keepsUnmarkedEpoch(dir.partitionID, dir.epoch) {
			r.partitionMu.RLock()
			p := r.partitions[dir.partitionID]
			r.partitionMu.RUnlock()
			if p != nil && p.epoch == dir.epoch {
				r.reopenLiveUnmarkedDir(p, dir)
			}
			kept = append(kept, dir)
			continue
		}
		r.adoptUnmarkedDir(dir, now, cutoff)
		r.clearResumeEpoch(dir.partitionID, dir.epoch)
	}
	r.unmarked = kept
}

// forgetUnusableResumeEpochs drops resume epochs that already overlap a
// restored frozen epoch, or that are older than another epoch of the
// same partition. Those directories are frozen by promoteUnownedResumeEpochs.
func (r *Readcache) forgetUnusableResumeEpochs() {
	r.frozenMu.RLock()
	frozenAt := make(map[int32]map[int]struct{}, len(r.frozen))
	for pid, eps := range r.frozen {
		set := make(map[int]struct{}, len(eps))
		for _, ep := range eps {
			set[ep.epoch] = struct{}{}
		}
		frozenAt[pid] = set
	}
	r.frozenMu.RUnlock()

	r.partitionMu.Lock()
	defer r.partitionMu.Unlock()
	for pid, epoch := range r.resumeEpoch {
		if _, overlap := frozenAt[pid][epoch]; overlap || epoch+1 < r.epochSeq[pid] {
			delete(r.resumeEpoch, pid)
		}
	}
}

// clearResumeEpoch forgets a resume epoch that this process just froze
// or deleted. A later addPartition then takes the next epoch.
func (r *Readcache) clearResumeEpoch(partitionID int32, epoch int) {
	r.partitionMu.Lock()
	defer r.partitionMu.Unlock()
	if current, ok := r.resumeEpoch[partitionID]; ok && current == epoch {
		delete(r.resumeEpoch, partitionID)
	}
}

// reopenUnmarkedLiveDirs opens on-disk TSDBs for p's epoch into the live
// tenant map. Called from addPartition before the Kafka reader starts,
// so quiet tenants are queryable once the partition is warm.
func (r *Readcache) reopenUnmarkedLiveDirs(p *partitionState) {
	for _, dir := range r.unmarked {
		if dir.partitionID == p.partitionID && dir.epoch == p.epoch {
			r.reopenLiveUnmarkedDir(p, dir)
		}
	}
}

// reopenLiveUnmarkedDir opens one unmarked directory as a live TSDB.
// A tenant the Kafka reader already opened is left as-is. An unopenable
// directory is deleted: readcache is a cache, and a dir we cannot open
// can never be served.
func (r *Readcache) reopenLiveUnmarkedDir(p *partitionState, dir unmarkedTSDBDir) {
	p.tenantsMu.Lock()
	defer p.tenantsMu.Unlock()
	if existing := p.tenants[dir.tenant]; existing != nil && !existing.IsClosed() {
		return
	}

	tsdbPromReg := prometheus.NewRegistry()
	opened, err := openPartitionTSDB(
		dir.tenant,
		dir.partitionID,
		dir.epoch,
		r.cfg.DataDir,
		r.cfg.BlocksStorage.TSDB,
		r.cfg.LocalBlockRetention,
		r.limits,
		r.cfg.MaxExemplarsPerPartitionTSDB,
		r.seriesHashCache,
		r.headPostingsForMatchersCacheFactory,
		r.blockPostingsForMatchersCacheFactory,
		r.lookupPlanMetrics,
		tsdbPromReg,
		r.logger,
	)
	if err != nil {
		level.Warn(r.logger).Log("msg", "reopening live partition TSDB failed; deleting dir",
			"user", dir.tenant, "partition", dir.partitionID, "epoch", dir.epoch, "dir", dir.dir, "err", err)
		if removeErr := os.RemoveAll(dir.dir); removeErr != nil {
			level.Warn(r.logger).Log("msg", "removing unopenable live partition TSDB dir",
				"user", dir.tenant, "partition", dir.partitionID, "epoch", dir.epoch, "dir", dir.dir, "err", removeErr)
		}
		return
	}
	if r.tsdbMetrics != nil {
		r.tsdbMetrics.SetRegistryForTenant(tsdbMetricsTenantID(dir.tenant, dir.partitionID), tsdbPromReg)
	}
	r.instrumentTSDB(opened)
	p.tenants[dir.tenant] = opened
}

// closeOpenedPartitionTSDBs closes TSDBs opened while a partition add
// was in flight. startKafkaReader has already stopped the reader before
// returning an error, so no pusher is still inside getOrOpenTSDB.
func (r *Readcache) closeOpenedPartitionTSDBs(p *partitionState) {
	p.tenantsMu.Lock()
	tenants := p.tenants
	p.tenants = map[string]*partitionTSDB{}
	p.tenantsMu.Unlock()
	for tenant, db := range tenants {
		if r.tsdbMetrics != nil {
			r.tsdbMetrics.RemoveRegistryForTenant(tsdbMetricsTenantID(tenant, p.partitionID))
		}
		if err := db.Close(); err != nil {
			level.Warn(r.logger).Log("msg", "closing partition TSDB after failed add",
				"user", tenant, "partition", p.partitionID, "err", err)
		}
	}
}

// keepsUnmarkedEpoch reports whether this partition is reopening dirEpoch
// as its live TSDB. A failed reader start puts the epoch back into
// resumeEpoch so the directory is not frozen out from under a retry.
func (r *Readcache) keepsUnmarkedEpoch(partitionID int32, dirEpoch int) bool {
	r.partitionMu.RLock()
	p := r.partitions[partitionID]
	r.partitionMu.RUnlock()
	if p != nil && p.epoch == dirEpoch {
		return true
	}
	if r.resumeEpoch == nil {
		return false
	}
	resume, ok := r.resumeEpoch[partitionID]
	return ok && resume == dirEpoch
}

func (r *Readcache) adoptUnmarkedDir(dir unmarkedTSDBDir, now time.Time, cutoff int64) {
	db, err := openPartitionTSDB(
		dir.tenant,
		dir.partitionID,
		dir.epoch,
		r.cfg.DataDir,
		r.cfg.BlocksStorage.TSDB,
		r.cfg.LocalBlockRetention,
		r.limits,
		r.cfg.MaxExemplarsPerPartitionTSDB,
		r.seriesHashCache,
		r.headPostingsForMatchersCacheFactory,
		r.blockPostingsForMatchersCacheFactory,
		r.lookupPlanMetrics,
		prometheus.NewRegistry(),
		r.logger,
	)
	if err != nil {
		level.Warn(r.logger).Log("msg", "reopening unmarked partition TSDB failed; deleting dir",
			"user", dir.tenant, "partition", dir.partitionID, "epoch", dir.epoch, "dir", dir.dir, "err", err)
		if removeErr := os.RemoveAll(dir.dir); removeErr != nil {
			level.Warn(r.logger).Log("msg", "removing unopenable unmarked partition TSDB dir",
				"user", dir.tenant, "partition", dir.partitionID, "epoch", dir.epoch, "dir", dir.dir, "err", removeErr)
		}
		return
	}
	r.instrumentTSDB(db)

	minT, maxT := db.sampleBounds()
	if maxT < cutoff {
		if closeErr := db.Close(); closeErr != nil {
			level.Warn(r.logger).Log("msg", "closing expired unmarked partition TSDB",
				"user", dir.tenant, "partition", dir.partitionID, "epoch", dir.epoch, "err", closeErr)
		}
		if removeErr := os.RemoveAll(dir.dir); removeErr != nil {
			level.Warn(r.logger).Log("msg", "removing expired unmarked partition TSDB dir",
				"user", dir.tenant, "partition", dir.partitionID, "epoch", dir.epoch, "dir", dir.dir, "err", removeErr)
		}
		return
	}

	marker := frozenMarker{
		PartitionID:        dir.partitionID,
		Epoch:              dir.epoch,
		MinT:               minT,
		MaxT:               maxT,
		StartOffset:        -1,
		EndOffset:          -1,
		StoppedConsumingAt: now.UnixMilli(),
		Tenant:             dir.tenant,
	}
	if err := writeFrozenMarkerData(dir.dir, marker); err != nil {
		level.Warn(r.logger).Log("msg", "writing marker for unmarked partition TSDB",
			"user", dir.tenant, "partition", dir.partitionID, "epoch", dir.epoch, "dir", dir.dir, "err", err)
	}
	r.frozenMu.Lock()
	var ep *frozenEpoch
	for _, existing := range r.frozen[dir.partitionID] {
		if existing.epoch == dir.epoch && existing.startOffset == -1 && existing.endOffset == -1 {
			ep = existing
			break
		}
	}
	if ep == nil {
		ep = &frozenEpoch{
			partitionID:        dir.partitionID,
			epoch:              dir.epoch,
			tenants:            map[string]*partitionTSDB{},
			minT:               minT,
			maxT:               maxT,
			startOffset:        -1,
			endOffset:          -1,
			stoppedConsumingAt: now.UnixMilli(),
		}
		r.frozen[dir.partitionID] = append(r.frozen[dir.partitionID], ep)
	}
	ep.tenants[dir.tenant] = db
	if minT < ep.minT {
		ep.minT = minT
	}
	if maxT > ep.maxT {
		ep.maxT = maxT
	}
	r.frozenMu.Unlock()
	level.Info(r.logger).Log("msg", "readcache: unmarked partition epoch restored as frozen",
		"user", dir.tenant, "partition", dir.partitionID, "epoch", dir.epoch, "minT", minT, "maxT", maxT)
}

// closeLiveForRestart stops a partition's reader and closes its TSDBs
// without writing a frozen marker or removing the offset file. The next
// process reopens the same directories when it still owns the partition.
func (r *Readcache) closeLiveForRestart(p *partitionState) error {
	if hook := r.stopPartitionHook; hook != nil {
		hook(p.partitionID)
	}
	p.cancelWarmup()
	var firstErr error
	if err := r.stopKafkaReaderLocked(p); err != nil {
		firstErr = err
		level.Warn(r.logger).Log("msg", "stopping partition reader before restart close", "partition", p.partitionID, "err", err)
	}

	p.tenantsMu.Lock()
	tenants := p.tenants
	p.tenants = map[string]*partitionTSDB{}
	p.tenantsMu.Unlock()

	for tenant, db := range tenants {
		if err := r.flushPartitionHead(db); err != nil && firstErr == nil {
			firstErr = err
		}
		if r.tsdbMetrics != nil {
			r.tsdbMetrics.RemoveRegistryForTenant(tsdbMetricsTenantID(tenant, p.partitionID))
		}
		if err := db.Close(); err != nil && firstErr == nil {
			firstErr = err
			level.Warn(r.logger).Log("msg", "closing partition TSDB for restart",
				"user", tenant, "partition", p.partitionID, "err", err)
		}
	}
	level.Info(r.logger).Log("msg", "readcache: partition closed for restart",
		"partition", p.partitionID, "epoch", p.epoch, "tenants", len(tenants))
	return firstErr
}

// removeUnownedFrozenPartitionOffsets drops restart-resume offsets for frozen
// partitions that the first assignment snapshot did not return to this pod.
// Keeping such an offset would let a later restart resume across another
// owner's stint and ingest records already served by that owner's epoch.
func (r *Readcache) removeUnownedFrozenPartitionOffsets(owned map[int32]struct{}) {
	r.frozenMu.RLock()
	partitionIDs := make([]int32, 0, len(r.frozen))
	for partitionID := range r.frozen {
		if _, ok := owned[partitionID]; !ok {
			partitionIDs = append(partitionIDs, partitionID)
		}
	}
	r.frozenMu.RUnlock()

	for _, partitionID := range partitionIDs {
		path := r.partitionOffsetFilePath(partitionID)
		if err := os.Remove(path); err == nil {
			level.Info(r.logger).Log("msg", "readcache: removed stale offset for unowned frozen partition",
				"partition", partitionID, "offset_file", path)
		} else if !os.IsNotExist(err) {
			level.Warn(r.logger).Log("msg", "removing stale offset for unowned frozen partition",
				"partition", partitionID, "offset_file", path, "err", err)
		}
	}
}
