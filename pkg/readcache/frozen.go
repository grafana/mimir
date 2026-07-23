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

// freezePartitionForShutdown freezes a live epoch during process shutdown while
// retaining its offset file. If the partition returns to this pod after restart,
// the new live epoch resumes immediately after the frozen epoch's final record.
func (r *Readcache) freezePartitionForShutdown(partitionID int32, p *partitionState) error {
	return r.freezePartitionWithOffsetPolicy(partitionID, p, false)
}

func (r *Readcache) freezePartitionWithOffsetPolicy(partitionID int32, p *partitionState, removeOffsetFile bool) error {
	if hook := r.stopPartitionHook; hook != nil {
		hook(partitionID)
	}

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
	// for process restarts within a single ownership stint, where the
	// file is intentionally left in place (process shutdown calls
	// freezePartitionForShutdown with removeOffsetFile=false).
	if removeOffsetFile {
		if err := os.Remove(r.partitionOffsetFilePath(partitionID)); err != nil && !os.IsNotExist(err) {
			level.Warn(r.logger).Log("msg", "removing partition offset file on freeze", "partition", partitionID, "err", err)
		}
	}

	// Detach the tenant TSDBs from the (now unreachable) live state.
	// The reader has returned from StopAndAwaitTerminated, so no
	// goroutine is appending; taking tenantsMu is safe.
	p.tenantsMu.Lock()
	tenants := p.tenants
	p.tenants = map[string]*partitionTSDB{}
	p.tenantsMu.Unlock()

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
		mn, mx := db.sampleBounds()
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
//   - still within the horizon: the TSDB is reopened (WAL replay
//     included — freezing does not compact the head, so recent
//     samples live in the WAL) and grouped back into its
//     (partition, epoch) frozenEpoch in r.frozen. From there it is
//     queryable and reaped exactly like an epoch frozen by this
//     process;
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
// Directories without a marker are live epochs left by an older binary
// or an abrupt crash. They are reopened as frozen epochs too: the first
// assignment snapshot may move the partition elsewhere, but distributors
// still route historical slices to this pod. If the partition returns,
// addPartition creates the next epoch and resumes from the retained offset.
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
				tenant := tenantEntry.Name()
				db, openErr := openPartitionTSDB(
					tenant,
					partitionID,
					epoch,
					r.cfg.DataDir,
					r.cfg.BlocksStorage.TSDB,
					r.cfg.LocalBlockRetention,
					r.limits,
					r.cfg.MaxExemplarsPerPartitionTSDB,
					r.seriesHashCache,
					r.headPostingsForMatchersCacheFactory,
					r.blockPostingsForMatchersCacheFactory,
					prometheus.NewRegistry(),
					r.logger,
				)
				if openErr != nil {
					level.Warn(r.logger).Log("msg", "reopening unmarked partition TSDB on startup failed; deleting dir",
						"user", tenant, "partition", partitionID, "epoch", epoch, "dir", dir, "err", openErr)
					if removeErr := os.RemoveAll(dir); removeErr != nil {
						level.Warn(r.logger).Log("msg", "removing unopenable unmarked partition TSDB dir",
							"user", tenant, "partition", partitionID, "epoch", epoch, "dir", dir, "err", removeErr)
					}
					continue
				}

				minT, maxT := db.sampleBounds()
				if maxT < cutoff {
					if closeErr := db.Close(); closeErr != nil {
						level.Warn(r.logger).Log("msg", "closing expired unmarked partition TSDB",
							"user", tenant, "partition", partitionID, "epoch", epoch, "err", closeErr)
					}
					if removeErr := os.RemoveAll(dir); removeErr != nil {
						level.Warn(r.logger).Log("msg", "removing expired unmarked partition TSDB dir on startup",
							"user", tenant, "partition", partitionID, "epoch", epoch, "dir", dir, "err", removeErr)
						continue
					}
					deleted++
					continue
				}

				marker := frozenMarker{
					PartitionID:        partitionID,
					Epoch:              epoch,
					MinT:               minT,
					MaxT:               maxT,
					StartOffset:        -1,
					EndOffset:          -1,
					StoppedConsumingAt: now.UnixMilli(),
					Tenant:             tenant,
				}
				if markerErr := writeFrozenMarkerData(dir, marker); markerErr != nil {
					level.Warn(r.logger).Log("msg", "writing marker for unmarked partition TSDB",
						"user", tenant, "partition", partitionID, "epoch", epoch, "dir", dir, "err", markerErr)
				}
				addRestored(marker, tenant, db)
				level.Info(r.logger).Log("msg", "readcache: unmarked partition epoch restored as frozen",
					"user", tenant, "partition", partitionID, "epoch", epoch, "minT", minT, "maxT", maxT)
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
