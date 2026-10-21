// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/storage/ingest"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

// seriesstoreCheckpointDuty is the share of a checkpoint's time it works between its batches of series, like the
// ingester's head snapshots: it takes ten times as long as it could, and leaves the CPU and disk to queries.
const seriesstoreCheckpointDuty = 0.1

// seriesstoreCheckpoint is what a partition's seriesstore engine checkpoints with: the partition's Kafka offset
// committed before the checkpoint started, which the head has every record up to.
type seriesstoreCheckpoint struct {
	Offset int64 `json:"offset"`
}

func (r *Readcache) seriesstoreCheckpointInterval() time.Duration {
	if r.cfg.Engine != mimir_tsdb.EngineSeriesstore {
		return 0
	}
	return r.cfg.BlocksStorage.TSDB.SeriesstoreSnapshotInterval
}

// runSeriesstoreCheckpoints checkpoints every live seriesstore engine each interval, one at a time.
func (r *Readcache) runSeriesstoreCheckpoints(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.checkpointSeriesstoreHeads(ctx)
		}
	}
}

func (r *Readcache) checkpointSeriesstoreHeads(ctx context.Context) {
	started := time.Now()
	checkpoints, failures := 0, 0
	for _, p := range r.ownedPartitionStates() {
		offset, ok := ingest.ReadOffsetFile(r.partitionOffsetFilePath(p.partitionID), p.partitionID, r.logger)
		if !ok {
			// Nothing committed yet: a restart doesn't resume the partition from an offset either.
			continue
		}
		checkpoint, err := json.Marshal(seriesstoreCheckpoint{Offset: offset})
		if err != nil {
			level.Warn(r.logger).Log("msg", "encoding a seriesstore checkpoint", "partition", p.partitionID, "err", err)
			continue
		}
		p.tenantsMu.RLock()
		dbs := make([]*partitionTSDB, 0, len(p.tenants))
		for _, db := range p.tenants {
			dbs = append(dbs, db)
		}
		p.tenantsMu.RUnlock()
		for _, db := range dbs {
			if ctx.Err() != nil {
				return
			}
			engine, ok := db.db.(seriesstoreStore)
			if !ok || db.IsClosed() {
				continue
			}
			if err := engine.Checkpoint(ctx, checkpoint, seriesstoreCheckpointDuty); err != nil {
				// A close stops a checkpoint at its next batch, and snapshots the head itself.
				if ctx.Err() == nil && !db.IsClosed() {
					failures++
					level.Warn(r.logger).Log("msg", "checkpointing a seriesstore engine", "user", db.tenantID, "partition", db.partitionID, "err", err)
				}
				continue
			}
			checkpoints++
		}
	}
	level.Info(r.logger).Log("msg", "checkpointed the seriesstore engines", "engines", checkpoints, "failures", failures, "duration", time.Since(started))
}

// rewindSeriesstoreResume moves a resumed partition's offset file back to the oldest offset its seriesstore engines
// restored a checkpoint of, which every one of them has the records up to. It returns true when an engine restored no
// snapshot at all: the reader then has to replay the partition from a timestamp.
func (r *Readcache) rewindSeriesstoreResume(p *partitionState, offsetFilePath string) (lost bool) {
	if r.cfg.Engine != mimir_tsdb.EngineSeriesstore {
		return false
	}
	current, ok := ingest.ReadOffsetFile(offsetFilePath, p.partitionID, r.logger)
	if !ok {
		return true
	}
	oldest := current
	p.tenantsMu.RLock()
	for _, db := range p.tenants {
		engine, ok := db.db.(seriesstoreStore)
		if !ok {
			continue
		}
		if !engine.Restored() {
			lost = true
			break
		}
		blob := engine.RestoredCheckpoint()
		if blob == nil {
			continue
		}
		var checkpoint seriesstoreCheckpoint
		if err := json.Unmarshal(blob, &checkpoint); err != nil {
			level.Warn(r.logger).Log("msg", "decoding a seriesstore checkpoint", "user", db.tenantID, "partition", db.partitionID, "err", err)
			lost = true
			break
		}
		oldest = min(oldest, checkpoint.Offset)
	}
	p.tenantsMu.RUnlock()
	if lost || oldest == current {
		return lost
	}
	if err := ingest.WriteOffsetFile(offsetFilePath, p.partitionID, oldest, r.logger); err != nil {
		level.Warn(r.logger).Log("msg", "rewinding the offset file to the oldest seriesstore checkpoint", "partition", p.partitionID, "err", err)
		return true
	}
	level.Warn(r.logger).Log("msg", "readcache: seriesstore engines restored checkpoints taken while ingesting, replaying from the oldest",
		"partition", p.partitionID, "offset", oldest, "committed_offset", current)
	return false
}
