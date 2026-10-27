// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/util/atomicfs"
)

const (
	// shardSizeRecordFilename is the file in the tenant's TSDB directory that keeps the tenant's shard size and
	// since when the ingester has seen it, so a restart doesn't lose when the shard last changed.
	shardSizeRecordFilename = "mimir.shard-size.json"

	shardSizeRecordVersion1 = 1
)

type shardSizeRecord struct {
	Version   int   `json:"version"`
	ShardSize int   `json:"shard_size"`
	SinceMs   int64 `json:"since_ms"`
}

// observeShardSize records the tenant's current shard size. When it differs from the size seen before, the time
// since which it is unchanged becomes now. The first call after the TSDB opens compares with the size kept in the
// TSDB directory, so a shard size that changed while the ingester was down is detected; with no kept size the time
// is now as well. SeriesCounts uses the time to tell whether counts can be added up across partitions: when a
// tenant's shard grows, some series move to a new partition while their older samples stay in the old one.
//
// Accepted gap: a shard size that changes and changes back to the kept size while the ingester is down is not
// detected, since the first call sees the kept size again and keeps its time. Counts across partitions may then
// count series written to other partitions during the change twice. This needs every answering ingester to have
// been down through both changes, so it is accepted.
//
// This method should be only called from the ownedSeries service goroutine.
func (u *userTSDB) observeShardSize(shardSize int, now time.Time, logger log.Logger) {
	if u.shardSizeSeenKnown && u.shardSizeSeen == shardSize {
		return
	}
	sinceMs := now.UnixMilli()
	kept := false
	if !u.shardSizeSeenKnown && u.db != nil {
		rec, err := readShardSizeRecord(u.db.Dir())
		if err != nil && !os.IsNotExist(errors.Cause(err)) {
			level.Warn(logger).Log("msg", "failed to read the kept shard size, treating it as changed now", "user", u.userID, "err", err)
		}
		if err == nil && rec.ShardSize == shardSize && rec.SinceMs > 0 {
			sinceMs = rec.SinceMs
			kept = true
		}
	}
	u.shardSizeSeen = shardSize
	u.shardSizeSeenKnown = true
	u.shardSizeUnchangedSinceMs.Store(sinceMs)

	if u.db == nil || kept {
		return
	}
	if err := writeShardSizeRecord(u.db.Dir(), shardSizeRecord{Version: shardSizeRecordVersion1, ShardSize: shardSize, SinceMs: sinceMs}); err != nil {
		// The time is still kept in memory. Remove the old file, so after a restart the shard size counts as
		// changed at the restart rather than as unchanged since the old file's time.
		level.Warn(logger).Log("msg", "failed to keep the shard size", "user", u.userID, "err", err)
		_ = os.Remove(filepath.Join(u.db.Dir(), shardSizeRecordFilename))
	}
}

// shardSizeUnchangedSince returns the time since which the tenant's shard size is unchanged, in milliseconds, or 0
// when the ownedSeries service hasn't checked the tenant yet or doesn't run.
func (u *userTSDB) shardSizeUnchangedSince() int64 {
	return u.shardSizeUnchangedSinceMs.Load()
}

func writeShardSizeRecord(dir string, rec shardSizeRecord) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	f, err := atomicfs.Create(filepath.Join(dir, shardSizeRecordFilename))
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.File.Close()
		_ = os.Remove(f.Name())
		return err
	}
	return f.Close()
}

func readShardSizeRecord(dir string) (shardSizeRecord, error) {
	fpath := filepath.Join(dir, shardSizeRecordFilename)
	b, err := os.ReadFile(fpath)
	if err != nil {
		return shardSizeRecord{}, errors.Wrapf(err, "failed to read %s", fpath)
	}
	var rec shardSizeRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		return shardSizeRecord{}, errors.Wrapf(err, "failed to parse %s as JSON", fpath)
	}
	if rec.Version != shardSizeRecordVersion1 {
		return shardSizeRecord{}, errors.Errorf("unexpected shard size file version %d", rec.Version)
	}
	return rec, nil
}
