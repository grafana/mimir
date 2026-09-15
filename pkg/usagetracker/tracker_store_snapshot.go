// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"fmt"
	"maps"
	"runtime"
	"time"

	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
	"github.com/grafana/mimir/pkg/usagetracker/tenantshard"
)

const (
	// snapshotEncodingVersionV1 is the original per-shard snapshot format:
	// [version][shard][time][tenants...]. It doesn't record how many shards the tenant was
	// split into, so a V1 snapshot can only have been written with legacySnapshotNumShards
	// shards: that was the only count that existed while V1 was the format being published.
	snapshotEncodingVersionV1 = 1

	// snapshotEncodingVersionV2 adds the total shard count right after the version byte:
	// [version][num shards][shard][time][tenants...]. The count is a uvarint because it can be
	// up to 256, which doesn't fit in a single byte (unlike the shard index, which is in [0, 256)).
	// Recording it lets a load detect a snapshot written with a different shard count and discard
	// it, instead of mis-routing its series: series are placed by hash % count.
	snapshotEncodingVersionV2 = 2

	// legacySnapshotNumShards is the shard count that a V1 snapshot implies.
	legacySnapshotNumShards = tenantshard.DefaultNumShards
)

// snapshotEncodingVersion returns the format to write a snapshot of a store with numShards shards.
//
// V1 can't express a shard count other than the legacy one, but it is what every usage-tracker
// already in the field can read, so we keep publishing it while the count is the legacy one.
// Only a non-legacy count needs V2, and an operator who picks one has already accepted that
// existing snapshots are discarded.
func snapshotEncodingVersion(numShards int) byte {
	if numShards == legacySnapshotNumShards {
		return snapshotEncodingVersionV1
	}
	return snapshotEncodingVersionV2
}

func (t *trackerStore) snapshot(shard uint8, now time.Time, buf []byte) []byte {
	t.mtx.RLock()
	// We'll use clonedTenants to know which tenants are present and avoid holding t.mtx
	clonedTenants := maps.Clone(t.tenants)
	t.mtx.RUnlock()

	version := snapshotEncodingVersion(t.numShards)
	snapshot := encoding.Encbuf{B: buf[:0]}
	snapshot.PutByte(version)
	if version == snapshotEncodingVersionV2 {
		snapshot.PutUvarint64(uint64(t.numShards))
	}
	snapshot.PutByte(shard)
	snapshot.PutBE64(uint64(now.Unix()))
	snapshot.PutUvarint64(uint64(len(clonedTenants)))
	for tenantID := range clonedTenants {
		snapshot.PutUvarintStr(tenantID)

		tenant := t.getOrCreateTenant(tenantID)
		m := tenant.shards[shard]
		m.Lock()
		length, series := m.Items()
		m.Unlock()
		// Once we have the series iterator we don't need to hold the mutex anymore.
		tenant.RUnlock()
		snapshot.PutUvarint64(uint64(length))
		for s, ts := range series {
			snapshot.PutBE64(s)
			snapshot.PutByte(byte(ts))
		}
	}
	return snapshot.Get()
}

// loadSnapshots loads the snapshots from the given shards concurrently with GOMAXPROCS workers.
// This speeds up the snapshot loading process as each shard can be loaded independently.
// This reduces the amount of time track requests spend waiting on shard locks.
func (t *trackerStore) loadSnapshots(shardSnapshots [][]byte, now time.Time) error {
	if len(shardSnapshots) == 0 {
		return nil
	}

	if len(shardSnapshots) == 1 {
		return t.loadSnapshot(shardSnapshots[0], now)
	}

	jobs := make(chan []byte, len(shardSnapshots))
	for _, shard := range shardSnapshots {
		jobs <- shard
	}
	close(jobs)

	workers := min(len(shardSnapshots), runtime.GOMAXPROCS(0))
	g := errgroup.Group{}
	for i := 0; i < workers; i++ {
		g.Go(func() error {
			for shard := range jobs {
				if err := t.loadSnapshot(shard, now); err != nil {
					return err
				}
			}
			return nil
		})
	}

	return g.Wait()
}

// loadSnapshot loads the snapshot data into the tracker.
// It returns an error if the snapshot is invalid.
// The method checks if each tenant shard is empty and uses Load() for empty shards (faster)
// or Put() for non-empty shards (handles concurrent loads and deduplication).
func (t *trackerStore) loadSnapshot(data []byte, now time.Time) error {
	snapshot := encoding.Decbuf{B: data}
	version := snapshot.Byte()
	if err := snapshot.Err(); err != nil {
		return fmt.Errorf("invalid snapshot format, expected version: %w", err)
	}
	var snapshotNumShards uint64
	switch version {
	case snapshotEncodingVersionV1:
		// V1 doesn't record the count, and it was only ever written with the legacy one.
		snapshotNumShards = legacySnapshotNumShards
	case snapshotEncodingVersionV2:
		snapshotNumShards = snapshot.Uvarint64()
		if err := snapshot.Err(); err != nil {
			return fmt.Errorf("invalid snapshot format, shard count expected: %w", err)
		}
	default:
		// A snapshot written by a newer binary, or a corrupt one. We can't safely interpret it,
		// so we discard it rather than failing startup: the state is rebuilt from events.
		level.Warn(t.logger).Log("msg", "discarding snapshot with unsupported encoding version", "version", version, "supported_versions", fmt.Sprintf("%d, %d", snapshotEncodingVersionV1, snapshotEncodingVersionV2))
		return nil
	}
	if snapshotNumShards != uint64(t.numShards) {
		// The snapshot was written with a different shard count. Series were placed by
		// hash % snapshotNumShards, so loading them under hash % t.numShards would route
		// them to the wrong shards. Discard rather than corrupt; events will rebuild state.
		level.Warn(t.logger).Log("msg", "discarding snapshot written with a different shard count", "snapshot_shards", snapshotNumShards, "configured_shards", t.numShards)
		return nil
	}

	shard := snapshot.Byte()
	if err := snapshot.Err(); err != nil {
		return fmt.Errorf("invalid snapshot format, shard expected: %w", err)
	}
	if int(shard) >= t.numShards {
		// Defensive: the shard count matched but the index is out of range, which means the
		// snapshot is inconsistent. Discard it rather than indexing out of bounds.
		level.Warn(t.logger).Log("msg", "discarding snapshot with out-of-bounds shard index", "shard", shard, "configured_shards", t.numShards)
		return nil
	}

	snapshotTime := time.Unix(int64(snapshot.Be64()), 0)
	if err := snapshot.Err(); err != nil {
		return fmt.Errorf("invalid snapshot format, time expected: %w", err)
	}
	if !clock.AreInValidSpanToCompareMinutes(now, snapshotTime) {
		return fmt.Errorf("snapshot is too old, snapshot time is %s, now is %s", snapshotTime, now)
	}

	tenantsLen := snapshot.Uvarint64()
	if err := snapshot.Err(); err != nil {
		return fmt.Errorf("invalid snapshot format, expected tenants len: %w", err)
	}

	// Some series might have been right on the boundary of being evicted when we took the snapshot.
	// Don't load them.
	expirationWatermark := clock.ToMinutes(now.Add(-t.idleTimeout))

	for i := 0; i < int(tenantsLen); i++ {
		// We don't check for userID string length here, because we don't require it to be non-empty when we track series.
		tenantID := snapshot.UvarintStr()
		if err := snapshot.Err(); err != nil {
			return fmt.Errorf("failed to read tenant ID %d: %w", i, err)
		}

		seriesLen := int(snapshot.Uvarint64())
		if err := snapshot.Err(); err != nil {
			return fmt.Errorf("failed to read series len: %w", err)
		}

		type refTimestamp struct {
			Ref       uint64
			Timestamp clock.Minutes
		}

		refs := make([]refTimestamp, 0, seriesLen)
		for i := 0; i < seriesLen; i++ {
			s := snapshot.Be64()
			if err := snapshot.Err(); err != nil {
				return fmt.Errorf("failed to read series ref %d: %w", i, err)
			}

			snapshotTs := clock.Minutes(snapshot.Byte())
			if err := snapshot.Err(); err != nil {
				return fmt.Errorf("failed to read series timestamp %d: %w", i, err)
			}
			if expirationWatermark.GreaterThan(snapshotTs) {
				// We're not interested in this series, it was about to be evicted.
				continue
			}
			refs = append(refs, refTimestamp{Ref: s, Timestamp: snapshotTs})
		}

		tenant := t.getOrCreateTenant(tenantID)
		m := tenant.shards[shard]
		m.Lock()
		// Ensure the shard has enough capacity for this snapshot to minimize the number of rehashes.
		m.EnsureCapacity(uint32(len(refs)))

		// Check if the shard is empty. If it is, we can use the faster Load() method
		// which doesn't check for duplicates. Otherwise, use Put() which handles
		// concurrent loads and deduplication.
		if m.Count() == 0 {
			for _, ref := range refs {
				m.Load(ref.Ref, ref.Timestamp)
			}
			tenant.series.Add(uint64(len(refs)))
		} else {
			for _, ref := range refs {
				_, _ = m.Put(ref.Ref, ref.Timestamp, tenant.series, nil, false)
			}
		}
		m.Unlock()
		tenant.RUnlock()
	}
	return nil
}
