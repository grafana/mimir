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
)

// snapshotEncodingVersion is the version of the per-shard snapshot binary format.
// Version 2 added the total shard count to the header (right after the version byte) so
// that snapshots written with a different shard count can be detected and discarded on load.
const snapshotEncodingVersion = 2

func (t *trackerStore) snapshot(shard uint8, now time.Time, buf []byte) []byte {
	t.mtx.RLock()
	// We'll use clonedTenants to know which tenants are present and avoid holding t.mtx
	clonedTenants := maps.Clone(t.tenants)
	t.mtx.RUnlock()

	snapshot := encoding.Encbuf{B: buf[:0]}
	snapshot.PutByte(snapshotEncodingVersion)
	// Encode the total shard count so that on load we can discard snapshots written with a
	// different shard count. Encoded as a uvarint because the count can be up to 256, which
	// does not fit in a single byte (unlike the shard index below, which is in [0, 256)).
	snapshot.PutUvarint64(uint64(t.numShards))
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
	if version != snapshotEncodingVersion {
		// This is likely a snapshot written by a different binary version (e.g. the pre-v2
		// format that didn't encode the shard count). We can't safely interpret it, so we
		// discard it rather than failing startup; the state will be rebuilt from events.
		level.Warn(t.logger).Log("msg", "discarding snapshot with unsupported encoding version", "version", version, "expected_version", snapshotEncodingVersion)
		return nil
	}

	snapshotNumShards := snapshot.Uvarint64()
	if err := snapshot.Err(); err != nil {
		return fmt.Errorf("invalid snapshot format, shard count expected: %w", err)
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
