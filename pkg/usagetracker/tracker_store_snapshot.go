// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"fmt"
	"maps"
	"runtime"
	"time"

	"github.com/prometheus/prometheus/tsdb/encoding"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
)

const snapshotEncodingVersion = 1

func (t *trackerStore) snapshot(shard uint8, now time.Time, buf []byte) []byte {
	t.mtx.RLock()
	// We'll use clonedTenants to know which tenants are present and avoid holding t.mtx
	clonedTenants := maps.Clone(t.tenants)
	t.mtx.RUnlock()

	snapshot := encoding.Encbuf{B: buf[:0]}
	snapshot.PutByte(snapshotEncodingVersion)
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
func (t *trackerStore) loadSnapshots(shards [][]byte, now time.Time) error {
	if len(shards) == 0 {
		return nil
	}

	if len(shards) == 1 {
		return t.loadSnapshot(shards[0], now)
	}

	jobs := make(chan []byte, len(shards))
	for _, shard := range shards {
		jobs <- shard
	}
	close(jobs)

	workers := min(len(shards), runtime.GOMAXPROCS(0))
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
		return fmt.Errorf("unexpected snapshot version %d", version)
	}
	shard := snapshot.Byte()
	if err := snapshot.Err(); err != nil {
		return fmt.Errorf("invalid snapshot format, shard expected: %w", err)
	}
	if shard >= shards {
		return fmt.Errorf("invalid snapshot format, shard %d out of bounds", shard)
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
