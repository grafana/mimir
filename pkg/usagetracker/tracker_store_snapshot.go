// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"fmt"
	"iter"
	"maps"
	"runtime"
	"time"

	"github.com/prometheus/prometheus/tsdb/encoding"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
)

const (
	snapshotEncodingVersion   = 2
	snapshotEncodingVersionV1 = 1
)

func (t *trackerStore) snapshot(shard uint8, now time.Time, buf []byte) []byte {
	t.mtx.RLock()
	// We'll use clonedTenants to know which tenants are present and avoid holding t.mtx
	clonedTenants := maps.Clone(t.tenants)
	t.mtx.RUnlock()

	// Stay on v1 until some series actually carries a locality hash. A v2
	// snapshot cannot be loaded by a usage-tracker that has not been upgraded,
	// so a rolling deploy with the distributor flag still off keeps today's format.
	version := byte(snapshotEncodingVersionV1)
	for tenantID := range clonedTenants {
		if t.shardHasLocality(tenantID, shard) {
			version = snapshotEncodingVersion
			break
		}
	}

	snapshot := encoding.Encbuf{B: buf[:0]}
	snapshot.PutByte(version)
	snapshot.PutByte(shard)
	snapshot.PutBE64(uint64(now.Unix()))
	snapshot.PutUvarint64(uint64(len(clonedTenants)))
	for tenantID := range clonedTenants {
		snapshot.PutUvarintStr(tenantID)

		tenant := t.getOrCreateTenant(tenantID)
		m := tenant.shards[shard]
		m.Lock()
		length, series := m.Items()
		var localities iter.Seq2[uint64, uint32]
		if version == snapshotEncodingVersion {
			localities = m.Localities()
		}
		m.Unlock()
		// Once we have the series iterator we don't need to hold the mutex anymore.
		tenant.RUnlock()
		locBySeries := make(map[uint64]uint32, length)
		if localities != nil {
			for hash, loc := range localities {
				if loc != 0 {
					locBySeries[hash] = loc
				}
			}
		}
		snapshot.PutUvarint64(uint64(length))
		for s, ts := range series {
			snapshot.PutBE64(s)
			snapshot.PutByte(byte(ts))
			if version == snapshotEncodingVersion {
				snapshot.PutBE32(locBySeries[s])
			}
		}
	}
	return snapshot.Get()
}

// loadSnapshots loads the snapshots from the given shards concurrently with GOMAXPROCS workers.
// This speeds up the snapshot loading process as each shard can be loaded independently.
// This reduces the amount of time track requests spend waiting on shard locks.
// shardHasLocality reports whether this tenant's shard holds any series with a
// non-zero locality hash. It releases the tenant lock it takes.
func (t *trackerStore) shardHasLocality(tenantID string, shard uint8) bool {
	tenant := t.getOrCreateTenant(tenantID)
	m := tenant.shards[shard]
	m.Lock()
	localities := m.Localities()
	m.Unlock()
	tenant.RUnlock()
	for _, loc := range localities {
		if loc != 0 {
			return true
		}
	}
	return false
}

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
	if version != snapshotEncodingVersion && version != snapshotEncodingVersionV1 {
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
			Locality  uint32
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
			var loc uint32
			if version == snapshotEncodingVersion {
				loc = snapshot.Be32()
				if err := snapshot.Err(); err != nil {
					return fmt.Errorf("failed to read series locality %d: %w", i, err)
				}
			}
			if expirationWatermark.GreaterThan(snapshotTs) {
				// We're not interested in this series, it was about to be evicted.
				continue
			}
			refs = append(refs, refTimestamp{Ref: s, Timestamp: snapshotTs, Locality: loc})
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
				t.noteLocality(tenant, m, ref.Ref, ref.Locality)
			}
			tenant.series.Add(uint64(len(refs)))
		} else {
			for _, ref := range refs {
				_, _ = m.Put(ref.Ref, ref.Timestamp, tenant.series, nil, false)
				t.noteLocality(tenant, m, ref.Ref, ref.Locality)
			}
		}
		m.Unlock()
		tenant.RUnlock()
	}
	return nil
}
