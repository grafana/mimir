// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"encoding/binary"
	"fmt"
	"maps"
	"runtime"
	"slices"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
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
	// Recording it lets a load detect a snapshot written with a different shard count and re-shard
	// its series, instead of mis-routing them: series are placed by hash % count.
	snapshotEncodingVersionV2 = 2

	// legacySnapshotNumShards is the shard count that a V1 snapshot implies.
	legacySnapshotNumShards = tenantshard.DefaultNumShards

	// snapshotSeriesSize is the encoded size of one series: its 8-byte ref and its 1-byte timestamp.
	snapshotSeriesSize = 8 + 1
)

// snapshotEncodingVersion returns the format to write a snapshot of a store with numShards shards.
//
// V1 can't express a shard count other than the legacy one, but it is what every usage-tracker
// already in the field can read, so we keep publishing it while the count is the legacy one.
// Only a non-legacy count needs V2, which a usage-tracker built before V2 existed can't read.
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

		// The series count goes before the series, but it is only known once they have all been read,
		// so leave room for it and fill it in at the end. See putPaddedUvarint.
		countAt := len(snapshot.B)
		snapshot.B = append(snapshot.B, make([]byte, binary.MaxVarintLen64)...)
		seriesAt := len(snapshot.B)
		snapshot.B = slices.Grow(snapshot.B, int(m.Stats().Resident)*snapshotSeriesSize)

		// Iterate reads the shard a chunk at a time, so a series that Cleanup removed and trackSeries put
		// again meanwhile can appear twice, first with its expired timestamp. loadSnapshot skips expired
		// series and Put deduplicates, so this only matters when the loading shard is empty, Load is used,
		// and the load happens in the same minute as that cleanup: then the series is counted twice until
		// the next cleanup removes the expired copy.
		count := 0
		m.Iterate(func(ref uint64, ts clock.Minutes) {
			snapshot.PutBE64(ref)
			snapshot.PutByte(byte(ts))
			count++
		})
		tenant.RUnlock()
		putPaddedUvarint(snapshot.B[countAt:seriesAt], uint64(count))
	}
	return snapshot.Get()
}

// putPaddedUvarint encodes v as a uvarint that fills b, which must be binary.MaxVarintLen64 bytes long: the
// bytes after the minimal encoding are continuation bytes that carry no bits, and the last one is 0.
// Uvarint decoders read bytes until one has no continuation bit, so they decode it like the minimal encoding.
func putPaddedUvarint(b []byte, v uint64) {
	for i := range len(b) - 1 {
		b[i] = byte(v) | 0x80
		v >>= 7
	}
	b[len(b)-1] = byte(v)
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
//
// A snapshot written with a different shard count than this store's is re-sharded: each series is
// loaded into the shard that this store's count assigns to it. Every series in a snapshot carries
// its full hash, so this doesn't lose any of them, and the shard count can change without
// discarding the snapshots that are already in storage.
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
		// Re-sharding relies on both counts being powers of 2, which the configuration enforces.
		if snapshotNumShards > tenantshard.MaxNumShards || !isPowerOfTwo(int(snapshotNumShards)) {
			return fmt.Errorf("invalid snapshot format, shard count %d is not a power of 2 between 1 and %d", snapshotNumShards, tenantshard.MaxNumShards)
		}
	default:
		return fmt.Errorf("unexpected snapshot version %d", version)
	}

	shard := snapshot.Byte()
	if err := snapshot.Err(); err != nil {
		return fmt.Errorf("invalid snapshot format, shard expected: %w", err)
	}
	if uint64(shard) >= snapshotNumShards {
		return fmt.Errorf("invalid snapshot format, shard %d out of bounds", shard)
	}

	if snapshotNumShards != uint64(t.numShards) {
		level.Debug(t.logger).Log("msg", "re-sharding snapshot written with a different shard count", "shard", shard, "snapshot_shards", snapshotNumShards, "configured_shards", t.numShards)
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

	// The series in this snapshot are the ones with hash % snapshotNumShards == shard. Both counts are
	// powers of 2, so if we have as many shards as the snapshot or fewer, they all belong to our shard
	// shard % t.numShards, and the loop below loads them in one pass. If we have more shards, they are
	// spread over our shards shard, shard+snapshotNumShards, shard+2*snapshotNumShards, and so on, and
	// the loop loads each of them with a pass over all the series.
	shardMask := t.shardMask()
	firstShard := uint64(shard) & shardMask
	passes := max(1, uint64(t.numShards)/snapshotNumShards)

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

		if _, _, err := tenant.ParseWithMetadata(tenantID); err != nil {
			level.Warn(t.logger).Log("msg", "skipping snapshot data for invalid tenant ID", "tenant", fmt.Sprintf("%q", tenantID), "err", err)
			continue
		}

		tenant := t.getOrCreateTenant(tenantID)
		for s := firstShard; s < uint64(t.numShards); s += snapshotNumShards {
			loadSnapshotSeries(tenant, uint8(s), refs, shardMask, len(refs)/int(passes))
		}
		tenant.RUnlock()
	}
	return nil
}

type refTimestamp struct {
	Ref       uint64
	Timestamp clock.Minutes
}

// loadSnapshotSeries loads the refs that belong to shard, the ones with ref&shardMask == shard,
// into that shard of tenant. capacity is how many of the refs are expected to belong to it.
// It checks if the shard is empty and uses Load() for an empty shard (faster)
// or Put() for a non-empty one (handles concurrent loads and deduplication).
func loadSnapshotSeries(tenant *trackedTenant, shard uint8, refs []refTimestamp, shardMask uint64, capacity int) {
	m := tenant.shards[shard]
	m.Lock()
	defer m.Unlock()
	// Ensure the shard has enough capacity for this snapshot to minimize the number of rehashes.
	m.EnsureCapacity(uint32(capacity))

	// Check if the shard is empty. If it is, we can use the faster Load() method
	// which doesn't check for duplicates. Otherwise, use Put() which handles
	// concurrent loads and deduplication.
	if m.Count() == 0 {
		loaded := 0
		for _, ref := range refs {
			if ref.Ref&shardMask == uint64(shard) {
				m.Load(ref.Ref, ref.Timestamp)
				loaded++
			}
		}
		tenant.series.Add(uint64(loaded))
	} else {
		for _, ref := range refs {
			if ref.Ref&shardMask == uint64(shard) {
				_, _ = m.Put(ref.Ref, ref.Timestamp, tenant.series, nil, false)
			}
		}
	}
}
