// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"fmt"
	"maps"
	"time"

	"github.com/prometheus/prometheus/tsdb/encoding"
	"go.uber.org/atomic"
)

const snapshotEncodingVersion = 1

func (t *trackerStore) snapshot(shard uint8, now time.Time, buf []byte) []byte {
	t.lock[shard].RLock()
	shardTenants := maps.Clone(t.data[shard])
	t.lock[shard].RUnlock()

	snapshot := encoding.Encbuf{B: buf[:0]}
	snapshot.PutByte(snapshotEncodingVersion)
	snapshot.PutByte(shard)
	snapshot.PutBE64(uint64(now.Unix()))
	snapshot.PutUvarint64(uint64(len(shardTenants)))
	for tenantID, shard := range shardTenants {
		shard.RLock()
		shardClone := maps.Clone(shard.series)
		shard.RUnlock()
		snapshot.PutUvarintStr(tenantID)
		snapshot.PutUvarint64(uint64(len(shardClone)))
		for s, ts := range shardClone {
			snapshot.PutBE64(s)
			snapshot.PutByte(byte(ts.Load()))
		}
	}
	return snapshot.Get()
}

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
	if !areInValidSpanToCompareMinutes(now, snapshotTime) {
		return fmt.Errorf("snapshot is too old, snapshot time is %s, now is %s", snapshotTime, now)
	}

	tenantsLen := snapshot.Uvarint64()
	if err := snapshot.Err(); err != nil {
		return fmt.Errorf("invalid snapshot format, expected tenants len: %w", err)
	}

	t.lock[shard].RLock()
	localShardTenantsClone := maps.Clone(t.data[shard])
	t.lock[shard].RUnlock()

	// We won't be holding the mutex on tenants series of each shard while checking timestamps.
	// If we find a too old lastSeen, we might try to update it on our copy of the pointer to atomic.Uint64,
	// but since we're not holding the mutex, it might be evicted at the same time.
	// We fix that by requiring a mutex (at least read mutex) for updating lastSeen on values that are beyond 3/4 expiration.
	mutexWatermark := toMinutes(now.Add(time.Duration(-3. / 4. * float64(t.idleTimeout))))

	// Some series might have been right on the boundary of being evicted when we took the snapshot.
	// Don't load them.
	expirationWatermark := toMinutes(now.Add(-t.idleTimeout))

	for i := 0; i < int(tenantsLen); i++ {
		// We don't check for tenantID string length here, because we don't require it to be non-empty when we track series.
		tenantID := snapshot.UvarintStr()
		if err := snapshot.Err(); err != nil {
			return fmt.Errorf("failed to read tenant ID %d: %w", i, err)
		}
		localTenant := localShardTenantsClone[tenantID]
		if localTenant == nil {
			// We know nothing about this tenant, maybe we need to create it
			localTenant = t.getOrCreateTenantShard(tenantID, shard, t.limiter.localSeriesLimit(tenantID))
		}

		if err := t.loadTenantSnapshot(tenantID, localTenant, &snapshot, mutexWatermark, expirationWatermark); err != nil {
			return fmt.Errorf("failed loading snapshot for tenant %s (%d): %w", tenantID, i, err)
		}
	}
	return nil
}

func (t *trackerStore) loadTenantSnapshot(tenantID string, shard *tenantShard, snapshot *encoding.Decbuf, mutexWatermark minutes, expirationWatermark minutes) error {
	info := t.getOrCreateTenantInfo(tenantID)
	defer info.release()

	return shard.loadSnapshot(snapshot, &info.series, mutexWatermark, expirationWatermark)
}

func (shard *tenantShard) loadSnapshot(snapshot *encoding.Decbuf, totalTenantSeries *atomic.Uint64, mutexWatermark, expirationWatermark minutes) error {
	type entry struct {
		series uint64
		ts     minutes
	}

	seriesLen := int(snapshot.Uvarint64())
	if err := snapshot.Err(); err != nil {
		return fmt.Errorf("failed to read series len: %w", err)
	}
	shard.RLock()
	seriesClone := maps.Clone(shard.series)
	shard.RUnlock()

	// TODO: reuse buffers here.
	// We could use ugly logic to use the same slice here for both cases, but it's probably not worth it.
	var newSeries []entry
	var belowMutexWatermark []entry

	for i := 0; i < seriesLen; i++ {
		s := snapshot.Be64()
		if err := snapshot.Err(); err != nil {
			return fmt.Errorf("failed to read series ref %d: %w", i, err)
		}

		snapshotTs := minutes(snapshot.Byte())
		if err := snapshot.Err(); err != nil {
			return fmt.Errorf("failed to read series timestamp %d: %w", i, err)
		}
		if expirationWatermark.greaterThan(snapshotTs) {
			// We're not interested in this series, it was about to be evicted.
			continue
		}

		ts, ok := seriesClone[s]
		if ok {
			lastSeen := casIfGreater(snapshotTs, ts)
			if mutexWatermark.greaterThan(lastSeen) {
				// We've CASed the last seen timestamp, but we're getting close to this value being evicted.
				// Since we're operating on a seriesClone, we might be updating an atomic value that isn't referenced by shard.series anymore.
				// So, try this series again later with mutex.
				belowMutexWatermark = append(belowMutexWatermark, entry{s, lastSeen})
			}
			continue
		}
		newSeries = append(newSeries, entry{s, snapshotTs})
	}

	// Check the series that were very close to expiration, if any.
	if len(belowMutexWatermark) > 0 {
		shard.RLock()
		for _, e := range belowMutexWatermark {
			ts, ok := shard.series[e.series]
			if ok {
				casIfGreater(e.ts, ts)
				continue
			}
			// See? It didn't exist anymore.
			newSeries = append(newSeries, e)
		}
		shard.RUnlock()
	}

	// Create series that didn't exist.
	if len(newSeries) > 0 {
		shard.Lock()
		for _, e := range newSeries {
			ts, ok := shard.series[e.series]
			if ok {
				casIfGreater(e.ts, ts)
				continue
			}
			shard.series[e.series] = atomic.NewInt32(int32(e.ts))
			// Replaying snapshot ignores limits. Series that were created elsewhere must be created here too.
			totalTenantSeries.Inc()
		}
		shard.Unlock()
	}
	return nil
}
