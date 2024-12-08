// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"fmt"
	"maps"
	"time"

	"github.com/prometheus/prometheus/tsdb/encoding"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
	"github.com/grafana/mimir/pkg/usagetracker/tenantshard"
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

		tenant := t.getOrCreateTenant(tenantID, zeroAsNoLimit(t.limiter.localSeriesLimit(tenantID)))
		clonser := make(chan func(tenantshard.LengthCallback, tenantshard.IteratorCallback))
		tenant.shards[shard].Events() <- tenantshard.Clone(clonser)
		clone := <-clonser
		// Once we have the clone we don't need to hold the mutex anymore as it works on a clone.
		tenant.RUnlock()

		clone(
			func(length int) {
				snapshot.PutUvarint64(uint64(length))
			},
			func(s uint64, ts clock.Minutes) {
				snapshot.PutBE64(s)
				snapshot.PutByte(byte(ts))
			},
		)
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

	done := make(chan struct{}, tenantsLen)
	sent := 0
	for i := 0; i < int(tenantsLen); i++ {
		// We don't check for tenantID string length here, because we don't require it to be non-empty when we track series.
		tenantID := snapshot.UvarintStr()
		if err := snapshot.Err(); err != nil {
			return fmt.Errorf("failed to read tenant ID %d: %w", i, err)
		}

		seriesLen := int(snapshot.Uvarint64())
		if err := snapshot.Err(); err != nil {
			return fmt.Errorf("failed to read series len: %w", err)
		}

		// TODO: it's probably less-blocking to work on a cloned iterator here.
		refs := make([]tenantshard.RefTimestamp, 0, seriesLen)
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
			refs = append(refs, tenantshard.RefTimestamp{Ref: s, Timestamp: snapshotTs})
		}

		tenant := t.getOrCreateTenant(tenantID, zeroAsNoLimit(t.limiter.localSeriesLimit(tenantID)))
		tenant.shards[shard].Events() <- tenantshard.Load(
			refs,
			tenant.series,
			done,
		)
		sent++
		tenant.RUnlock()
	}
	for i := 0; i < sent; i++ {
		<-done
	}
	return nil
}
