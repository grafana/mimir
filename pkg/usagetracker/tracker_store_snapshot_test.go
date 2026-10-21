// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
	"github.com/grafana/mimir/pkg/usagetracker/tenantshard"
)

const (
	snapshotTestIdleTimeout = 20 * time.Minute
	snapshotTestUser        = "user1"

	defaultSnapshotTestNumShards = tenantshard.DefaultNumShards
)

// snapshotTestNow is inside the first 2-hour bucket of the day, so clock.ToMinutes of it is 62.
var snapshotTestNow = time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC)

func TestPutPaddedUvarint(t *testing.T) {
	for _, v := range []uint64{0, 1, 127, 128, 300, 16383, 16384, 1 << 20, 1<<32 - 1, 1 << 32, 1<<56 - 1, 1 << 56, 1<<63 - 1, 1 << 63, math.MaxUint64} {
		t.Run(strconv.FormatUint(v, 10), func(t *testing.T) {
			// Pad the value on both sides, to check that the decoders consume exactly the padded encoding.
			const before, after = 0xAA, 0xBB
			buf := []byte{before}
			buf = append(buf, make([]byte, binary.MaxVarintLen64)...)
			buf = append(buf, after)
			putPaddedUvarint(buf[1:1+binary.MaxVarintLen64], v)

			got, n := binary.Uvarint(buf[1:])
			require.Equal(t, v, got)
			require.Equal(t, binary.MaxVarintLen64, n)

			dec := encoding.Decbuf{B: buf}
			require.Equal(t, byte(before), dec.Byte())
			require.Equal(t, v, dec.Uvarint64())
			require.NoError(t, dec.Err())
			require.Equal(t, byte(after), dec.Byte())
			require.NoError(t, dec.Err())
			require.Empty(t, dec.B)
		})
	}

}

func TestTrackerStore_Snapshot_PublishedFormat(t *testing.T) {
	for _, numShards := range []int{1, defaultSnapshotTestNumShards, 32} {
		t.Run(fmt.Sprintf("numShards=%d", numShards), func(t *testing.T) {
			store := newSnapshotTestStore(t, numShards, log.NewNopLogger())
			trackSnapshotTestSeries(t, store, 0, numShards)

			header, rest := decodeSnapshotHeader(t, store.snapshot(0, snapshotTestNow, nil))
			require.Equal(t, byte(snapshotEncodingVersion), header.version)
			require.Equal(t, uint64(numShards), header.numShards)
			require.Equal(t, uint8(0), header.shard)
			require.Equal(t, snapshotTestSeries(0, numShards), rest[snapshotTestUser])
		})
	}
}

func TestTrackerStore_LoadSnapshot_ReadsEveryShardCount(t *testing.T) {
	// Every combination of "format the snapshot was written in" and "count the loading store is
	// configured with" a rollout can produce. All of them load every series: a snapshot written
	// with a different count is re-sharded.
	for _, tc := range []struct {
		name              string
		version           byte
		snapshotNumShards int
		storeNumShards    int
	}{
		{
			name:              "v2 snapshot into a store with the same count",
			version:           snapshotEncodingVersion,
			snapshotNumShards: 32,
			storeNumShards:    32,
		},
		{
			name:              "v2 snapshot with the legacy count into a store with the legacy count",
			version:           snapshotEncodingVersion,
			snapshotNumShards: defaultSnapshotTestNumShards,
			storeNumShards:    defaultSnapshotTestNumShards,
		},
		{
			name:              "v2 snapshot into a store with fewer shards",
			version:           snapshotEncodingVersion,
			snapshotNumShards: 32,
			storeNumShards:    defaultSnapshotTestNumShards,
		},
		{
			name:              "v2 snapshot with the max count into a store with two shards",
			version:           snapshotEncodingVersion,
			snapshotNumShards: tenantshard.MaxNumShards,
			storeNumShards:    2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const shard = 3
			series := snapshotTestSeries(shard, tc.snapshotNumShards)
			data := encodeSnapshot(tc.version, tc.snapshotNumShards, shard, snapshotTestNow, map[string]map[uint64]clock.Minutes{snapshotTestUser: series})

			logs := bytes.NewBuffer(nil)
			store := newSnapshotTestStore(t, tc.storeNumShards, log.NewLogfmtLogger(logs))

			require.NoError(t, store.loadSnapshot(data, snapshotTestNow))
			require.Equal(t, map[string]uint64{snapshotTestUser: uint64(len(series))}, store.seriesCountsForTests())
			if tc.snapshotNumShards == tc.storeNumShards {
				require.Empty(t, logs.String())
			} else {
				require.Contains(t, logs.String(), "re-sharding snapshot written with a different shard count")
			}

			require.Equal(t, map[string]map[uint64]clock.Minutes{snapshotTestUser: series}, requireSeriesInTheirShards(t, store))
		})
	}
}

func TestTrackerStore_Snapshot_ShardCountMigration(t *testing.T) {
	// The rollout of a new shard count: a store with the legacy count publishes v1 snapshots, and a
	// store with another count loads them and publishes its own. The next step loads those, until
	// the last one goes back to the legacy count, as a rollback of the change would.
	// No step may lose a series, or put one in a shard where tracking it again wouldn't find it.
	series := make([]uint64, 0, 4*tenantshard.MaxNumShards)
	for i := 0; i < 4*tenantshard.MaxNumShards; i++ {
		series = append(series, uint64(i))
	}
	expectedCounts := map[string]uint64{snapshotTestUser: uint64(len(series))}

	source := newSnapshotTestStore(t, defaultSnapshotTestNumShards, log.NewNopLogger())
	rejected, err := source.trackSeries(t.Context(), snapshotTestUser, slices.Clone(series), snapshotTestNow)
	require.NoError(t, err)
	require.Empty(t, rejected)
	snapshots := snapshotAllShards(t, source)

	for _, numShards := range []int{32, tenantshard.MaxNumShards, 1, defaultSnapshotTestNumShards} {
		ok := t.Run(fmt.Sprintf("%d shards to %d shards", source.numShards, numShards), func(t *testing.T) {
			created := createdSeriesCounter{count: atomic.NewUint64(0)}
			target := newTrackerStore(snapshotTestIdleTimeout, 85, log.NewNopLogger(), limiterMock{}, created, false, 0, newTestShardFactoryWithShards(numShards))

			require.NoError(t, target.loadSnapshots(snapshots, snapshotTestNow))
			require.Equal(t, expectedCounts, target.seriesCountsForTests())
			requireSeriesInTheirShards(t, target)

			// A series that was loaded into the wrong shard would be created again.
			rejected, err := target.trackSeries(t.Context(), snapshotTestUser, slices.Clone(series), snapshotTestNow)
			require.NoError(t, err)
			require.Empty(t, rejected)
			require.Zero(t, created.count.Load())
			require.Equal(t, expectedCounts, target.seriesCountsForTests())

			source, snapshots = target, snapshotAllShards(t, target)
			header, _ := decodeSnapshotHeader(t, snapshots[0])
			require.Equal(t, uint64(numShards), header.numShards)
		})
		if !ok {
			return
		}
	}
}

func TestTrackerStore_LoadSnapshot_Invalid(t *testing.T) {
	for _, tc := range []struct {
		name        string
		data        []byte
		expectedErr string
	}{
		{
			name:        "unsupported encoding version",
			data:        encodeSnapshot(snapshotEncodingVersion+1, defaultSnapshotTestNumShards, 0, snapshotTestNow, nil),
			expectedErr: "unexpected snapshot version 3",
		},
		{
			name:        "zero shard count",
			data:        encodeSnapshot(snapshotEncodingVersion, 0, 0, snapshotTestNow, nil),
			expectedErr: "invalid snapshot format, shard count 0 is not a power of 2 between 1 and 256",
		},
		{
			name:        "shard count above the maximum",
			data:        encodeSnapshot(snapshotEncodingVersion, 2*tenantshard.MaxNumShards, 0, snapshotTestNow, nil),
			expectedErr: "invalid snapshot format, shard count 512 is not a power of 2 between 1 and 256",
		},
		{
			name:        "shard count not a power of 2",
			data:        encodeSnapshot(snapshotEncodingVersion, 24, 0, snapshotTestNow, nil),
			expectedErr: "invalid snapshot format, shard count 24 is not a power of 2 between 1 and 256",
		},
		{
			name:        "shard index out of bounds",
			data:        encodeSnapshot(snapshotEncodingVersion, 2, 7, snapshotTestNow, nil),
			expectedErr: "invalid snapshot format, shard 7 out of bounds",
		},
		{
			name:        "v1 snapshot",
			data:        encodeSnapshot(1, 0, 0, snapshotTestNow, nil),
			expectedErr: "unexpected snapshot version 1",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newSnapshotTestStore(t, defaultSnapshotTestNumShards, log.NewNopLogger())
			require.EqualError(t, store.loadSnapshot(tc.data, snapshotTestNow), tc.expectedErr)
			require.Empty(t, store.seriesCountsForTests())
		})
	}
}
func TestTrackerStore_LoadSnapshot_RoundTripForEveryShardCount(t *testing.T) {
	for _, numShards := range []int{1, 2, defaultSnapshotTestNumShards, 32, tenantshard.MaxNumShards} {
		t.Run(fmt.Sprintf("numShards=%d", numShards), func(t *testing.T) {
			source := newSnapshotTestStore(t, numShards, log.NewNopLogger())
			series := make([]uint64, 0, 4*numShards)
			for i := 0; i < 4*numShards; i++ {
				series = append(series, uint64(i))
			}
			rejected, err := source.trackSeries(context.Background(), snapshotTestUser, series, snapshotTestNow)
			require.NoError(t, err)
			require.Empty(t, rejected)

			target := newSnapshotTestStore(t, numShards, log.NewNopLogger())
			var data []byte
			for shard := 0; shard < numShards; shard++ {
				data = source.snapshot(uint8(shard), snapshotTestNow, data[:0])
				require.NoError(t, target.loadSnapshot(data, snapshotTestNow))
			}

			require.Equal(t, source.seriesCountsForTests(), target.seriesCountsForTests())
			for shard := 0; shard < numShards; shard++ {
				_, sourceShard := decodeSnapshotHeader(t, source.snapshot(uint8(shard), snapshotTestNow, nil))
				_, targetShard := decodeSnapshotHeader(t, target.snapshot(uint8(shard), snapshotTestNow, nil))
				require.Equalf(t, sourceShard, targetShard, "shard %d", shard)
			}
		})
	}
}

func TestTrackerStore_LoadSnapshot_TruncatedData(t *testing.T) {
	// A truncated blob is a real error: it means we read something we shouldn't have.
	full := encodeSnapshot(snapshotEncodingVersion, 32, 3, snapshotTestNow, map[string]map[uint64]clock.Minutes{
		snapshotTestUser: snapshotTestSeries(3, 32),
	})

	for _, tc := range []struct {
		name   string
		data   []byte
		errMsg string
	}{
		{
			name:   "empty",
			data:   nil,
			errMsg: "invalid snapshot format, expected version: ",
		},
		{
			name:   "cut after the version byte",
			data:   full[:1],
			errMsg: "invalid snapshot format, shard count expected: ",
		},
		{
			name:   "cut after the shard count",
			data:   full[:2],
			errMsg: "invalid snapshot format, shard expected: ",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newSnapshotTestStore(t, 32, log.NewNopLogger())
			err := store.loadSnapshot(tc.data, snapshotTestNow)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func newSnapshotTestStore(t *testing.T, numShards int, logger log.Logger) *trackerStore {
	t.Helper()
	return newTrackerStore(snapshotTestIdleTimeout, 85, logger, limiterMock{}, noopEvents{}, false, 0, newTestShardFactoryWithShards(numShards))
}

// snapshotTestSeries returns 3 series that belong to shard when a tenant is split numShards ways,
// with the timestamp they'd have if they were tracked at snapshotTestNow.
func snapshotTestSeries(shard uint8, numShards int) map[uint64]clock.Minutes {
	series := make(map[uint64]clock.Minutes, 3)
	for i := 0; i < 3; i++ {
		series[uint64(shard)+uint64(i*numShards)] = clock.ToMinutes(snapshotTestNow)
	}
	return series
}

func trackSnapshotTestSeries(t *testing.T, store *trackerStore, shard uint8, numShards int) {
	t.Helper()
	rejected, err := store.trackSeries(context.Background(), snapshotTestUser, slices.Sorted(maps.Keys(snapshotTestSeries(shard, numShards))), snapshotTestNow)
	require.NoError(t, err)
	require.Empty(t, rejected)
}

// encodeSnapshot writes a snapshot in the given encoding version, the way the binary that
// published that version wrote it: v1 has no shard count in its header, v2 does.
func encodeSnapshot(version byte, numShards int, shard uint8, now time.Time, tenants map[string]map[uint64]clock.Minutes) []byte {
	buf := encoding.Encbuf{}
	buf.PutByte(version)
	buf.PutUvarint64(uint64(numShards))
	buf.PutByte(shard)
	buf.PutBE64(uint64(now.Unix()))
	buf.PutUvarint64(uint64(len(tenants)))
	for _, tenantID := range slices.Sorted(maps.Keys(tenants)) {
		buf.PutUvarintStr(tenantID)
		series := tenants[tenantID]
		buf.PutUvarint64(uint64(len(series)))
		for _, ref := range slices.Sorted(maps.Keys(series)) {
			buf.PutBE64(ref)
			buf.PutByte(byte(series[ref]))
		}
	}
	return buf.Get()
}

type snapshotHeader struct {
	version   byte
	numShards uint64
	shard     uint8
}

// decodeSnapshotHeader decodes a snapshot's header and its tenants, without going through
// loadSnapshot, so that tests can assert on what was actually written to the blob.
func decodeSnapshotHeader(t *testing.T, data []byte) (snapshotHeader, map[string]map[uint64]clock.Minutes) {
	t.Helper()
	snapshot := encoding.Decbuf{B: data}

	header := snapshotHeader{version: snapshot.Byte()}
	require.NoError(t, snapshot.Err())
	header.numShards = snapshot.Uvarint64()
	require.NoError(t, snapshot.Err())
	header.shard = snapshot.Byte()
	require.NoError(t, snapshot.Err())

	_ = snapshot.Be64() // Snapshot time, not asserted here.
	require.NoError(t, snapshot.Err())

	tenantsLen := snapshot.Uvarint64()
	require.NoError(t, snapshot.Err())

	tenants := make(map[string]map[uint64]clock.Minutes, tenantsLen)
	for i := uint64(0); i < tenantsLen; i++ {
		tenantID := snapshot.UvarintStr()
		require.NoError(t, snapshot.Err())

		seriesLen := snapshot.Uvarint64()
		require.NoError(t, snapshot.Err())

		series := make(map[uint64]clock.Minutes, seriesLen)
		for j := uint64(0); j < seriesLen; j++ {
			ref := snapshot.Be64()
			require.NoError(t, snapshot.Err())
			series[ref] = clock.Minutes(snapshot.Byte())
			require.NoError(t, snapshot.Err())
		}
		tenants[tenantID] = series
	}
	return header, tenants
}

// snapshotAllShards returns the snapshot of every shard of store.
func snapshotAllShards(t *testing.T, store *trackerStore) [][]byte {
	t.Helper()
	snapshots := make([][]byte, 0, store.numShards)
	for shard := 0; shard < store.numShards; shard++ {
		snapshots = append(snapshots, store.snapshot(uint8(shard), snapshotTestNow, nil))
	}
	return snapshots
}

// requireSeriesInTheirShards requires every series of store to be in the shard that store's
// shard count assigns to it, and returns the series of every tenant.
func requireSeriesInTheirShards(t *testing.T, store *trackerStore) map[string]map[uint64]clock.Minutes {
	t.Helper()
	all := map[string]map[uint64]clock.Minutes{}
	for shard, data := range snapshotAllShards(t, store) {
		_, tenants := decodeSnapshotHeader(t, data)
		for tenantID, series := range tenants {
			if all[tenantID] == nil {
				all[tenantID] = map[uint64]clock.Minutes{}
			}
			for ref, ts := range series {
				require.Equalf(t, uint64(shard), ref%uint64(store.numShards), "tenant %s, series %d", tenantID, ref)
				all[tenantID][ref] = ts
			}
		}
	}
	return all
}

// lockCountingMap counts how many times the shard lock is taken.
type lockCountingMap struct {
	tenantshard.Map
	locks int
}

func (m *lockCountingMap) Lock() {
	m.locks++
	m.Map.Lock()
}

func TestLoadSnapshotSeries(t *testing.T) {
	const shard, numShards = 1, 4
	refs := func(n int) []refTimestamp {
		refs := make([]refTimestamp, 0, n)
		for i := 0; i < n; i++ {
			refs = append(refs, refTimestamp{Ref: uint64(shard + i*numShards), Timestamp: clock.ToMinutes(snapshotTestNow)})
		}
		return refs
	}
	newTenant := func(t *testing.T) (*trackerStore, *trackedTenant, *lockCountingMap) {
		store := newSnapshotTestStore(t, numShards, log.NewNopLogger())
		tenant := store.getOrCreateTenant(snapshotTestUser)
		tenant.RUnlock()
		m := &lockCountingMap{Map: tenant.shards[shard]}
		tenant.shards[shard] = m
		return store, tenant, m
	}

	t.Run("empty shard is loaded in one lock hold", func(t *testing.T) {
		store, tenant, m := newTenant(t)
		n := 2*loadSnapshotBatchSize + 1
		loadSnapshotSeries(tenant, shard, refs(n), store.shardMask(), n)
		require.Equal(t, 1, m.locks)
		require.Equal(t, n, m.Count())
		require.Equal(t, uint64(n), tenant.series.Load())
	})

	t.Run("shard with series is loaded a batch per lock hold", func(t *testing.T) {
		store, tenant, m := newTenant(t)
		trackSnapshotTestSeries(t, store, shard, numShards)
		tracked := m.Count()
		m.locks = 0

		n := 2*loadSnapshotBatchSize + 1
		loadSnapshotSeries(tenant, shard, refs(n), store.shardMask(), n)
		require.Equal(t, 1+3, m.locks, "one hold to check the shard, then one per batch")
		require.Equal(t, n, m.Count(), "the tracked series are in the snapshot too, so they are not counted twice")
		require.Equal(t, uint64(n), tenant.series.Load())
		require.Greater(t, tracked, 0)
	})
}
