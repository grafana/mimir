// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
	"github.com/grafana/mimir/pkg/usagetracker/tenantshard"
)

const (
	snapshotTestIdleTimeout = 20 * time.Minute
	snapshotTestUser        = "user1"
)

// snapshotTestNow is inside the first 2-hour bucket of the day, so clock.ToMinutes of it is 62.
var snapshotTestNow = time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC)

func TestSnapshotEncodingVersion(t *testing.T) {
	// The legacy count is the default, so an unconfigured usage-tracker keeps publishing the
	// format that every usage-tracker in the field can read.
	require.Equal(t, tenantshard.DefaultNumShards, legacySnapshotNumShards)
	require.Equal(t, byte(snapshotEncodingVersionV1), snapshotEncodingVersion(legacySnapshotNumShards))

	// Any other count needs the format that records it.
	for _, numShards := range []int{1, 2, 15, 17, 32, tenantshard.MaxNumShards} {
		require.Equalf(t, byte(snapshotEncodingVersionV2), snapshotEncodingVersion(numShards), "numShards=%d", numShards)
	}
}

func TestTrackerStore_Snapshot_PublishedFormat(t *testing.T) {
	t.Run("legacy shard count publishes v1", func(t *testing.T) {
		store := newSnapshotTestStore(t, legacySnapshotNumShards, log.NewNopLogger())
		trackSnapshotTestSeries(t, store, 3, legacySnapshotNumShards)

		header, rest := decodeSnapshotHeader(t, store.snapshot(3, snapshotTestNow, nil))
		require.Equal(t, byte(snapshotEncodingVersionV1), header.version)
		require.Equal(t, uint64(legacySnapshotNumShards), header.numShards)
		require.Equal(t, uint8(3), header.shard)

		// A usage-tracker that only knows v1 reads the rest of the blob straight after the shard
		// byte, so the header must hold no extra bytes.
		require.Equal(t, snapshotTestSeries(3, legacySnapshotNumShards), rest[snapshotTestUser])
	})

	t.Run("non-legacy shard count publishes v2 with the count", func(t *testing.T) {
		const numShards = 32
		store := newSnapshotTestStore(t, numShards, log.NewNopLogger())
		trackSnapshotTestSeries(t, store, 3, numShards)

		header, rest := decodeSnapshotHeader(t, store.snapshot(3, snapshotTestNow, nil))
		require.Equal(t, byte(snapshotEncodingVersionV2), header.version)
		require.Equal(t, uint64(numShards), header.numShards)
		require.Equal(t, uint8(3), header.shard)
		require.Equal(t, snapshotTestSeries(3, numShards), rest[snapshotTestUser])
	})
}

func TestTrackerStore_LoadSnapshot_ReadsBothVersions(t *testing.T) {
	// Every combination of "format the snapshot was written in" and "count the loading store is
	// configured with" a rollout can produce.
	for _, tc := range []struct {
		name              string
		version           byte
		snapshotNumShards int
		storeNumShards    int
		expectLoaded      bool
	}{
		{
			name:              "v1 snapshot into a store with the legacy count",
			version:           snapshotEncodingVersionV1,
			snapshotNumShards: legacySnapshotNumShards,
			storeNumShards:    legacySnapshotNumShards,
			expectLoaded:      true,
		},
		{
			name:              "v2 snapshot into a store with the same count",
			version:           snapshotEncodingVersionV2,
			snapshotNumShards: 32,
			storeNumShards:    32,
			expectLoaded:      true,
		},
		{
			name:              "v2 snapshot with the legacy count into a store with the legacy count",
			version:           snapshotEncodingVersionV2,
			snapshotNumShards: legacySnapshotNumShards,
			storeNumShards:    legacySnapshotNumShards,
			expectLoaded:      true,
		},
		{
			name:              "v1 snapshot into a store with a different count",
			version:           snapshotEncodingVersionV1,
			snapshotNumShards: legacySnapshotNumShards,
			storeNumShards:    32,
			expectLoaded:      false,
		},
		{
			name:              "v2 snapshot into a store with a different count",
			version:           snapshotEncodingVersionV2,
			snapshotNumShards: 32,
			storeNumShards:    legacySnapshotNumShards,
			expectLoaded:      false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const shard = 3
			series := snapshotTestSeries(shard, tc.snapshotNumShards)
			data := encodeSnapshot(tc.version, tc.snapshotNumShards, shard, snapshotTestNow, map[string]map[uint64]clock.Minutes{snapshotTestUser: series})

			logs := bytes.NewBuffer(nil)
			store := newSnapshotTestStore(t, tc.storeNumShards, log.NewLogfmtLogger(logs))

			// A snapshot that can't be interpreted is dropped, not an error: the instance starts
			// empty and rebuilds its state from the event stream.
			require.NoError(t, store.loadSnapshot(data, snapshotTestNow))

			if !tc.expectLoaded {
				require.Empty(t, store.seriesCountsForTests())
				require.Contains(t, logs.String(), "discarding snapshot written with a different shard count")
				return
			}

			require.Equal(t, map[string]uint64{snapshotTestUser: uint64(len(series))}, store.seriesCountsForTests())
			require.Empty(t, logs.String())

			// The series are in the shard the snapshot said they were in.
			_, rest := decodeSnapshotHeader(t, store.snapshot(shard, snapshotTestNow, nil))
			require.Equal(t, series, rest[snapshotTestUser])
		})
	}
}

func TestTrackerStore_LoadSnapshot_RoundTripForEveryShardCount(t *testing.T) {
	for _, numShards := range []int{1, 2, legacySnapshotNumShards, 32, tenantshard.MaxNumShards} {
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

func TestTrackerStore_LoadSnapshot_Discarded(t *testing.T) {
	for _, tc := range []struct {
		name           string
		storeNumShards int
		data           []byte
		expectedLog    string
	}{
		{
			name:           "unsupported encoding version",
			storeNumShards: legacySnapshotNumShards,
			data:           encodeSnapshot(snapshotEncodingVersionV2+1, legacySnapshotNumShards, 0, snapshotTestNow, nil),
			expectedLog:    "discarding snapshot with unsupported encoding version",
		},
		{
			name:           "shard index out of bounds",
			storeNumShards: 2,
			data:           encodeSnapshot(snapshotEncodingVersionV2, 2, 7, snapshotTestNow, nil),
			expectedLog:    "discarding snapshot with out-of-bounds shard index",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := bytes.NewBuffer(nil)
			store := newSnapshotTestStore(t, tc.storeNumShards, log.NewLogfmtLogger(logs))

			require.NoError(t, store.loadSnapshot(tc.data, snapshotTestNow))
			require.Empty(t, store.seriesCountsForTests())
			require.Contains(t, logs.String(), tc.expectedLog)
		})
	}
}

func TestTrackerStore_LoadSnapshot_TruncatedData(t *testing.T) {
	// A truncated blob is a real error: it means we read something we shouldn't have.
	full := encodeSnapshot(snapshotEncodingVersionV2, 32, 3, snapshotTestNow, map[string]map[uint64]clock.Minutes{
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
	if version >= snapshotEncodingVersionV2 {
		buf.PutUvarint64(uint64(numShards))
	}
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

	header := snapshotHeader{version: snapshot.Byte(), numShards: legacySnapshotNumShards}
	require.NoError(t, snapshot.Err())
	if header.version >= snapshotEncodingVersionV2 {
		header.numShards = snapshot.Uvarint64()
		require.NoError(t, snapshot.Err())
	}
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
