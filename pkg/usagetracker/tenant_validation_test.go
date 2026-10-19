// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerpb"
)

func TestUsageTracker_InvalidTenantID(t *testing.T) {
	tracker := newReadyTestUsageTracker(t, nil)
	for _, userID := range []string{"tenant-\xb5", "bad tenant", "tenant:source=\xb5"} {
		t.Run(fmt.Sprintf("%q", userID), func(t *testing.T) {
			resp, err := tracker.TrackSeries(t.Context(), &usagetrackerpb.TrackSeriesRequest{
				UserID: userID, Partition: 0, SeriesHashes: []uint64{1},
			})
			require.Nil(t, resp)
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			require.Equal(t, fmt.Sprintf("invalid tenant ID %q", userID), status.Convert(err).Message())
			batchResp, err := tracker.TrackSeriesBatch(t.Context(), &usagetrackerpb.TrackSeriesBatchRequest{
				Partitions: []*usagetrackerpb.TrackSeriesBatchPartition{
					{Partition: 0, Users: []*usagetrackerpb.TrackSeriesBatchUser{{UserID: "valid", SeriesHashes: []uint64{1}}}},
					{Partition: 1, Users: []*usagetrackerpb.TrackSeriesBatchUser{{UserID: userID, SeriesHashes: []uint64{2}}}},
				},
			})
			require.Nil(t, batchResp)
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			require.Equal(t, fmt.Sprintf("invalid tenant ID %q", userID), status.Convert(err).Message())
			withRLock(&tracker.partitionsMtx, func() {
				for _, partition := range tracker.partitions {
					require.Empty(t, partition.store.seriesCountsForTests(), "invalid batches must not partially update usage")
				}
			})
		})
	}

	// Tracking keys can include validated tenant metadata.
	_, err := tracker.TrackSeries(t.Context(), &usagetrackerpb.TrackSeriesRequest{
		UserID: "tenant:compartment=foo", Partition: 0, SeriesHashes: []uint64{1},
	})
	require.NoError(t, err)
	_, err = tracker.TrackSeriesBatch(t.Context(), &usagetrackerpb.TrackSeriesBatchRequest{
		Partitions: []*usagetrackerpb.TrackSeriesBatchPartition{
			{Partition: 0, Users: []*usagetrackerpb.TrackSeriesBatchUser{{UserID: "tenant:compartment=foo", SeriesHashes: []uint64{2}}}},
		},
	})
	require.NoError(t, err)
	withRLock(&tracker.partitionsMtx, func() {
		require.Equal(t, map[string]uint64{"tenant:compartment=foo": 2}, tracker.partitions[0].store.seriesCountsForTests())
	})
}

func TestTrackerStore_CollectWithCorruptTenantID(t *testing.T) {
	for _, verbose := range []bool{false, true} {
		t.Run(fmt.Sprintf("verbose=%v", verbose), func(t *testing.T) {
			var logs bytes.Buffer
			store := newTrackerStore(20*time.Minute, 85, log.NewLogfmtLogger(&logs), limiterMock{}, noopEvents{}, verbose, 0, newTestShardFactory())
			_, err := store.trackSeries(t.Context(), "valid", []uint64{1}, time.Now())
			require.NoError(t, err)
			// Simulate an ID that was already corrupted in memory before validation was added.
			tenant := store.getOrCreateTenant("tenant-\xb5")
			tenant.series.Store(1)
			tenant.RUnlock()
			reg := prometheus.NewPedanticRegistry()
			require.NoError(t, reg.Register(store))
			metrics, err := reg.Gather()
			require.ErrorContains(t, err, `label value "tenant-\xb5" is not valid UTF-8`)
			require.Contains(t, logs.String(), "failed to collect usage-tracker metric")
			require.Contains(t, logs.String(), `tenant="\"tenant-\\xb5\""`)
			require.Contains(t, logs.String(), activeSeriesMetricName)
			require.Contains(t, logs.String(), "not valid UTF-8")
			require.NotEmpty(t, metrics, "healthy tenants' metrics should still be collected")
			for _, family := range metrics {
				if family.GetName() == "cortex_usage_tracker_shard_cleanup_duration_seconds" {
					continue
				}
				require.Len(t, family.Metric, 1)
				require.Equal(t, "valid", family.Metric[0].Label[0].GetValue())
			}
		})
	}
}

func TestTrackerStore_ReplaySkipsInvalidTenantIDs(t *testing.T) {
	now := time.Now()
	store := newTrackerStore(20*time.Minute, 85, log.NewNopLogger(), limiterMock{}, noopEvents{}, false, 0, newTestShardFactory())
	for _, userID := range []string{"tenant-\xb5", "bad tenant", "tenant:source=\xb5", "valid", "tenant:compartment=foo"} {
		store.processCreatedSeriesEvent(userID, []uint64{1}, now, now)
	}
	require.Equal(t, map[string]uint64{"valid": 1, "tenant:compartment=foo": 1}, store.seriesCountsForTests())
}

func TestTrackerStore_LoadSnapshotSkipsInvalidTenantIDs(t *testing.T) {
	now := time.Now()
	data := encoding.Encbuf{}
	data.PutByte(snapshotEncodingVersionV1)
	data.PutByte(1)
	data.PutBE64(uint64(now.Unix()))
	users := []string{"tenant-\xb5", "bad tenant", "tenant:source=\xb5", "valid", "tenant:compartment=foo"}
	data.PutUvarint64(uint64(len(users)))
	for _, userID := range users {
		data.PutUvarintStr(userID)
		data.PutUvarint64(1)
		data.PutBE64(1)
		data.PutByte(byte(clock.ToMinutes(now)))
	}
	store := newTrackerStore(20*time.Minute, 85, log.NewNopLogger(), limiterMock{}, noopEvents{}, false, 0, newTestShardFactory())
	require.NoError(t, store.loadSnapshot(data.Get(), now))
	require.Equal(t, map[string]uint64{"valid": 1, "tenant:compartment=foo": 1}, store.seriesCountsForTests())
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(store))
	_, err := reg.Gather()
	require.NoError(t, err)
}
