// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func BenchmarkGenerateSeriesHashes(b *testing.B) {
	now := time.Now()
	for i := 0; i < b.N; i++ {
		generateSeriesHashesWithChurn(1, 1, 100e6, time.Hour, now)
	}
}

func TestGenerateSeriesHashes_ShouldGenerateUniqueHashes(t *testing.T) {
	hashes := make(map[uint64]bool)
	duplicates := 0
	now := time.Now()

	for replicaSeed := uint64(0); replicaSeed < 10; replicaSeed++ {
		for workerID := 0; workerID < 300; workerID++ {
			for _, hash := range generateSeriesHashesWithChurn(replicaSeed, workerID, 10, time.Hour, now) {
				if _, ok := hashes[hash]; ok {
					duplicates++
				}

				hashes[hash] = true
			}
		}
	}

	require.Zero(t, duplicates)
}

func TestGenerateSeriesHashes_ShouldChurn(t *testing.T) {
	const series = 4
	const lifetime = time.Hour
	now := time.Unix(0, 0).Add(lifetime)
	hashes1 := generateSeriesHashesWithChurn(1, 1, series, lifetime, now)
	hashes2 := generateSeriesHashesWithChurn(1, 1, series, lifetime, now.Add(lifetime/2))
	hashes3 := generateSeriesHashesWithChurn(1, 1, series, lifetime, now.Add(lifetime))
	hashes4 := generateSeriesHashesWithChurn(1, 1, series, lifetime, now.Add(lifetime+lifetime/2))

	require.Greater(t, len(hashes1), 0)
	require.Greater(t, len(hashes2), 0)
	require.Greater(t, len(hashes3), 0)
	require.Equal(t, int(series/2), countDifferent(hashes1, hashes2))
	require.Equal(t, int(series/2), countDifferent(hashes2, hashes3))
	require.Equal(t, int(series), countDifferent(hashes1, hashes3))
	require.Equal(t, int(series/2), countDifferent(hashes3, hashes4))
	require.Equal(t, int(series), countDifferent(hashes2, hashes4))
	require.Equal(t, int(series), countDifferent(hashes1, hashes4))
}

func countDifferent(a []uint64, b []uint64) int {
	slices.Sort(a)
	slices.Sort(b)

	different := 0
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			i++
			j++
		case a[i] < b[j]:
			different++
			i++
		case a[i] > b[j]:
			different++
			j++
		}
	}
	different += len(a) - i
	different += len(b) - j

	return different / 2
}

func TestSendSeriesHashes(t *testing.T) {
	var (
		ctx      = context.Background()
		workerID = 1
		tenantID = "user-1"
	)

	client := &usageTrackerClientMock{}
	sendSeriesHashes(ctx, workerID, tenantID, []uint64{1, 2, 3, 4, 5, 6, 7}, 3, 0, 0, client)

	require.Len(t, client.trackSeriesCalls, 3)
	require.Equal(t, tenantID, client.trackSeriesCalls[0].userID)
	require.Equal(t, []uint64{1, 2, 3}, client.trackSeriesCalls[0].series)
	require.Equal(t, tenantID, client.trackSeriesCalls[1].userID)
	require.Equal(t, []uint64{4, 5, 6}, client.trackSeriesCalls[1].series)
	require.Equal(t, tenantID, client.trackSeriesCalls[2].userID)
	require.Equal(t, []uint64{7}, client.trackSeriesCalls[2].series)
}

type usageTrackerClientMock struct {
	trackSeriesCalls []trackSeriesCall
}

func (m *usageTrackerClientMock) TrackSeries(_ context.Context, userID string, series []uint64) (_ []uint64, returnErr error) {
	m.trackSeriesCalls = append(m.trackSeriesCalls, trackSeriesCall{userID, series})
	return nil, nil
}

type trackSeriesCall struct {
	userID string
	series []uint64
}
