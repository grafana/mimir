// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerateSeriesHashes_ShouldGenerateUniqueHashes(t *testing.T) {
	hashes := make(map[uint64]bool)
	duplicates := 0

	for replicaID := 0; replicaID < 10; replicaID++ {
		for workerID := 0; workerID < 300; workerID++ {
			for _, hash := range generateSeriesHashes(replicaID, workerID, 10) {
				if _, ok := hashes[hash]; ok {
					duplicates++
				}

				hashes[hash] = true
			}
		}
	}

	require.Zero(t, duplicates)
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
