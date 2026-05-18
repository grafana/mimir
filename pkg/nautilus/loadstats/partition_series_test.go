// SPDX-License-Identifier: AGPL-3.0-only

package loadstats

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartitionSeries_SetCountsAndSnapshot(t *testing.T) {
	p := NewPartitionSeries()
	p.SetCounts(map[int32]int64{2: 200, 0: 100, 1: 50})

	snap := p.Snapshot()
	assert.Equal(t, int64(350), snap.Total)
	require.Len(t, snap.Partitions, 3)
	assert.Equal(t, int32(0), snap.Partitions[0].PartitionID)
	assert.Equal(t, int64(100), snap.Partitions[0].ActiveSeries)
	assert.Equal(t, int32(1), snap.Partitions[1].PartitionID)
	assert.Equal(t, int64(50), snap.Partitions[1].ActiveSeries)
	assert.Equal(t, int32(2), snap.Partitions[2].PartitionID)
	assert.Equal(t, int64(200), snap.Partitions[2].ActiveSeries)
}

func TestPartitionSeries_EmptySnapshot(t *testing.T) {
	snap := NewPartitionSeries().Snapshot()
	assert.Equal(t, int64(0), snap.Total)
	assert.Empty(t, snap.Partitions)
}
