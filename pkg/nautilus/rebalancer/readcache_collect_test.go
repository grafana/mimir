// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
)

// TestPartitionLByPID_ReadcacheUsesPartitionTotals verifies that
// partitionLByPID uses per-partition head series from HashRangeStats.
func TestPartitionLByPID_ReadcacheUsesPartitionTotals(t *testing.T) {
	r := &Rebalancer{
		logger: log.NewNopLogger(),
	}

	partitionTotals := map[int32]int64{
		0: 5000,
		1: 3000,
		2: 4000,
	}

	got := r.partitionLByPID(partitionTotals, []int32{0, 1, 2, 3})
	assert.Equal(t, map[int32]int64{
		0: 5000,
		1: 3000,
		2: 4000,
		3: 0, // not reported this round
	}, got)
}
