// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
)

// TestPartitionLByPID_ReadcacheUsesPartitionTotals verifies that with
// the readcache pool wired, partitionLByPID uses per-partition head
// series from HashRangeStats rather than fanning out instance totals.
func TestPartitionLByPID_ReadcacheUsesPartitionTotals(t *testing.T) {
	r := &Rebalancer{
		logger:        log.NewNopLogger(),
		readcachePool: &ReadcachePool{},
	}

	partitionTotals := map[int32]int64{
		0: 5000,
		1: 3000,
		2: 4000,
	}

	got := r.partitionLByPID(nil, partitionTotals, nil, []int32{0, 1, 2, 3}, time.Now())
	assert.Equal(t, map[int32]int64{
		0: 5000,
		1: 3000,
		2: 4000,
		3: 0, // not reported this round
	}, got)
}

// TestPartitionLByPID_FallbackToIngesterRing makes sure that when
// the readcache pool isn't wired, the helper preserves the legacy
// behaviour (max over owners of the ingester partition ring). That
// keeps tests and pre-readcache phase 1 deployments working.
func TestPartitionLByPID_FallbackToIngesterRing(t *testing.T) {
	r := &Rebalancer{
		logger:         log.NewNopLogger(),
		readcacheStore: newReadcacheLogStore(),
		readcachePool:  nil, // forces fallback
	}

	pRing := stubPartitionRing{
		0: {"ingester-a", "ingester-b"}, // two replicas
		1: {"ingester-b"},
	}
	instanceTotals := map[string]int64{
		"ingester-a": 5000,
		"ingester-b": 7000,
	}

	got := r.partitionLByPID(instanceTotals, nil, pRing, []int32{0, 1}, time.Now())
	assert.Equal(t, map[int32]int64{
		0: 7000, // max(5000, 7000)
		1: 7000,
	}, got)
}
