// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
)

// TestPartitionLByPID_ReadcacheLog verifies that with the readcache
// pool wired (rc != nil), partitionLByPID maps each partition to
// the instance total of its current owner from the readcache log.
// This is the central piece of "load follows the slicer": the
// hashrange slicer sees per-partition L proportional to the owning
// readcache's memory pressure, so a readcache that's drowning in
// series pushes its hottest hash ranges (= heaviest partitions)
// toward a less-loaded peer.
func TestPartitionLByPID_ReadcacheLog(t *testing.T) {
	r := &Rebalancer{
		logger:         log.NewNopLogger(),
		readcacheStore: newReadcacheLogStore(),
		// Non-nil readcachePool tag selects the readcache code path.
		// We don't dial anything in this test; we only exercise the
		// L-mapping function which reads readcacheStore directly.
		readcachePool: &ReadcachePool{},
	}

	now := time.Now()
	// Seed the log: partition 0 -> readcache-a, partition 1 -> readcache-b,
	// partition 2 -> readcache-a, partition 3 has no active lease
	// (cold).
	r.readcacheStore.seedFromEntries([]readcacheassignment.LogEntry{
		{PartitionID: 0, InstanceID: "readcache-a", From: now.Add(-time.Minute), To: now.Add(time.Hour)},
		{PartitionID: 1, InstanceID: "readcache-b", From: now.Add(-time.Minute), To: now.Add(time.Hour)},
		{PartitionID: 2, InstanceID: "readcache-a", From: now.Add(-time.Minute), To: now.Add(time.Hour)},
	})

	instanceTotals := map[string]int64{
		"readcache-a": 9000,
		"readcache-b": 3000,
	}

	got := r.partitionLByPID(instanceTotals, nil, []int32{0, 1, 2, 3}, now)
	assert.Equal(t, map[int32]int64{
		0: 9000,
		1: 3000,
		2: 9000,
		3: 0, // no owner -> zero (slicer treats as cold)
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

	got := r.partitionLByPID(instanceTotals, pRing, []int32{0, 1}, time.Now())
	assert.Equal(t, map[int32]int64{
		0: 7000, // max(5000, 7000)
		1: 7000,
	}, got)
}
