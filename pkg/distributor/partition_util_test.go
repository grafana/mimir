// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"testing"

	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// simpleBatchRing is a minimal DoBatchRing implementation for testing.
// Each key is assigned to partition: key % numPartitions.
type simpleBatchRing struct {
	numPartitions int
}

func (r *simpleBatchRing) InstancesCount() int {
	return r.numPartitions
}

func (r *simpleBatchRing) ReplicationFactor() int {
	return 1
}

func (r *simpleBatchRing) Get(key uint32, _ ring.Operation, bufInstances []ring.InstanceDesc, _, _ []string) (ring.ReplicationSet, error) {
	partitionID := int(key) % r.numPartitions

	if cap(bufInstances) < 1 {
		bufInstances = make([]ring.InstanceDesc, 1)
	} else {
		bufInstances = bufInstances[:1]
	}

	addr := string(rune('A' + partitionID))
	bufInstances[0] = ring.InstanceDesc{
		Addr: addr,
		Id:   addr,
	}

	return ring.ReplicationSet{
		Instances: bufInstances,
		MaxErrors: 0,
	}, nil
}

func TestSplitKeysByInstance(t *testing.T) {
	t.Run("empty keys returns nil", func(t *testing.T) {
		r := &simpleBatchRing{numPartitions: 3}
		result, err := SplitKeysByInstance(context.Background(), ring.WriteNoExtend, r, nil)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("single partition", func(t *testing.T) {
		r := &simpleBatchRing{numPartitions: 1}
		keys := []uint32{10, 20, 30}
		result, err := SplitKeysByInstance(context.Background(), ring.WriteNoExtend, r, keys)
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.Equal(t, []int{0, 1, 2}, result[0].Indexes)
	})

	t.Run("keys distributed across partitions", func(t *testing.T) {
		r := &simpleBatchRing{numPartitions: 3}
		// key%3: 0->0, 1->1, 2->2, 3->0, 4->1, 5->2
		keys := []uint32{0, 1, 2, 3, 4, 5}
		result, err := SplitKeysByInstance(context.Background(), ring.WriteNoExtend, r, keys)
		require.NoError(t, err)
		require.Len(t, result, 3)

		// Build a map from instance addr to indexes for easier assertion.
		byAddr := map[string][]int{}
		for _, ik := range result {
			byAddr[ik.Instance.Addr] = ik.Indexes
		}

		assert.Equal(t, []int{0, 3}, byAddr["A"]) // keys 0, 3
		assert.Equal(t, []int{1, 4}, byAddr["B"]) // keys 1, 4
		assert.Equal(t, []int{2, 5}, byAddr["C"]) // keys 2, 5
	})

	t.Run("context cancelled", func(t *testing.T) {
		r := &simpleBatchRing{numPartitions: 2}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := SplitKeysByInstance(ctx, ring.WriteNoExtend, r, []uint32{1, 2, 3})
		require.Error(t, err)
	})

	t.Run("zero instances returns error", func(t *testing.T) {
		r := &simpleBatchRing{numPartitions: 0}
		_, err := SplitKeysByInstance(context.Background(), ring.WriteNoExtend, r, []uint32{1})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "InstancesCount <= 0")
	})
}
