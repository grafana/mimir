// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectSecondary(t *testing.T) {
	tests := map[string]struct {
		topic     string
		partition int32
		primary   int32
		all       []int32
		wantOk    bool
	}{
		"single agent: no secondary available": {
			topic: "t", partition: 0, primary: 1, all: []int32{1},
			wantOk: false,
		},
		"empty agent list: no secondary": {
			topic: "t", partition: 0, primary: 1, all: []int32{},
			wantOk: false,
		},
		"two agents: returns the non-primary": {
			topic: "t", partition: 0, primary: 1, all: []int32{1, 2},
			wantOk: true,
		},
		"two agents: primary=2 returns the non-primary": {
			topic: "t", partition: 0, primary: 2, all: []int32{1, 2},
			wantOk: true,
		},
		"three agents: result is deterministic": {
			topic: "t", partition: 0, primary: 1, all: []int32{1, 2, 3},
			wantOk: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			nodeID, ok := selectSecondary(tc.topic, tc.partition, tc.primary, tc.all)
			assert.Equal(t, tc.wantOk, ok)
			if ok {
				assert.NotEqual(t, tc.primary, nodeID, "secondary must differ from primary")
				nodeID2, _ := selectSecondary(tc.topic, tc.partition, tc.primary, tc.all)
				assert.Equal(t, nodeID, nodeID2)
			}
		})
	}
}

func TestSelectSecondary_NeverReturnsPrimary(t *testing.T) {
	agents := []int32{1, 2, 3, 4, 5}
	for _, primary := range agents {
		for part := int32(0); part < 50; part++ {
			t.Run(fmt.Sprintf("primary=%d partition=%d", primary, part), func(t *testing.T) {
				nodeID, ok := selectSecondary("t", part, primary, agents)
				require.True(t, ok)
				assert.NotEqual(t, primary, nodeID)
			})
		}
	}
}

func TestSelectSecondary_NodeIDZeroIsValid(t *testing.T) {
	t.Run("NodeID 0 can be the secondary", func(t *testing.T) {
		all := []int32{0, 1}
		nodeID, ok := selectSecondary("t", 0, 1, all)
		require.True(t, ok)
		assert.Equal(t, int32(0), nodeID)
	})

	t.Run("NodeID 0 can be the primary without aliasing no-secondary sentinel", func(t *testing.T) {
		all := []int32{0, 1}
		nodeID, ok := selectSecondary("t", 0, 0, all)
		require.True(t, ok)
		assert.Equal(t, int32(1), nodeID)
	})
}

func TestDefaultPartitionAssignmentStrategy_Secondary(t *testing.T) {
	agents := []int32{1, 2, 3}
	leaders := map[topicPartition]int32{
		{topic: "t", partition: 0}: 1,
		{topic: "t", partition: 1}: 2,
	}
	s := newDefaultPartitionAssignmentStrategy(agents, leaders)

	t.Run("Primary returns the leader", func(t *testing.T) {
		id, ok := s.Primary("t", 0)
		require.True(t, ok)
		assert.Equal(t, int32(1), id)
		id, ok = s.Primary("t", 1)
		require.True(t, ok)
		assert.Equal(t, int32(2), id)
	})

	t.Run("Primary returns false for unknown partition", func(t *testing.T) {
		_, ok := s.Primary("t", 99)
		assert.False(t, ok)
	})

	t.Run("Secondary differs from Primary", func(t *testing.T) {
		sec, ok := s.Secondary("t", 0)
		require.True(t, ok)
		primary, _ := s.Primary("t", 0)
		assert.NotEqual(t, primary, sec)
	})

	t.Run("Secondary is deterministic across repeated calls", func(t *testing.T) {
		id1, ok1 := s.Secondary("t", 0)
		id2, ok2 := s.Secondary("t", 0)
		assert.Equal(t, ok1, ok2)
		assert.Equal(t, id1, id2)
	})

	t.Run("each Refresh creates a fresh strategy with its own cache", func(t *testing.T) {
		s1 := newDefaultPartitionAssignmentStrategy(agents, leaders)
		s2 := newDefaultPartitionAssignmentStrategy(agents, leaders)
		// Both strategies should produce the same deterministic result independently.
		id1, ok1 := s1.Secondary("t", 0)
		id2, ok2 := s2.Secondary("t", 0)
		assert.Equal(t, ok1, ok2)
		assert.Equal(t, id1, id2)
	})
}

func BenchmarkSelectSecondary(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		all := makeNodeIDs(n)
		b.Run(fmt.Sprintf("agents=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_, _ = selectSecondary("mimir-ingest", 7, all[0], all)
			}
		})
	}
}

// BenchmarkNewDefaultPartitionAssignmentStrategy measures the constructor cost,
// which precomputes the secondaries map for every partition. This is where the
// hot-path work moved when the lazy sync.Map cache was replaced with eager
// precomputation. A new strategy is built once per AgentPool.Refresh.
//
// In all configurations partitions >= 2 * agents: an agent without partitions
// assigned to it is idle, so deployments always have more partitions than agents.
func BenchmarkNewDefaultPartitionAssignmentStrategy(b *testing.B) {
	configs := []struct{ agents, partitions int }{
		{agents: 10, partitions: 32},
		{agents: 10, partitions: 128},
		{agents: 100, partitions: 256},
		{agents: 100, partitions: 1024},
		{agents: 1000, partitions: 2048},
		{agents: 1000, partitions: 8192},
	}
	for _, cfg := range configs {
		b.Run(fmt.Sprintf("agents=%d/partitions=%d", cfg.agents, cfg.partitions), func(b *testing.B) {
			agents := makeNodeIDs(cfg.agents)
			leaders := make(map[topicPartition]int32, cfg.partitions)
			for p := int32(0); p < int32(cfg.partitions); p++ {
				leaders[topicPartition{topic: "mimir-ingest", partition: p}] = agents[int(p)%len(agents)]
			}
			b.ResetTimer()
			b.ReportAllocs()
			for range b.N {
				_ = newDefaultPartitionAssignmentStrategy(agents, leaders)
			}
		})
	}
}

func makeNodeIDs(n int) []int32 {
	ids := make([]int32, n)
	for i := range ids {
		ids[i] = int32(i + 1)
	}
	return ids
}

// partitionKey identifies a (topic, partition) pair. Used as a map key by
// the test mocks below.
type partitionKey struct {
	topic     string
	partition int32
}

// mockPartitionAssignmentStrategy returns deterministic primary/secondary
// mappings for tests. Either map can be left nil if the test does not need
// that lookup direction.
type mockPartitionAssignmentStrategy struct {
	primary   map[partitionKey]int32
	secondary map[partitionKey]int32
}

func (s *mockPartitionAssignmentStrategy) Primary(topic string, partition int32) (int32, bool) {
	id, ok := s.primary[partitionKey{topic, partition}]
	return id, ok
}

func (s *mockPartitionAssignmentStrategy) Secondary(topic string, partition int32) (int32, bool) {
	id, ok := s.secondary[partitionKey{topic, partition}]
	return id, ok
}
