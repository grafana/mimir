// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestAgentPoolRefresh(t *testing.T) {
	const (
		topicName     = "test-topic"
		numPartitions = int32(3)
	)

	_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
	client, err := kgo.NewClient(kgo.SeedBrokers(clusterAddr))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	pool := NewAgentPool(client)

	t.Run("populates strategy from metadata", func(t *testing.T) {
		removed, err := pool.Refresh(context.Background())
		require.NoError(t, err)
		assert.Empty(t, removed)
		assert.NotNil(t, pool.Strategy())
	})

	t.Run("topic ID is populated after refresh", func(t *testing.T) {
		_, err := pool.Refresh(context.Background())
		require.NoError(t, err)
		assert.NotEqual(t, [16]byte{}, pool.TopicID(topicName))
	})

	t.Run("Strategy.Primary returns a non-zero leader for each partition", func(t *testing.T) {
		_, err := pool.Refresh(context.Background())
		require.NoError(t, err)
		strategy := pool.Strategy()
		for part := int32(0); part < numPartitions; part++ {
			nodeID := strategy.Primary(topicName, part)
			assert.GreaterOrEqual(t, nodeID, int32(0))
		}
	})

	t.Run("repeated refresh with unchanged topology returns no removed agents", func(t *testing.T) {
		removed, err := pool.Refresh(context.Background())
		require.NoError(t, err)
		assert.Empty(t, removed)
	})

	t.Run("each Refresh returns a new strategy instance", func(t *testing.T) {
		_, err := pool.Refresh(context.Background())
		require.NoError(t, err)
		s1 := pool.Strategy()
		_, err = pool.Refresh(context.Background())
		require.NoError(t, err)
		s2 := pool.Strategy()
		assert.NotSame(t, s1, s2)
	})

	t.Run("TopicID returns zero for an unknown topic", func(t *testing.T) {
		_, err := pool.Refresh(context.Background())
		require.NoError(t, err)
		assert.Equal(t, [16]byte{}, pool.TopicID("does-not-exist"))
	})
}

func TestAgentPoolStrategyBeforeRefresh(t *testing.T) {
	t.Run("Strategy returns a non-nil empty strategy before first Refresh", func(t *testing.T) {
		pool := NewAgentPool(nil)
		s := pool.Strategy()
		require.NotNil(t, s)
		assert.Equal(t, int32(0), s.Primary("topic", 0))
		_, ok := s.Secondary("topic", 0)
		assert.False(t, ok)
	})

	t.Run("TopicID returns zero before first Refresh", func(t *testing.T) {
		pool := NewAgentPool(nil)
		assert.Equal(t, [16]byte{}, pool.TopicID("topic"))
	})
}

func TestAgentPoolRefreshMultiTopic(t *testing.T) {
	const (
		topicA        = "topic-a"
		topicB        = "topic-b"
		numPartitions = int32(2)
	)

	_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicA)

	// Set kgo's metadata cache to its minimum allowed value (10ms; kgo rejects
	// MetadataMinAge=0) so the test sees CreateTopics / DeleteTopics changes on
	// the next Refresh without waiting for the default 5-second cache window.
	// In production the longer default is desirable; the test just needs
	// deterministic visibility.
	client, err := kgo.NewClient(
		kgo.SeedBrokers(clusterAddr),
		kgo.MetadataMinAge(10*time.Millisecond),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	// Create the second topic via the standard CreateTopics protocol.
	createReq := kmsg.NewPtrCreateTopicsRequest()
	createReq.Topics = []kmsg.CreateTopicsRequestTopic{{
		Topic:             topicB,
		NumPartitions:     numPartitions,
		ReplicationFactor: 1,
	}}
	_, err = client.Request(context.Background(), createReq)
	require.NoError(t, err)

	pool := NewAgentPool(client)

	t.Run("Refresh discovers all topics in the cluster", func(t *testing.T) {
		_, err := pool.Refresh(context.Background())
		require.NoError(t, err)

		idA := pool.TopicID(topicA)
		idB := pool.TopicID(topicB)
		assert.NotEqual(t, [16]byte{}, idA)
		assert.NotEqual(t, [16]byte{}, idB)
		assert.NotEqual(t, idA, idB)

		strategy := pool.Strategy()
		for part := int32(0); part < numPartitions; part++ {
			assert.GreaterOrEqual(t, strategy.Primary(topicA, part), int32(0))
			assert.GreaterOrEqual(t, strategy.Primary(topicB, part), int32(0))
		}
	})

	t.Run("topics deleted from the cluster are evicted from the snapshot", func(t *testing.T) {
		_, err := pool.Refresh(context.Background())
		require.NoError(t, err)
		require.NotEqual(t, [16]byte{}, pool.TopicID(topicB))

		deleteReq := kmsg.NewPtrDeleteTopicsRequest()
		deleteReq.TopicNames = []string{topicB}
		deleteReq.Topics = []kmsg.DeleteTopicsRequestTopic{{Topic: kmsg.StringPtr(topicB)}}
		_, err = client.Request(context.Background(), deleteReq)
		require.NoError(t, err)

		// Wait for kgo's metadata cache (MetadataMinAge=10ms above) to expire so
		// the next Refresh fetches fresh metadata reflecting the deletion.
		time.Sleep(20 * time.Millisecond)

		_, err = pool.Refresh(context.Background())
		require.NoError(t, err)
		assert.Equal(t, [16]byte{}, pool.TopicID(topicB), "deleted topic must be evicted")
		assert.NotEqual(t, [16]byte{}, pool.TopicID(topicA), "remaining topic must still be tracked")
	})
}

func TestBuildLeadersAndTopicIDs(t *testing.T) {
	knownAgents := map[int32]struct{}{1: {}, 2: {}, 3: {}}
	idA := [16]byte{0x42}
	idB := [16]byte{0x43}

	tests := map[string]struct {
		respTopics   []kmsg.MetadataResponseTopic
		prevTopicIDs map[string][16]byte
		wantLeaders  map[topicPartition]int32
		wantTopicIDs map[string][16]byte
	}{
		"happy path: single topic, all leaders known": {
			respTopics: []kmsg.MetadataResponseTopic{{
				Topic:   stringPtr("a"),
				TopicID: idA,
				Partitions: []kmsg.MetadataResponseTopicPartition{
					{Partition: 0, Leader: 1},
					{Partition: 1, Leader: 2},
				},
			}},
			wantLeaders: map[topicPartition]int32{
				{topic: "a", partition: 0}: 1,
				{topic: "a", partition: 1}: 2,
			},
			wantTopicIDs: map[string][16]byte{"a": idA},
		},
		"multiple topics: each gets its own leaders and UUID": {
			respTopics: []kmsg.MetadataResponseTopic{
				{Topic: stringPtr("a"), TopicID: idA, Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0, Leader: 1}}},
				{Topic: stringPtr("b"), TopicID: idB, Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0, Leader: 2}}},
			},
			wantLeaders: map[topicPartition]int32{
				{topic: "a", partition: 0}: 1,
				{topic: "b", partition: 0}: 2,
			},
			wantTopicIDs: map[string][16]byte{"a": idA, "b": idB},
		},
		"leader pointing to unknown agent is dropped": {
			respTopics: []kmsg.MetadataResponseTopic{{
				Topic:   stringPtr("a"),
				TopicID: idA,
				Partitions: []kmsg.MetadataResponseTopicPartition{
					{Partition: 0, Leader: 1},
					{Partition: 1, Leader: 99}, // unknown
					{Partition: 2, Leader: 3},
				},
			}},
			wantLeaders: map[topicPartition]int32{
				{topic: "a", partition: 0}: 1,
				{topic: "a", partition: 2}: 3,
			},
			wantTopicIDs: map[string][16]byte{"a": idA},
		},
		"topic absent from response is evicted (deletion is authoritative)": {
			respTopics:   []kmsg.MetadataResponseTopic{},
			prevTopicIDs: map[string][16]byte{"a": idA},
			wantLeaders:  map[topicPartition]int32{},
			wantTopicIDs: map[string][16]byte{},
		},
		"topic with non-zero error code: previous UUID preserved": {
			respTopics: []kmsg.MetadataResponseTopic{{
				Topic:      stringPtr("a"),
				ErrorCode:  5, // LEADER_NOT_AVAILABLE
				TopicID:    [16]byte{}, // brokers often return zero UUID alongside the error
				Partitions: nil,
			}},
			prevTopicIDs: map[string][16]byte{"a": idA},
			wantLeaders:  map[topicPartition]int32{},
			wantTopicIDs: map[string][16]byte{"a": idA},
		},
		"all leaders unknown: empty leader map but UUID still picked up": {
			respTopics: []kmsg.MetadataResponseTopic{{
				Topic:   stringPtr("a"),
				TopicID: idA,
				Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0, Leader: 99}},
			}},
			wantLeaders:  map[topicPartition]int32{},
			wantTopicIDs: map[string][16]byte{"a": idA},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			leaders, topicIDs := buildLeadersAndTopicIDs(tc.respTopics, knownAgents, tc.prevTopicIDs)
			assert.Equal(t, tc.wantLeaders, leaders)
			assert.Equal(t, tc.wantTopicIDs, topicIDs)
		})
	}
}

func stringPtr(s string) *string { return &s }

func TestDiffRemovedAgents(t *testing.T) {
	tests := map[string]struct {
		old      []int32
		newSet   []int32
		expected []int32
	}{
		"all agents still present": {
			old: []int32{1, 2, 3}, newSet: []int32{1, 2, 3}, expected: nil,
		},
		"one agent removed": {
			old: []int32{1, 2, 3}, newSet: []int32{1, 3}, expected: []int32{2},
		},
		"multiple agents removed": {
			old: []int32{1, 2, 3, 4}, newSet: []int32{2, 4}, expected: []int32{1, 3},
		},
		"all agents removed": {
			old: []int32{1, 2}, newSet: []int32{}, expected: []int32{1, 2},
		},
		"agents added (not removed)": {
			old: []int32{1, 2}, newSet: []int32{1, 2, 3}, expected: nil,
		},
		"empty old set": {
			old: nil, newSet: []int32{1, 2}, expected: nil,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			set := make(map[int32]struct{}, len(tc.newSet))
			for _, id := range tc.newSet {
				set[id] = struct{}{}
			}
			got := diffRemovedAgents(tc.old, set)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestAgentPoolStrategyConcurrent(t *testing.T) {
	const (
		topicName     = "test-topic"
		numPartitions = int32(4)
	)

	_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topicName)
	client, err := kgo.NewClient(kgo.SeedBrokers(clusterAddr))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	pool := NewAgentPool(client)
	_, err = pool.Refresh(context.Background())
	require.NoError(t, err)

	const goroutines = 20
	done := make(chan struct{})
	for range goroutines {
		go func() {
			defer func() { done <- struct{}{} }()
			strategy := pool.Strategy()
			for part := int32(0); part < numPartitions; part++ {
				_, _ = strategy.Secondary(topicName, part)
				_ = strategy.Primary(topicName, part)
			}
		}()
	}
	for range goroutines {
		<-done
	}
}
