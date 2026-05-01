// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type recordedTrack struct {
	nodeID  int32
	latency time.Duration
	err     error
}

type recordingTracker struct {
	mu    sync.Mutex
	calls []recordedTrack
	noopAgentStatsTracker
}

func (r *recordingTracker) TrackAgentRequest(_ time.Time, nodeID int32, latency time.Duration, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, recordedTrack{nodeID: nodeID, latency: latency, err: err})
}

// noopAgentStatsTracker satisfies the read methods of AgentStatsTracker for
// tests that only care about TrackAgentRequest writes.
type noopAgentStatsTracker struct{}

func (noopAgentStatsTracker) AgentStats(time.Time, int32) (AgentStats, bool) {
	return AgentStats{}, false
}
func (noopAgentStatsTracker) ClusterStats(time.Time, float64, float64) (ClusterStats, bool) {
	return ClusterStats{}, false
}
func (noopAgentStatsTracker) PurgeAgents([]int32) {}

func TestTrackingProducer_Produce(t *testing.T) {
	t.Run("records latency and nil error on success", func(t *testing.T) {
		inner := newMockDirectProducer()
		inner.delays[7] = 5 * time.Millisecond
		tr := &recordingTracker{}
		tp := NewTrackingProducer(inner, tr)

		_, err := tp.ProduceSync(context.Background(), 7, nil)
		require.NoError(t, err)

		require.Len(t, tr.calls, 1)
		assert.Equal(t, int32(7), tr.calls[0].nodeID)
		assert.NoError(t, tr.calls[0].err)
		assert.GreaterOrEqual(t, tr.calls[0].latency, 5*time.Millisecond)
	})

	t.Run("records error from inner", func(t *testing.T) {
		inner := newMockDirectProducer()
		boom := errors.New("boom")
		inner.errs[3] = boom
		tr := &recordingTracker{}
		tp := NewTrackingProducer(inner, tr)

		_, err := tp.ProduceSync(context.Background(), 3, nil)
		require.ErrorIs(t, err, boom)

		require.Len(t, tr.calls, 1)
		assert.Equal(t, int32(3), tr.calls[0].nodeID)
		assert.ErrorIs(t, tr.calls[0].err, boom)
	})

	t.Run("records per-partition error code as agent error", func(t *testing.T) {
		inner := newMockDirectProducer()
		resp := kmsg.NewPtrProduceResponse()
		resp.Topics = []kmsg.ProduceResponseTopic{{
			Topic: "t",
			Partitions: []kmsg.ProduceResponseTopicPartition{
				{Partition: 0, ErrorCode: kerr.NotLeaderForPartition.Code},
			},
		}}
		inner.respFn = func(int32, []topicPartitionRecords) (*kmsg.ProduceResponse, error) { return resp, nil }
		tr := &recordingTracker{}
		tp := NewTrackingProducer(inner, tr)

		_, err := tp.ProduceSync(context.Background(), 5, nil)
		require.NoError(t, err)

		require.Len(t, tr.calls, 1)
		assert.ErrorIs(t, tr.calls[0].err, kerr.NotLeaderForPartition)
	})

	t.Run("forwards inner response unchanged", func(t *testing.T) {
		inner := newMockDirectProducer()
		want := kmsg.NewPtrProduceResponse()
		inner.respFn = func(int32, []topicPartitionRecords) (*kmsg.ProduceResponse, error) { return want, nil }
		tp := NewTrackingProducer(inner, &recordingTracker{})

		got, err := tp.ProduceSync(context.Background(), 1, nil)
		require.NoError(t, err)
		assert.Same(t, want, got)
	})
}
