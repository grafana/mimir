// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestHedger_Produce(t *testing.T) {
	const (
		primaryID   = int32(1)
		secondaryID = int32(2)
		topic       = "t"
		partition   = int32(0)
	)

	stratPrimaryAndSecondary := &mockPartitionAssignmentStrategy{
		primary:   map[partitionKey]int32{{topic, partition}: primaryID},
		secondary: map[partitionKey]int32{{topic, partition}: secondaryID},
	}

	// Seed against the real current time so the Hedger's internal
	// time.Now() lands within the seeded observation window.
	healthyTracker := func() *AverageAgentStatsTracker {
		tr := NewAverageAgentStatsTracker()
		nowNs := time.Now().UnixNano()
		for _, id := range []int32{primaryID, secondaryID, 3, 4} {
			seedFullWindow(tr, id, nowNs, 20, 1, 0)
		}
		return tr
	}
	slowPrimaryTracker := func() *AverageAgentStatsTracker {
		tr := healthyTracker()
		ps := tr.stats[primaryID]
		ps.bucketsMu.Lock()
		for i := range ps.buckets {
			if ps.buckets[i].successfulLatencyCount > 0 {
				ps.buckets[i].successfulLatencySumMs = ps.buckets[i].successfulLatencyCount * 100
			}
		}
		ps.bucketsMu.Unlock()
		return tr
	}

	cfg := HedgerConfig{
		SlowMultiplier:    2.0,
		MaxSlowFraction:   0.3,
		FaultyThreshold:   0.05,
		MaxFaultyFraction: 0.3,
		MinHedgeDelay:     10 * time.Millisecond,
	}

	makeReq := func() *kmsg.ProduceRequest {
		return &kmsg.ProduceRequest{
			Topics: []kmsg.ProduceRequestTopic{{
				Topic:      topic,
				Partitions: []kmsg.ProduceRequestTopicPartition{{Partition: partition}},
			}},
		}
	}
	successResp := func(_ int32, _ *kmsg.ProduceRequest) *kmsg.ProduceResponse {
		return &kmsg.ProduceResponse{
			Topics: []kmsg.ProduceResponseTopic{{
				Topic: topic,
				Partitions: []kmsg.ProduceResponseTopicPartition{
					{Partition: partition, ErrorCode: 0, BaseOffset: 42},
				},
			}},
		}
	}

	t.Run("no hedge decision: only primary is called", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.respFn = successResp
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), stratPrimaryAndSecondary, cfg, m)

		resp, err := h.Produce(context.Background(), primaryID, makeReq())
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Len(t, producer.recordedCalls(), 1)
		assert.Equal(t, primaryID, producer.recordedCalls()[0].nodeID)
	})

	t.Run("primary slow: hedge fires and secondary wins", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.respFn = successResp
		gate := make(chan struct{})
		producer.blockCh[primaryID] = gate

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, slowPrimaryTracker(), stratPrimaryAndSecondary, cfg, m)

		respCh := make(chan *kmsg.ProduceResponse, 1)
		errCh := make(chan error, 1)
		go func() {
			resp, err := h.Produce(context.Background(), primaryID, makeReq())
			respCh <- resp
			errCh <- err
		}()

		select {
		case err := <-errCh:
			require.NoError(t, err)
			require.NotNil(t, <-respCh)
		case <-time.After(time.Second):
			t.Fatal("Produce did not return; secondary failed to win the race")
		}
		close(gate)

		assert.Eventually(t, func() bool {
			return len(producer.recordedCalls()) == 2
		}, time.Second, 10*time.Millisecond)
	})

	t.Run("primary fails before hedge: secondary fanout retry", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.respFn = successResp
		producer.errs[primaryID] = errors.New("primary failed")

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), stratPrimaryAndSecondary, cfg, m)

		resp, err := h.Produce(context.Background(), primaryID, makeReq())
		require.NoError(t, err)
		require.NotNil(t, resp)
		callIDs := producer.recordedCallNodeIDs()
		assert.Contains(t, callIDs, primaryID)
		assert.Contains(t, callIDs, secondaryID)
	})

	t.Run("hedge timer fires and secondary wins: hedgeWinsTotal incremented", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.respFn = successResp
		gate := make(chan struct{})
		producer.blockCh[primaryID] = gate

		reg := prometheus.NewPedanticRegistry()
		m := newMetrics(reg)
		fastCfg := cfg
		fastCfg.MinHedgeDelay = time.Millisecond
		h := NewHedger(producer, slowPrimaryTracker(), stratPrimaryAndSecondary, fastCfg, m)

		_, err := h.Produce(context.Background(), primaryID, makeReq())
		require.NoError(t, err)
		close(gate)

		assert.Eventually(t, func() bool {
			return testutil.ToFloat64(m.hedgeAttemptsTotal) == 1 && testutil.ToFloat64(m.hedgeWinsTotal) == 1
		}, time.Second, 10*time.Millisecond)
	})

	t.Run("hedge decision but primary wins before timer: no hedgeAttemptsTotal increment", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.respFn = successResp
		reg := prometheus.NewPedanticRegistry()
		m := newMetrics(reg)
		h := NewHedger(producer, slowPrimaryTracker(), stratPrimaryAndSecondary, cfg, m)

		_, err := h.Produce(context.Background(), primaryID, makeReq())
		require.NoError(t, err)
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeAttemptsTotal),
			"primary returned before the hedge timer; no attempt should be counted")
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
	})

	t.Run("primary error: fanout merges responses from per-partition secondaries", func(t *testing.T) {
		const topicMulti = "tm"
		const secondaryB = int32(3)
		makeMultiReq := func() *kmsg.ProduceRequest {
			return &kmsg.ProduceRequest{
				Topics: []kmsg.ProduceRequestTopic{{
					Topic: topicMulti,
					Partitions: []kmsg.ProduceRequestTopicPartition{
						{Partition: 0},
						{Partition: 1},
					},
				}},
			}
		}
		// Each partition has a different designated secondary so the
		// fanout actually splits the request and the merge has to
		// concatenate two sub-responses.
		multiStrat := &mockPartitionAssignmentStrategy{
			primary: map[partitionKey]int32{
				{topicMulti, 0}: primaryID,
				{topicMulti, 1}: primaryID,
			},
			secondary: map[partitionKey]int32{
				{topicMulti, 0}: secondaryID,
				{topicMulti, 1}: secondaryB,
			},
		}
		producer := newMockDirectProducer()
		producer.respFn = func(nodeID int32, req *kmsg.ProduceRequest) *kmsg.ProduceResponse {
			// Each secondary returns its own subset.
			var part int32
			var offset int64
			if nodeID == secondaryID {
				part, offset = 0, 200
			} else {
				part, offset = 1, 201
			}
			_ = req
			return &kmsg.ProduceResponse{Topics: []kmsg.ProduceResponseTopic{{
				Topic: topicMulti,
				Partitions: []kmsg.ProduceResponseTopicPartition{
					{Partition: part, ErrorCode: 0, BaseOffset: offset},
				},
			}}}
		}
		producer.errs[primaryID] = errors.New("primary failed")

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), multiStrat, cfg, m)

		resp, err := h.Produce(context.Background(), primaryID, makeMultiReq())
		require.NoError(t, err)
		require.Len(t, resp.Topics, 1)
		require.Len(t, resp.Topics[0].Partitions, 2)
		// Both partitions resolved by the merged fanout response.
		offsets := []int64{
			resp.Topics[0].Partitions[0].BaseOffset,
			resp.Topics[0].Partitions[1].BaseOffset,
		}
		assert.ElementsMatch(t, []int64{200, 201}, offsets)
	})

	t.Run("single-agent cluster (no secondaries): primary error propagates", func(t *testing.T) {
		emptyStrat := &mockPartitionAssignmentStrategy{
			primary: map[partitionKey]int32{{topic, partition}: primaryID},
		}
		producer := newMockDirectProducer()
		producer.errs[primaryID] = errors.New("primary failed")
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), emptyStrat, cfg, m)

		_, err := h.Produce(context.Background(), primaryID, makeReq())
		require.Error(t, err)
		assert.ErrorContains(t, err, "primary failed")
	})

	t.Run("both fail: returns primary error and discards fanout error", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.respFn = successResp
		primaryErr := errors.New("primary failed")
		secondaryErr := errors.New("secondary failed")
		producer.errs[primaryID] = primaryErr
		producer.errs[secondaryID] = secondaryErr

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), stratPrimaryAndSecondary, cfg, m)

		_, err := h.Produce(context.Background(), primaryID, makeReq())
		require.Error(t, err)
		assert.ErrorIs(t, err, primaryErr)
		assert.NotErrorIs(t, err, secondaryErr)
		assert.Len(t, producer.recordedCalls(), 2)
	})
}

func TestMaxFractionFloor(t *testing.T) {
	tests := map[string]struct {
		configured   float64
		contributors int64
		want         float64
	}{
		"single contributor: floor at 1.0":      {configured: 0.10, contributors: 1, want: 1.0},
		"two contributors: floor at 0.5 raises": {configured: 0.10, contributors: 2, want: 0.5},
		"large cluster: configured dominates":   {configured: 0.10, contributors: 100, want: 0.10},
		"configured wins when above 1/N":        {configured: 0.50, contributors: 5, want: 0.50},
		"zero contributors: returns configured": {configured: 0.10, contributors: 0, want: 0.10},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert.InDelta(t, tc.want, maxFractionFloor(tc.configured, tc.contributors), 1e-9)
		})
	}
}
