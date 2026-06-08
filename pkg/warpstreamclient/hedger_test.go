// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// histogramCountSum returns the observation count and sum of a histogram.
// Works for native histograms, where SampleCount/SampleSum are always set.
func histogramCountSum(t *testing.T, h prometheus.Histogram) (uint64, float64) {
	t.Helper()
	var m dto.Metric
	require.NoError(t, h.Write(&m))
	return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
}

// hedgerResult captures one partition's terminal outcome as fired by the
// Hedger's per-partition done callback.
type hedgerResult struct {
	resp *kmsg.ProduceResponse
	err  error
}

// resultCapture lets a Hedger test install a done callback per
// (topic, partition) and inspect the captured outcomes after
// ProduceSync returns.
type resultCapture struct {
	mu      sync.Mutex
	results map[topicPartition]hedgerResult
}

func newResultCapture() *resultCapture {
	return &resultCapture{results: make(map[topicPartition]hedgerResult)}
}

func (c *resultCapture) doneFor(topic string, partition int32) func(*kmsg.ProduceResponse, error) {
	return func(resp *kmsg.ProduceResponse, err error) {
		c.mu.Lock()
		c.results[topicPartition{topic: topic, partition: partition}] = hedgerResult{resp: resp, err: err}
		c.mu.Unlock()
	}
}

// runHedger bridges old test-call sites — which build routed entries with
// per-partition `done` callbacks — onto the new Hedger.ProduceSync
// signature. It extracts bare partitions, calls the Hedger, then fires
// each routed entry's done with the per-partition outcome from the merged
// response (wrapping kerr.RequestTimedOut with kgo.ErrRecordTimeout to
// match perPartitionDone's behaviour in the real WarpstreamClient).
func runHedger(h *Hedger, ctx context.Context, partitions []routedTopicPartitionRecords) {
	if len(partitions) == 0 {
		return
	}
	primaryID := partitions[0].nodeID
	resp, err := h.ProduceSync(ctx, primaryID, partitions)
	for _, p := range partitions {
		if p.done == nil {
			continue
		}
		// Mirror perPartitionDone: prefer the per-partition entry from
		// the merged response; fall back to the top-level err only when
		// no response is available.
		if resp == nil {
			p.done(resp, err)
			continue
		}
		e := partitionErrorsFromResp(resp)[topicPartition{topic: p.topic, partition: p.partition}]
		if kerr.IsRetriable(e) {
			e = fmt.Errorf("%w: %w", kgo.ErrRecordTimeout, e)
		}
		p.done(resp, e)
	}
}

func (c *resultCapture) get(topic string, partition int32) hedgerResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.results[topicPartition{topic: topic, partition: partition}]
}

func TestHedger_ProduceSync(t *testing.T) {
	const (
		primaryID   = int32(1)
		secondaryID = int32(2)
		topic       = "t"
		partition   = int32(0)
	)

	stratPrimaryAndSecondary := &mockPartitionAssignmentStrategy{
		candidates: map[partitionKey][]Agent{{topic, partition}: healthyAgents(primaryID, secondaryID)},
	}

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

	health := HealthCheckConfig{
		SlowMultiplier:    2.0,
		MaxSlowFraction:   0.3,
		FaultyThreshold:   0.05,
		MaxFaultyFraction: 0.3,
	}
	cfg := HedgerConfig{
		MinHedgeDelay:  10 * time.Millisecond,
		MaxHedgeAgents: 3,
	}

	// makeReq builds a single-partition request whose done feeds capture.
	makeReq := func(capture *resultCapture) []routedTopicPartitionRecords {
		return []routedTopicPartitionRecords{{
			topicPartitionRecords: topicPartitionRecords{
				topic:     topic,
				partition: partition,
				records:   []*kgo.Record{{Topic: topic, Partition: partition}},
			},
			nodeID: primaryID,
			done:   capture.doneFor(topic, partition),
		}}
	}
	successResp := func(_ int32, _ []topicPartitionRecords) (*kmsg.ProduceResponse, error) {
		return &kmsg.ProduceResponse{
			Topics: []kmsg.ProduceResponseTopic{{
				Topic: topic,
				Partitions: []kmsg.ProduceResponseTopicPartition{
					{Partition: partition, ErrorCode: 0, BaseOffset: 42},
				},
			}},
		}, nil
	}

	t.Run("no hedge decision: only primary is called", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.respFn = successResp
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))

		require.NoError(t, capture.get(topic, partition).err)
		assert.Len(t, producer.recordedCalls(), 1)
		assert.Equal(t, primaryID, producer.recordedCalls()[0].nodeID)
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.produceRequestsPrimaryTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.produceRequestsHedgeTotal))
		// Primary won → one success observation of attempt depth 1.
		count, sum := histogramCountSum(t, m.produceRequestsAttemptsSuccess.(prometheus.Histogram))
		assert.Equal(t, uint64(1), count)
		assert.Equal(t, float64(1), sum)
	})

	t.Run("hedging suppressed when primary has no agent stats yet", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.respFn = successResp
		m := newMetrics(prometheus.NewPedanticRegistry())
		// Empty tracker: AgentStats returns !ok for the primary, so
		// shouldHedge bails at the no-agent-stats gate.
		h := NewHedger(producer, NewAverageAgentStatsTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))

		require.NoError(t, capture.get(topic, partition).err)
		assert.Len(t, producer.recordedCalls(), 1)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsSuppressedTotal.WithLabelValues(hedgeSuppressedNoAgentStats)))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeAttemptsTotal))
	})

	t.Run("primary slow: hedge fires and secondary wins", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.respFn = successResp
		gate := make(chan struct{})
		producer.blockCh[primaryID] = gate
		t.Cleanup(func() { close(gate) })

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, slowPrimaryTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m)

		capture := newResultCapture()
		syncDone := make(chan struct{})
		go func() {
			defer close(syncDone)
			runHedger(h, context.Background(), makeReq(capture))
		}()

		select {
		case <-syncDone:
			require.NoError(t, capture.get(topic, partition).err)
		case <-time.After(time.Second):
			t.Fatal("ProduceSync did not return; secondary failed to win the race")
		}
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeWinsTotal))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.produceRequestsPrimaryTotal))
		assert.GreaterOrEqual(t, testutil.ToFloat64(m.produceRequestsHedgeTotal), float64(1))
	})

	t.Run("primary fails with retriable error: cascade to secondary", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.respFn = successResp
		producer.errs[primaryID] = kerr.RequestTimedOut

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))

		require.NoError(t, capture.get(topic, partition).err)
		callIDs := producer.recordedCallNodeIDs()
		assert.Contains(t, callIDs, primaryID)
		assert.Contains(t, callIDs, secondaryID)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeWinsTotal))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.produceRequestsPrimaryTotal))
		assert.GreaterOrEqual(t, testutil.ToFloat64(m.produceRequestsHedgeTotal), float64(1))
		// Primary failed, one hedge wave resolved it → success attempt depth 2.
		count, sum := histogramCountSum(t, m.produceRequestsAttemptsSuccess.(prometheus.Histogram))
		assert.Equal(t, uint64(1), count)
		assert.Equal(t, float64(2), sum)
	})

	t.Run("hedge timer fires and secondary wins: hedgeWinsTotal incremented", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.respFn = successResp
		gate := make(chan struct{})
		producer.blockCh[primaryID] = gate
		t.Cleanup(func() { close(gate) })

		reg := prometheus.NewPedanticRegistry()
		m := newMetrics(reg)
		fastCfg := cfg
		fastCfg.MinHedgeDelay = time.Millisecond
		h := NewHedger(producer, slowPrimaryTracker(), stratPrimaryAndSecondary, health, fastCfg, 0, 1<<20, m)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))
		require.NoError(t, capture.get(topic, partition).err)

		assert.Eventually(t, func() bool {
			return testutil.ToFloat64(m.hedgeAttemptsTotal) == 1 && testutil.ToFloat64(m.hedgeWinsTotal) == 1
		}, time.Second, 10*time.Millisecond)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.produceRequestsPrimaryTotal))
		assert.GreaterOrEqual(t, testutil.ToFloat64(m.produceRequestsHedgeTotal), float64(1))
	})

	t.Run("hedge timer fires but primary wins the race: hedgeAttemptsTotal++ but no hedgeWinsTotal", func(t *testing.T) {
		// Hedge timer fires before primary returns (so the racing variant
		// is entered and runHedgingAttempts is invoked once), but primary
		// then completes ahead of the fallback. Racing variant cancels
		// workCtx; the defer must NOT count a win.
		producer := newMockDirectProducer()
		producer.respFn = successResp

		// Primary unblocks after a delay long enough for the hedge timer
		// (≥ BaselineLatency) to fire — then completes ahead of any fallback.
		primaryGate := make(chan struct{})
		producer.blockCh[primaryID] = primaryGate
		go func() {
			time.Sleep(150 * time.Millisecond)
			close(primaryGate)
		}()

		// Secondary blocks forever so the fallback can never finish before
		// the primary's success cancels it.
		secondaryGate := make(chan struct{})
		producer.blockCh[secondaryID] = secondaryGate
		t.Cleanup(func() { close(secondaryGate) })

		reg := prometheus.NewPedanticRegistry()
		m := newMetrics(reg)
		h := NewHedger(producer, slowPrimaryTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))
		require.NoError(t, capture.get(topic, partition).err)

		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.produceRequestsPrimaryTotal))
		assert.GreaterOrEqual(t, testutil.ToFloat64(m.produceRequestsHedgeTotal), float64(1))
	})

	t.Run("hedge decision but primary wins before timer: no hedgeAttemptsTotal increment", func(t *testing.T) {
		producer := newMockDirectProducer()
		producer.respFn = successResp
		reg := prometheus.NewPedanticRegistry()
		m := newMetrics(reg)
		h := NewHedger(producer, slowPrimaryTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))
		require.NoError(t, capture.get(topic, partition).err)
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.produceRequestsPrimaryTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.produceRequestsHedgeTotal))
	})

	t.Run("per-partition fanout: each partition resolves independently", func(t *testing.T) {
		// Two partitions, both routed to the same primary. Primary errors
		// out. Each partition has a distinct per-partition secondary —
		// the per-partition retry loop sends each to its own next agent
		// and both succeed.
		const topicMulti = "tm"
		const secondaryB = int32(3)
		multiStrat := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{
				{topicMulti, 0}: healthyAgents(primaryID, secondaryID),
				{topicMulti, 1}: healthyAgents(primaryID, secondaryB),
			},
		}
		producer := newMockDirectProducer()
		producer.respFn = func(nodeID int32, partitions []topicPartitionRecords) (*kmsg.ProduceResponse, error) {
			parts := make([]kmsg.ProduceResponseTopicPartition, 0, len(partitions))
			for _, p := range partitions {
				parts = append(parts, kmsg.ProduceResponseTopicPartition{Partition: p.partition, ErrorCode: 0, BaseOffset: 200 + int64(p.partition)})
			}
			return &kmsg.ProduceResponse{Topics: []kmsg.ProduceResponseTopic{{
				Topic:      topicMulti,
				Partitions: parts,
			}}}, nil
		}
		producer.errs[primaryID] = kerr.RequestTimedOut

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), multiStrat, health, cfg, 0, 1<<20, m)

		capture := newResultCapture()
		req := []routedTopicPartitionRecords{
			{topicPartitionRecords: topicPartitionRecords{topic: topicMulti, partition: 0, records: []*kgo.Record{{Topic: topicMulti, Partition: 0}}}, nodeID: primaryID, done: capture.doneFor(topicMulti, 0)},
			{topicPartitionRecords: topicPartitionRecords{topic: topicMulti, partition: 1, records: []*kgo.Record{{Topic: topicMulti, Partition: 1}}}, nodeID: primaryID, done: capture.doneFor(topicMulti, 1)},
		}
		runHedger(h, context.Background(), req)

		assert.NoError(t, capture.get(topicMulti, 0).err)
		assert.NoError(t, capture.get(topicMulti, 1).err)
		// Each partition routed to its distinct secondary.
		callIDs := producer.recordedCallNodeIDs()
		assert.Contains(t, callIDs, secondaryID)
		assert.Contains(t, callIDs, secondaryB)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeWinsTotal))
	})

	t.Run("single-agent cluster (no secondaries): primary error propagates", func(t *testing.T) {
		emptyStrat := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, partition}: healthyAgents(primaryID)},
		}
		producer := newMockDirectProducer()
		producer.errs[primaryID] = kerr.RequestTimedOut
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), emptyStrat, health, cfg, 0, 1<<20, m)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))

		err := capture.get(topic, partition).err
		require.Error(t, err)
		assert.ErrorIs(t, err, kerr.RequestTimedOut)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
		// Primary fires once; hedge never fires because there is no
		// fallback candidate.
		assert.Equal(t, float64(1), testutil.ToFloat64(m.produceRequestsPrimaryTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.produceRequestsHedgeTotal))
		// No hedge candidate → only the primary attempt was made before
		// giving up, recorded under the failure outcome.
		count, sum := histogramCountSum(t, m.produceRequestsAttemptsFailure.(prometheus.Histogram))
		assert.Equal(t, uint64(1), count)
		assert.Equal(t, float64(1), sum)
	})

	t.Run("partial fallback coverage: any partition exhausting candidates stops the whole attempt", func(t *testing.T) {
		// Two partitions; partition 0 has a fallback (secondaryID),
		// partition 1 has none. Primary fails. runHedgingAttempt bails
		// out the first wave because partition 1 has nothing to try —
		// partition 0's fallback never runs, since the batch can never
		// fully succeed anyway.
		const topicMulti = "tm"
		partialStrat := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{
				{topicMulti, 0}: healthyAgents(primaryID, secondaryID),
				{topicMulti, 1}: healthyAgents(primaryID), // no fallback configured
			},
		}
		producer := newMockDirectProducer()
		producer.respFn = func(nodeID int32, partitions []topicPartitionRecords) (*kmsg.ProduceResponse, error) {
			parts := make([]kmsg.ProduceResponseTopicPartition, 0, len(partitions))
			for _, p := range partitions {
				parts = append(parts, kmsg.ProduceResponseTopicPartition{Partition: p.partition, ErrorCode: 0, BaseOffset: 100})
			}
			return &kmsg.ProduceResponse{Topics: []kmsg.ProduceResponseTopic{{
				Topic:      topicMulti,
				Partitions: parts,
			}}}, nil
		}
		producer.errs[primaryID] = kerr.RequestTimedOut

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), partialStrat, health, cfg, 0, 1<<20, m)

		capture := newResultCapture()
		req := []routedTopicPartitionRecords{
			{topicPartitionRecords: topicPartitionRecords{topic: topicMulti, partition: 0, records: []*kgo.Record{{Topic: topicMulti, Partition: 0}}}, nodeID: primaryID, done: capture.doneFor(topicMulti, 0)},
			{topicPartitionRecords: topicPartitionRecords{topic: topicMulti, partition: 1, records: []*kgo.Record{{Topic: topicMulti, Partition: 1}}}, nodeID: primaryID, done: capture.doneFor(topicMulti, 1)},
		}
		runHedger(h, context.Background(), req)

		require.Error(t, capture.get(topicMulti, 0).err)
		assert.ErrorIs(t, capture.get(topicMulti, 0).err, kerr.RequestTimedOut)
		require.Error(t, capture.get(topicMulti, 1).err)
		assert.ErrorIs(t, capture.get(topicMulti, 1).err, kerr.RequestTimedOut)
		// Only the primary leg ran on the inner producer; the fallback
		// wave bailed out the moment partition 1 had no candidate left.
		callIDs := producer.recordedCallNodeIDs()
		assert.NotContains(t, callIDs, secondaryID)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
	})

	t.Run("primary marked as probe (demoted): hedge fires immediately, ignoring hedge delay", func(t *testing.T) {
		// healthyTracker means the stats-based shouldHedge would return
		// false. A long MinHedgeDelay further proves the point: if the
		// hedge were gated on the timer, the test would deadlock on the
		// blocked primary. The Demoter-driven nodeState=Demoted must
		// short-circuit both gates.
		producer := newMockDirectProducer()
		producer.respFn = successResp
		gate := make(chan struct{})
		producer.blockCh[primaryID] = gate
		t.Cleanup(func() { close(gate) })

		probeCfg := cfg
		probeCfg.MinHedgeDelay = time.Hour

		probeStrat := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, partition}: {
				{NodeID: primaryID, State: AgentStateDemoted},
				{NodeID: secondaryID, State: AgentStateHealthy},
			}},
		}
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), probeStrat, health, probeCfg, 0, 1<<20, m)

		capture := newResultCapture()
		probeReq := []routedTopicPartitionRecords{{
			topicPartitionRecords: topicPartitionRecords{
				topic:     topic,
				partition: partition,
				records:   []*kgo.Record{{Topic: topic, Partition: partition}},
			},
			nodeID:    primaryID,
			nodeState: AgentStateDemoted, // primary is a probe
			done:      capture.doneFor(topic, partition),
		}}

		start := time.Now()
		runHedger(h, context.Background(), probeReq)
		elapsed := time.Since(start)

		require.NoError(t, capture.get(topic, partition).err)
		// Probe primary must fast-hedge: the fallback wins immediately
		// instead of waiting for MinHedgeDelay (1h here).
		assert.Less(t, elapsed, 250*time.Millisecond)

		var sawFallback bool
		for _, c := range producer.recordedCalls() {
			if c.nodeID == secondaryID {
				sawFallback = true
			}
		}
		assert.True(t, sawFallback)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeWinsTotal))
	})

	t.Run("demoted primary is the only candidate: hedge fires but finds no fallback; primary's success wins", func(t *testing.T) {
		// shouldHedge triggers from nodeState=Demoted alone, but
		// runHedgingAttempt exhausts immediately (primary is the only
		// candidate). The primary still runs and decides the outcome; the
		// producer is only ever called once.
		producer := newMockDirectProducer()
		producer.respFn = successResp

		probeStrat := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, partition}: {
				{NodeID: primaryID, State: AgentStateDemoted},
			}},
		}
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), probeStrat, health, cfg, 0, 1<<20, m)

		capture := newResultCapture()
		req := []routedTopicPartitionRecords{{
			topicPartitionRecords: topicPartitionRecords{
				topic:     topic,
				partition: partition,
				records:   []*kgo.Record{{Topic: topic, Partition: partition}},
			},
			nodeID:    primaryID,
			nodeState: AgentStateDemoted,
			done:      capture.doneFor(topic, partition),
		}}

		runHedger(h, context.Background(), req)

		require.NoError(t, capture.get(topic, partition).err)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
		// Primary fires once; the hedge cascade has no candidates to try
		// so no hedge wire request is issued.
		assert.Equal(t, float64(1), testutil.ToFloat64(m.produceRequestsPrimaryTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.produceRequestsHedgeTotal))
		callIDs := producer.recordedCallNodeIDs()
		assert.Equal(t, []int32{primaryID}, callIDs)
	})

	t.Run("racing variant safety net: fallback returns empty before primary; primary's success is honoured", func(t *testing.T) {
		// Forces shouldHedge true (fallback exists at shouldHedge time)
		// but makes runHedgingAttempt exhaust instantly (fallback nodeID
		// equals primary, already in tried). The racing variant must
		// wait for primary instead of preempting on the empty fallback.
		producer := newMockDirectProducer()
		producer.respFn = successResp
		producer.delays[primaryID] = 50 * time.Millisecond

		strategy := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, partition}: {
				{NodeID: primaryID, State: AgentStateDemoted},
				{NodeID: primaryID, State: AgentStateHealthy},
			}},
		}
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), strategy, health, cfg, 0, 1<<20, m)

		capture := newResultCapture()
		req := []routedTopicPartitionRecords{{
			topicPartitionRecords: topicPartitionRecords{
				topic:     topic,
				partition: partition,
				records:   []*kgo.Record{{Topic: topic, Partition: partition}},
			},
			nodeID:    primaryID,
			nodeState: AgentStateDemoted,
			done:      capture.doneFor(topic, partition),
		}}

		runHedger(h, context.Background(), req)

		require.NoError(t, capture.get(topic, partition).err,
			"primary's slow but successful probe must be honoured even when the racing-variant fallback returned an empty result")
	})

	t.Run("both legs fail with retriable errors: surfaces the last error", func(t *testing.T) {
		// Primary and secondary both error out with retriable errors
		// (request timed out). After both have been tried and there are
		// no more candidates, the partition's done fires with the last
		// retriable error seen.
		producer := newMockDirectProducer()
		producer.errs[primaryID] = kerr.RequestTimedOut
		producer.errs[secondaryID] = kerr.LeaderNotAvailable

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))

		err := capture.get(topic, partition).err
		require.Error(t, err)
		assert.True(t,
			errors.Is(err, kerr.RequestTimedOut) || errors.Is(err, kerr.LeaderNotAvailable))
		assert.Len(t, producer.recordedCalls(), 2)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
	})

	t.Run("non-retriable fallback err aborts: no further waves even with candidates left", func(t *testing.T) {
		// Primary fails retriably; the first fallback wave hits a
		// non-retriable err (kerr.MessageTooLarge), which aborts the
		// accumulator. Even though MaxHedgeAgents=4 still leaves
		// candidates untried, runHedgingAttempt must stop iterating —
		// no extra agents are contacted.
		const (
			agentA = int32(1) // primary
			agentB = int32(2) // first fallback (the aborting one)
			agentC = int32(3)
			agentD = int32(4)
		)
		strategy := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, partition}: healthyAgents(agentA, agentB, agentC, agentD)},
		}
		producer := newMockDirectProducer()
		producer.errs[agentA] = kerr.RequestTimedOut // retriable
		producer.errs[agentB] = kerr.MessageTooLarge // non-retriable

		failingCfg := cfg
		failingCfg.MaxHedgeAgents = 4

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), strategy, health, failingCfg, 0, 1<<20, m)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))

		err := capture.get(topic, partition).err
		require.Error(t, err)
		assert.ErrorIs(t, err, kerr.MessageTooLarge)
		// Only primary (A) and the first fallback (B) should have been
		// contacted — C and D are still in the candidate list but the
		// accumulator aborted, so iteration must stop.
		seen := map[int32]int{}
		for _, c := range producer.recordedCalls() {
			seen[c.nodeID]++
		}
		assert.Equal(t, 1, seen[agentA])
		assert.Equal(t, 1, seen[agentB])
		assert.Zero(t, seen[agentC])
		assert.Zero(t, seen[agentD])
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
	})

	t.Run("primary and secondary legs share the tried set: no agent is attempted twice", func(t *testing.T) {
		// Both legs running concurrently must coordinate so no single
		// agent receives two attempts for the same partition. We force
		// the race by blocking every agent's response on a shared gate:
		// each agent's attempt parks until the test releases the gate,
		// guaranteeing both legs are simultaneously inside the claim
		// loop. Then we release the gate, let everything fail with a
		// retriable error, and verify the recorded nodeIDs are unique.
		const (
			agentA = int32(1)
			agentB = int32(2)
			agentC = int32(3)
			agentD = int32(4)
		)
		strategy := &mockPartitionAssignmentStrategy{
			// Pad Candidates beyond just primary+secondary so the second
			// worker can keep claiming.
			candidates: map[partitionKey][]Agent{{topic, partition}: healthyAgents(agentA, agentB, agentC, agentD)},
		}

		producer := newMockDirectProducer()
		gate := make(chan struct{})
		for _, id := range []int32{agentA, agentB, agentC, agentD} {
			producer.blockCh[id] = gate
		}
		producer.respFn = func(int32, []topicPartitionRecords) (*kmsg.ProduceResponse, error) {
			// After gate releases, every leg fails with the same
			// retriable error so the loop keeps cycling.
			return nil, kerr.RequestTimedOut
		}

		fastCfg := cfg
		fastCfg.MinHedgeDelay = time.Millisecond
		fastCfg.MaxHedgeAgents = 4

		tr := healthyTracker()
		// Mark primary slow so shouldHedge fires.
		ps := tr.stats[agentA]
		ps.bucketsMu.Lock()
		for i := range ps.buckets {
			if ps.buckets[i].successfulLatencyCount > 0 {
				ps.buckets[i].successfulLatencySumMs = ps.buckets[i].successfulLatencyCount * 100
			}
		}
		ps.bucketsMu.Unlock()

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, tr, strategy, health, fastCfg, 0, 1<<20, m)

		capture := newResultCapture()
		req := []routedTopicPartitionRecords{{
			topicPartitionRecords: topicPartitionRecords{
				topic:     topic,
				partition: partition,
				records:   []*kgo.Record{{Topic: topic, Partition: partition}},
			},
			nodeID: agentA,
			done:   capture.doneFor(topic, partition),
		}}

		syncDone := make(chan struct{})
		go func() {
			defer close(syncDone)
			runHedger(h, context.Background(), req)
		}()

		// Release the gate; both legs unwind in parallel.
		close(gate)
		select {
		case <-syncDone:
		case <-time.After(time.Second):
			t.Fatal("ProduceSync did not return")
		}

		seen := map[int32]int{}
		for _, c := range producer.recordedCalls() {
			seen[c.nodeID]++
		}
		// The two legs must share the tried set so no agent is contacted
		// twice for the same partition.
		for agent, count := range seen {
			assert.Equal(t, 1, count, "agent %d was attempted %d times", agent, count)
		}
		// All four agents should appear (cap was 4); the order is
		// non-deterministic across legs but the set is fixed.
		require.Len(t, seen, 4)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
	})

	t.Run("racing path: primary fails before fallback exhausts → underlying kerr preserved", func(t *testing.T) {
		// slowPrimaryTracker + a hedge delay forces ProduceSync into
		// the racing wrapper. Primary takes long enough for the timer
		// to fire and short enough to return before the slow fallback
		// agent. The fallback agent also fails. Before the fix the
		// wrapper returned (synthetic_resp, nil); the caller's
		// perPartitionDone fell back to extracting REQUEST_TIMED_OUT
		// from the resp, masking the actual leg kerrs. After the fix
		// the wrapper propagates a wrapped error chain that retains
		// primary's and/or fallback's specific kerrs.
		producer := newMockDirectProducer()
		producer.delays[primaryID] = 20 * time.Millisecond
		producer.errs[primaryID] = kerr.LeaderNotAvailable
		// Make the fallback slow enough that primary returns first
		// inside the wrapper's select.
		producer.delays[secondaryID] = 200 * time.Millisecond
		producer.errs[secondaryID] = kerr.NotLeaderForPartition

		failingCfg := cfg
		failingCfg.MinHedgeDelay = time.Millisecond
		failingCfg.MaxHedgeAgents = 2

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, slowPrimaryTracker(), stratPrimaryAndSecondary, health, failingCfg, 0, 1<<20, m)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))

		err := capture.get(topic, partition).err
		require.Error(t, err)
		assert.ErrorIs(t, err, kgo.ErrRecordTimeout)
		assert.True(t,
			errors.Is(err, kerr.LeaderNotAvailable) || errors.Is(err, kerr.NotLeaderForPartition),
			"got %v", err)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
	})

	t.Run("racing path: fallback exhausts before primary returns → fallback kerr preserved", func(t *testing.T) {
		// Fallback exhausts fast; racing variant waits for the
		// (slower) primary and merges both failures into the chain.
		producer := newMockDirectProducer()
		producer.delays[primaryID] = 50 * time.Millisecond
		producer.errs[primaryID] = kerr.RequestTimedOut
		producer.errs[secondaryID] = kerr.NotLeaderForPartition

		failingCfg := cfg
		failingCfg.MinHedgeDelay = time.Millisecond
		failingCfg.MaxHedgeAgents = 2

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, slowPrimaryTracker(), stratPrimaryAndSecondary, health, failingCfg, 0, 1<<20, m)

		capture := newResultCapture()
		runHedger(h, context.Background(), makeReq(capture))

		err := capture.get(topic, partition).err
		require.Error(t, err)
		assert.ErrorIs(t, err, kgo.ErrRecordTimeout)
		assert.ErrorIs(t, err, kerr.NotLeaderForPartition, "got %v", err)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
	})

	t.Run("ctx canceled mid-fallback: result surfaces ctx err, not synthesized timeout", func(t *testing.T) {
		// Primary fails immediately so we cascade into runHedgingAttempts.
		// The fallback agent blocks forever; cancelling the caller's ctx
		// must unblock the loop, and runHedgingAttempts must surface the
		// ctx err directly (not acc.result()'s ErrRecordTimeout envelope).
		producer := newMockDirectProducer()
		producer.errs[primaryID] = kerr.RequestTimedOut
		gate := make(chan struct{})
		producer.blockCh[secondaryID] = gate
		t.Cleanup(func() { close(gate) })

		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m)

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		_, err := h.ProduceSync(ctx, primaryID, []routedTopicPartitionRecords{{
			topicPartitionRecords: topicPartitionRecords{
				topic:     topic,
				partition: partition,
				records:   []*kgo.Record{{Topic: topic, Partition: partition}},
			},
			nodeID: primaryID,
		}})
		require.Error(t, err)
		// The ctx err is chained inside the primary+fallback envelope by
		// selectProduceResult.
		assert.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, float64(1), testutil.ToFloat64(m.hedgeAttemptsTotal))
		assert.Equal(t, float64(0), testutil.ToFloat64(m.hedgeWinsTotal))
	})

	t.Run("fallback wins: primary leg is canceled via workCtx instead of running until its per-attempt deadline", func(t *testing.T) {
		producer := newMockDirectProducer()
		// Primary blocks until workCtx is cancelled (or the gate closes,
		// which the test never does — the cancellation path is what we
		// want to exercise).
		producer.blockCh[primaryID] = make(chan struct{})
		producer.respFn = successResp

		// Demoted primary forces shouldHedge=true with delay=0 so the
		// fallback fires immediately and wins the race.
		probeStrat := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, partition}: {
				{NodeID: primaryID, State: AgentStateDemoted},
				{NodeID: secondaryID, State: AgentStateHealthy},
			}},
		}
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), probeStrat, health, cfg, 0, 1<<20, m)

		capture := newResultCapture()
		req := []routedTopicPartitionRecords{{
			topicPartitionRecords: topicPartitionRecords{
				topic:     topic,
				partition: partition,
				records:   []*kgo.Record{{Topic: topic, Partition: partition}},
			},
			nodeID:    primaryID,
			nodeState: AgentStateDemoted,
			done:      capture.doneFor(topic, partition),
		}}
		runHedger(h, context.Background(), req)
		require.NoError(t, capture.get(topic, partition).err)

		// Primary's blocked attempt must observe workCtx cancellation and
		// record a ctx-err call, not keep blocking.
		require.Eventually(t, func() bool {
			for _, c := range producer.recordedCalls() {
				if c.nodeID == primaryID && errors.Is(c.err, context.Canceled) {
					return true
				}
			}
			return false
		}, time.Second, 10*time.Millisecond, "primary leg was not canceled after fallback won")
	})

	t.Run("returns error when a routed partition's nodeID disagrees with primaryID", func(t *testing.T) {
		producer := newMockDirectProducer()
		m := newMetrics(prometheus.NewPedanticRegistry())
		h := NewHedger(producer, healthyTracker(), stratPrimaryAndSecondary, health, cfg, 0, 1<<20, m)

		req := []routedTopicPartitionRecords{
			{
				topicPartitionRecords: topicPartitionRecords{topic: topic, partition: 0},
				nodeID:                primaryID,
			},
			{
				topicPartitionRecords: topicPartitionRecords{topic: topic, partition: 1},
				nodeID:                secondaryID,
			},
		}
		_, err := h.ProduceSync(context.Background(), primaryID, req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "primaryID=1")
		assert.Empty(t, producer.recordedCalls())
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

func TestHedgerCandidates_fetch(t *testing.T) {
	const topic = "t"

	t.Run("first call queries strategy and caches the result", func(t *testing.T) {
		strategy := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, 0}: healthyAgents(1, 2)},
		}
		hc := newHedgerCandidates(strategy, 3)
		tp := topicPartition{topic: topic, partition: 0}

		first := hc.fetch(tp)
		require.Len(t, first, 2)
		assert.Equal(t, int32(1), first[0].NodeID)
		assert.Equal(t, int32(2), first[1].NodeID)
		assert.Equal(t, 1, strategy.candidatesCalls(topic, 0))

		// Repeated calls reuse the cached slice and never hit the strategy.
		for i := 0; i < 5; i++ {
			again := hc.fetch(tp)
			assert.Equal(t, first, again)
		}
		assert.Equal(t, 1, strategy.candidatesCalls(topic, 0))
	})

	t.Run("different (topic, partition) keys cache independently", func(t *testing.T) {
		strategy := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{
				{topic, 0}: healthyAgents(1),
				{topic, 1}: healthyAgents(2),
			},
		}
		hc := newHedgerCandidates(strategy, 3)

		_ = hc.fetch(topicPartition{topic: topic, partition: 0})
		_ = hc.fetch(topicPartition{topic: topic, partition: 1})
		_ = hc.fetch(topicPartition{topic: topic, partition: 0})
		_ = hc.fetch(topicPartition{topic: topic, partition: 1})

		assert.Equal(t, 1, strategy.candidatesCalls(topic, 0))
		assert.Equal(t, 1, strategy.candidatesCalls(topic, 1))
	})

	t.Run("forwards maxHedgeAgents to the strategy", func(t *testing.T) {
		strategy := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, 0}: healthyAgents(1, 2, 3, 4, 5)},
		}
		hc := newHedgerCandidates(strategy, 2)

		got := hc.fetch(topicPartition{topic: topic, partition: 0})
		assert.Len(t, got, 2)
		assert.Equal(t, 2, strategy.lastMaxCandidates(topic, 0))
	})
}
