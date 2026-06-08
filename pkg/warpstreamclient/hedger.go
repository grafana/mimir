// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// HedgerConfig holds the Hedger-specific timing knobs. The health
// classification thresholds (slow/faulty cutoffs, max-fraction guards)
// live on HealthCheckConfig because they are shared with the Demoter.
type HedgerConfig struct {
	// MinHedgeDelay is the floor on the dynamically-computed hedge delay.
	// The delay is otherwise the cluster baseline latency.
	MinHedgeDelay time.Duration

	// MaxHedgeAgents caps how many per-partition candidates each leg
	// walks before giving up on that partition. Across both legs (primary
	// + secondary) a partition can be attempted on up to this many distinct
	// agents in total; the two legs share the tried-history.
	MaxHedgeAgents int
}

// Hedger orchestrates produce attempts across multiple agents for the
// same batch of partitions. The whole "retry on a different agent,
// possibly fire a hedge to race the primary, give up when
// MaxHedgeAgents is reached" story lives here; the layer below
// (TrackingProducer → KafkaDirectProducer) is a single per-attempt
// produce with no retry of its own.
//
// Mental model: ProduceSync fires the primary leg (a straight
// passthrough to inner.ProduceSync) and then waits for one of three
// outcomes:
//
//  1. Primary returns first with full success → return its response
//     verbatim. No hedging machinery is touched.
//  2. Primary returns first with a failure or partial result → call
//     runHedgingAttempts, which retries the partitions across other
//     agents.
//  3. The hedge timer fires before primary returns → call
//     runHedgingAttemptsAndRaceWithPrimary, which runs the fallback
//     waves alongside the still-in-flight primary and merges whichever
//     finishes first.
//
// Per-partition fanout lives inside runHedgingAttempt: each wave picks
// the next per-partition candidate (so partitions whose strategy
// orderings differ end up at different agents in the same wave), groups
// by chosen agent, and fans out concurrently through hedgeBuffer.
//
// Hedge-decision details:
//   - shouldHedge consults the rolling per-agent stats (same window the
//     Demoter uses via HealthCheckConfig) for primaryID specifically.
//     When the primary is healthy and the cluster as a whole is healthy,
//     hedge=false and we just wait for the primary synchronously.
//   - anyProbeFor(primaryID) forces hedge=true delay=0: when the
//     strategy surfaces primaryID as a AgentStateDemoted (a probe), we
//     expect it to fail, so we don't pay the full delay before kicking
//     off the fallback.
//
// hedge_wins_total counts the timer-fired race: the fallback waves
// completed before primary returned. (Pure cascade retries — primary
// returned first with a failure — don't count.)
//
// Inner must be wrapped by a TrackingProducer so every attempt —
// primary and fallback — contributes to the rolling agent stats window.
type Hedger struct {
	inner    DirectProducer
	tracker  AgentStatsReader
	strategy PartitionAssignmentStrategy
	health   HealthCheckConfig
	cfg      HedgerConfig
	metrics  *metrics

	// hedgeBuffer batches per-wave per-agent groups across concurrent
	// ProduceSync calls heading to the same agent. Its AgentFlushFunc is
	// h.inner.ProduceSync directly, so hedge-origin flushes go straight
	// through the DirectProducer chain without recursing into this Hedger.
	hedgeBuffer *ClusterRecordBuffer
}

// NewHedger wraps inner with the orchestration described on Hedger.
func NewHedger(inner DirectProducer, tracker AgentStatsReader, strategy PartitionAssignmentStrategy, health HealthCheckConfig, cfg HedgerConfig, linger time.Duration, maxBatchBytes int32, m *metrics) *Hedger {
	h := &Hedger{
		inner:    inner,
		tracker:  tracker,
		strategy: strategy,
		health:   health,
		cfg:      cfg,
		metrics:  m,
	}
	h.hedgeBuffer = NewClusterRecordBuffer(linger, maxBatchBytes, func(ctx context.Context, nodeID int32, parts []routedTopicPartitionRecords) (*kmsg.ProduceResponse, error) {
		// Every hedge-buffer flush is one hedge wire request (possibly
		// covering multiple partitions). The companion primary counter is
		// incremented in ProduceSync.
		m.produceRequestsHedgeTotal.Inc()

		// DirectProducer takes unrouted partitions (the nodeID is specified separately), so we strip the routing.
		return inner.ProduceSync(ctx, nodeID, unrouteTopicPartitionRecords(parts))
	}, m)
	return h
}

// Close releases the hedge buffer's resources, draining any pending hedge
// legs. Safe to call multiple times.
func (h *Hedger) Close() {
	h.hedgeBuffer.Close()
}

// ProduceSync resolves every partition to a terminal outcome and returns a
// single merged ProduceResponse encoding each partition's outcome.
func (h *Hedger) ProduceSync(ctx context.Context, primaryID int32, routedPartitions []routedTopicPartitionRecords) (*kmsg.ProduceResponse, error) {
	if len(routedPartitions) == 0 {
		return &kmsg.ProduceResponse{}, nil
	}

	// All routed partitions in one call must share primaryID — the buffer
	// bins by destination nodeID. A mismatch is a routing bug.
	for _, p := range routedPartitions {
		if p.nodeID != primaryID {
			return nil, fmt.Errorf("hedger: partition %s/%d routed to nodeID=%d but primaryID=%d", p.topic, p.partition, p.nodeID, primaryID)
		}
	}

	// observeAttempts records the attempt depth of a resolved produce call,
	// split by outcome: 1 = resolved on the primary, N = resolved after N-1
	// hedge waves.
	observeAttempts := func(result produceResult, attempts int) {
		if result.succeeded() {
			h.metrics.produceRequestsAttemptsSuccess.Observe(float64(attempts))
		} else {
			h.metrics.produceRequestsAttemptsFailure.Observe(float64(attempts))
		}
	}

	// Check the hedging delay to apply to this request.
	delay, shouldHedge := h.shouldHedge(time.Now(), primaryID, routedPartitions)

	// The rest of the Hedger works with unrouted partitions, because it will be
	// responsible to route partitions to other candidate agents during hedging
	// and retries.
	partitions := unrouteTopicPartitionRecords(routedPartitions)

	// workCtx scopes both the primary and any hedge waves to this single
	// ProduceSync call. Canceling it (fallback wins, or function returns)
	// unwinds in-flight attempts at the next ctx check instead of letting
	// them run out their per-attempt deadline.
	workCtx, cancelWorkCtx := context.WithCancel(ctx)
	defer cancelWorkCtx()

	// Primary leg: straight passthrough to inner.ProduceSync. The result
	// lands on primaryCh whenever the call completes; the buffered slot
	// guarantees the goroutine doesn't block on send when ProduceSync has
	// already moved on to the fallback path.
	primaryCh := make(chan produceResult, 1)
	go func() {
		h.metrics.produceRequestsPrimaryTotal.Inc()
		resp, err := h.inner.ProduceSync(workCtx, primaryID, partitions)
		primaryCh <- produceResult{resp: resp, err: err}
	}()

	candidates := newHedgerCandidates(h.strategy, h.cfg.MaxHedgeAgents)

	if shouldHedge {
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case primaryResult := <-primaryCh:
			if primaryResult.succeeded() {
				observeAttempts(primaryResult, 1)
				return primaryResult.resp, nil
			}

			hedged := h.runHedgingAttempts(workCtx, primaryID, partitions, candidates)
			result := selectProduceResult(primaryResult, hedged.result)
			observeAttempts(result, hedged.attempts)
			return result.resp, result.err
		case <-timer.C:
			hedged := h.runHedgingAttemptsAndRaceWithPrimary(workCtx, primaryID, partitions, candidates, primaryCh)
			observeAttempts(hedged.result, hedged.attempts)
			return hedged.result.resp, hedged.result.err
		}
	}

	// No hedging — wait for the primary synchronously.
	primaryResult := <-primaryCh
	if primaryResult.succeeded() {
		observeAttempts(primaryResult, 1)
		return primaryResult.resp, nil
	}

	// The primary has failed. Try secondaries.
	hedged := h.runHedgingAttempts(workCtx, primaryID, partitions, candidates)
	result := selectProduceResult(primaryResult, hedged.result)
	observeAttempts(result, hedged.attempts)
	return result.resp, result.err
}

// runHedgingAttemptsAndRaceWithPrimary races the hedge fallback against
// the in-flight primary; the first to produce a usable outcome wins.
// workCtx scopes both legs and is cancelled by the caller's deferred
// cancel as soon as this function returns, which unwinds the losing leg.
// The reported attempts is the depth of the winning leg: 1 when the
// primary wins, the fallback's own depth when the fallback wins.
func (h *Hedger) runHedgingAttemptsAndRaceWithPrimary(workCtx context.Context, primaryID int32, partitions []topicPartitionRecords, candidates *hedgerCandidates, primaryCh <-chan produceResult) hedgerProduceResult {
	fallbackCh := make(chan hedgerProduceResult, 1)
	go func() {
		fallbackCh <- h.runHedgingAttempts(workCtx, primaryID, partitions, candidates)
	}()

	// Either side's "wait for the other" branch is bounded: both legs
	// ultimately call KafkaDirectProducer.ProduceSync, which wraps its
	// Broker.Request in a per-attempt deadline (ProduceRequestTimeout +
	// overhead), so primaryCh and fallbackCh always fire in finite time.
	select {
	case primaryResult := <-primaryCh:
		if primaryResult.succeeded() {
			// We don't wait for the fallback goroutine here — fallbackCh
			// is buffered (1) so the goroutine self-drains and exits on
			// its next ctx check after the caller's defer cancels workCtx.
			return hedgerProduceResult{result: primaryResult, attempts: 1}
		}
		// Primary failed; the fallback's view supersedes primary's
		// (per-partition errors are equivalent or richer there).
		fb := <-fallbackCh
		return hedgerProduceResult{result: selectProduceResult(primaryResult, fb.result), attempts: fb.attempts}
	case fb := <-fallbackCh:
		if fb.result.succeeded() {
			return fb
		}
		// Fallback failed (e.g. exhausted candidates) — the primary may
		// still produce a usable outcome, so let it finish. If the primary
		// then succeeds it won on its single attempt (depth 1).
		return hedgerProduceResult{result: selectProduceResult(<-primaryCh, fb.result), attempts: 1}
	}
}

// runHedgingAttempts runs the per-partition retry loop across other
// agents, returning when every partition is resolved, has exhausted
// MaxHedgeAgents, or ctx is canceled. The reported attempts is the total
// attempt depth: 1 (the primary, which this function models as the first
// tried agent) plus one per hedge wave dispatched.
func (h *Hedger) runHedgingAttempts(workCtx context.Context, primaryID int32, partitions []topicPartitionRecords, candidates *hedgerCandidates) (out hedgerProduceResult) {
	h.metrics.hedgeAttemptsTotal.Inc()
	// Count a win only when the result is successful AND we weren't
	// preempted by ctx cancellation (e.g. the racing variant aborting
	// the fallback because the primary won).
	defer func() {
		if out.result.succeeded() && workCtx.Err() == nil {
			h.metrics.hedgeWinsTotal.Inc()
		}
	}()

	acc, err := newProduceResultAccumulator(partitions)
	if err != nil {
		// Duplicate (topic, partition) in the input — caller (the
		// cluster buffer) violated its bin-by-partition invariant.
		// Surface so the bug isn't silenced.
		out.result = produceResult{err: err}
		return out
	}

	// tried lives outside the accumulator: it's strategy/retry concern,
	// not response-merging concern. Pre-seed with primaryID so the
	// fallback never picks the agent that already failed.
	tried := make(map[topicPartition][]int32, len(partitions))
	for _, p := range partitions {
		tried[topicPartition{topic: p.topic, partition: p.partition}] = []int32{primaryID}
	}

	// The primary is attempt 1; each dispatched hedge wave adds one. A wave
	// that bails before dispatching (candidates exhausted) must not count.
	out.attempts = 1
	for workCtx.Err() == nil {
		dispatched, canRetry := h.runHedgingAttempt(workCtx, acc, tried, candidates)
		if dispatched {
			out.attempts++
		}
		if !canRetry {
			break
		}
	}

	// If the caller's ctx was canceled mid-loop, surface that directly:
	// the work was preempted, not exhausted. We still want the merged
	// per-partition state, so we keep acc.result()'s resp.
	out.result = acc.result()
	if ctxErr := workCtx.Err(); ctxErr != nil {
		out.result.err = ctxErr
	}
	return out
}

// runHedgingAttempt runs one wave of the per-partition retry loop, fanning
// out per agent. dispatched reports whether this wave actually issued any
// produce requests (false when it bailed before dispatching: nothing
// pending or a partition exhausted its candidates). canRetry reports
// whether the caller should iterate again — false when no further work
// would change the outcome (every partition resolved, accumulator aborted,
// a partition exhausted candidates, or ctx canceled mid-wave).
func (h *Hedger) runHedgingAttempt(workCtx context.Context, acc *produceResultAccumulator, tried map[topicPartition][]int32, candidates *hedgerCandidates) (dispatched, canRetry bool) {
	pending := acc.remaining()
	if len(pending) == 0 {
		return false, false
	}

	// Pick the next candidate per partition; group by chosen agent.
	// If any partition has exhausted its candidates we stop the whole
	// loop: the batch can never produce every partition successfully, so
	// further waves on the remaining partitions waste effort. Pending
	// partitions will surface as per-partition errors in result().
	groups := map[int32][]topicPartitionRecords{}
	for _, p := range pending {
		tp := topicPartition{topic: p.topic, partition: p.partition}

		// Hard cap on distinct agents tried per partition. Needed because
		// Candidates() may surface a different set across waves (pool
		// refresh, probe rotation, ordering changes) so the per-call
		// MaxHedgeAgents limit alone doesn't bound the total.
		if len(tried[tp]) >= h.cfg.MaxHedgeAgents {
			return false, false
		}

		var (
			next  int32
			found bool
		)
		for _, c := range candidates.fetch(tp) {
			if !slices.Contains(tried[tp], c.NodeID) {
				next = c.NodeID
				found = true
				break
			}
		}

		// We exhausted all candidates for this partition. We give up.
		if !found {
			return false, false
		}

		tried[tp] = append(tried[tp], next)
		groups[next] = append(groups[next], p)
	}

	// Wave-local workCtx so we can short-circuit the wait the moment the
	// batch is fully resolved (or aborted). Cancellation flows through
	// hedgeBuffer.Add's ctx-watch, making slow agents fire their legDone
	// with ctx.Err() instead of blocking us on their actual flush.
	attemptCtx, cancel := context.WithCancel(workCtx)
	defer cancel()

	// hedgeBuffer.Add is async: issue every group's Add then collect
	// outcomes on a shared channel. One slot per group; sync.Once gates
	// legDone so each group sends at most once. The buffered capacity
	// ensures a late legDone (e.g. fired after we returned because the
	// batch resolved early) never blocks the deliverer.
	results := make(chan produceResult, len(groups))
	for agent, parts := range groups {
		var once sync.Once
		legDone := func(resp *kmsg.ProduceResponse, err error) {
			once.Do(func() { results <- produceResult{resp: resp, err: err} })
		}
		h.hedgeBuffer.Add(attemptCtx, newMultiRoutedTopicPartitionRecords(parts, agent, legDone))
	}

	for remaining := len(groups); remaining > 0; remaining-- {
		select {
		case res := <-results:
			acc.accumulate(res)
			if isDone, _ := acc.done(); isDone {
				// Either every partition resolved or a non-retriable
				// err aborted the batch — either way, no further wave
				// would change the outcome.
				return true, false
			}
		case <-attemptCtx.Done():
			return true, false
		}
	}
	return true, true
}

// shouldHedge returns the hedge delay (and whether to hedge) for primaryID.
func (h *Hedger) shouldHedge(now time.Time, primaryID int32, partitions []routedTopicPartitionRecords) (time.Duration, bool) {
	// Probe routing: trust the routing-time nodeState. Re-querying the
	// strategy would consume another probe slot through the Demoter.
	for _, p := range partitions {
		if p.nodeID == primaryID && p.nodeState == AgentStateDemoted {
			return 0, true
		}
	}

	primary, ok := h.tracker.AgentStats(now, primaryID)
	if !ok {
		h.metrics.hedgeAttemptsSuppressedTotal.WithLabelValues(hedgeSuppressedNoAgentStats).Inc()
		return 0, false
	}

	clusterStats, hasClusterStats := h.tracker.ClusterStats(now, h.health.SlowMultiplier, h.health.FaultyThreshold)
	if !hasClusterStats {
		h.metrics.hedgeAttemptsSuppressedTotal.WithLabelValues(hedgeSuppressedNoClusterStats).Inc()
		return 0, false
	}

	// Apply a scale-aware floor of 1/N to each fraction gate so a single
	// bad agent never trips suppression, regardless of cluster size. This
	// matters most in small clusters: with N=2 a single faulty agent gives
	// FaultyFraction=0.5, which would trip any reasonable configured
	// MaxFaultyFraction even though hedging to the only healthy agent is
	// exactly what we want. At large N the configured fraction dominates
	// (1/N is tiny) so this is a no-op in production-sized clusters.
	if clusterStats.SlowFraction > maxFractionFloor(h.health.MaxSlowFraction, clusterStats.SlowContributorsCount) {
		h.metrics.hedgeAttemptsSuppressedTotal.WithLabelValues(hedgeSuppressedSlowFraction).Inc()
		return 0, false
	}
	if clusterStats.FaultyFraction > maxFractionFloor(h.health.MaxFaultyFraction, clusterStats.FaultyContributorsCount) {
		h.metrics.hedgeAttemptsSuppressedTotal.WithLabelValues(hedgeSuppressedFaultyFraction).Inc()
		return 0, false
	}

	primarySlow := primary.Latency > clusterStats.SlowThreshold
	// Suppress error-rate noise on low-volume agents: with N requests the
	// smallest non-zero rate is 1/N, so a single error trips any threshold
	// finer than that. Require N >= ceil(1/FaultyThreshold) before the
	// faulty signal is trustworthy.
	primaryFaulty := primary.RequestCount >= errorRateMinRequests(h.health.FaultyThreshold) && primary.ErrorRate > clusterStats.FaultyThreshold

	baseline := clusterStats.BaselineLatency
	if !primarySlow && !primaryFaulty {
		// Healthy primary: hedge anyway but with a longer delay so we don't
		// stampede the cluster. SlowMultiplier represents "how much above
		// baseline counts as slow", so it's the natural ceiling — at that
		// point the primary would have been classified as slow anyway.
		baseline = time.Duration(float64(baseline) * h.health.SlowMultiplier)
	}

	return max(baseline, h.cfg.MinHedgeDelay), true
}

// hedgerProduceResult bundles a hedge cascade's result with its attempt
// depth (1 = resolved on the primary, N = resolved after N-1 hedge waves).
// It is the common return of the hedging functions and the payload the
// racing variant collects its fallback leg on.
type hedgerProduceResult struct {
	result   produceResult
	attempts int
}

// hedgerCandidates memoizes strategy.Candidates() per (topic, partition)
// for the duration of one Hedger.ProduceSync, so every wave sees the same
// list. Re-querying would re-consume a Demoter probe slot or surface a
// different set across waves. Not safe for concurrent use.
type hedgerCandidates struct {
	strategy       PartitionAssignmentStrategy
	maxHedgeAgents int
	cache          map[topicPartition][]Agent
}

func newHedgerCandidates(s PartitionAssignmentStrategy, maxHedgeAgents int) *hedgerCandidates {
	return &hedgerCandidates{
		strategy:       s,
		maxHedgeAgents: maxHedgeAgents,
		cache:          map[topicPartition][]Agent{},
	}
}

func (h *hedgerCandidates) fetch(tp topicPartition) []Agent {
	if cached, ok := h.cache[tp]; ok {
		return cached
	}
	list := h.strategy.Candidates(tp.topic, tp.partition, h.maxHedgeAgents)
	h.cache[tp] = list
	return list
}

// maxFractionFloor returns max(configured, 1/contributors) so a single bad
// agent never trips a fraction-based suppression gate. Returns configured
// alone when contributors <= 0 (no signal to scale against).
func maxFractionFloor(configured float64, contributors int64) float64 {
	if contributors <= 0 {
		return configured
	}
	return max(configured, 1.0/float64(contributors))
}
