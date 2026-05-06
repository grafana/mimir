// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// HedgerConfig holds the dynamic hedging policy parameters.
type HedgerConfig struct {
	// SlowMultiplier marks the primary as "slow" if its window-average
	// latency exceeds the cluster baseline by this factor.
	SlowMultiplier float64

	// MaxSlowFraction suppresses hedging when this fraction (or more) of
	// agents is also slow — a widespread latency issue would be made
	// worse by amplifying load.
	MaxSlowFraction float64

	// FaultyThreshold marks the primary as "faulty" if its error rate
	// exceeds this absolute fraction (in [0,1]).
	FaultyThreshold float64

	// MaxFaultyFraction suppresses hedging when this fraction (or more) of
	// agents is also faulty — a widespread error-rate issue would be made
	// worse by amplifying load.
	MaxFaultyFraction float64

	// MinHedgeDelay is the floor on the dynamically-computed hedge delay.
	// The delay is otherwise the cluster baseline latency.
	MinHedgeDelay time.Duration
}

// Hedger is the layer that turns Warpstream's stateless-agent property
// into actual reliability for produce traffic. In vanilla Kafka the same
// ProduceRequest cannot be sent to two brokers — a Produce must go to the
// partition leader, period — so a slow leader stalls the request until
// either retries kick in or the timeout fires. Warpstream lets us race a
// slow primary against a *different* agent because every agent can serve
// every partition. This component owns that race.
//
// The decision to actually hedge is dynamic, not unconditional. Issuing a
// duplicate request always costs cluster CPU and bandwidth; doing it on
// every request would multiply load by 2× even when the cluster is healthy.
// The hedger consults the AgentStatsTracker for two signals:
//
//   - Per-agent latency vs. cluster baseline (SlowMultiplier).
//   - Per-agent error rate vs. configured floor (FaultyThreshold).
//
// The hedge fires only when the *primary* looks worse than the cluster on
// at least one axis. It is then suppressed when *too many* agents look bad
// — a cluster-wide latency or error spike means hedging would amplify the
// pain rather than route around a single bad agent. This is the difference
// between "one node is sick, route around it" and "everyone is sick, don't
// make it worse" and is the whole reason this lives in a dedicated layer
// instead of being a fixed-delay hedge timer.
//
// There are two paths into the secondary leg:
//
//   - The hedge timer expires first → fan out to per-partition secondaries
//     (computed via PartitionAssignmentStrategy). The first leg to fully
//     succeed wins; the loser's response is discarded.
//   - The primary fails (with or without a hedge having fired) → fall
//     through to the same fan-out as a retry. The Hedger therefore doubles
//     as a full-request retry layer.
//
// The fan-out is all-or-nothing per Warpstream's request semantics: a
// single Produce request to one agent either succeeds entirely or fails
// entirely, so a sub-request error fails the whole fan-out leg. The
// component records hedgeAttemptsTotal / hedgeWinsTotal so an operator can
// tell at a glance whether hedging is helping or just adding load.
//
// Hedger does not record per-agent stats itself; the inner DirectProducer
// must already be wrapped by a TrackingProducer so every leg, including
// the discarded ones, contributes to the rolling latency/error window.
type Hedger struct {
	inner    DirectProducer
	tracker  AgentStatsTracker
	strategy PartitionAssignmentStrategy
	cfg      HedgerConfig
	metrics  *metrics
}

// NewHedger wraps inner with dynamic hedging. tracker is read-only here —
// per-leg observations must already be recorded by a TrackingProducer
// inside inner.
func NewHedger(inner DirectProducer, tracker AgentStatsTracker, strategy PartitionAssignmentStrategy, cfg HedgerConfig, m *metrics) *Hedger {
	return &Hedger{
		inner:    inner,
		tracker:  tracker,
		strategy: strategy,
		cfg:      cfg,
		metrics:  m,
	}
}

// Produce sends req to primaryID, optionally racing a per-partition fan-out
// when the primary looks slow, and falling back to the same fan-out as a
// retry on primary failure. Implements DirectProducer.
func (h *Hedger) Produce(ctx context.Context, primaryID int32, req *kmsg.ProduceRequest) (*kmsg.ProduceResponse, error) {
	delay, hedge := h.shouldHedge(time.Now(), primaryID)

	// The primary leg may outlive this function (e.g. when the fanout wins
	// the race); cancellation is the caller's ctx. The buffered channel
	// guarantees the goroutine doesn't block on send when its result is no
	// longer awaited.
	primaryCh := make(chan directProduceResult, 1)
	go func() {
		resp, err := h.inner.Produce(ctx, primaryID, req)
		primaryCh <- directProduceResult{resp: resp, err: err}
	}()

	if hedge {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case primary := <-primaryCh:
			if primary.err == nil {
				return primary.resp, nil
			}
			// Primary failed before the hedge timer — fall through to fanout retry.
			return h.fanoutToSecondaryAgents(ctx, req, bufferedPrimary(primary))
		case <-timer.C:
			return h.fanoutToSecondaryAgents(ctx, req, primaryCh)
		}
	}

	primary := <-primaryCh
	if primary.err == nil {
		return primary.resp, nil
	}
	return h.fanoutToSecondaryAgents(ctx, req, bufferedPrimary(primary))
}

// shouldHedge returns the hedge delay (and whether to hedge) for primaryID.
// Suppresses when stats are missing, when the primary is healthy, or when
// the cluster as a whole is unhealthy — see the Hedger doc for the why.
func (h *Hedger) shouldHedge(now time.Time, primaryID int32) (time.Duration, bool) {
	primary, ok := h.tracker.AgentStats(now, primaryID)
	if !ok {
		return 0, false
	}

	cluster, ok := h.tracker.ClusterStats(now, h.cfg.SlowMultiplier, h.cfg.FaultyThreshold)
	if !ok {
		return 0, false
	}

	primarySlow := primary.Latency > cluster.SlowThreshold
	// Suppress error-rate noise on low-volume agents: with N requests the
	// smallest non-zero rate is 1/N, so a single error trips any threshold
	// finer than that. Require N >= ceil(1/FaultyThreshold) before the
	// faulty signal is trustworthy.
	primaryFaulty := primary.RequestCount >= errorRateMinRequests(h.cfg.FaultyThreshold) &&
		primary.ErrorRate > cluster.FaultyThreshold
	if !primarySlow && !primaryFaulty {
		return 0, false
	}

	// Apply a scale-aware floor of 1/N to each fraction gate so a single
	// bad agent never trips suppression, regardless of cluster size. This
	// matters most in small clusters: with N=2 a single faulty agent gives
	// FaultyFraction=0.5, which would trip any reasonable configured
	// MaxFaultyFraction even though hedging to the only healthy agent is
	// exactly what we want. At large N the configured fraction dominates
	// (1/N is tiny) so this is a no-op in production-sized clusters.
	if cluster.SlowFraction > maxFractionFloor(h.cfg.MaxSlowFraction, cluster.SlowContributorsCount) {
		return 0, false
	}
	if cluster.FaultyFraction > maxFractionFloor(h.cfg.MaxFaultyFraction, cluster.FaultyContributorsCount) {
		return 0, false
	}

	return max(cluster.BaselineLatency, h.cfg.MinHedgeDelay), true
}

// fanoutToSecondaryAgents splits req across per-partition secondaries and
// races the fan-out against primaryCh; first full success wins. All-or-
// nothing per leg. If req can't be split (no secondary for some partition)
// the primary's outcome is returned alone. On both-fail the primary error
// is returned and the fan-out error is discarded.
func (h *Hedger) fanoutToSecondaryAgents(ctx context.Context, req *kmsg.ProduceRequest, primaryCh <-chan directProduceResult) (*kmsg.ProduceResponse, error) {
	// Split the request to per-secondary agent requests.
	secondaryReqs, err := splitProduceRequestToSecondaryAgents(req, h.strategy)
	if err != nil {
		// No fanout possible; just wait for the primary.
		primary := <-primaryCh
		return primary.resp, primary.err
	}

	h.metrics.hedgeAttemptsTotal.Inc()

	// Fan out the requests to secondary agents.
	secondaryCh := make(chan directProduceResult, len(secondaryReqs))
	for secondaryID, secondaryReq := range secondaryReqs {
		go func(secondaryID int32, secondaryReq *kmsg.ProduceRequest) {
			resp, err := h.inner.Produce(ctx, secondaryID, secondaryReq)
			secondaryCh <- directProduceResult{resp: resp, err: err}
		}(secondaryID, secondaryReq)
	}

	var (
		primaryErr     error
		secondaryErr   error
		secondaryResps = make([]*kmsg.ProduceResponse, 0, len(secondaryReqs))
	)

	// Iterate until BOTH primary and secondary have errored: any success on
	// either leg returns inline, so the only reason the loop falls through to
	// the bottom is that there is nothing left to wait for.
	for primaryErr == nil || secondaryErr == nil {
		select {
		case primaryRes := <-primaryCh:
			if primaryRes.err == nil {
				return primaryRes.resp, nil
			}
			primaryErr = primaryRes.err
		case secondaryRes := <-secondaryCh:
			if secondaryRes.err != nil {
				secondaryErr = secondaryRes.err
				continue
			}
			secondaryResps = append(secondaryResps, secondaryRes.resp)
			if len(secondaryResps) == len(secondaryReqs) {
				h.metrics.hedgeWinsTotal.Inc()
				return mergeProduceResponses(secondaryResps), nil
			}
		}
	}
	return nil, primaryErr
}

// directProduceResult carries the outcome of a single inner.Produce call
// through the channels the orchestration uses internally. It serves both
// "primary leg" and "fanout sub-leg / aggregate" outcomes — they all carry
// the same data shape (response + error).
type directProduceResult struct {
	resp *kmsg.ProduceResponse
	err  error
}

// bufferedPrimary returns a 1-buffered channel preloaded with primary so it
// can be passed to fanoutToSecondaryAgents in cases where the primary leg has
// already completed before the fanout was dispatched.
func bufferedPrimary(primary directProduceResult) <-chan directProduceResult {
	ch := make(chan directProduceResult, 1)
	ch <- primary
	return ch
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
