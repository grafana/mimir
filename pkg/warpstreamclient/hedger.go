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

// Hedger wraps another DirectProducer with dynamic latency-aware hedging
// and per-partition fallback. It implements DirectProducer itself, so it
// composes with other DirectProducer decorators.
//
// Hedger has two distinct triggers for the secondary leg:
//   - Hedge timer: when the latency tracker reports the primary is slow
//     relative to the cluster baseline, a fanout to per-partition
//     secondaries is launched after MinHedgeDelay (or the baseline,
//     whichever is larger). The first leg to fully succeed wins.
//   - Primary failure: any error returned by the primary (with or without
//     a hedge having been triggered) falls through to per-partition fanout
//     as a retry. The Hedger therefore acts as both a hedge layer and a
//     full-request retry layer in one component.
//
// The fanout is treated as all-or-nothing: a sub-request that errors fails
// the whole fanout. This matches Warpstream's semantics, where a single
// Produce request to one agent either succeeds entirely or fails
// entirely; it would be unusual for one partition in a request to succeed
// while another in the same request fails for an agent-level reason.
//
// Hedger does not record per-agent latency or error stats itself: the
// inner DirectProducer is expected to be wrapped by a TrackingProducer
// (or equivalent) so every leg — including legs whose results the Hedger
// races and discards — is observed exactly once.
type Hedger struct {
	inner    DirectProducer
	tracker  AgentStatsTracker
	strategy PartitionAssignmentStrategy
	cfg      HedgerConfig
	metrics  *metrics
}

// NewHedger returns a Hedger that adds dynamic hedging on top of inner.
// tracker is used only to read AgentStats/ClusterStats for the hedge
// decision; per-leg observations must be recorded by a tracking decorator
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

// Produce sends req to primaryID via inner.Produce, optionally hedging to
// per-partition secondaries when the latency tracker indicates the primary
// is slow. On primary failure (with or without hedging), unconditionally
// retries via per-partition fanout.
//
// Implements DirectProducer.
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

// shouldHedge consults the stats tracker and returns the hedge delay (and
// whether to hedge at all) for this primary. Hedging is suppressed when
// either query lacks reliable data (no per-agent signal for the primary
// or no quorum of qualifying agents in the cluster), when the primary is
// healthy on both axes, or when the cluster has a widespread slow- or
// faulty-fraction issue.
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
// races the resulting fanout against the primary leg supplied via
// primaryCh; the first leg to fully succeed wins. The fanout is
// all-or-nothing: any sub-request error fails the whole fanout. If both
// legs fail the primary's error is returned and the fanout error is
// discarded. If the request cannot be split (some partition has no
// secondary) the fanout never runs and the primary's outcome is returned
// alone.
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
