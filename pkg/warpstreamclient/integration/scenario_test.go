// SPDX-License-Identifier: AGPL-3.0-only

package integration

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// runScenario runs the same schedule against both clients with the user-
// supplied behaviors, asserts only the WarpstreamClient's outcome, and
// emits the full comparison report via a single t.Log call.
//
// Each scenario goes through two phases:
//
//  1. Warmup (scenarioWarmupDuration) — every agent runs at healthy
//     latency regardless of the scenario, so the WarpstreamClient stats
//     tracker has enough filled buckets to qualify its baseline. The
//     observations from this phase are intentionally discarded.
//
//  2. Observed (scenarioObservedDuration) — the scenario behaviors are
//     swapped in atomically and the second schedule drives the comparison.
//     Per-event app latency, per-partition latency, and the WarpstreamClient
//     produce-attempt count are recorded and reported.
func runScenario(t *testing.T, name string, behaviors map[int32]brokerBehavior) summary {
	t.Helper()
	skipIfNotIntegration(t)
	t.Parallel()

	report := &reportBuilder{}
	defer func() { t.Log(report.string()) }()

	h := newScenarioHarness(t, healthyBehaviors())

	totalBudget := scenarioWarmupDuration + scenarioObservedDuration + 30*time.Second
	ctx, cancel := context.WithTimeout(context.Background(), totalBudget)
	t.Cleanup(cancel)

	runWarmup(ctx, h)

	// Sample the Hedger's primary/hedge wire-request counters at exact
	// 10s bucket boundaries. Sampling is anchored at observedStart so the
	// per-bucket deltas correspond to the same windows as the per-event
	// observation buckets in the report.
	const reportBucket = 10 * time.Second
	observedStart := time.Now()
	sampler := startCounterSampler(observedStart, reportBucket, h.readWSProduceCounters)
	wsApp, wsPart, kgoApp, kgoPart := runObserved(ctx, h, behaviors)
	produceSamples := sampler.stop()
	produceDeltas := bucketDeltas(produceSamples)

	wsSummary := summarize(wsApp.snapshot())
	kgoSummary := summarize(kgoApp.snapshot())
	report.writeSummary(name, wsSummary, kgoSummary)
	report.writef("per-partition (warpstream): %s\n", formatPartitionSummary(summarize(wsPart.snapshot())))
	report.writef("per-partition (kgo):        %s\n", formatPartitionSummary(summarize(kgoPart.snapshot())))
	report.writeTimeBucketedTable(wsApp.snapshot(), produceDeltas, kgoApp.snapshot(), reportBucket)

	// Overall surge: counter deltas across the observed window.
	var totalPrimary, totalHedge int64
	if len(produceSamples) >= 2 {
		first, last := produceSamples[0], produceSamples[len(produceSamples)-1]
		totalPrimary = last.primary - first.primary
		totalHedge = last.hedge - first.hedge
	}
	if totalPrimary > 0 {
		report.writef("warpstream hedge surge: %d primary + %d hedge wire requests (%.1f%% extra)\n",
			totalPrimary, totalHedge, 100*float64(totalHedge)/float64(totalPrimary))
	}
	if wsSummary.failures > 0 {
		report.writef("warpstream errors:\n  %s\n", strings.Join(errorBreakdown(wsApp.snapshot()), "\n  "))
	}
	if kgoSummary.failures > 0 {
		report.writef("kgo errors:\n  %s\n", strings.Join(errorBreakdown(kgoApp.snapshot()), "\n  "))
	}
	return wsSummary
}

// runWarmup drives a healthy schedule against both clients, discarding the
// observations. Its only purpose is to feed the WarpstreamClient stats
// tracker enough data to qualify a cluster baseline before the scenario
// behaviors are activated.
func runWarmup(ctx context.Context, h *scenarioHarness) {
	events := buildSchedule(h.topic, h.numPartitions, scenarioWarmupDuration, scenarioEventSpacing, "warmup")
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); runSchedule(ctx, h.wsClient, events) }()
	go func() { defer wg.Done(); runSchedule(ctx, h.kgoClient, events) }()
	wg.Wait()
}

// runObserved swaps in the scenario behaviors and drives the observed
// schedule against both clients. The harness's produce counter is already
// recording; the caller filters its snapshot by issue time to isolate the
// observed phase.
func runObserved(ctx context.Context, h *scenarioHarness, behaviors map[int32]brokerBehavior) (wsApp, wsPart, kgoApp, kgoPart *observations) {
	h.behaviors.swap(behaviors)
	events := buildSchedule(h.topic, h.numPartitions, scenarioObservedDuration, scenarioEventSpacing, "run")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); wsApp, wsPart = runSchedule(ctx, h.wsClient, events) }()
	go func() { defer wg.Done(); kgoApp, kgoPart = runSchedule(ctx, h.kgoClient, events) }()
	wg.Wait()
	return wsApp, wsPart, kgoApp, kgoPart
}

// assertHighSuccessRate fails the test if the WarpstreamClient delivered
// fewer than threshold of the records.
func assertHighSuccessRate(t *testing.T, ws summary, threshold float64) {
	t.Helper()
	if ws.total == 0 {
		t.Fatal("no records were produced")
	}
	got := float64(ws.successes) / float64(ws.total)
	assert.GreaterOrEqualf(t, got, threshold, "warpstream success rate %.4f below threshold %.4f", got, threshold)
}
