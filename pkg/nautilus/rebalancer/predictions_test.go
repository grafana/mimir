// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/loadstats"
)

// TestPredictionStore_EmptyApplyIsNoOp covers the common cold-state
// case: a freshly-constructed store does nothing when applyTo is
// called.
func TestPredictionStore_EmptyApplyIsNoOp(t *testing.T) {
	var s predictionStore
	rates := map[int32]float64{1: 100, 2: 200}
	kept, dropped := s.applyTo(time.Now(), rates)
	assert.Equal(t, 0, kept)
	assert.Equal(t, 0, dropped)
	assert.Equal(t, map[int32]float64{1: 100, 2: 200}, rates)
}

// TestPredictionStore_FreshPredictionAddsFullRate confirms the
// boundary condition: at the instant a prediction is committed
// (elapsed = 0), the entire rate is added to the destination
// partition's load. This is the "no-lag-yet" state the rebalancer
// MUST believe in immediately after a move, otherwise the slicer
// repeats the same decision next round.
func TestPredictionStore_FreshPredictionAddsFullRate(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := predictionStore{
		preds: []ratePrediction{
			{pid: 5, rate: 1000.0, committedAt: now},
		},
	}
	rates := map[int32]float64{5: 200}
	kept, dropped := s.applyTo(now, rates)
	assert.Equal(t, 1, kept)
	assert.Equal(t, 0, dropped)
	assert.InDelta(t, 1200.0, rates[5], 1e-9, "fresh prediction should add full rate")
}

// TestPredictionStore_HalfLifeMatchesEWMA verifies the decay curve
// against the readcache's actual EWMA. After one half-life
// (alpha=0.1591, tick=15s → 60s), the EWMA at the destination has
// absorbed 50% of the moved load, so the prediction should still
// contribute the remaining 50%.
func TestPredictionStore_HalfLifeMatchesEWMA(t *testing.T) {
	commit := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Half-life: ln(2) / (-ln(1-alpha)/tick) ≈ 60s at alpha=0.1591.
	halfLife := time.Duration(math.Round(math.Ln2/(-math.Log(1-loadstats.Alpha)/loadstats.TickInterval.Seconds()))) * time.Second
	s := predictionStore{
		preds: []ratePrediction{
			{pid: 7, rate: 1000.0, committedAt: commit},
		},
	}
	rates := map[int32]float64{7: 0}
	kept, _ := s.applyTo(commit.Add(halfLife), rates)
	require.Equal(t, 1, kept)
	assert.InDelta(t, 500.0, rates[7], 5.0,
		"at one half-life the prediction should contribute ~50% of the original rate")
}

// TestPredictionStore_FullSettleSumIsConstant is the headline
// invariant of the design: the prediction's residual + the EWMA's
// observed rate should sum to the true rate at every point along
// the settle curve. We simulate the readcache-side EWMA growth and
// confirm the rebalancer's view (prediction + EWMA) tracks the true
// rate within tight bounds for the full ~20 minutes of settle.
func TestPredictionStore_FullSettleSumIsConstant(t *testing.T) {
	commit := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	const trueRate = 1000.0
	s := predictionStore{
		preds: []ratePrediction{
			{pid: 3, rate: trueRate, committedAt: commit},
		},
	}
	// Sample at 30s intervals up to 30 minutes after commit.
	for offset := time.Duration(0); offset <= 30*time.Minute; offset += 30 * time.Second {
		// "Actual" EWMA reading from the destination readcache.
		// EWMA at time t with true rate constant R: R * (1 - (1-alpha)^(t/tick))
		ewmaResidualFraction := math.Pow(1.0-loadstats.Alpha, offset.Seconds()/loadstats.TickInterval.Seconds())
		ewmaObserved := trueRate * (1.0 - ewmaResidualFraction)

		rates := map[int32]float64{3: ewmaObserved}
		// Make a copy of the store for each iteration so applyTo's
		// pruning of decayed predictions doesn't bleed across samples.
		sCopy := predictionStore{preds: append([]ratePrediction(nil), s.preds...)}
		sCopy.applyTo(commit.Add(offset), rates)

		// The sum (prediction residual + EWMA observation) should
		// equal the true rate. Once the prediction is dropped past
		// predictionFloor, rates[3] is just ewmaObserved which by
		// then is within (1 - predictionFloor)=95% of trueRate; we
		// scale the tolerance to predictionFloor*trueRate at that
		// point.
		expected := trueRate
		if ewmaResidualFraction < predictionFloor {
			expected = ewmaObserved
		}
		assert.InDeltaf(t, expected, rates[3], predictionFloor*trueRate,
			"at offset=%s the sum should equal the true rate", offset)
	}
}

// TestPredictionStore_DropsAfterFloor confirms that predictions
// older than the floor (~22 minutes at default config) are evicted
// from the store, bounding memory under sustained move volume.
func TestPredictionStore_DropsAfterFloor(t *testing.T) {
	commit := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := predictionStore{
		preds: []ratePrediction{
			{pid: 1, rate: 500.0, committedAt: commit},
			{pid: 2, rate: 500.0, committedAt: commit},
		},
	}
	// 30 minutes is comfortably past 4 half-lives (~20min) and so
	// past predictionFloor=0.05.
	rates := map[int32]float64{1: 500, 2: 500}
	kept, dropped := s.applyTo(commit.Add(30*time.Minute), rates)
	assert.Equal(t, 0, kept)
	assert.Equal(t, 2, dropped)
	assert.Equal(t, 0, s.len(), "store should compact dropped predictions")
}

// TestPredictionStore_MultipleMovesAccumulate verifies that several
// predictions for the same destination sum cleanly. This is the
// case the rebalancer hits routinely — Phase 3 often moves multiple
// ranges to the same coldest partition in one round.
func TestPredictionStore_MultipleMovesAccumulate(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := predictionStore{
		preds: []ratePrediction{
			{pid: 9, rate: 100.0, committedAt: now},
			{pid: 9, rate: 200.0, committedAt: now},
			{pid: 9, rate: 50.0, committedAt: now},
		},
	}
	rates := map[int32]float64{9: 0}
	s.applyTo(now, rates)
	assert.InDelta(t, 350.0, rates[9], 1e-9)
}

// TestPredictionStore_RecordOnlyMovesAndReassigns ensures that
// merge and split actions are not recorded — they don't shift
// inter-partition load, so predicting against them would add phantom
// load to the wrong partitions.
func TestPredictionStore_RecordOnlyMovesAndReassigns(t *testing.T) {
	hr := assignment.HashRange{Lo: 0, Hi: 100}
	lm := buildLoadMap([]rangeRate{
		{partitionID: 1, hr: hr, sampleRate: 500.0, series: 10},
	})
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var s predictionStore

	actions := []Action{
		{Kind: ActionMove, Range: hr, FromPart: 1, ToPart: 2},
		{Kind: ActionReassign, Range: hr, FromPart: 1, ToPart: 3},
		{Kind: ActionSplit, Range: hr, FromPart: 1, ToPart: 0},
		{Kind: ActionMerge, Range: hr, FromPart: 1, ToPart: 0},
	}
	s.record(now, actions, lm)
	assert.Equal(t, 2, s.len(), "split and merge must not be recorded")
}

// TestPredictionStore_RecordSkipsZeroRate guards against polluting
// the store with no-op predictions for cold ranges. The slicer can
// reassign a cold range (e.g., from an inactive partition); without
// this guard those would consume memory and CPU until they decayed
// past the floor for no benefit.
func TestPredictionStore_RecordSkipsZeroRate(t *testing.T) {
	hr := assignment.HashRange{Lo: 0, Hi: 100}
	lm := buildLoadMap([]rangeRate{
		{partitionID: 1, hr: hr, sampleRate: 0, series: 0},
	})
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var s predictionStore
	s.record(now, []Action{
		{Kind: ActionMove, Range: hr, FromPart: 1, ToPart: 2},
	}, lm)
	assert.Equal(t, 0, s.len())
}

// TestPredictionStore_RecordSkipsSelfMove guards a degenerate case
// the slicer's correctness layer should already prevent but that
// would cause double-counting if it slipped through.
func TestPredictionStore_RecordSkipsSelfMove(t *testing.T) {
	hr := assignment.HashRange{Lo: 0, Hi: 100}
	lm := buildLoadMap([]rangeRate{
		{partitionID: 1, hr: hr, sampleRate: 500, series: 10},
	})
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var s predictionStore
	s.record(now, []Action{
		{Kind: ActionMove, Range: hr, FromPart: 1, ToPart: 1},
	}, lm)
	assert.Equal(t, 0, s.len())
}

// TestPredictionStore_NilLoadMapIsNoOp covers the cold-start path
// where the rebalancer hasn't built a loadMap yet (e.g.,
// reconstructRound). record should silently no-op.
func TestPredictionStore_NilLoadMapIsNoOp(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var s predictionStore
	s.record(now, []Action{
		{Kind: ActionMove, Range: assignment.HashRange{Lo: 0, Hi: 100}, FromPart: 1, ToPart: 2},
	}, nil)
	assert.Equal(t, 0, s.len())
}

// TestPredictionStore_NegativeElapsedTimeIsHandled covers the
// defensive clock-skew path: if `now` is before committedAt (clock
// moved backwards) we treat the prediction as freshly committed and
// apply the full residual rather than NaN-ing out via math.Pow on
// a negative exponent argument.
func TestPredictionStore_NegativeElapsedTimeIsHandled(t *testing.T) {
	committedAt := time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	s := predictionStore{
		preds: []ratePrediction{{pid: 1, rate: 1000.0, committedAt: committedAt}},
	}
	rates := map[int32]float64{1: 0}
	earlier := committedAt.Add(-1 * time.Minute)
	kept, _ := s.applyTo(earlier, rates)
	assert.Equal(t, 1, kept)
	assert.InDelta(t, 1000.0, rates[1], 1e-9)
}
