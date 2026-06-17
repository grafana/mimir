// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"math"
	"time"

	"github.com/grafana/mimir/pkg/nautilus/loadstats"
)

// ratePrediction records that the slicer moved (or reassigned) a hash
// range with a known sample rate to a destination partition at a
// specific wall-clock moment. Until the destination readcache's
// per-partition EWMA has had time to accumulate that load (~5 minutes
// for one half-life), the rebalancer's view of the destination's
// load is systematically understated. predictions let the rebalancer
// compensate by adding the unobserved-but-expected load to the
// partition-level rate.
//
// See predictionStore for the full design rationale and the math
// connecting decay to the readcache's EWMA settle curve.
type ratePrediction struct {
	// pid is the destination partition that just gained the moved
	// range. We don't track the source partition because
	// filterRatesByCurrentOwnership already removes the source's
	// stale reading instantly (the source side has zero readcache
	// lag).
	pid int32

	// rate is the moved range's per-second sample rate as observed
	// at the source just before the move, in the units the slicer
	// uses (loadMap.sampleRateAt). This is the slicer's best
	// estimate of what the destination's load for that range will
	// settle at.
	rate float64

	// committedAt is the wall-clock time the move was persisted to
	// the assignment log (the `now` of the rebalance round that
	// produced the action). Combined with the EWMA half-life, it
	// drives how much of the prediction is still unobserved by the
	// destination readcache's reported rate.
	committedAt time.Time
}

// predictionStore holds the outstanding ratePredictions and exposes
// the two operations the rebalancer needs:
//
//   - record: append predictions produced by a round of slicer
//     actions (typically called immediately after a successful
//     Apply).
//   - applyTo: at the start of the next round, fold the still-
//     unobserved portion of every prediction into the
//     partitionRateByPID map the slicer will see as its input.
//
// The decay model matches the readcache's EWMA exactly: at commit
// time the destination's reported rate is 0 (it doesn't yet own the
// range) and grows toward the true rate as the EWMA absorbs samples.
// The portion of the prediction that the EWMA has NOT yet observed
// at wall-clock t is rate * (1 - alpha)^((t - committedAt) / tick).
// At t == committedAt that's the full rate; at t == committedAt +
// half-life that's rate/2; at t == committedAt + 3 half-lives that's
// ~12%. We add exactly that residual to the partition rate the
// slicer sees, so the sum (residual + actual EWMA reading) equals
// the predicted-true rate at every point along the settle curve.
//
// Predictions are dropped once their residual decays below
// predictionFloor (5% of the original rate, ~4 half-lives), past
// which their contribution is dwarfed by EWMA estimation noise and
// not worth carrying.
//
// The store is NOT goroutine-safe: rebalance() runs single-threaded
// from running(), and that's the only caller.
type predictionStore struct {
	preds []ratePrediction
}

// predictionFloor is the decay fraction below which we discard a
// prediction. At alpha=0.1591 and tick=15s, decay = 0.05 happens at
// ~4.3 minutes after commit (just over 4 half-lives), which is
// plenty of time for the destination EWMA to have settled to within
// ~5% of the true rate.
const predictionFloor = 0.05

// record adds a prediction for each "move" or "reassign" action,
// using the per-(source, range) sample rate looked up from lm. We
// skip merges and splits because they don't change inter-partition
// load distribution (a merge/split keeps the affected hash space on
// the same partition).
//
// Callers MUST only invoke record after the round's Apply call
// succeeded; recording before Apply would predict load shifts that
// never made it to the log and so never made it to distributors.
func (s *predictionStore) record(now time.Time, actions []Action, lm *loadMap) {
	if lm == nil {
		return
	}
	for _, a := range actions {
		if a.Kind != ActionMove && a.Kind != ActionReassign {
			continue
		}
		if a.FromPart == a.ToPart {
			continue
		}
		rate := lm.sampleRateAt(a.FromPart, a.Range)
		if rate <= 0 {
			// Nothing to predict for cold ranges. (Also covers the
			// case where lm has no entry for (FromPart, Range)
			// because the readcache's reporting was incomplete.)
			continue
		}
		s.preds = append(s.preds, ratePrediction{
			pid:         a.ToPart,
			rate:        rate,
			committedAt: now,
		})
	}
}

// applyTo folds every still-significant prediction into
// partitionRateByPID and drops the ones that have decayed past
// predictionFloor. Returns the number of predictions kept and the
// number dropped, for logging.
//
// Idempotency: applyTo mutates the prediction store (dropping decayed
// entries) so calling it twice for the same `now` would double-count
// nothing — the second call simply sees the (already-compacted) set.
// But callers must NOT call applyTo twice with the same `now` and
// the same partitionRateByPID, because the second call would add
// the (still-not-decayed) predictions a second time. The rebalancer
// calls it exactly once per round, between collecting rates and
// invoking runSlicer.
func (s *predictionStore) applyTo(now time.Time, partitionRateByPID map[int32]float64) (kept, dropped int) {
	if len(s.preds) == 0 {
		return 0, 0
	}
	// Decay factor per second is (1 - alpha) ^ (1 / tick_seconds).
	// We compute decay per-prediction below using elapsed seconds
	// directly, which is equivalent and avoids accumulating
	// rounding error.
	tickSeconds := loadstats.TickInterval.Seconds()
	out := s.preds[:0]
	for _, p := range s.preds {
		elapsed := now.Sub(p.committedAt).Seconds()
		if elapsed < 0 {
			// Defensive: clock went backwards. Treat as committed
			// at `now` and apply the full residual.
			elapsed = 0
		}
		decay := math.Pow(1.0-loadstats.Alpha, elapsed/tickSeconds)
		if decay < predictionFloor {
			dropped++
			continue
		}
		partitionRateByPID[p.pid] += p.rate * decay
		out = append(out, p)
		kept++
	}
	s.preds = out
	return kept, dropped
}

// len reports the number of currently-tracked predictions, useful
// for metrics and bound-checking.
func (s *predictionStore) len() int { return len(s.preds) }
