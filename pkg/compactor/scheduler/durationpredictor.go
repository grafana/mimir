// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benbjohnson/clock"
	"gonum.org/v1/gonum/mat"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
)

// The duration predictor estimates how long a compaction job will take from metadata known up front
// (block count, total bytes, total series). It is fit online per compaction job type with forgetting
// least squares (accumulate sufficient statistics, solve periodically), warm-started from global
// defaults. See P3_GO_HANDOFF.md for the derivation. All durations here are in seconds.
const (
	predictorFeatures       = 5     // model dimension k
	predictorLambda         = 0.997 // forgetting factor (~333-job effective window)
	predictorRidge          = 1e-2  // diagonal regularizer, keeps the solve well-conditioned
	predictorWarmStrength   = 20    // warm-start confidence, in pseudo-jobs
	planJobPredictedSeconds = 1.0   // fixed estimate for a planning job
	durationPredictorSchema = 1     // bumped when the persisted layout or model changes
)

// Global cold-start default weights (scaled feature units, seconds), one vector per compaction job
// type. Derived by pooling all cells; see the handoff. Values are the handoff's millisecond defaults
// divided by 1000.
var (
	mergeDefaultWeights = [predictorFeatures]float64{0.395, 1.525, 0.026, 0.009, 0}
	splitDefaultWeights = [predictorFeatures]float64{0, 1.024, 0.046, 0, 0.008}
)

// features scales the raw job metadata into the model's feature vector.
func features(blocks int, bytes, series uint64) [predictorFeatures]float64 {
	b := float64(blocks)
	s := float64(series) / 1e3
	return [predictorFeatures]float64{
		1.0,
		b / 10.0,
		float64(bytes) / 1e6,
		s,
		s * math.Log1p(b),
	}
}

func dot(w, f *[predictorFeatures]float64) float64 {
	var s float64
	for i := 0; i < predictorFeatures; i++ {
		s += w[i] * f[i]
	}
	return s
}

func clampNonNeg(v float64) float64 {
	if v < 0 {
		return 0
	}
	return v
}

// jobPredictor accounts for the pending jobs of one job type and estimates their total drain time.
// Each implementation owns its own accumulator and model; the composing layer only routes and sums.
type jobPredictor interface {
	recordPending(j TrackedJob)
	// recordUnpending accounts for a job leaving the incomplete set (now is the current time). A job
	// that completed successfully under a lease (success is true and LeaseStartTime is non-zero) is a
	// training sample of duration now - leaseStart; any other job (dropped from pending, or a failed or
	// abandoned lease) just leaves the set without being learned from.
	recordUnpending(j TrackedJob, now time.Time, success bool)
	estimatePending() float64 // estimated drain time (seconds) for this type's pending jobs
}

// planPredictor is a trivial predictor for planning jobs: a hardcoded duration, no learning.
// It is its own type so it can grow a real model later without touching callers.
type planPredictor struct {
	pending atomic.Int64
}

func (p *planPredictor) recordPending(TrackedJob)                    { p.pending.Add(1) }
func (p *planPredictor) recordUnpending(TrackedJob, time.Time, bool) { p.pending.Add(-1) }
func (p *planPredictor) estimatePending() float64 {
	return float64(p.pending.Load()) * planJobPredictedSeconds
}

// compactionJobPredictor is the forgetting-least-squares model for one compaction job type.
// It keeps the sufficient statistics (a, b) under a mutex and publishes solved weights (w) via an
// atomic pointer so predict/estimateSum are lock-free.
type compactionJobPredictor struct {
	mu          sync.Mutex
	a           *mat.SymDense              // Σ xxᵀ with forgetting + warm-start seed
	b           *mat.VecDense              // Σ x·y with forgetting + warm-start seed
	dirty       bool                       // true when a/b changed since the last solve
	unpersisted bool                       // true when a/b changed since the last marshal
	pending     [predictorFeatures]float64 // Σ features of this type's incomplete jobs

	w atomic.Pointer[[predictorFeatures]float64]
}

func newCompactionJobPredictor(defaults [predictorFeatures]float64) *compactionJobPredictor {
	a := mat.NewSymDense(predictorFeatures, nil)
	b := mat.NewVecDense(predictorFeatures, nil)
	for i := 0; i < predictorFeatures; i++ {
		a.SetSym(i, i, predictorWarmStrength)          // A₀ = strength·I
		b.SetVec(i, predictorWarmStrength*defaults[i]) // b₀ = strength·w_default
	}
	p := &compactionJobPredictor{a: a, b: b}
	w := defaults
	p.w.Store(&w)
	return p
}

// estimateSum returns the estimate for a feature vector as clamp(wᵀ·x). Because the estimate is a dot
// product it works for both a single job's features and a sum of them (Σ predict(xⱼ) = wᵀ·(Σ xⱼ)),
// with the clamp applied to the aggregate.
func (p *compactionJobPredictor) estimateSum(f [predictorFeatures]float64) float64 {
	return clampNonNeg(dot(p.w.Load(), &f))
}

// predict estimates a single job's duration; it is estimateSum over that job's own features.
func (p *compactionJobPredictor) predict(f [predictorFeatures]float64) float64 {
	return p.estimateSum(f)
}

// observe folds one finished job into the running totals: A ← λ·A + x·xᵀ, b ← λ·b + y·x.
func (p *compactionJobPredictor) observe(f [predictorFeatures]float64, durSec float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.observeLocked(f, durSec)
}

func (p *compactionJobPredictor) observeLocked(f [predictorFeatures]float64, durSec float64) {
	x := mat.NewVecDense(predictorFeatures, f[:])
	p.a.ScaleSym(predictorLambda, p.a)
	p.a.SymRankOne(p.a, 1.0, x)
	p.b.ScaleVec(predictorLambda, p.b)
	p.b.AddScaledVec(p.b, durSec, x)
	p.dirty = true
	p.unpersisted = true
}

// resolve re-solves (A + ridge·I)·w = b and atomically publishes the new weights. It reports whether
// there was anything to solve (drives persistence). If factorization fails the last weights are kept.
func (p *compactionJobPredictor) resolve() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.dirty {
		return false
	}
	m := mat.NewSymDense(predictorFeatures, nil)
	m.CopySym(p.a)
	for i := 0; i < predictorFeatures; i++ {
		m.SetSym(i, i, m.At(i, i)+predictorRidge)
	}
	var chol mat.Cholesky
	if chol.Factorize(m) {
		var solved mat.VecDense
		if err := chol.SolveVecTo(&solved, p.b); err == nil {
			var w [predictorFeatures]float64
			for i := 0; i < predictorFeatures; i++ {
				w[i] = solved.AtVec(i)
			}
			p.w.Store(&w)
		}
	}
	p.dirty = false
	return true
}

// marshalState returns the raw (row-major) a matrix and b vector, and clears the unpersisted flag.
func (p *compactionJobPredictor) marshalState() (a, b []float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	a = make([]float64, predictorFeatures*predictorFeatures)
	b = make([]float64, predictorFeatures)
	for i := 0; i < predictorFeatures; i++ {
		for j := 0; j < predictorFeatures; j++ {
			a[i*predictorFeatures+j] = p.a.At(i, j)
		}
		b[i] = p.b.AtVec(i)
	}
	p.unpersisted = false
	return a, b
}

func (p *compactionJobPredictor) hasUnpersisted() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.unpersisted
}

// loadState replaces the sufficient statistics from persisted slices and marks a re-solve pending.
func (p *compactionJobPredictor) loadState(a, b []float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := 0; i < predictorFeatures; i++ {
		for j := i; j < predictorFeatures; j++ {
			p.a.SetSym(i, j, a[i*predictorFeatures+j])
		}
		p.b.SetVec(i, b[i])
	}
	p.dirty = true
}

// recordPending adds a job's features to the running sum of this type's incomplete jobs.
func (p *compactionJobPredictor) recordPending(j TrackedJob) {
	cj, ok := j.(*TrackedCompactionJob)
	if !ok {
		return
	}
	f := features(len(cj.value.blocks), cj.totalBlockBytes, cj.totalSeries)
	p.mu.Lock()
	for i := range f {
		p.pending[i] += f[i]
	}
	p.mu.Unlock()
}

// recordUnpending removes a job's features from the running sum and, if it completed successfully
// under a lease, folds its measured duration (now minus the lease start) into the model under the
// same lock. Failed or abandoned leases are not learned from: they would otherwise be folded in as
// completions of duration ~= the lease timeout, biasing the model upward.
func (p *compactionJobPredictor) recordUnpending(j TrackedJob, now time.Time, success bool) {
	cj, ok := j.(*TrackedCompactionJob)
	if !ok {
		return
	}
	f := features(len(cj.value.blocks), cj.totalBlockBytes, cj.totalSeries)
	p.mu.Lock()
	for i := range f {
		p.pending[i] -= f[i]
	}
	if success && !cj.LeaseStartTime().IsZero() {
		if dur := now.Sub(cj.LeaseStartTime()); dur > 0 {
			p.observeLocked(f, dur.Seconds())
		}
	}
	p.mu.Unlock()
}

// estimatePending returns the estimated drain time (seconds) for this type's accumulated pending jobs.
func (p *compactionJobPredictor) estimatePending() float64 {
	p.mu.Lock()
	sum := p.pending
	p.mu.Unlock()
	return p.estimateSum(sum)
}

// perJobTypePredictor is a thin routing layer over the per-job-type predictors. It holds no model
// state itself: it dispatches jobs to the right predictor and sums their contributions. It owns the
// clock used to time completions.
type perJobTypePredictor struct {
	clock  clock.Clock
	plans  *planPredictor
	merges *compactionJobPredictor
	splits *compactionJobPredictor
}

func newPerJobTypePredictor(clk clock.Clock) *perJobTypePredictor {
	return &perJobTypePredictor{
		clock:  clk,
		plans:  &planPredictor{},
		merges: newCompactionJobPredictor(mergeDefaultWeights),
		splits: newCompactionJobPredictor(splitDefaultWeights),
	}
}

func (p *perJobTypePredictor) route(j TrackedJob) jobPredictor {
	cj, ok := j.(*TrackedCompactionJob)
	switch {
	case !ok:
		return p.plans
	case cj.value.isSplit:
		return p.splits
	default:
		return p.merges
	}
}

// RecordPending accounts for a job entering the incomplete (pending or active) set.
func (p *perJobTypePredictor) RecordPending(j TrackedJob) { p.route(j).recordPending(j) }

// RecordUnpending accounts for a job leaving the incomplete set. A job that completed successfully
// under a lease is learned from (duration = now - its lease start); any other job (a pending drop, or
// a failed or abandoned lease) just leaves the pending set.
func (p *perJobTypePredictor) RecordUnpending(j TrackedJob, success bool) {
	p.route(j).recordUnpending(j, p.clock.Now(), success)
}

// Resolve re-solves both compaction models and reports whether either actually re-solved.
func (p *perJobTypePredictor) Resolve() bool {
	r := p.merges.resolve()
	return p.splits.resolve() || r
}

// Estimate returns the estimated drain time in seconds for all currently pending jobs.
func (p *perJobTypePredictor) Estimate() float64 {
	return p.plans.estimatePending() + p.merges.estimatePending() + p.splits.estimatePending()
}

// MarshalStateIfChanged snapshots the learned state for persistence, or returns nil if nothing has
// been learned since the last marshal (so periodic callers can skip writing unchanged state).
func (p *perJobTypePredictor) MarshalStateIfChanged() *compactorschedulerpb.StoredDurationPredictor {
	if !p.merges.hasUnpersisted() && !p.splits.hasUnpersisted() {
		return nil
	}
	return p.MarshalState()
}

// MarshalState snapshots the learned state for persistence.
func (p *perJobTypePredictor) MarshalState() *compactorschedulerpb.StoredDurationPredictor {
	ma, mb := p.merges.marshalState()
	sa, sb := p.splits.marshalState()
	return &compactorschedulerpb.StoredDurationPredictor{
		Version: durationPredictorSchema,
		K:       predictorFeatures,
		MergeA:  ma,
		MergeB:  mb,
		SplitA:  sa,
		SplitB:  sb,
	}
}

// LoadState restores learned state produced by a compatible schema. On any mismatch it reports false
// and leaves the warm-started defaults in place, so changing the model later is safe.
func (p *perJobTypePredictor) LoadState(s *compactorschedulerpb.StoredDurationPredictor) bool {
	if s == nil || s.Version != durationPredictorSchema || s.K != predictorFeatures {
		return false
	}
	if len(s.MergeA) != predictorFeatures*predictorFeatures || len(s.MergeB) != predictorFeatures ||
		len(s.SplitA) != predictorFeatures*predictorFeatures || len(s.SplitB) != predictorFeatures {
		return false
	}
	p.merges.loadState(s.MergeA, s.MergeB)
	p.splits.loadState(s.SplitA, s.SplitB)
	p.merges.resolve()
	p.splits.resolve()
	return true
}

var (
	_ jobPredictor = (*planPredictor)(nil)
	_ jobPredictor = (*compactionJobPredictor)(nil)
)
