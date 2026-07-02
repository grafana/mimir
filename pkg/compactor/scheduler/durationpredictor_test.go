// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"math"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
)

func dotStatic(w, f [predictorFeatures]float64) float64 {
	var s float64
	for i := 0; i < predictorFeatures; i++ {
		s += w[i] * f[i]
	}
	return s
}

func TestCompactionJobPredictor_WarmStart(t *testing.T) {
	p := newCompactionJobPredictor(mergeDefaultWeights)
	f := [predictorFeatures]float64{1, 3, 2, 4, 1}
	require.InDelta(t, dotStatic(mergeDefaultWeights, f), p.predict(f), 1e-9)
}

func TestCompactionJobPredictor_ConvergesToTruth(t *testing.T) {
	trueW := [predictorFeatures]float64{0.5, 2.0, 0.03, 0.01, 0.002}
	p := newCompactionJobPredictor(mergeDefaultWeights)

	genF := func(i int) [predictorFeatures]float64 {
		return [predictorFeatures]float64{
			1,
			float64(1+i%50) / 10,
			float64(1 + i%23),
			float64(1 + i%17),
			float64(i%13) / 10,
		}
	}
	for i := 0; i < 4000; i++ {
		f := genF(i)
		p.observe(f, dotStatic(trueW, f))
	}
	require.True(t, p.resolve())

	for i := 0; i < 50; i++ {
		f := genF(i*7 + 3)
		require.InEpsilon(t, dotStatic(trueW, f), p.predict(f), 0.05, "i=%d", i)
	}
}

func TestCompactionJobPredictor_TracksRegimeChange(t *testing.T) {
	p := newCompactionJobPredictor(mergeDefaultWeights)
	f := [predictorFeatures]float64{1, 5, 10, 3, 1}

	for i := 0; i < 2000; i++ {
		p.observe(f, 100)
	}
	require.True(t, p.resolve())
	require.InDelta(t, 100, p.predict(f), 5)

	for i := 0; i < 2000; i++ {
		p.observe(f, 300)
	}
	require.True(t, p.resolve())
	require.InDelta(t, 300, p.predict(f), 5)
}

func TestCompactionJobPredictor_RidgeKeepsSolveStable(t *testing.T) {
	p := newCompactionJobPredictor(mergeDefaultWeights)
	// A single, degenerate observation would make the normal equations singular without the ridge term.
	p.observe([predictorFeatures]float64{1, 0, 0, 0, 0}, 50)
	require.True(t, p.resolve())

	got := p.predict([predictorFeatures]float64{1, 2, 3, 4, 5})
	require.False(t, math.IsNaN(got))
	require.GreaterOrEqual(t, got, 0.0)
}

func TestCompactionJobPredictor_PredictClampsAtZero(t *testing.T) {
	p := newCompactionJobPredictor([predictorFeatures]float64{-100, 0, 0, 0, 0})
	require.Equal(t, 0.0, p.predict([predictorFeatures]float64{1, 0, 0, 0, 0}))
}

func TestCompactionJobPredictor_PendingAccumulator(t *testing.T) {
	p := newCompactionJobPredictor(mergeDefaultWeights)
	require.Equal(t, 0.0, p.estimatePending())

	j := NewTrackedCompactionJob("m", &CompactionJob{blocks: make([][]byte, 2)}, 1, 5e6, 3000, time.Now())
	p.recordPending(j)
	require.InDelta(t, p.predict(features(2, 5e6, 3000)), p.estimatePending(), 1e-9)

	p.recordUnpending(j, time.Now(), false) // not leased, so this only unpends (no learning)
	require.Equal(t, 0.0, p.estimatePending())
}

func TestPlanPredictor_PendingAccumulator(t *testing.T) {
	p := &planPredictor{}
	require.Equal(t, 0.0, p.estimatePending())
	p.recordPending(NewTrackedPlanJob(time.Now()))
	p.recordPending(NewTrackedPlanJob(time.Now()))
	require.Equal(t, 2*planJobPredictedSeconds, p.estimatePending())
	p.recordUnpending(NewTrackedPlanJob(time.Now()), time.Now(), false)
	require.Equal(t, planJobPredictedSeconds, p.estimatePending())
}

func TestPerJobTypePredictor_EstimateEqualsSumOfPredictions(t *testing.T) {
	p := newPerJobTypePredictor(clock.New())
	jobs := []*TrackedCompactionJob{
		NewTrackedCompactionJob("1", &CompactionJob{isSplit: false, blocks: make([][]byte, 3)}, 1, 4e6, 2000, time.Now()),
		NewTrackedCompactionJob("2", &CompactionJob{isSplit: true, blocks: make([][]byte, 5)}, 1, 9e6, 6000, time.Now()),
		NewTrackedCompactionJob("3", &CompactionJob{isSplit: false, blocks: make([][]byte, 1)}, 1, 1e6, 500, time.Now()),
	}

	var want float64
	for _, j := range jobs {
		p.RecordPending(j)
		f := features(len(j.value.blocks), j.totalBlockBytes, j.totalSeries)
		if j.value.isSplit {
			want += p.splits.predict(f)
		} else {
			want += p.merges.predict(f)
		}
	}
	p.RecordPending(NewTrackedPlanJob(time.Now()))
	p.RecordPending(NewTrackedPlanJob(time.Now()))
	want += 2 * planJobPredictedSeconds

	require.InDelta(t, want, p.Estimate(), 1e-9)

	// Removing every job leaves only the plan-job contribution, then nothing.
	for _, j := range jobs {
		p.RecordUnpending(j, false)
	}
	require.InDelta(t, 2*planJobPredictedSeconds, p.Estimate(), 1e-9)
}

func TestPerJobTypePredictor_PersistenceRoundTrip(t *testing.T) {
	mj := NewTrackedCompactionJob("m", &CompactionJob{blocks: make([][]byte, 3)}, 1, 4e6, 2000, time.Now())
	sj := NewTrackedCompactionJob("s", &CompactionJob{isSplit: true, blocks: make([][]byte, 5)}, 1, 9e6, 6000, time.Now())

	p1 := newPerJobTypePredictor(clock.New())
	fmj := features(len(mj.value.blocks), mj.totalBlockBytes, mj.totalSeries)
	fsj := features(len(sj.value.blocks), sj.totalBlockBytes, sj.totalSeries)
	for i := 0; i < 300; i++ {
		p1.merges.observe(fmj, float64(i%50+10))
		p1.splits.observe(fsj, float64(i%30+20))
	}
	require.True(t, p1.Resolve())

	p2 := newPerJobTypePredictor(clock.New())
	require.True(t, p2.LoadState(p1.MarshalState()))

	fm := features(3, 4e6, 2000)
	fs := features(5, 9e6, 6000)
	require.InDelta(t, p1.merges.estimateSum(fm), p2.merges.estimateSum(fm), 1e-9)
	require.InDelta(t, p1.splits.estimateSum(fs), p2.splits.estimateSum(fs), 1e-9)
}

func TestPerJobTypePredictor_LoadStateRejectsIncompatible(t *testing.T) {
	p := newPerJobTypePredictor(clock.New())
	require.False(t, p.LoadState(nil))
	require.False(t, p.LoadState(&compactorschedulerpb.StoredDurationPredictor{Version: durationPredictorSchema + 1, K: predictorFeatures}))
	require.False(t, p.LoadState(&compactorschedulerpb.StoredDurationPredictor{Version: durationPredictorSchema, K: predictorFeatures + 1}))
	// Correct version and k, but truncated slices.
	require.False(t, p.LoadState(&compactorschedulerpb.StoredDurationPredictor{Version: durationPredictorSchema, K: predictorFeatures}))
}

func TestTrackedCompactionJob_SerializeRoundTripSeries(t *testing.T) {
	j := NewTrackedCompactionJob("c", &CompactionJob{isSplit: true, blocks: [][]byte{{1, 2}}}, 3, 5e6, 7777, time.Unix(100, 0))
	data, err := j.Serialize()
	require.NoError(t, err)

	got, err := deserializeCompactionJob([]byte("c"), data)
	require.NoError(t, err)
	require.Equal(t, uint64(7777), got.totalSeries)
	require.Equal(t, uint64(5e6), got.totalBlockBytes)
	require.True(t, got.value.isSplit)
}

type fakeDurationModel struct {
	pending   []string
	unpending []string
	completed []string // jobs unpended with success == true
}

func (f *fakeDurationModel) RecordPending(j TrackedJob) { f.pending = append(f.pending, j.ID()) }
func (f *fakeDurationModel) RecordUnpending(j TrackedJob, success bool) {
	f.unpending = append(f.unpending, j.ID())
	if success {
		f.completed = append(f.completed, j.ID())
	}
}

// TestQueueMetrics_ForwardsToModel verifies queueMetrics forwards every job movement to the model,
// generically across job types. The model derives timing/learning from the job itself.
func TestQueueMetrics_ForwardsToModel(t *testing.T) {
	fm := &fakeDurationModel{}
	sm := newSchedulerMetrics(prometheus.NewPedanticRegistry())
	sm.model = fm
	q := sm.newTrackerMetricsForTenant("t").queue

	job := NewTrackedCompactionJob("c", &CompactionJob{isSplit: true, blocks: make([][]byte, 2)}, 1, 10, 20, time.Now())
	failed := NewTrackedCompactionJob("f", &CompactionJob{blocks: make([][]byte, 1)}, 1, 1, 1, time.Now())
	q.Pending(job)
	q.Complete(job, true)
	q.Pending(failed)
	q.Complete(failed, false) // failed/abandoned lease: unpended but not learned from
	q.DropPending(NewTrackedCompactionJob("d", &CompactionJob{blocks: make([][]byte, 1)}, 1, 1, 1, time.Now()))
	q.Complete(NewTrackedPlanJob(time.Now()), true)

	require.Equal(t, []string{"c", "f"}, fm.pending)
	require.Equal(t, []string{"c", "f", "d", "p"}, fm.unpending)
	require.Equal(t, []string{"c", "p"}, fm.completed)
}

// TestCompactionJobPredictor_LearnsDurationFromLeaseStart checks the run duration is measured from the
// lease start, ignoring later renewals that advance the job's status time.
func TestCompactionJobPredictor_LearnsDurationFromLeaseStart(t *testing.T) {
	p := newCompactionJobPredictor(mergeDefaultWeights)
	base := time.Unix(1000, 0)
	job := NewTrackedCompactionJob("c", &CompactionJob{blocks: make([][]byte, 3)}, 1, 4e6, 2000, base)
	job.MarkLeased(base)                      // lease start = base
	job.RenewLease(base.Add(4 * time.Minute)) // renewal must be ignored
	completedAt := base.Add(5 * time.Minute)  // 300s since lease start (60s since renewal)

	for i := 0; i < 500; i++ {
		p.recordUnpending(job, completedAt, true)
	}
	require.True(t, p.resolve())
	require.InDelta(t, 300, p.predict(features(3, 4e6, 2000)), 5)
}

// TestCompactionJobPredictor_DoesNotLearnFromFailures checks that unpending a leased job with
// success == false (a failed or abandoned lease) removes it from the pending accounting but does not
// fold its (typically lease-timeout-sized) duration into the model.
func TestCompactionJobPredictor_DoesNotLearnFromFailures(t *testing.T) {
	p := newCompactionJobPredictor(mergeDefaultWeights)
	f := features(3, 4e6, 2000)
	before := p.predict(f)

	base := time.Unix(1000, 0)
	job := NewTrackedCompactionJob("c", &CompactionJob{blocks: make([][]byte, 3)}, 1, 4e6, 2000, base)
	job.MarkLeased(base)
	completedAt := base.Add(10 * time.Minute) // a large lease-timeout-sized duration

	p.recordPending(job)
	for i := 0; i < 500; i++ {
		p.recordUnpending(job, completedAt, false)
		p.recordPending(job)
	}
	p.recordUnpending(job, completedAt, false)

	require.False(t, p.resolve()) // nothing was folded in, so there is nothing to re-solve
	require.Equal(t, before, p.predict(f))
	require.Equal(t, 0.0, p.estimatePending())
}
