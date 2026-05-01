// SPDX-License-Identifier: AGPL-3.0-only

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

// counterSample is one snapshot of (primary, hedge) Produce request counts
// at a known wall-clock time.
type counterSample struct {
	at      time.Time
	primary int64
	hedge   int64
}

// counterSampler captures (primary, hedge) snapshots at exact bucket
// boundaries. Per-bucket deltas are computed from consecutive snapshots so
// the boundaries are aligned regardless of ticker jitter. The samples
// always include:
//
//   - t = start (the initial sample, before the observed phase begins)
//   - t = start + k * bucketSize, for k = 1, 2, … until stop is called
//   - t = stop time (a final snapshot at whatever wall-clock stop runs at)
//
// The "exact at boundary" guarantee comes from sleeping to absolute target
// times instead of using a periodic ticker.
type counterSampler struct {
	bucketSize time.Duration
	read       func() (primary, hedge int64)

	mu        sync.Mutex
	start     time.Time
	snapshots []counterSample

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// startCounterSampler takes the initial sample synchronously at start and
// then schedules background samples at start + N*bucketSize.
func startCounterSampler(start time.Time, bucketSize time.Duration, read func() (primary, hedge int64)) *counterSampler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &counterSampler{
		bucketSize: bucketSize,
		read:       read,
		start:      start,
		cancel:     cancel,
	}
	s.record(start)
	s.wg.Add(1)
	go s.run(ctx)
	return s
}

func (s *counterSampler) record(at time.Time) {
	p, h := s.read()
	s.mu.Lock()
	s.snapshots = append(s.snapshots, counterSample{at: at, primary: p, hedge: h})
	s.mu.Unlock()
}

func (s *counterSampler) run(ctx context.Context) {
	defer s.wg.Done()
	for n := int64(1); ; n++ {
		target := s.start.Add(time.Duration(n) * s.bucketSize)
		wait := time.Until(target)
		if wait <= 0 {
			// Target already passed (test paused / slow scheduler). Record
			// immediately to keep the bucket sequence dense, then continue
			// to the next target.
			s.record(target)
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		s.record(target)
	}
}

// stop cancels the background goroutine and records one final snapshot at
// the current wall-clock time. The returned snapshots are a defensive
// copy. Safe to call exactly once.
func (s *counterSampler) stop() []counterSample {
	s.cancel()
	s.wg.Wait()
	s.record(time.Now())
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]counterSample, len(s.snapshots))
	copy(out, s.snapshots)
	return out
}

// counterBucketDelta is one bucket's primary + hedge wire-request totals.
type counterBucketDelta struct {
	primary, hedge int64
}

// bucketDeltas turns the consecutive (sample[i], sample[i+1]) pairs into
// per-bucket primary/hedge deltas. The final pair (anchored at stop time
// rather than a bucket boundary) is included so trailing wire requests
// during scenario drain are accounted for.
func bucketDeltas(samples []counterSample) []counterBucketDelta {
	if len(samples) < 2 {
		return nil
	}
	out := make([]counterBucketDelta, 0, len(samples)-1)
	for i := 1; i < len(samples); i++ {
		out = append(out, counterBucketDelta{
			primary: samples[i].primary - samples[i-1].primary,
			hedge:   samples[i].hedge - samples[i-1].hedge,
		})
	}
	return out
}

func TestCounterSampler_RecordsInitialAndBucketBoundaries(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewPedanticRegistry()
	prim := prometheus.NewCounter(prometheus.CounterOpts{Name: "p_total"})
	hedg := prometheus.NewCounter(prometheus.CounterOpts{Name: "h_total"})
	reg.MustRegister(prim, hedg)

	read := func() (int64, int64) {
		return int64(testutil.ToFloat64(prim)), int64(testutil.ToFloat64(hedg))
	}

	// Drive the counters over time and check we get one sample per bucket.
	start := time.Now()
	s := startCounterSampler(start, 50*time.Millisecond, read)

	// Bump counters between boundaries so deltas are testable.
	prim.Add(3)
	time.Sleep(60 * time.Millisecond) // bucket 0 → 1 transition
	prim.Add(5)
	hedg.Add(2)
	time.Sleep(60 * time.Millisecond) // bucket 1 → 2 transition

	samples := s.stop()
	// At least: initial + 2 boundaries + 1 final = 4 samples.
	assert.GreaterOrEqual(t, len(samples), 4)

	// Initial sample is at zero counts.
	assert.Equal(t, int64(0), samples[0].primary)
	assert.Equal(t, int64(0), samples[0].hedge)

	// Final cumulative state matches what we added.
	final := samples[len(samples)-1]
	assert.Equal(t, int64(8), final.primary)
	assert.Equal(t, int64(2), final.hedge)

	deltas := bucketDeltas(samples)
	assert.Equal(t, len(samples)-1, len(deltas))
	// Sum of deltas equals the cumulative final value.
	var sumP, sumH int64
	for _, d := range deltas {
		sumP += d.primary
		sumH += d.hedge
	}
	assert.Equal(t, int64(8), sumP)
	assert.Equal(t, int64(2), sumH)
}

func TestCounterSampler_BoundariesAreAbsolute(t *testing.T) {
	t.Parallel()

	prim := prometheus.NewCounter(prometheus.CounterOpts{Name: "p"})
	hedg := prometheus.NewCounter(prometheus.CounterOpts{Name: "h"})
	read := func() (int64, int64) {
		return int64(testutil.ToFloat64(prim)), int64(testutil.ToFloat64(hedg))
	}

	start := time.Now()
	s := startCounterSampler(start, 30*time.Millisecond, read)
	time.Sleep(110 * time.Millisecond) // span 3+ boundaries
	samples := s.stop()

	// First sample is exactly at start.
	assert.Equal(t, start, samples[0].at)
	// Subsequent boundary samples land near start + k*30ms (allow 20ms slop
	// for scheduling) up to the final one.
	for k := 1; k < len(samples)-1; k++ {
		target := start.Add(time.Duration(k) * 30 * time.Millisecond)
		diff := samples[k].at.Sub(target)
		if diff < 0 {
			diff = -diff
		}
		assert.LessOrEqualf(t, diff, 20*time.Millisecond, "sample %d at %v target %v diff %v", k, samples[k].at, target, diff)
	}
}

func TestGatherCounter_ReadsByName(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewPedanticRegistry()
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "thing_total"})
	reg.MustRegister(c)
	c.Add(7)
	assert.Equal(t, int64(7), gatherCounter(reg, "thing_total"))
}

func TestGatherCounter_PanicsOnMissingMetric(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewPedanticRegistry()
	assert.PanicsWithError(t, `gatherCounter: metric "missing_total" not registered`, func() {
		gatherCounter(reg, "missing_total")
	})
}

func TestBucketDeltas_EmptyAndSingle(t *testing.T) {
	t.Parallel()
	assert.Nil(t, bucketDeltas(nil))
	assert.Nil(t, bucketDeltas([]counterSample{{}}))
	deltas := bucketDeltas([]counterSample{
		{primary: 5, hedge: 1},
		{primary: 9, hedge: 3},
	})
	assert.Equal(t, []counterBucketDelta{{primary: 4, hedge: 2}}, deltas)
}
