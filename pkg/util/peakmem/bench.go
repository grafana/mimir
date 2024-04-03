// SPDX-License-Identifier: AGPL-3.0-only

package peakmem

import (
	"runtime"
	"testing"
	"time"
)

const minimumSamplingInterval = time.Millisecond

// Capture records the peak memory utilisation observed during test.
//
// It does this by recording the peak value of runtime.MemStats.HeapAlloc observed
// at regular intervals.
//
// Note that runtime.MemStats.HeapAlloc does not represent the full memory consumption of
// a Golang app.
//
// Capture calls b.ResetTimer() and runtime.GC() before starting test, and b.StopTimer()
// after test returns.
func Capture(b *testing.B, benchmark func(b *testing.B)) {
	ready := make(chan struct{})
	start := make(chan struct{})
	stop := make(chan struct{})
	done := make(chan struct{})
	peakHeap := float64(0)

	go func() {
		defer close(done)

		samplingInterval := minimumSamplingInterval
		ticker := time.NewTicker(samplingInterval)
		defer ticker.Stop()
		memStats := &runtime.MemStats{}

		captureNow := func() {
			runtime.ReadMemStats(memStats)
			newHeap := float64(memStats.HeapAlloc) // TODO: is this the best measure to use? Should we include stack bytes? Any of the other values from MemStats?
			peakHeap = max(newHeap, peakHeap)

			// Aim to sample 10 times per GC cycle, but not more than once every 1ms.
			// Note that the estimated GC period may be based on cycles that aren't relevant to this benchmark.
			newSamplingInterval := estimateGCPeriod(memStats) / 10
			if newSamplingInterval < minimumSamplingInterval {
				newSamplingInterval = minimumSamplingInterval
			}

			ticker.Reset(newSamplingInterval)
		}

		close(ready)
		<-start

		for {
			select {
			case <-ticker.C:
				captureNow()
			case <-stop:
				// Capture one last time, to ensure we don't report 0 B if the benchmark concludes before the first tick.
				captureNow()

				// Nothing more to do here: we'll close 'done' as part of the deferred close above.
				return
			}
		}
	}()

	// Wait for the sampling goroutine to be ready.
	<-ready

	// Run GC once now to ensure we're starting from a relatively stable starting point.
	runtime.GC()

	// Reset the benchmark timer, start sampling, then run the benchmark.
	b.ResetTimer()
	close(start)
	benchmark(b)

	// Stop the timer and wait for sampling to finish.
	b.StopTimer()
	close(stop)
	<-done

	b.ReportMetric(peakHeap, "B")
}

// estimateGCPeriod returns the average period between GC cycles reported in memStats.
func estimateGCPeriod(memStats *runtime.MemStats) time.Duration {
	// MemStats contains timing information for up to 256 GC cycles.
	// If there are less, then use whatever we can.
	lastGC := memStats.NumGC

	if lastGC <= 1 {
		// Need at least two complete GC cycles.
		return 0
	}

	firstGC := lastGC - 255

	if lastGC < 256 {
		// If there haven't been 256 GC cycles, then use all available GC cycles.
		// Note that the first GC cycle is cycle number 1, not 0.
		firstGC = 1
	}

	cycleCount := uint64(lastGC - firstGC)

	if cycleCount == 0 {
		return 0
	}

	lastGCTime := memStats.PauseEnd[(lastGC+255)%256]
	firstGCTime := memStats.PauseEnd[(firstGC+255)%256]

	return time.Duration((lastGCTime-firstGCTime)/cycleCount) * time.Nanosecond
}
