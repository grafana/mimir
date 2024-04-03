// SPDX-License-Identifier: AGPL-3.0-only

package peakmem

import (
	"runtime"
	"testing"
	"time"
)

// Capture records the peak memory utilisation observed during test.
//
// It does this by recording the peak value of runtime.MemStats.HeapAlloc observed
// at 10ms intervals.
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

		// TODO: make interval configurable?
		// TODO: make interval smarter (eg. adapt to GC rate, avoid aliasing issues)
		samplingInterval := 10 * time.Millisecond
		ticker := time.NewTicker(samplingInterval)
		defer ticker.Stop()
		memStats := &runtime.MemStats{}

		captureNow := func() {
			runtime.ReadMemStats(memStats)
			newHeap := float64(memStats.HeapAlloc) // TODO: is this the best measure to use? Should we include stack bytes? Any of the other values from MemStats?
			peakHeap = max(newHeap, peakHeap)
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
