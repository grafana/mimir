// SPDX-License-Identifier: AGPL-3.0-only

package peakmem

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

const KB = 1024
const MB = 1024 * KB

func BenchmarkCaptureExample(b *testing.B) {
	Capture(b, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = make([]byte, 20*MB)
		}
	})
}

func BenchmarkCaptureImpact(b *testing.B) {
	// TODO: exercise other scenarios (eg. high CPU utilisation, high concurrency)
	benchmark := func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = make([]byte, 20*MB)
		}
	}

	b.Run("without", benchmark)
	b.Run("with", func(b *testing.B) {
		Capture(b, benchmark)
	})
}

func TestCapture(t *testing.T) {
	runtime.GC()
	memStats := &runtime.MemStats{}
	runtime.ReadMemStats(memStats)
	baseline := memStats.HeapAlloc

	result := testing.Benchmark(BenchmarkCaptureExample)
	actual := result.Extra["B"]

	expected := float64(baseline + 20*MB)
	require.GreaterOrEqual(t, actual, expected)   // Actual result will be higher than expected value, as we don't account for any other memory consumption in the expected value.
	require.LessOrEqual(t, actual, expected*1.05) // Make sure it's not wildly off though.
}
