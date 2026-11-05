// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"os"
	"runtime"
	"testing"
)

func requireBench(t *testing.T) {
	if os.Getenv("GO_INGESTER_BENCH") == "" {
		t.Skip("set GO_INGESTER_BENCH=1 to run")
	}
}

func liveHeapBytes() uint64 {
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.HeapAlloc
}
