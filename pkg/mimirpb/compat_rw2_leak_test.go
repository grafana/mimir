// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"runtime"
	"testing"
)

// TestRW2SymbolsMemoryLeak demonstrates the difference between the buggy and fixed releasePages().
func TestRW2SymbolsMemoryLeak(t *testing.T) {
	bufferSize := 10 * 1024 * 1024 // 10MB

	// Test 1: Buggy version (current releasePages)
	t.Run("buggy version", func(t *testing.T) {
		buffer := make([]byte, bufferSize)
		for i := range buffer {
			buffer[i] = byte(i % 256)
		}

		ps := &rw2PagedSymbols{}
		numSymbols := 1000
		symbolSize := bufferSize / numSymbols
		for i := 0; i < numSymbols; i++ {
			start := i * symbolSize
			end := start + symbolSize
			if end > bufferSize {
				end = bufferSize
			}
			ps.append(yoloString(buffer[start:end]))
		}

		buffer = nil

		runtime.GC()
		var m1 runtime.MemStats
		runtime.ReadMemStats(&m1)
		t.Logf("Heap with yoloStrings referenced: %.2f MB", float64(m1.HeapAlloc)/(1024*1024))

		// Call the BUGGY releasePages()
		ps.releasePages()

		runtime.GC()
		var m2 runtime.MemStats
		runtime.ReadMemStats(&m2)

		freed := int64(m1.HeapAlloc) - int64(m2.HeapAlloc)
		t.Logf("Memory freed by buggy releasePages(): %d bytes (%.2f MB)", freed, float64(freed)/(1024*1024))

		// The buggy version should NOT free the buffer
		if freed < int64(bufferSize/2) {
			t.Logf("✓ Confirmed: Buggy version did NOT free the buffer (freed only %.2f MB)", float64(freed)/(1024*1024))
		} else {
			t.Logf("✗ Unexpected: Buggy version freed %.2f MB", float64(freed)/(1024*1024))
		}
	})

	// Test 2: Fixed version
	t.Run("fixed version", func(t *testing.T) {
		buffer := make([]byte, bufferSize)
		for i := range buffer {
			buffer[i] = byte(i % 256)
		}

		ps := &rw2PagedSymbols{}
		numSymbols := 1000
		symbolSize := bufferSize / numSymbols
		for i := 0; i < numSymbols; i++ {
			start := i * symbolSize
			end := start + symbolSize
			if end > bufferSize {
				end = bufferSize
			}
			ps.append(yoloString(buffer[start:end]))
		}

		buffer = nil

		runtime.GC()
		var m1 runtime.MemStats
		runtime.ReadMemStats(&m1)
		t.Logf("Heap with yoloStrings referenced: %.2f MB", float64(m1.HeapAlloc)/(1024*1024))

		// Call the FIXED releasePages()
		ps.releasePagesFixed()

		runtime.GC()
		var m2 runtime.MemStats
		runtime.ReadMemStats(&m2)

		freed := int64(m1.HeapAlloc) - int64(m2.HeapAlloc)
		t.Logf("Memory freed by fixed releasePages(): %d bytes (%.2f MB)", freed, float64(freed)/(1024*1024))

		// The fixed version should free the buffer
		minExpectedFreed := int64(bufferSize * 9 / 10)
		if freed >= minExpectedFreed {
			t.Logf("✓ Confirmed: Fixed version freed the buffer (%.2f MB)", float64(freed)/(1024*1024))
		} else {
			t.Logf("✗ Unexpected: Fixed version only freed %.2f MB", float64(freed)/(1024*1024))
		}
	})
}
