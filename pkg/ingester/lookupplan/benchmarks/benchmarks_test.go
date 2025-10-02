// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkStartIngesterAndLoadBlocks benchmarks the ingester startup with block loading
func BenchmarkStartIngesterAndLoadBlocks(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Create temporary directory
		tempDir, err := os.MkdirTemp("", "benchmark-ingester-*")
		require.NoError(b, err)

		// Create data subdirectory
		dataDir := filepath.Join(tempDir, "data")
		err = os.MkdirAll(dataDir, 0755)
		require.NoError(b, err)

		b.StartTimer()

		// Benchmark the ingester startup
		addr, cleanup, err := StartIngesterAndLoadBlocks(tempDir, nil)
		require.NoError(b, err)
		require.NotEmpty(b, addr)

		b.StopTimer()
		cleanup()
		os.RemoveAll(tempDir)
	}
}
