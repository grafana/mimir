// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkQueryExecution benchmarks query execution against a running ingester
func BenchmarkQueryExecution(b *testing.B) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "benchmark-query-*")
	require.NoError(b, err)
	defer os.RemoveAll(tempDir)

	// Create data subdirectory
	dataDir := filepath.Join(tempDir, "data")
	err = os.MkdirAll(dataDir, 0755)
	require.NoError(b, err)

	// Start ingester once for all iterations
	addr, cleanup, err := StartIngesterAndLoadBlocks(tempDir, nil)
	require.NoError(b, err)
	defer cleanup()

	b.ResetTimer()

	// Benchmark query execution
	for i := 0; i < b.N; i++ {
		_, err := ExecuteQuery(addr, "__name__", "test_metric")
		require.NoError(b, err)
	}
}
