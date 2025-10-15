// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
)

var dataDirFlag = flag.String("data-dir", "", "Directory containing an ingester data dir (WAL + blocks for multiple tenants).")

// BenchmarkQueryExecution benchmarks query execution against a running ingester
func BenchmarkQueryExecution(b *testing.B) {
	require.NotEmpty(b, *dataDirFlag, "-data-dir flag is required")

	addr, cleanupFunc, err := StartIngesterAndLoadBlocks(*dataDirFlag)
	require.NoError(b, err)
	defer cleanupFunc()

	b.ReportAllocs()
	b.ResetTimer()

	// Benchmark query execution
	for i := 0; i < b.N; i++ {
		_, err := ExecuteQuery(addr, "__name__", "mimir_continuous_test_sine_wave_v2")
		require.NoError(b, err)
	}
}
