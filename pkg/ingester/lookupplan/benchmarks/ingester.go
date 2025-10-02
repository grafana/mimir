// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks

import (
	"github.com/grafana/mimir/pkg/streamingpromql/benchmarks"
)

// StartIngesterAndLoadBlocks starts a local ingester with blocks automatically loaded
// from the specified rootDataDir. This is a simplified wrapper around the existing
// StartIngesterAndLoadData function that reuses the ingester's automatic block discovery.
//
// Parameters:
//   - rootDataDir: Directory containing blocks to load (passed as ingester data directory)
//   - blockDirs: Currently unused, blocks are discovered automatically from rootDataDir
//
// Returns:
//   - address: gRPC address of the started ingester
//   - cleanup: Function to call to stop the ingester and cleanup resources
//   - error: Any error that occurred during startup
func StartIngesterAndLoadBlocks(rootDataDir string, blockDirs []string) (string, func(), error) {
	// Reuse existing function with empty metric sizes - no synthetic data needed
	// The ingester will automatically discover and load existing blocks from rootDataDir
	// via openExistingTSDB() method during startup
	return benchmarks.StartIngesterAndLoadData(rootDataDir, []int{})
}
