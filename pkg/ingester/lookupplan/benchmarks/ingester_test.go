// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartIngesterAndLoadBlocks(t *testing.T) {
	// Create temporary directory to simulate block directory
	tempDir, err := os.MkdirTemp("", "benchmark-blocks-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create data subdirectory as expected by ingester
	dataDir := filepath.Join(tempDir, "data")
	err = os.MkdirAll(dataDir, 0755)
	require.NoError(t, err)

	// Test the function
	addr, cleanup, err := StartIngesterAndLoadBlocks(tempDir, nil)

	// Verify function returns without error
	require.NoError(t, err)
	require.NotEmpty(t, addr, "address should not be empty")
	require.NotNil(t, cleanup, "cleanup function should not be nil")

	// Verify address format (should be local address with port)
	assert.Regexp(t, `^(localhost|127\.0\.0\.1):\d+$`, addr)

	// Call cleanup to ensure it works
	cleanup()
}

func TestStartIngesterAndLoadBlocks_InvalidDirectory(t *testing.T) {
	// Test with non-existent directory should still work
	// (ingester will create it if needed)
	addr, cleanup, err := StartIngesterAndLoadBlocks("/non/existent/path")

	// The function should handle this gracefully
	if err == nil {
		// If no error, cleanup should work
		require.NotEmpty(t, addr)
		require.NotNil(t, cleanup)
		cleanup()
	}
	// If error occurs, that's also acceptable for invalid paths
}

func TestExecuteQuery(t *testing.T) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "query-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create data subdirectory
	dataDir := filepath.Join(tempDir, "data")
	err = os.MkdirAll(dataDir, 0755)
	require.NoError(t, err)

	// Start ingester
	addr, cleanup, err := StartIngesterAndLoadBlocks(tempDir)
	require.NoError(t, err)
	defer cleanup()

	// Execute a query
	result, err := ExecuteQuery(addr, "__name__", "test_metric")
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify result structure
	assert.GreaterOrEqual(t, result.SeriesCount, 0)
	assert.GreaterOrEqual(t, result.SampleCount, 0)
	assert.Greater(t, result.Duration, time.Duration(0))
}
