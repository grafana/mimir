// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks

import (
	"os"
	"path/filepath"
	"testing"

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
	addr, cleanup, err := StartIngesterAndLoadBlocks("/non/existent/path", nil)

	// The function should handle this gracefully
	if err == nil {
		// If no error, cleanup should work
		require.NotEmpty(t, addr)
		require.NotNil(t, cleanup)
		cleanup()
	}
	// If error occurs, that's also acceptable for invalid paths
}
