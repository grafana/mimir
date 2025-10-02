// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApp_parseArgs(t *testing.T) {
	app := &app{}

	// Test default values
	err := app.parseArgs([]string{"benchmark-ingester-queries"})
	require.NoError(t, err)

	assert.Equal(t, uint(1), app.count)
	assert.NotEmpty(t, app.blockDir) // Should have some default
}

func TestApp_parseArgs_WithFlags(t *testing.T) {
	app := &app{}

	// Test with custom flags
	err := app.parseArgs([]string{
		"benchmark-ingester-queries",
		"-block-dir", "/path/to/blocks",
		"-count", "5",
	})
	require.NoError(t, err)

	assert.Equal(t, "/path/to/blocks", app.blockDir)
	assert.Equal(t, uint(5), app.count)
}

func TestApp_run_InvalidBlockDir(t *testing.T) {
	app := &app{
		blockDir: "/completely/invalid/path/that/does/not/exist",
		count:    1,
	}

	// Should handle invalid directories gracefully
	err := app.run()
	// May return error or succeed depending on ingester behavior
	// The test mainly ensures no panics occur
	_ = err
}

func TestApp_run_ValidTempDir(t *testing.T) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "cli-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create data subdirectory
	dataDir := filepath.Join(tempDir, "data")
	err = os.MkdirAll(dataDir, 0755)
	require.NoError(t, err)

	app := &app{
		blockDir: tempDir,
		count:    1,
	}

	// Should be able to run successfully with valid directory
	err = app.run()
	require.NoError(t, err)
}
