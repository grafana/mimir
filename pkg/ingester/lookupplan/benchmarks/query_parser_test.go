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

func TestParseQueriesFromFile(t *testing.T) {
	// Create a temporary file with sample query data
	tmpDir := t.TempDir()
	queryFile := filepath.Join(tmpDir, "queries.json")

	// Sample log entries in the format we expect (JSON array of objects)
	content := `[
{"line":"ts=2025-10-15T14:56:24.335968016Z","timestamp":"2025-10-15T14:56:24.337Z","date":"2025-10-15T14:56:24.337Z","fields":{"method":"POST","param_query":"up","param_start":"2025-10-15T14:56:20Z","param_end":"2025-10-15T14:56:23Z","param_step":"15","user":"279486","status":"success"}},
{"line":"ts=2025-10-15T14:56:24.430058859Z","timestamp":"2025-10-15T14:56:24.437Z","date":"2025-10-15T14:56:24.437Z","fields":{"method":"POST","param_query":"node_cpu_seconds_total","param_start":"1728994584","param_end":"1728994884","param_step":"30","user":"test-user","status":"success"}},
{"line":"ts=2025-10-15T14:56:24.430058859Z","timestamp":"2025-10-15T14:56:24.437Z","date":"2025-10-15T14:56:24.437Z","fields":{"method":"GET","param_query":"should_be_skipped","user":"test-user","status":"success"}},
{"line":"ts=2025-10-15T14:56:24.430058859Z","timestamp":"2025-10-15T14:56:24.437Z","date":"2025-10-15T14:56:24.437Z","fields":{"method":"POST","param_query":"should_be_skipped","user":"test-user","status":"failed"}}
]`

	err := os.WriteFile(queryFile, []byte(content), 0644)
	require.NoError(t, err)

	// Parse queries
	queries, err := ParseQueriesFromFile(queryFile)
	require.NoError(t, err)

	// We should have 2 valid queries (POST with success status)
	assert.Len(t, queries, 2)

	// Check first query
	assert.Equal(t, "up", queries[0].Query)
	assert.Equal(t, "279486", queries[0].OrgID)
	expectedStart, _ := time.Parse(time.RFC3339, "2025-10-15T14:56:20Z")
	assert.Equal(t, expectedStart, queries[0].Start)
	expectedEnd, _ := time.Parse(time.RFC3339, "2025-10-15T14:56:23Z")
	assert.Equal(t, expectedEnd, queries[0].End)
	assert.Equal(t, 15*time.Second, queries[0].Step)

	// Check second query with Unix timestamps
	assert.Equal(t, "node_cpu_seconds_total", queries[1].Query)
	assert.Equal(t, "test-user", queries[1].OrgID)
	assert.NotZero(t, queries[1].Start)
	assert.NotZero(t, queries[1].End)
	assert.Equal(t, 30*time.Second, queries[1].Step)
}

func TestParseQueriesFromFile_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	queryFile := filepath.Join(tmpDir, "empty.json")

	err := os.WriteFile(queryFile, []byte("[]"), 0644)
	require.NoError(t, err)

	queries, err := ParseQueriesFromFile(queryFile)
	require.NoError(t, err)
	assert.Empty(t, queries)
}

func TestParseQueriesFromFile_NonExistent(t *testing.T) {
	_, err := ParseQueriesFromFile("/nonexistent/file.json")
	assert.Error(t, err)
}
