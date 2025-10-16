// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrepareVectorQueries_Caching(t *testing.T) {
	tmpDir := t.TempDir()
	queryFile := filepath.Join(tmpDir, "cached_queries.json")

	// Sample log entries with valid PromQL queries
	content := `{"labels":{"method":"POST","param_query":"up","param_start":"2025-10-15T14:56:20Z","param_end":"2025-10-15T14:56:23Z","param_step":"15"},"timestamp":"2025-10-15T14:56:24.337Z"}
{"labels":{"method":"POST","param_query":"node_cpu_seconds_total"},"timestamp":"2025-10-15T14:56:24.437Z"}
`

	err := os.WriteFile(queryFile, []byte(content), 0644)
	require.NoError(t, err)

	// First call
	vectorQueries1, err := PrepareVectorQueries(queryFile, "", "", 1.0, 1)
	require.NoError(t, err)
	require.Len(t, vectorQueries1, 2)

	// Second call with same parameters - should be cached
	vectorQueries2, err := PrepareVectorQueries(queryFile, "", "", 1.0, 1)
	require.NoError(t, err)
	require.Len(t, vectorQueries2, 2)

	// Verify same underlying slice (pointer equality)
	assert.Equal(t, &vectorQueries1[0], &vectorQueries2[0], "cached result should return same slice")

	// Call with different sample fraction - should not be cached (different cache key)
	vectorQueries3, err := PrepareVectorQueries(queryFile, "", "", 0.5, 1)
	require.NoError(t, err)
	// With small dataset, sampling may still return same queries, but they should be different slice instances
	require.NotEqual(t, fmt.Sprintf("%p", vectorQueries1), fmt.Sprintf("%p", vectorQueries3), "different parameters should create new slice")

	// Modify file content
	newContent := `{"labels":{"method":"POST","param_query":"up"},"timestamp":"2025-10-15T14:56:24.337Z"}
`
	err = os.WriteFile(queryFile, []byte(newContent), 0644)
	require.NoError(t, err)

	// Call again with original parameters - should still return cached result (doesn't detect file changes)
	vectorQueries4, err := PrepareVectorQueries(queryFile, "", "", 1.0, 1)
	require.NoError(t, err)
	assert.Len(t, vectorQueries4, 2, "cache should return original result even after file modification")

	// Clear cache for cleanup
	vectorQueryCache.Delete(fmt.Sprintf("%s|||%f|%d", queryFile, 1.0, int64(1)))
	vectorQueryCache.Delete(fmt.Sprintf("%s|||%f|%d", queryFile, 0.5, int64(1)))
}

func TestPrepareVectorQueries_MultipleCaches(t *testing.T) {
	tmpDir := t.TempDir()

	// Create two different files
	file1 := filepath.Join(tmpDir, "queries1.json")
	file2 := filepath.Join(tmpDir, "queries2.json")

	content1 := `{"labels":{"method":"POST","param_query":"metric1"},"timestamp":"2025-10-15T14:56:24.337Z"}
`
	content2 := `{"labels":{"method":"POST","param_query":"metric2"},"timestamp":"2025-10-15T14:56:24.337Z"}
{"labels":{"method":"POST","param_query":"metric3"},"timestamp":"2025-10-15T14:56:24.437Z"}
`

	err := os.WriteFile(file1, []byte(content1), 0644)
	require.NoError(t, err)
	err = os.WriteFile(file2, []byte(content2), 0644)
	require.NoError(t, err)

	// Prepare vector queries from both files
	vectorQueries1, err := PrepareVectorQueries(file1, "", "", 1.0, 1)
	require.NoError(t, err)
	require.Len(t, vectorQueries1, 1)
	assert.Equal(t, "metric1", vectorQueries1[0].originalQuery.Query)

	vectorQueries2, err := PrepareVectorQueries(file2, "", "", 1.0, 1)
	require.NoError(t, err)
	require.Len(t, vectorQueries2, 2)
	assert.Equal(t, "metric2", vectorQueries2[0].originalQuery.Query)
	assert.Equal(t, "metric3", vectorQueries2[1].originalQuery.Query)

	// Call again - should get cached results
	vectorQueries1Again, err := PrepareVectorQueries(file1, "", "", 1.0, 1)
	require.NoError(t, err)
	assert.Equal(t, &vectorQueries1[0], &vectorQueries1Again[0])

	vectorQueries2Again, err := PrepareVectorQueries(file2, "", "", 1.0, 1)
	require.NoError(t, err)
	assert.Equal(t, &vectorQueries2[0], &vectorQueries2Again[0])

	// Clean up
	vectorQueryCache.Delete(fmt.Sprintf("%s|||%f|%d", file1, 1.0, int64(1)))
	vectorQueryCache.Delete(fmt.Sprintf("%s|||%f|%d", file2, 1.0, int64(1)))
}

func TestPrepareVectorQueries_TenantFiltering(t *testing.T) {
	tmpDir := t.TempDir()
	queryFile := filepath.Join(tmpDir, "tenant_queries.json")

	// Sample log entries with different tenant IDs
	content := `{"labels":{"method":"POST","param_query":"up","user":"tenant1"},"timestamp":"2025-10-15T14:56:24.337Z"}
{"labels":{"method":"POST","param_query":"node_cpu_seconds_total","user":"tenant2"},"timestamp":"2025-10-15T14:56:24.437Z"}
{"labels":{"method":"POST","param_query":"process_cpu_seconds_total","user":"tenant1"},"timestamp":"2025-10-15T14:56:24.537Z"}
{"labels":{"method":"POST","param_query":"go_goroutines","user":"tenant3"},"timestamp":"2025-10-15T14:56:24.637Z"}
`

	err := os.WriteFile(queryFile, []byte(content), 0644)
	require.NoError(t, err)

	// No tenant filter - should get all queries
	allQueries, err := PrepareVectorQueries(queryFile, "", "", 1.0, 1)
	require.NoError(t, err)
	require.Len(t, allQueries, 4, "should have all queries when no tenant filter")

	// Filter by tenant1
	tenant1Queries, err := PrepareVectorQueries(queryFile, "tenant1", "", 1.0, 1)
	require.NoError(t, err)
	require.Len(t, tenant1Queries, 2, "should have only tenant1 queries")
	assert.Equal(t, "tenant1", tenant1Queries[0].originalQuery.OrgID)
	assert.Equal(t, "tenant1", tenant1Queries[1].originalQuery.OrgID)

	// Filter by tenant2
	tenant2Queries, err := PrepareVectorQueries(queryFile, "tenant2", "", 1.0, 1)
	require.NoError(t, err)
	require.Len(t, tenant2Queries, 1, "should have only tenant2 queries")
	assert.Equal(t, "tenant2", tenant2Queries[0].originalQuery.OrgID)

	// Filter by non-existent tenant
	noQueries, err := PrepareVectorQueries(queryFile, "nonexistent", "", 1.0, 1)
	require.NoError(t, err)
	require.Len(t, noQueries, 0, "should have no queries for non-existent tenant")

	// Clean up
	vectorQueryCache.Delete(fmt.Sprintf("%s|||%f|%d", queryFile, 1.0, int64(1)))
	vectorQueryCache.Delete(fmt.Sprintf("%s|tenant1||%f|%d", queryFile, 1.0, int64(1)))
	vectorQueryCache.Delete(fmt.Sprintf("%s|tenant2||%f|%d", queryFile, 1.0, int64(1)))
	vectorQueryCache.Delete(fmt.Sprintf("%s|nonexistent||%f|%d", queryFile, 1.0, int64(1)))
}
