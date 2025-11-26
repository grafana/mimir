// SPDX-License-Identifier: AGPL-3.0-only

package bench

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExtractLabelMatchers tests the extractLabelMatchers function
func TestExtractLabelMatchers(t *testing.T) {
	tests := []struct {
		name                  string
		query                 string
		expectedSelectorCount int        // number of vector selectors
		expectedMetrics       []string   // expected metric names across all selectors
		expectedMatchers      [][]string // expected matchers for each selector (as strings)
	}{
		{
			name:                  "simple metric",
			query:                 "up",
			expectedSelectorCount: 1,
			expectedMetrics:       []string{"up"},
			expectedMatchers:      [][]string{{`__name__="up"`}},
		},
		{
			name:                  "metric with label",
			query:                 `container_memory_working_set_bytes{namespace="default"}`,
			expectedSelectorCount: 1,
			expectedMetrics:       []string{"container_memory_working_set_bytes"},
			expectedMatchers:      [][]string{{`__name__="container_memory_working_set_bytes"`, `namespace="default"`}},
		},
		{
			name:                  "metric with multiple labels",
			query:                 `node_cpu_seconds_total{mode="idle",cpu="0"}`,
			expectedSelectorCount: 1,
			expectedMetrics:       []string{"node_cpu_seconds_total"},
			expectedMatchers:      [][]string{{`__name__="node_cpu_seconds_total"`, `mode="idle"`, `cpu="0"`}},
		},
		{
			name:                  "aggregation query",
			query:                 `sum by(pod) (container_memory_working_set_bytes{namespace="default"})`,
			expectedSelectorCount: 1,
			expectedMetrics:       []string{"container_memory_working_set_bytes"},
			expectedMatchers:      [][]string{{`__name__="container_memory_working_set_bytes"`, `namespace="default"`}},
		},
		{
			name:                  "binary operation",
			query:                 `up + down`,
			expectedSelectorCount: 2, // Two separate vector selectors
			expectedMetrics:       []string{"up", "down"},
			expectedMatchers:      [][]string{{`__name__="up"`}, {`__name__="down"`}},
		},
		{
			name:                  "rate with label matchers",
			query:                 `rate(container_cpu_usage_seconds_total{namespace=~"kube.*"}[5m])`,
			expectedSelectorCount: 1,
			expectedMetrics:       []string{"container_cpu_usage_seconds_total"},
			expectedMatchers:      [][]string{{`__name__="container_cpu_usage_seconds_total"`, `namespace=~"kube.*"`}},
		},
		{
			name:                  "scalar query",
			query:                 "time()",
			expectedSelectorCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcherSets, err := extractLabelMatchers(tt.query)
			require.NoError(t, err)
			assert.Len(t, matcherSets, tt.expectedSelectorCount)

			// Check that expected metrics are present across all selectors
			foundMetrics := make(map[string]bool)
			for _, matchers := range matcherSets {
				for _, m := range matchers {
					if m.Name == model.MetricNameLabel {
						foundMetrics[m.Value] = true
					}
				}
			}

			for _, expectedMetric := range tt.expectedMetrics {
				assert.True(t, foundMetrics[expectedMetric], "expected to find metric %s", expectedMetric)
			}

			// Check that extracted matchers match expectations
			if len(tt.expectedMatchers) > 0 {
				require.Len(t, matcherSets, len(tt.expectedMatchers), "number of matcher sets should match")
				for i, expectedMatcherStrings := range tt.expectedMatchers {
					// Convert actual matchers to strings
					actualMatcherStrings := make([]string, 0, len(matcherSets[i]))
					for _, m := range matcherSets[i] {
						actualMatcherStrings = append(actualMatcherStrings, m.String())
					}
					assert.ElementsMatch(t, expectedMatcherStrings, actualMatcherStrings,
						"matchers for selector %d should match", i)
				}
			}
		})
	}
}

func TestExtractLabelMatchers_InvalidQuery(t *testing.T) {
	_, err := extractLabelMatchers("invalid query {{{")
	assert.Error(t, err)
}

// TestPrepareQueries_Caching tests query caching
func TestPrepareQueries_Caching(t *testing.T) {
	qc := NewQueryLoader()
	tmpDir := t.TempDir()
	queryFile := filepath.Join(tmpDir, "cached_queries.json")

	// Sample log entries with valid PromQL queries
	content := `{"labels":{"method":"POST","param_query":"up","param_start":"2025-10-15T14:56:20Z","param_end":"2025-10-15T14:56:23Z","param_step":"15"},"timestamp":"2025-10-15T14:56:24.337Z"}
{"labels":{"method":"POST","param_query":"node_cpu_seconds_total"},"timestamp":"2025-10-15T14:56:24.437Z"}
`

	err := os.WriteFile(queryFile, []byte(content), 0644)
	require.NoError(t, err)

	// First call
	config := QueryLoaderConfig{Filepath: queryFile, TenantID: "", QueryIDs: nil, SampleFraction: 1.0, Seed: 1}
	queries1, _, err := qc.PrepareQueries(config)
	require.NoError(t, err)
	require.Len(t, queries1, 2)

	// Second call with same parameters - should be cached
	queries2, _, err := qc.PrepareQueries(config)
	require.NoError(t, err)
	require.Len(t, queries2, 2)

	// Verify same underlying slice (pointer equality)
	assert.Equal(t, &queries1[0], &queries2[0], "cached result should return same slice")

	// Call with different sample fraction - should not be cached (different cache key)
	config2 := QueryLoaderConfig{Filepath: queryFile, TenantID: "", QueryIDs: nil, SampleFraction: 0.5, Seed: 1}
	queries3, _, err := qc.PrepareQueries(config2)
	require.NoError(t, err)
	// With small dataset, sampling may still return same queries, but they should be different slice instances
	require.NotEqual(t, fmt.Sprintf("%p", queries1), fmt.Sprintf("%p", queries3), "different parameters should create new slice")

	// Modify file content
	newContent := `{"labels":{"method":"POST","param_query":"up"},"timestamp":"2025-10-15T14:56:24.337Z"}
`
	err = os.WriteFile(queryFile, []byte(newContent), 0644)
	require.NoError(t, err)

	// Call again with original parameters - should still return cached result (doesn't detect file changes)
	queries4, _, err := qc.PrepareQueries(config)
	require.NoError(t, err)
	assert.Len(t, queries4, 2, "cache should return original result even after file modification")
}

func TestPrepareQueries_TenantFiltering(t *testing.T) {
	qc := NewQueryLoader()
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
	allQueries, _, err := qc.PrepareQueries(QueryLoaderConfig{Filepath: queryFile, TenantID: "", QueryIDs: nil, SampleFraction: 1.0, Seed: 1})
	require.NoError(t, err)
	require.Len(t, allQueries, 4, "should have all queries when no tenant filter")

	// Filter by tenant1
	tenant1Queries, _, err := qc.PrepareQueries(QueryLoaderConfig{Filepath: queryFile, TenantID: "tenant1", QueryIDs: nil, SampleFraction: 1.0, Seed: 1})
	require.NoError(t, err)
	require.Len(t, tenant1Queries, 2, "should have only tenant1 queries")
	assert.Equal(t, "tenant1", tenant1Queries[0].User)
	assert.Equal(t, "tenant1", tenant1Queries[1].User)

	// Filter by tenant2
	tenant2Queries, _, err := qc.PrepareQueries(QueryLoaderConfig{Filepath: queryFile, TenantID: "tenant2", QueryIDs: nil, SampleFraction: 1.0, Seed: 1})
	require.NoError(t, err)
	require.Len(t, tenant2Queries, 1, "should have only tenant2 queries")
	assert.Equal(t, "tenant2", tenant2Queries[0].User)

	// Filter by non-existent tenant
	noQueries, _, err := qc.PrepareQueries(QueryLoaderConfig{Filepath: queryFile, TenantID: "nonexistent", QueryIDs: nil, SampleFraction: 1.0, Seed: 1})
	require.NoError(t, err)
	require.Len(t, noQueries, 0, "should have no queries for non-existent tenant")
}

// TestLoadQueryLogsFromFile tests loading queries from a file
func TestLoadQueryLogsFromFile(t *testing.T) {
	// Create a temporary file with sample query data
	tmpDir := t.TempDir()
	queryFile := filepath.Join(tmpDir, "queries.json")

	// Sample log entries in newline-delimited JSON format
	content := `{"labels":{"method":"POST","param_query":"up","param_start":"2025-10-15T14:56:20Z","param_end":"2025-10-15T14:56:23Z","param_step":"15"},"timestamp":"2025-10-15T14:56:24.337Z"}
{"labels":{"method":"POST","param_query":"node_cpu_seconds_total","param_start":"2025-10-15T15:00:20Z","param_end":"2025-10-15T15:01:20Z","param_step":"30"},"timestamp":"2025-10-15T14:56:24.437Z"}
{"labels":{"method":"GET","param_query":"should_be_skipped"},"timestamp":"2025-10-15T14:56:24.437Z"}
{"labels":{"param_query":"query_without_method"},"timestamp":"2025-10-15T14:56:24.437Z"}
`

	err := os.WriteFile(queryFile, []byte(content), 0644)
	require.NoError(t, err)

	// Parse queries
	queries, stats, err := loadQueryLogsFromFile(queryFile, nil)
	require.NoError(t, err)

	// We should have 4 valid queries (2 POST + 1 GET + 1 without method - only GET is filtered)
	// Actually we don't filter GET anymore, we filter based on having a query
	assert.Len(t, queries, 4)
	assert.Equal(t, 0, stats.MalformedLines)

	// Check first query
	assert.Equal(t, "up", queries[0].Query)
	expectedStart, _ := time.Parse(time.RFC3339, "2025-10-15T14:56:20Z")
	assert.Equal(t, expectedStart, queries[0].Start)
	expectedEnd, _ := time.Parse(time.RFC3339, "2025-10-15T14:56:23Z")
	assert.Equal(t, expectedEnd, queries[0].End)
	assert.Equal(t, 15*time.Second, queries[0].Step)

	// Check second query with Unix timestamps
	assert.Equal(t, "node_cpu_seconds_total", queries[1].Query)
	assert.NotZero(t, queries[1].Start)
	assert.NotZero(t, queries[1].End)
	assert.Equal(t, 30*time.Second, queries[1].Step)

	// Check third query (GET request - now included)
	assert.Equal(t, "should_be_skipped", queries[2].Query)

	// Check fourth query (one without method field)
	assert.Equal(t, "query_without_method", queries[3].Query)
}

func TestLoadQueryLogsFromFile_EmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	queryFile := filepath.Join(tmpDir, "empty.json")

	err := os.WriteFile(queryFile, []byte(""), 0644)
	require.NoError(t, err)

	queries, stats, err := loadQueryLogsFromFile(queryFile, nil)
	require.NoError(t, err)
	assert.Empty(t, queries)
	assert.Equal(t, 0, stats.MalformedLines)
}

func TestLoadQueryLogsFromFile_NonExistent(t *testing.T) {
	_, _, err := loadQueryLogsFromFile("/nonexistent/file.json", nil)
	assert.Error(t, err)
}

func TestLoadQueryLogsFromFile_MalformedLines(t *testing.T) {
	tmpDir := t.TempDir()
	queryFile := filepath.Join(tmpDir, "malformed.json")

	// Content with both valid and malformed lines
	content := `{"labels":{"param_query":"up"},"timestamp":"2025-10-15T14:56:24.337Z"}
this is not valid json
{"labels":{"param_query":"down"},"timestamp":"2025-10-15T14:56:24.437Z"}
{incomplete json
{"labels":{"param_query":"process_cpu_seconds_total"},"timestamp":"2025-10-15T14:56:24.537Z"}
`

	err := os.WriteFile(queryFile, []byte(content), 0644)
	require.NoError(t, err)

	queries, stats, err := loadQueryLogsFromFile(queryFile, nil)
	require.NoError(t, err)

	// Should have 3 valid queries
	assert.Len(t, queries, 3)
	assert.Equal(t, "up", queries[0].Query)
	assert.Equal(t, "down", queries[1].Query)
	assert.Equal(t, "process_cpu_seconds_total", queries[2].Query)

	// Should have counted 2 malformed lines
	assert.Equal(t, 2, stats.MalformedLines)
}

// TestSampleQueries tests the sampleQueries function
func TestSampleQueries(t *testing.T) {
	// Create test queries
	queries := make([]Query, 1000)
	for i := range queries {
		queries[i] = Query{
			QueryID: i,
			Query:   fmt.Sprintf("metric_%d", i),
		}
	}

	tests := []struct {
		name            string
		sampleFraction  float64
		seed            int64
		expectedCount   int
		checkDistribute bool
	}{
		{
			name:           "100% sample returns all",
			sampleFraction: 1.0,
			seed:           42,
			expectedCount:  1000,
		},
		{
			name:           "50% sample returns ~500",
			sampleFraction: 0.5,
			seed:           42,
			expectedCount:  500,
		},
		{
			name:           "10% sample returns ~100",
			sampleFraction: 0.1,
			seed:           42,
			expectedCount:  100,
		},
		{
			name:           "0% sample returns none",
			sampleFraction: 0.0,
			seed:           42,
			expectedCount:  0,
		},
		{
			name:            "samples are distributed across segments",
			sampleFraction:  0.1,
			seed:            123,
			expectedCount:   100,
			checkDistribute: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sampled := sampleQueries(queries, tt.sampleFraction, tt.seed)
			assert.Len(t, sampled, tt.expectedCount)

			if tt.checkDistribute && tt.expectedCount > 0 {
				// Check that samples are distributed across the query set
				// by verifying we have samples from different segments
				segmentSize := len(queries) / numSegments
				segmentsHit := make(map[int]bool)

				for _, q := range sampled {
					// Find original index by comparing query IDs
					for origIdx, orig := range queries {
						if q.QueryID == orig.QueryID {
							segment := origIdx / segmentSize
							segmentsHit[segment] = true
							break
						}
					}
				}

				// Should hit most segments (at least 80%)
				minSegments := int(float64(numSegments) * 0.8)
				assert.GreaterOrEqual(t, len(segmentsHit), minSegments,
					"samples should be distributed across segments")
			}
		})
	}
}

func TestSampleQueries_Deterministic(t *testing.T) {
	// Create test queries with distinct values
	queries := make([]Query, 1000)
	for i := range queries {
		queries[i] = Query{
			QueryID: i,
			Query:   fmt.Sprintf("metric_%d", i),
		}
	}

	// Same seed should produce same results
	sample1 := sampleQueries(queries, 0.5, 42)
	sample2 := sampleQueries(queries, 0.5, 42)

	assert.Len(t, sample1, len(sample2))
	for i := range sample1 {
		assert.Equal(t, sample1[i].QueryID, sample2[i].QueryID)
	}

	// Different seed should produce different results
	sample3 := sampleQueries(queries, 0.5, 99)
	assert.Len(t, sample3, len(sample1))

	// Convert samples to sets for comparison
	sample1Set := make(map[int]bool)
	for _, q := range sample1 {
		sample1Set[q.QueryID] = true
	}

	sample3Set := make(map[int]bool)
	for _, q := range sample3 {
		sample3Set[q.QueryID] = true
	}

	// Count queries that are in sample1 but not in sample3
	differentCount := 0
	for qid := range sample1Set {
		if !sample3Set[qid] {
			differentCount++
		}
	}

	// Should have some difference (at least 10% different)
	assert.Greater(t, differentCount, len(sample1)/10, "different seeds should produce different samples")
}

func TestSampleQueries_SmallSet(t *testing.T) {
	// Test with fewer queries than segments
	queries := make([]Query, 10)
	for i := range queries {
		queries[i] = Query{
			QueryID: i,
			Query:   fmt.Sprintf("metric_%d", i),
		}
	}

	sampled := sampleQueries(queries, 0.5, 42)
	assert.Greater(t, len(sampled), 0)
	assert.LessOrEqual(t, len(sampled), len(queries))
}
