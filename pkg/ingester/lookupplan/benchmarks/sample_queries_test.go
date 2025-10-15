// SPDX-License-Identifier: AGPL-3.0-only

package benchmarks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSampleQueries(t *testing.T) {
	// Create test queries
	queries := make([]vectorSelectorQuery, 1000)
	for i := range queries {
		queries[i] = vectorSelectorQuery{
			originalQuery: &Query{Query: string(rune(i))},
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
					// Find original index by comparing query strings
					for origIdx, orig := range queries {
						if q.originalQuery == orig.originalQuery {
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
	queries := make([]vectorSelectorQuery, 1000)
	for i := range queries {
		queries[i] = vectorSelectorQuery{
			originalQuery: &Query{Query: string(rune(i))},
		}
	}

	// Same seed should produce same results
	sample1 := sampleQueries(queries, 0.5, 42)
	sample2 := sampleQueries(queries, 0.5, 42)

	assert.Len(t, sample1, len(sample2))
	for i := range sample1 {
		assert.Equal(t, sample1[i].originalQuery, sample2[i].originalQuery)
	}

	// Different seed should produce different results
	sample3 := sampleQueries(queries, 0.5, 99)
	assert.Len(t, sample3, len(sample1))

	// Convert samples to sets for comparison
	sample1Set := make(map[*Query]bool)
	for _, q := range sample1 {
		sample1Set[q.originalQuery] = true
	}

	sample3Set := make(map[*Query]bool)
	for _, q := range sample3 {
		sample3Set[q.originalQuery] = true
	}

	// Count queries that are in sample1 but not in sample3
	differentCount := 0
	for q := range sample1Set {
		if !sample3Set[q] {
			differentCount++
		}
	}

	// Should have some difference (at least 10% different)
	assert.Greater(t, differentCount, len(sample1)/10, "different seeds should produce different samples")
}

func TestSampleQueries_SmallSet(t *testing.T) {
	// Test with fewer queries than segments
	queries := make([]vectorSelectorQuery, 10)
	for i := range queries {
		queries[i] = vectorSelectorQuery{
			originalQuery: &Query{Query: string(rune(i))},
		}
	}

	sampled := sampleQueries(queries, 0.5, 42)
	assert.Greater(t, len(sampled), 0)
	assert.LessOrEqual(t, len(sampled), len(queries))
}

func TestSampleQueries_EdgeCases(t *testing.T) {
	queries := make([]vectorSelectorQuery, 100)
	for i := range queries {
		queries[i] = vectorSelectorQuery{
			originalQuery: &Query{Query: string(rune(i))},
		}
	}

	// Empty input
	empty := sampleQueries([]vectorSelectorQuery{}, 0.5, 42)
	assert.Empty(t, empty)

	// Fraction > 1.0 should return all
	all := sampleQueries(queries, 1.5, 42)
	assert.Len(t, all, len(queries))

	// Negative fraction should return none
	none := sampleQueries(queries, -0.1, 42)
	assert.Nil(t, none)
}
