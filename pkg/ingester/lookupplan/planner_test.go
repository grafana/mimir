// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/util"
)

type plannerTestCase struct {
	name                  string
	inputMatchers         []*labels.Matcher
	expectedIndexMatchers []*labels.Matcher
	expectedScanMatchers  []*labels.Matcher
	queryShard            string // Format: "shardIndex_of_shardCount" (e.g., "1_of_16") or empty for no sharding
}

func TestCostBasedPlannerPlanIndexLookup(t *testing.T) {
	ctx := context.Background()

	data := newCSVTestData(
		[]string{"queryShard", "testName", "inputMatchers", "expectedIndexMatchers", "expectedScanMatchers"},
		filepath.Join("testdata", "planner_test_cases.csv"),
		func(record []string) plannerTestCase {
			return plannerTestCase{
				queryShard:            record[0],
				name:                  record[1],
				inputMatchers:         parseVectorSelector(t, record[2]),
				expectedIndexMatchers: parseVectorSelector(t, record[3]),
				expectedScanMatchers:  parseVectorSelector(t, record[4]),
			}
		},
		func(tc plannerTestCase) []string {
			return []string{
				tc.queryShard,
				tc.name,
				fmt.Sprintf("{%s}", util.MatchersStringer(tc.inputMatchers)),
				fmt.Sprintf("{%s}", util.MatchersStringer(tc.expectedIndexMatchers)),
				fmt.Sprintf("{%s}", util.MatchersStringer(tc.expectedScanMatchers)),
			}
		},
	)

	testCases := data.ParseTestCases(t)

	stats := newHighCardinalityMockStatistics()
	metrics := NewMetrics(nil).ForUser("test-user")
	planner := NewCostBasedPlanner(metrics, stats, defaultCostConfig)

	const writeOutNewResults = false
	if writeOutNewResults {
		t.Cleanup(func() { data.WriteTestCases(t, testCases) })
	}

	for tcIdx, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a basic lookup plan with the input matchers
			inputPlan := &basicLookupPlan{
				indexMatchers: tc.inputMatchers,
			}

			// Parse query shard if specified
			var hints *storage.SelectHints
			if tc.queryShard != "" {
				shardIndex, shardCount, err := sharding.ParseShardIDLabelValue(tc.queryShard)
				require.NoError(t, err, "failed to parse queryShard: %q", tc.queryShard)
				hints = &storage.SelectHints{
					ShardIndex: shardIndex,
					ShardCount: shardCount,
				}
			}

			result, err := planner.PlanIndexLookup(ctx, inputPlan, hints)
			require.NoError(t, err)
			require.NotNil(t, result)

			require.NotEmpty(t, tc.expectedIndexMatchers, "PostingsForMatchers doesn't support empty index matchers, your test case is wrong")

			// Verify that the partitioning is correct
			assert.ElementsMatch(t, matchersStrings(tc.expectedIndexMatchers), matchersStrings(result.IndexMatchers()))
			assert.ElementsMatch(t, matchersStrings(tc.expectedScanMatchers), matchersStrings(result.ScanMatchers()))

			// Verify that all input matchers are accounted for
			var allActualMatchers []string
			allActualMatchers = append(allActualMatchers, matchersStrings(result.IndexMatchers())...)
			allActualMatchers = append(allActualMatchers, matchersStrings(result.ScanMatchers())...)
			assert.ElementsMatch(t, matchersStrings(tc.inputMatchers), allActualMatchers)

			testCases[tcIdx].expectedIndexMatchers = result.IndexMatchers()
			testCases[tcIdx].expectedScanMatchers = result.ScanMatchers()
		})
	}
}

func BenchmarkCostBasedPlannerPlanIndexLookup(b *testing.B) {
	ctx := context.Background()

	data := newCSVTestData(
		[]string{"queryShard", "testName", "inputMatchers", "expectedIndexMatchers", "expectedScanMatchers"},
		filepath.Join("testdata", "planner_test_cases.csv"),
		func(record []string) plannerTestCase {
			return plannerTestCase{
				queryShard:            record[0],
				name:                  record[1],
				inputMatchers:         parseVectorSelector(b, record[2]),
				expectedIndexMatchers: parseVectorSelector(b, record[3]),
				expectedScanMatchers:  parseVectorSelector(b, record[4]),
			}
		},
		func(tc plannerTestCase) []string {
			require.FailNow(b, "benchmark shouldn't change test cases")
			return nil
		},
	)

	// Load test cases for benchmarking
	testCases := data.ParseTestCases(b)

	stats := newHighCardinalityMockStatistics()
	metrics := NewMetrics(nil).ForUser("test-user")
	planner := NewCostBasedPlanner(metrics, stats, defaultCostConfig)

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			// Create a basic lookup plan with the input matchers
			inputPlan := &basicLookupPlan{
				indexMatchers: tc.inputMatchers,
			}

			// Parse query shard if specified
			var hints *storage.SelectHints
			if tc.queryShard != "" {
				shardIndex, shardCount, err := sharding.ParseShardIDLabelValue(tc.queryShard)
				require.NoError(b, err, "failed to parse queryShard: %q", tc.queryShard)
				hints = &storage.SelectHints{
					ShardIndex: shardIndex,
					ShardCount: shardCount,
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := planner.PlanIndexLookup(ctx, inputPlan, hints)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func matchersStrings(ms []*labels.Matcher) []string {
	matchers := make([]string, 0, len(ms))
	for _, m := range ms {
		matchers = append(matchers, m.String())
	}
	return matchers
}

func TestCostBasedPlannerWithManyMatchers(t *testing.T) {
	ctx := context.Background()
	stats := newHighCardinalityMockStatistics()
	metrics := NewMetrics(nil).ForUser("test-user")
	planner := NewCostBasedPlanner(metrics, stats, defaultCostConfig)

	// Generate 1000 matchers with different label names and values
	matchers := make([]*labels.Matcher, 1000)
	for i := 0; i < 1000; i++ {
		matchers[i] = labels.MustNewMatcher(labels.MatchEqual, fmt.Sprintf("label_%d", i), fmt.Sprintf("value_%d", i))
	}

	inputPlan := &basicLookupPlan{
		indexMatchers: matchers,
	}

	start := time.Now()
	result, err := planner.PlanIndexLookup(ctx, inputPlan, nil)
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Less(t, elapsed.Seconds(), 30.0, "Planning with 1000 matchers should complete in less than 30 seconds, took %v", elapsed)

	// Verify all matchers are preserved
	var allResultMatchers []string
	allResultMatchers = append(allResultMatchers, matchersStrings(result.IndexMatchers())...)
	allResultMatchers = append(allResultMatchers, matchersStrings(result.ScanMatchers())...)
	assert.ElementsMatch(t, matchersStrings(matchers), allResultMatchers)
	assert.NotEmpty(t, result.IndexMatchers(), "Result should have index matchers")
}

// basicLookupPlan is a simple implementation of index.LookupPlan for testing
type basicLookupPlan struct {
	indexMatchers []*labels.Matcher
	scanMatchers  []*labels.Matcher
}

func (p *basicLookupPlan) IndexMatchers() []*labels.Matcher {
	return p.indexMatchers
}

func (p *basicLookupPlan) ScanMatchers() []*labels.Matcher {
	return p.scanMatchers
}

func TestCostBasedPlannerPreservesAllMatchers(t *testing.T) {
	ctx := context.Background()
	stats := newHighCardinalityMockStatistics()
	metrics := NewMetrics(nil).ForUser("test-user")
	planner := NewCostBasedPlanner(metrics, stats, defaultCostConfig)

	t.Run("mixed_index_and_scan_matchers", func(t *testing.T) {
		// Create a plan that already has both index and scan matchers
		indexMatchers := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "http_requests_total"),
			labels.MustNewMatcher(labels.MatchEqual, "method", "GET"),
		}
		scanMatchers := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "status", "200"),
			labels.MustNewMatcher(labels.MatchRegexp, "instance", "web-.*"),
		}

		inputPlan := &basicLookupPlan{
			indexMatchers: indexMatchers,
			scanMatchers:  scanMatchers,
		}

		result, err := planner.PlanIndexLookup(ctx, inputPlan, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify all original matchers are preserved (may be repartitioned)
		var allOriginalMatchers []string
		allOriginalMatchers = append(allOriginalMatchers, matchersStrings(indexMatchers)...)
		allOriginalMatchers = append(allOriginalMatchers, matchersStrings(scanMatchers)...)

		var allResultMatchers []string
		allResultMatchers = append(allResultMatchers, matchersStrings(result.IndexMatchers())...)
		allResultMatchers = append(allResultMatchers, matchersStrings(result.ScanMatchers())...)

		assert.ElementsMatch(t, allOriginalMatchers, allResultMatchers, "Planner should preserve all matchers, just potentially repartition them")

		assert.NotEmpty(t, result.IndexMatchers(), "Result should have index matchers")
	})

	t.Run("scan_only_input_gets_optimized", func(t *testing.T) {
		// Create a plan that only has scan matchers
		scanOnlyMatchers := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "cpu_usage_percent"),
			labels.MustNewMatcher(labels.MatchEqual, "job", "prometheus"),
			labels.MustNewMatcher(labels.MatchEqual, "method", "POST"),
		}

		inputPlan := &basicLookupPlan{
			indexMatchers: []*labels.Matcher{}, // No index matchers
			scanMatchers:  scanOnlyMatchers,
		}

		result, err := planner.PlanIndexLookup(ctx, inputPlan, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify all original matchers are preserved
		allOriginalMatchers := matchersStrings(scanOnlyMatchers)
		var allResultMatchers []string
		allResultMatchers = append(allResultMatchers, matchersStrings(result.IndexMatchers())...)
		allResultMatchers = append(allResultMatchers, matchersStrings(result.ScanMatchers())...)

		assert.ElementsMatch(t, allOriginalMatchers, allResultMatchers, "Planner should preserve all matchers from scan-only input")
	})
}

func TestCostBasedPlannerPrefersIndexMatchersOverCheapestPlan(t *testing.T) {
	ctx := context.Background()
	stats := newSingleValueStatistics()
	metrics := NewMetrics(nil).ForUser("test-user")
	planner := NewCostBasedPlanner(metrics, stats, defaultCostConfig)

	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "label", "value"),
	}

	inputPlan := &basicLookupPlan{
		indexMatchers: matchers,
		scanMatchers:  []*labels.Matcher{},
	}

	result, err := planner.PlanIndexLookup(ctx, inputPlan, nil)
	assert.NoError(t, err)
	assert.NotEmpty(t, result.IndexMatchers(), "Result should have index matchers despite potentially higher cost")
}

func TestCostBasedPlannerDoesntAllowNoMatcherLookups(t *testing.T) {
	ctx := context.Background()
	stats := newMockStatistics()
	metrics := NewMetrics(nil).ForUser("test-user")
	planner := NewCostBasedPlanner(metrics, stats, defaultCostConfig)

	result, err := planner.PlanIndexLookup(ctx, &basicLookupPlan{}, nil)
	assert.ErrorContains(t, err, "no plan with index matchers found")
	assert.Nil(t, result, "Result should be nil when no matchers are provided")
}

func TestCostBasedPlannerWithDisabledPlanning(t *testing.T) {
	stats := newHighCardinalityMockStatistics()
	metrics := NewMetrics(nil).ForUser("test-user")
	planner := NewCostBasedPlanner(metrics, stats, defaultCostConfig)

	inputPlan := &basicLookupPlan{
		indexMatchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "cpu_usage_percent"),
			labels.MustNewMatcher(labels.MatchEqual, "job", "prometheus"),
			labels.MustNewMatcher(labels.MatchNotEqual, "non_existent", ""), // this would normally be set as a scan matcher since it doesn't exist
		},
		scanMatchers: []*labels.Matcher{},
	}

	t.Run("disabled_planning_returns_input_plan", func(t *testing.T) {
		ctx := ContextWithDisabledPlanning(context.Background())
		result, err := planner.PlanIndexLookup(ctx, inputPlan, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		// The result should be exactly the same as the input plan
		assert.Equal(t, inputPlan, result)
		assert.Equal(t, matchersStrings(inputPlan.IndexMatchers()), matchersStrings(result.IndexMatchers()))
		assert.Equal(t, matchersStrings(inputPlan.ScanMatchers()), matchersStrings(result.ScanMatchers()))
	})

	t.Run("disabled_planning_works_with_mixed_matchers", func(t *testing.T) {
		mixedPlan := &basicLookupPlan{
			indexMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "cpu_usage_percent"),
			},
			scanMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "prometheus"),
				labels.MustNewMatcher(labels.MatchRegexp, "instance", "localhost.*"),
			},
		}

		ctx := ContextWithDisabledPlanning(context.Background())
		result, err := planner.PlanIndexLookup(ctx, mixedPlan, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		// The result should be exactly the same as the input plan
		assert.Equal(t, mixedPlan, result)
		assert.Equal(t, matchersStrings(mixedPlan.IndexMatchers()), matchersStrings(result.IndexMatchers()))
		assert.Equal(t, matchersStrings(mixedPlan.ScanMatchers()), matchersStrings(result.ScanMatchers()))
	})

	t.Run("disabled_planning_works_with_empty_plan", func(t *testing.T) {
		emptyPlan := &basicLookupPlan{
			indexMatchers: []*labels.Matcher{},
			scanMatchers:  []*labels.Matcher{},
		}

		ctx := ContextWithDisabledPlanning(context.Background())
		result, err := planner.PlanIndexLookup(ctx, emptyPlan, nil)
		require.NoError(t, err)
		require.NotNil(t, result)

		// The result should be exactly the same as the input plan, even if empty
		assert.Equal(t, emptyPlan, result)
	})
}
