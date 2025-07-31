// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util"
)

func TestCostBasedPlannerPlanIndexLookup(t *testing.T) {
	ctx := context.Background()

	type testCase struct {
		inputMatchers         []*labels.Matcher
		expectedIndexMatchers []*labels.Matcher
		expectedScanMatchers  []*labels.Matcher
	}

	data := newCSVTestData(
		[]string{"inputMatchers", "expectedIndexMatchers", "expectedScanMatchers"},
		filepath.Join("testdata", "planner_test_cases.csv"),
		func(record []string) testCase {
			return testCase{
				inputMatchers:         parseVectorSelector(t, record[0]),
				expectedIndexMatchers: parseVectorSelector(t, record[1]),
				expectedScanMatchers:  parseVectorSelector(t, record[2]),
			}
		},
		func(tc testCase) []string {
			return []string{
				fmt.Sprintf("{%s}", util.MatchersStringer(tc.inputMatchers)),
				fmt.Sprintf("{%s}", util.MatchersStringer(tc.expectedIndexMatchers)),
				fmt.Sprintf("{%s}", util.MatchersStringer(tc.expectedScanMatchers)),
			}
		},
	)

	testCases := data.ParseTestCases(t)

	stats := newHighCardinalityMockStatistics()
	metrics := NewMetrics(nil)
	planner := NewCostBasedPlanner(metrics, stats)

	const writeOutNewResults = false
	if writeOutNewResults {
		t.Cleanup(func() { data.WriteTestCases(t, testCases) })
	}

	for tcIdx, tc := range testCases {
		t.Run(fmt.Sprintf("%s", tc.inputMatchers), func(t *testing.T) {
			// Create a basic lookup plan with the input matchers
			inputPlan := &basicLookupPlan{
				indexMatchers: tc.inputMatchers,
			}

			result, err := planner.PlanIndexLookup(ctx, inputPlan, 0, 0)
			require.NoError(t, err)
			require.NotNil(t, result)

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

	t.Run("error_statistics", func(t *testing.T) {
		errorStats := newErrorMockStatistics()
		metrics := NewMetrics(nil)
		planner := NewCostBasedPlanner(metrics, errorStats)

		for _, tc := range testCases {
			// Skip empty matcher case since it doesn't call statistics methods
			if len(tc.inputMatchers) == 0 {
				continue
			}

			t.Run(fmt.Sprintf("%s", tc.inputMatchers), func(t *testing.T) {
				// Create a basic lookup plan with the input matchers
				inputPlan := &basicLookupPlan{
					indexMatchers: tc.inputMatchers,
				}

				result, err := planner.PlanIndexLookup(ctx, inputPlan, 0, 0)
				// Should get an error back due to statistics errors
				assert.Error(t, err)
				assert.Nil(t, result)
			})
		}
	})
}

func matchersStrings(ms []*labels.Matcher) []string {
	matchers := make([]string, 0, len(ms))
	for _, m := range ms {
		matchers = append(matchers, m.String())
	}
	return matchers
}

func TestCostBasedPlannerTooManyMatchers(t *testing.T) {
	ctx := context.Background()
	stats := newMockStatistics()
	metrics := NewMetrics(nil)
	planner := NewCostBasedPlanner(metrics, stats)

	// Create more than 10 matchers to trigger the limit
	var matchers []*labels.Matcher
	for i := 0; i < 12; i++ {
		matcher := labels.MustNewMatcher(labels.MatchEqual, fmt.Sprintf("label_%d", i), "value")
		matchers = append(matchers, matcher)
	}

	inputPlan := &basicLookupPlan{
		indexMatchers: matchers,
		scanMatchers:  []*labels.Matcher{},
	}

	result, err := planner.PlanIndexLookup(ctx, inputPlan, 0, 0)

	// Should return the original plan without error (aborted early)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, inputPlan, result)
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
	metrics := NewMetrics(nil)
	planner := NewCostBasedPlanner(metrics, stats)

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

		result, err := planner.PlanIndexLookup(ctx, inputPlan, 0, 0)
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

		// Verify we have both index and scan matchers in result (planner should optimize)
		assert.NotEmpty(t, result.IndexMatchers(), "Result should have index matchers")
		assert.NotEmpty(t, result.ScanMatchers(), "Result should have scan matchers")
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

		result, err := planner.PlanIndexLookup(ctx, inputPlan, 0, 0)
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
