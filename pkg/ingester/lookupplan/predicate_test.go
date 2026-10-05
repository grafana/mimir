// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
)

func TestFilterCost(t *testing.T) {
	ctx := t.Context()
	stats := newMockStatistics()

	type testCase struct {
		matcher      *labels.Matcher
		seriesCount  uint64
		expectedCost float64
	}

	data := newCSVTestData(
		[]string{"matcher", "seriesCount", "expectedCost"},
		filepath.Join("testdata", "filter_cost_test_cases.csv"),
		func(record []string) testCase {
			return testCase{
				matcher:      parseMatcher(t, record[0]),
				seriesCount:  parseUint(t, record[1]),
				expectedCost: parseFloat(t, record[2]),
			}
		},
		func(tc testCase) []string {
			return []string{tc.matcher.String(), fmt.Sprintf("%d", tc.seriesCount), formatFloat(tc.expectedCost)}
		},
	)

	testCases := data.ParseTestCases(t)

	const writeOutNewResults = false
	if writeOutNewResults {
		t.Cleanup(func() { data.WriteTestCases(t, testCases) })
	}

	for tcIdx, tc := range testCases {
		testName := fmt.Sprintf("%d_series_%s", tc.seriesCount, tc.matcher)
		t.Run(testName, func(t *testing.T) {
			pred := newPlanPredicate(ctx, tc.matcher, stats, defaultCostConfig)

			cost := pred.filterCost(tc.seriesCount)
			assert.GreaterOrEqual(t, cost, 0.0, "can't have negative costs")
			assert.Equal(t, tc.expectedCost, cost)
			testCases[tcIdx].expectedCost = cost
		})
	}
}

func TestIndexLookupCost(t *testing.T) {
	ctx := t.Context()
	stats := newMockStatistics()

	type testCase struct {
		matcher      *labels.Matcher
		expectedCost float64
		actualCost   float64
	}

	data := newCSVTestData(
		[]string{"matcher", "expectedCost"},
		filepath.Join("testdata", "index_lookup_cost_test_cases.csv"),
		func(record []string) testCase {
			return testCase{
				matcher:      parseMatcher(t, record[0]),
				expectedCost: parseFloat(t, record[1]),
			}
		},
		func(tc testCase) []string {
			return []string{tc.matcher.String(), formatFloat(tc.actualCost)}
		},
	)

	testCases := data.ParseTestCases(t)

	const writeOutNewCost = false
	if writeOutNewCost {
		t.Cleanup(func() { data.WriteTestCases(t, testCases) })
	}

	for tcIdx, tc := range testCases {
		t.Run(tc.matcher.String(), func(t *testing.T) {
			pred := newPlanPredicate(ctx, tc.matcher, stats, defaultCostConfig)

			actualCost := pred.indexLookupCost()
			assert.GreaterOrEqual(t, actualCost, 0.0, "can't have negative cost")
			assert.Equal(t, tc.expectedCost, actualCost, "Expected cost doesn't match actual cost. If you want to keep the new cost, set `writeOutNewCost = true` to persist the new cost in /testdata")
			testCases[tcIdx].actualCost = actualCost
		})
	}
}

func TestIndexLookupCost_CostConfig(t *testing.T) {
	ctx := t.Context()
	stats := newMockStatistics()

	// The method label has these unique values in the mock: GET, POST, PUT, DELETE.
	// method=~"P.*" has prefix "P", selectivity=0.1, singleMatchCost=1.0.
	//
	// indexLookupCost = indexScanCost + RetrievedPostingListCost × uniqueVals × selectivity
	//
	// Without PrefixScanOptimisation (ingester):
	//   indexScanCost = singleMatchCost × uniqueVals = 1 × 4 = 4
	//   indexLookupCost = 4 + 10 × 4 × 0.1 = 8
	//
	// With PrefixScanOptimisation (store-gateway):
	//   indexScanCost = singleMatchCost × uniqueVals × selectivity = 1 × 4 × 0.1 = 0.4
	//   indexLookupCost = 0.4 + 10 × 4 × 0.1 = 4.4
	m := labels.MustNewMatcher(labels.MatchRegexp, "method", "P.*")

	tests := []struct {
		name          string
		newConfigFunc func() CostConfig
		assertionFunc func(t *testing.T, config CostConfig)
	}{
		{
			name:          "default config",
			newConfigFunc: func() CostConfig { return defaultCostConfig },
			assertionFunc: func(t *testing.T, testConfig CostConfig) {
				pred := newPlanPredicate(ctx, m, stats, testConfig)
				assert.Equal(t, 8.0, pred.indexLookupCost())
			},
		},
		{
			name: "with PrefixScanOptimisation",
			newConfigFunc: func() CostConfig {
				config := defaultCostConfig
				config.PrefixScanOptimisation = true
				return config
			},
			assertionFunc: func(t *testing.T, testConfig CostConfig) {
				pred := newPlanPredicate(ctx, m, stats, testConfig)
				assert.Equal(t, 4.4, pred.indexLookupCost())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := tt.newConfigFunc()
			tt.assertionFunc(t, config)
		})
	}
}

func TestCardinalityEstimation(t *testing.T) {
	ctx := t.Context()
	stats := newMockStatistics()

	type testCase struct {
		matcher             *labels.Matcher
		expectedCardinality uint64
		deltaTolerance      float64
		actualCardinality   uint64
	}

	data := newCSVTestData(
		[]string{"matcher", "expectedCardinality", "deltaTolerance"},
		filepath.Join("testdata", "cardinality_estimation_test_cases.csv"),
		func(record []string) testCase {
			return testCase{
				matcher:             parseMatcher(t, record[0]),
				expectedCardinality: parseUint(t, record[1]),
				deltaTolerance:      parseFloat(t, record[2]),
			}
		},
		func(tc testCase) []string {
			return []string{tc.matcher.String(), fmt.Sprintf("%.0f", float64(tc.actualCardinality)), fmt.Sprintf("%.5f", tc.deltaTolerance)}
		},
	)

	testCases := data.ParseTestCases(t)

	const writeOutNewCardinality = false
	if writeOutNewCardinality {
		t.Cleanup(func() { data.WriteTestCases(t, testCases) })
	}

	for tcIdx, tc := range testCases {
		t.Run(tc.matcher.String(), func(t *testing.T) {
			pred := newPlanPredicate(ctx, tc.matcher, stats, defaultCostConfig)
			actualCardinality := pred.cardinality

			if tc.deltaTolerance > 0 {
				assert.InDelta(t, tc.expectedCardinality, actualCardinality, tc.deltaTolerance)
			} else {
				assert.Equal(t, int(tc.expectedCardinality), int(actualCardinality), "Expected cardinality doesn't match actual cardinality. If you want to keep the new cardinality, set `writeOutNewCardinality = true` to persist the new cardinality in testdata")
			}
			testCases[tcIdx].actualCardinality = actualCardinality
		})
	}
}
