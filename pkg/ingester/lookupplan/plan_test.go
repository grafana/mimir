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

func TestPlanCosts(t *testing.T) {
	ctx := context.Background()
	stats := newMockStatistics()

	type testCase struct {
		name             string
		indexMatchers    []*labels.Matcher
		scanMatchers     []*labels.Matcher
		cardinality      uint64
		indexCost        float64
		intersectionCost float64
		filterCost       float64
		totalCost        float64
	}

	data := newCSVTestData(
		[]string{"testName", "indexMatchers", "scanMatchers", "cardinality", "indexCost", "intersectionCost", "filterCost", "totalCost"},
		filepath.Join("testdata", "plan_cost_test_cases.csv"),
		func(record []string) testCase {
			return testCase{
				name:             record[0],
				indexMatchers:    parseVectorSelector(t, record[1]),
				scanMatchers:     parseVectorSelector(t, record[2]),
				cardinality:      parseUint(t, record[3]),
				indexCost:        parseFloat(t, record[4]),
				intersectionCost: parseFloat(t, record[5]),
				filterCost:       parseFloat(t, record[6]),
				totalCost:        parseFloat(t, record[7]),
			}
		},
		func(tc testCase) []string {
			return []string{
				tc.name,
				fmt.Sprintf("{%s}", util.MatchersStringer(tc.indexMatchers)),
				fmt.Sprintf("{%s}", util.MatchersStringer(tc.scanMatchers)),
				fmt.Sprintf("%d", tc.cardinality),
				fmt.Sprintf("%.2f", tc.indexCost),
				fmt.Sprintf("%.2f", tc.intersectionCost),
				fmt.Sprintf("%.2f", tc.filterCost),
				fmt.Sprintf("%.2f", tc.totalCost),
			}
		},
	)

	testCases := data.ParseTestCases(t)

	const writeOutNewCost = false
	if writeOutNewCost {
		t.Cleanup(func() { data.WriteTestCases(t, testCases) })
	}

	for tcIdx, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var allMatchers []*labels.Matcher

			allMatchers = append(allMatchers, tc.indexMatchers...)
			allMatchers = append(allMatchers, tc.scanMatchers...)

			// Create a scan-only plan with all matchers
			p, err := newScanOnlyPlan(ctx, allMatchers, stats)
			require.NoError(t, err)

			// Use index for the first N predicates (corresponding to index matchers)
			for i := 0; i < len(tc.indexMatchers); i++ {
				p = p.useIndexFor(i)
			}

			const errorMsg = "Expected cost doesn't match actual cost. If you want to keep the new cost, set `writeOutNewCost = true` to persist the new cost in /testdata"
			const delta = 1e-5
			assert.Equal(t, int(tc.cardinality), int(p.cardinality()), errorMsg)
			assert.InDelta(t, tc.indexCost, p.indexLookupCost(), delta, errorMsg)
			assert.InDelta(t, tc.intersectionCost, p.intersectionCost(), delta, errorMsg)
			assert.InDelta(t, tc.filterCost, p.filterCost(), delta, errorMsg)
			assert.InDelta(t, tc.totalCost, p.totalCost(), delta, errorMsg)

			assert.GreaterOrEqual(t, tc.indexCost, 0.0, "can't have negative costs")
			assert.GreaterOrEqual(t, tc.intersectionCost, 0.0, "can't have negative costs")
			assert.GreaterOrEqual(t, tc.filterCost, 0.0, "can't have negative costs")
			assert.GreaterOrEqual(t, tc.totalCost, 0.0, "can't have negative costs")

			testCases[tcIdx].cardinality = p.cardinality()
			testCases[tcIdx].indexCost = p.indexLookupCost()
			testCases[tcIdx].intersectionCost = p.intersectionCost()
			testCases[tcIdx].filterCost = p.filterCost()
			testCases[tcIdx].totalCost = p.totalCost()
		})
	}
}
