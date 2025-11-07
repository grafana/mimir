// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/util"
)

func TestPlanCosts(t *testing.T) {
	ctx := context.Background()
	stats := newMockStatistics()

	type testCase struct {
		name                string
		indexMatchers       []*labels.Matcher
		scanMatchers        []*labels.Matcher
		cardinality         uint64
		indexCost           float64
		intersectionCost    float64
		seriesRetrievalCost float64
		filterCost          float64
		totalCost           float64
	}

	data := newCSVTestData(
		[]string{"testName", "indexMatchers", "scanMatchers", "cardinality", "indexCost", "intersectionCost", "seriesRetrievalCost", "filterCost", "totalCost"},
		filepath.Join("testdata", "plan_cost_test_cases.csv"),
		func(record []string) testCase {
			return testCase{
				name:                record[0],
				indexMatchers:       parseVectorSelector(t, record[1]),
				scanMatchers:        parseVectorSelector(t, record[2]),
				cardinality:         parseUint(t, record[3]),
				indexCost:           parseFloat(t, record[4]),
				intersectionCost:    parseFloat(t, record[5]),
				seriesRetrievalCost: parseFloat(t, record[6]),
				filterCost:          parseFloat(t, record[7]),
				totalCost:           parseFloat(t, record[8]),
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
				fmt.Sprintf("%.2f", tc.seriesRetrievalCost),
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

			// Create a scan-only plan with all matchers (no sharding)
			p := newScanOnlyPlan(ctx, stats, defaultCostConfig, allMatchers, nil, nil)

			// Use index for the first N predicates (corresponding to index matchers)
			for i := 0; i < len(tc.indexMatchers); i++ {
				p = p.UseIndexFor(i)
			}

			const errorMsg = "Expected cost doesn't match actual cost. If you want to keep the new cost, set `writeOutNewCost = true` to persist the new cost in /testdata"
			const delta = 1e-5
			assert.Equal(t, int(tc.cardinality), int(p.FinalCardinality()), errorMsg)
			assert.InDelta(t, tc.indexCost, p.indexLookupCost(), delta, errorMsg)
			assert.InDelta(t, tc.intersectionCost, p.intersectionCost(), delta, errorMsg)
			assert.InDelta(t, tc.seriesRetrievalCost, p.seriesRetrievalCost(), delta, errorMsg)
			assert.InDelta(t, tc.filterCost, p.filterCost(), delta, errorMsg)
			assert.InDelta(t, tc.totalCost, p.TotalCost(), delta, errorMsg)

			assert.GreaterOrEqual(t, tc.indexCost, 0.0, "can't have negative costs")
			assert.GreaterOrEqual(t, tc.intersectionCost, 0.0, "can't have negative costs")
			assert.GreaterOrEqual(t, tc.seriesRetrievalCost, 0.0, "can't have negative costs")
			assert.GreaterOrEqual(t, tc.filterCost, 0.0, "can't have negative costs")
			assert.GreaterOrEqual(t, tc.totalCost, 0.0, "can't have negative costs")

			testCases[tcIdx].cardinality = p.FinalCardinality()
			testCases[tcIdx].indexCost = p.indexLookupCost()
			testCases[tcIdx].intersectionCost = p.intersectionCost()
			testCases[tcIdx].seriesRetrievalCost = p.seriesRetrievalCost()
			testCases[tcIdx].filterCost = p.filterCost()
			testCases[tcIdx].totalCost = p.TotalCost()
		})
	}
}

// Benchmark iterating over postings lists of different sizes
func BenchmarkIteratePostings(b *testing.B) {
	benchmarks := []struct {
		name string
		size int
	}{
		{"size=128", 128},
		{"size=128K", 128 * 1024},
		{"size=1M", 1024 * 1024},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			// Create and populate mock index reader in benchmark setup
			mockReader := newMockIndexReader()
			for i := 0; i < bm.size; i++ {
				// Create diverse labels for each series
				labelName := fmt.Sprintf("label_%d", i%10) // 10 different label names
				labelValue := fmt.Sprintf("value_%d", i)
				ls := labels.FromStrings("__name__", fmt.Sprintf("metric_%d", i), labelName, labelValue)
				mockReader.add(storage.SeriesRef(i+1), ls) // SeriesRef 0 is invalid, start from 1
			}

			for _, sharded := range []bool{false, true} {
				b.Run("sharded="+strconv.FormatBool(sharded), func(b *testing.B) {
					startTime := time.Now()
					b.ResetTimer()
					var count int
					for i := 0; i < b.N; i++ {
						// Get all postings by using the special empty label name
						labelName, labelValue := index.AllPostingsKey()
						allPostings, err := mockReader.Postings(context.Background(), labelName, labelValue)
						if err != nil {
							b.Fatal(err)
						}

						p := allPostings
						if sharded {
							p = mockReader.ShardedPostings(allPostings, 0, 2)
						}

						count = 0
						for p.Next() {
							_ = p.At()
							count++
						}
					}
					b.ReportMetric(float64(time.Since(startTime).Nanoseconds())/float64(b.N*bm.size), "ns/posting")
				})
			}
		})
	}
}
