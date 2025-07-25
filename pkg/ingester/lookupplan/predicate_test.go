// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"
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

	mapper := newCSVMapper(
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
			return []string{tc.matcher.String(), fmt.Sprintf("%d", tc.seriesCount), fmt.Sprintf("%.1f", tc.expectedCost)}
		},
	)

	testCases := mapper.ParseTestCases(t)

	const writeOutNewResults = false
	if writeOutNewResults {
		t.Cleanup(func() { mapper.WriteTestCases(t, testCases) })
	}

	for _, tc := range testCases {
		testName := fmt.Sprintf("%d_series_%s", tc.seriesCount, tc.matcher)
		t.Run(testName, func(t *testing.T) {
			pred, err := newPlanPredicate(ctx, tc.matcher, stats)
			assert.NoError(t, err)

			cost := pred.filterCost(tc.seriesCount)
			assert.GreaterOrEqual(t, cost, 0.0, "can't have negative costs")
			assert.Equal(t, tc.expectedCost, cost)
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

	mapper := newCSVMapper(
		[]string{"matcher", "expectedCost"},
		filepath.Join("testdata", "index_lookup_cost_test_cases.csv"),
		func(record []string) testCase {
			return testCase{
				matcher:      parseMatcher(t, record[0]),
				expectedCost: parseFloat(t, record[1]),
			}
		},
		func(tc testCase) []string {
			return []string{tc.matcher.String(), fmt.Sprintf("%.1f", tc.actualCost)}
		},
	)

	testCases := mapper.ParseTestCases(t)

	const writeOutNewCost = false
	if writeOutNewCost {
		t.Cleanup(func() { mapper.WriteTestCases(t, testCases) })
	}

	for tcIdx, tc := range testCases {
		t.Run(tc.matcher.String(), func(t *testing.T) {
			pred, err := newPlanPredicate(ctx, tc.matcher, stats)
			assert.NoError(t, err)

			actualCost := pred.indexLookupCost()
			assert.GreaterOrEqual(t, actualCost, 0.0, "can't have negative cost")
			assert.Equal(t, tc.expectedCost, actualCost, "Expected cost doesn't match actual cost. If you want to keep the new cost, set `writeOutNewCost = true` to persist the new cost in /testdata")
			testCases[tcIdx].actualCost = actualCost
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

	mapper := newCSVMapper(
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

	testCases := mapper.ParseTestCases(t)

	const writeOutNewCardinality = false
	if writeOutNewCardinality {
		t.Cleanup(func() { mapper.WriteTestCases(t, testCases) })
	}

	for tcIdx, tc := range testCases {
		t.Run(tc.matcher.String(), func(t *testing.T) {
			pred, err := newPlanPredicate(ctx, tc.matcher, stats)
			assert.NoError(t, err)

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

// mockStatistics implements the Statistics interface with hardcoded data for testing
type mockStatistics struct {
	// seriesPerValue maps label name -> label value -> number of series
	seriesPerValue map[string]map[string]uint64
	totalSeries    uint64
}

func newMockStatistics() *mockStatistics {
	return &mockStatistics{
		seriesPerValue: map[string]map[string]uint64{
			"__name__": {
				"http_requests_total": 1000,
				"cpu_usage_percent":   500,
				"memory_usage_bytes":  300,
				"disk_io_operations":  200,
				"network_bytes_sent":  150,
			},
			"job": {
				"prometheus":   800,
				"grafana":      600,
				"alertmanager": 400,
				"node":         300,
			},
			"instance": {
				"localhost:9090": 200,
				"localhost:3000": 150,
				"localhost:9093": 100,
				"localhost:9100": 80,
				"prod-server-1":  300,
				"prod-server-2":  250,
				"prod-server-3":  200,
			},
			"status": {
				"200": 800,
				"404": 300,
				"500": 100,
			},
			"method": {
				"GET":    600,
				"POST":   300,
				"PUT":    150,
				"DELETE": 50,
			},
		},
		totalSeries: 2100,
	}
}

func (m *mockStatistics) TotalSeries() uint64 {
	return m.totalSeries
}

func (m *mockStatistics) LabelValuesCount(_ context.Context, name string) (uint64, error) {
	values := m.seriesPerValue[name]

	count := uint64(0)
	for _, seriesCount := range values {
		if seriesCount > 0 {
			count++
		}
	}
	return count, nil
}

func (m *mockStatistics) LabelValuesCardinality(_ context.Context, name string, values ...string) (uint64, error) {
	labelValues := m.seriesPerValue[name]

	if len(values) == 0 {
		// Return total cardinality for all values of this label
		total := uint64(0)
		for _, seriesCount := range labelValues {
			total += seriesCount
		}
		return total, nil
	}

	// Return cardinality for specific values
	total := uint64(0)
	for _, value := range values {
		if seriesCount, exists := labelValues[value]; exists {
			total += seriesCount
		}
	}
	return total, nil
}
