// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"
	"sort"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
)

func TestFilterCostInvariants(t *testing.T) {
	ctx := t.Context()
	stats := newMockStatistics()
	testCases := []struct {
		name        string
		seriesCount uint64
		matchers    []*labels.Matcher
	}{
		{
			name:        "general case",
			seriesCount: 1000,
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "job", "prometheus"),
				labels.MustNewMatcher(labels.MatchRegexp, "job", "prometheus|grafana"),
				labels.MustNewMatcher(labels.MatchRegexp, "job", ".*prometheus.*"),
			},
		},
		{
			name: "fewer series than set matchers",
		},
		{
			name: "no series",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// TODO dimitarvdimitrov dedup code with TestIndexLookupCostInvariants
			type matcherWithCost struct {
				matcher string
				cost    float64
			}

			matchersWithCost := make([]matcherWithCost, 0, len(tc.matchers))
			stringMatchers := make([]string, 0, len(tc.matchers))
			for _, m := range tc.matchers {
				pred, err := newPlanPredicate(ctx, m, stats)
				assert.NoError(t, err)

				cost := pred.filterCost(tc.seriesCount)
				matchersWithCost = append(matchersWithCost, matcherWithCost{m.String(), cost})
				stringMatchers = append(stringMatchers, m.String())
				assert.GreaterOrEqualf(t, cost, 0.0, "can't have negative cost on matcher {%s}", m.String())
			}

			sort.SliceStable(matchersWithCost, func(i, j int) bool {
				return matchersWithCost[i].cost < matchersWithCost[j].cost
			})

			matchersByCost := make([]string, len(tc.matchers))
			for i, c := range matchersWithCost {
				matchersByCost[i] = c.matcher
			}
			assert.Equal(t, stringMatchers, matchersByCost)
		})
	}
}

func TestIndexLookupCostInvariants(t *testing.T) {
	ctx := t.Context()
	stats := newMockStatistics()

	testCases := []struct {
		name     string
		matchers []*labels.Matcher
	}{
		{
			name: "mixed matchers",
			matchers: []*labels.Matcher{
				// TODO dimitarvdimitrov
				// non-existent labels should be free to look up
				labels.MustNewMatcher(labels.MatchEqual, "non_existent_label", "foo"),
				labels.MustNewMatcher(labels.MatchRegexp, "non_existent_label", ".+"),
				labels.MustNewMatcher(labels.MatchNotEqual, "non_existent_label", ".+"),

				// few label values are easy to use the index for
				labels.MustNewMatcher(labels.MatchEqual, "job", ""),
				labels.MustNewMatcher(labels.MatchRegexp, "job", ""),
				labels.MustNewMatcher(labels.MatchNotRegexp, "job", ""),
				labels.MustNewMatcher(labels.MatchNotEqual, "job", ""),

				// non-existent value
				labels.MustNewMatcher(labels.MatchEqual, "method", "foo"),

				// low-cardinality value
				labels.MustNewMatcher(labels.MatchEqual, "method", "DELETE"),
				labels.MustNewMatcher(labels.MatchNotEqual, "method", "DELETE"),
				labels.MustNewMatcher(labels.MatchRegexp, "method", "DELETE"),
				labels.MustNewMatcher(labels.MatchEqual, "method", "PUT"),
				labels.MustNewMatcher(labels.MatchRegexp, "method", "PUT|DELETE"),
				labels.MustNewMatcher(labels.MatchRegexp, "method", "P.*"),

				// higher-cardinality matchers
				labels.MustNewMatcher(labels.MatchEqual, "job", "prometheus"),
				labels.MustNewMatcher(labels.MatchNotEqual, "method", ""),
				labels.MustNewMatcher(labels.MatchRegexp, "method", ".+"),
				labels.MustNewMatcher(labels.MatchRegexp, "job", "prometheus|grafana"),
				labels.MustNewMatcher(labels.MatchRegexp, "job", ".*prometheus.*"),
			},
		},
		{
			name:     "regex matchers",
			matchers: []*labels.Matcher{},
		},
		{
			name:     "exclusive matchers are more expensive than the equivalent inclusive matchers",
			matchers: []*labels.Matcher{
				// TODO dimitarvdimitrov
			},
		},
		{
			name: "matching a non-existent label name is free",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			type matcherWithCost struct {
				matcher string
				cost    float64
			}

			matchersWithCost := make([]matcherWithCost, 0, len(tc.matchers))
			stringMatchers := make([]string, 0, len(tc.matchers))
			for _, m := range tc.matchers {
				pred, err := newPlanPredicate(ctx, m, stats)
				assert.NoError(t, err)

				cost := pred.indexLookupCost()
				matchersWithCost = append(matchersWithCost, matcherWithCost{m.String(), cost})
				stringMatchers = append(stringMatchers, m.String())
				assert.GreaterOrEqualf(t, cost, 0.0, "can't have negative cost on matcher {%s}", m.String())
			}

			sort.Slice(matchersWithCost, func(i, j int) bool {
				return matchersWithCost[i].cost < matchersWithCost[j].cost
			})

			matchersByCost := make([]string, len(tc.matchers))
			for i, c := range matchersWithCost {
				matchersByCost[i] = c.matcher
			}
			assert.Equal(t, stringMatchers, matchersByCost)
		})
	}
}

func TestCardinalityEstimation(t *testing.T) {
	ctx := t.Context()
	stats := newMockStatistics()

	testCases := []struct {
		name                string
		matcher             *labels.Matcher
		expectedCardinality uint64
		epsilonTolerance    float64 // for regex matchers that are estimated
	}{
		{
			name:                "exact match existing value",
			matcher:             labels.MustNewMatcher(labels.MatchEqual, "job", "prometheus"),
			expectedCardinality: 800,
		},
		{
			name:                "exact match non-existing value",
			matcher:             labels.MustNewMatcher(labels.MatchEqual, "job", "nonexistent"),
			expectedCardinality: 0,
		},
		{
			name:                "exact match empty value",
			matcher:             labels.MustNewMatcher(labels.MatchEqual, "job", ""),
			expectedCardinality: 0,
		},
		{
			name:                "not equal existing value",
			matcher:             labels.MustNewMatcher(labels.MatchNotEqual, "job", "prometheus"),
			expectedCardinality: stats.totalSeries - 800, // 2100 - 800 = 1300
		},
		{
			name:                "not equal empty value",
			matcher:             labels.MustNewMatcher(labels.MatchNotEqual, "job", ""),
			expectedCardinality: stats.totalSeries,
		},
		{
			name:                "regex with set matches",
			matcher:             labels.MustNewMatcher(labels.MatchRegexp, "job", "prometheus|grafana"),
			expectedCardinality: 800 + 600, // 1400
		},
		{
			name:                "regex general pattern",
			matcher:             labels.MustNewMatcher(labels.MatchRegexp, "job", ".*prometheus.*"),
			expectedCardinality: 210, // estimated as totalSeries * 0.1 selectivity = 2100 * 0.1 = 210
			epsilonTolerance:    0.1,
		},
		{
			name:                "not regex with set matches",
			matcher:             labels.MustNewMatcher(labels.MatchNotRegexp, "job", "prometheus|grafana"),
			expectedCardinality: stats.totalSeries - (800 + 600),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pred, err := newPlanPredicate(ctx, tc.matcher, stats)
			assert.NoError(t, err)

			actualCardinality := pred.cardinality

			if tc.epsilonTolerance > 0 {
				assert.InEpsilon(t, tc.expectedCardinality, actualCardinality, tc.epsilonTolerance)
			} else {
				assert.Equal(t, tc.expectedCardinality, actualCardinality)
			}
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

func (m *mockStatistics) LabelValuesCount(ctx context.Context, name string) (uint64, error) {
	values := m.seriesPerValue[name]

	count := uint64(0)
	for _, seriesCount := range values {
		if seriesCount > 0 {
			count++
		}
	}
	return count, nil
}

func (m *mockStatistics) LabelValuesCardinality(ctx context.Context, name string, values ...string) (uint64, error) {
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
