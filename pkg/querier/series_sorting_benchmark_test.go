// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
)

func BenchmarkMergingAndSortingSeries(b *testing.B) {
	for _, ingestersPerZone := range []int{1, 2, 4, 10, 100} {
		for _, zones := range []int{1, 2, 3} {
			for _, seriesPerIngester := range []int{1, 10, 100, 1000, 10000} {
				seriesSets := generateSeriesSets(ingestersPerZone, zones, seriesPerIngester)

				b.Run(fmt.Sprintf("%v ingesters per zone, %v zones, %v series per ingester", ingestersPerZone, zones, seriesPerIngester), func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						naiveMergeAndSortSeriesSets(seriesSets)
					}
				})
			}
		}
	}
}

func TestMergingAndSortingSeries(t *testing.T) {
	testCases := map[string]struct {
		seriesSets []ingesterSeries
		expected   []mergedSeries
	}{
		"no ingesters": {
			seriesSets: []ingesterSeries{},
			expected:   []mergedSeries{},
		},
		"single ingester, single series": {
			seriesSets: []ingesterSeries{
				{IngesterName: "ingester-1", Series: []labels.Labels{labels.FromStrings("some-label", "some-value")}},
			},
			expected: []mergedSeries{
				{
					Labels: labels.FromStrings("some-label", "some-value"),
					Sources: []mergedSeriesSource{
						{Ingester: "ingester-1", SeriesIndex: 0},
					},
				},
			},
		},
		"multiple ingesters, each with single series": {
			seriesSets: []ingesterSeries{
				{IngesterName: "zone-a-ingester-1", Series: []labels.Labels{labels.FromStrings("some-label", "some-value")}},
				{IngesterName: "zone-b-ingester-1", Series: []labels.Labels{labels.FromStrings("some-label", "some-value")}},
				{IngesterName: "zone-c-ingester-1", Series: []labels.Labels{labels.FromStrings("some-label", "some-value")}},
			},
			expected: []mergedSeries{
				{
					Labels: labels.FromStrings("some-label", "some-value"),
					Sources: []mergedSeriesSource{
						{Ingester: "zone-a-ingester-1", SeriesIndex: 0},
						{Ingester: "zone-b-ingester-1", SeriesIndex: 0},
						{Ingester: "zone-c-ingester-1", SeriesIndex: 0},
					},
				},
			},
		},
		"multiple ingesters, each with different series": {
			seriesSets: []ingesterSeries{
				{IngesterName: "zone-a-ingester-1", Series: []labels.Labels{labels.FromStrings("some-label", "value-a")}},
				{IngesterName: "zone-b-ingester-1", Series: []labels.Labels{labels.FromStrings("some-label", "value-b")}},
				{IngesterName: "zone-c-ingester-1", Series: []labels.Labels{labels.FromStrings("some-label", "value-c")}},
			},
			expected: []mergedSeries{
				{
					Labels: labels.FromStrings("some-label", "value-a"),
					Sources: []mergedSeriesSource{
						{Ingester: "zone-a-ingester-1", SeriesIndex: 0},
					},
				},
				{
					Labels: labels.FromStrings("some-label", "value-b"),
					Sources: []mergedSeriesSource{
						{Ingester: "zone-b-ingester-1", SeriesIndex: 0},
					},
				},
				{
					Labels: labels.FromStrings("some-label", "value-c"),
					Sources: []mergedSeriesSource{
						{Ingester: "zone-c-ingester-1", SeriesIndex: 0},
					},
				},
			},
		},
		"multiple ingesters, each with different series, with earliest ingesters having last series": {
			seriesSets: []ingesterSeries{
				{IngesterName: "zone-c-ingester-1", Series: []labels.Labels{labels.FromStrings("some-label", "value-c")}},
				{IngesterName: "zone-b-ingester-1", Series: []labels.Labels{labels.FromStrings("some-label", "value-b")}},
				{IngesterName: "zone-a-ingester-1", Series: []labels.Labels{labels.FromStrings("some-label", "value-a")}},
			},
			expected: []mergedSeries{
				{
					Labels: labels.FromStrings("some-label", "value-a"),
					Sources: []mergedSeriesSource{
						{Ingester: "zone-a-ingester-1", SeriesIndex: 0},
					},
				},
				{
					Labels: labels.FromStrings("some-label", "value-b"),
					Sources: []mergedSeriesSource{
						{Ingester: "zone-b-ingester-1", SeriesIndex: 0},
					},
				},
				{
					Labels: labels.FromStrings("some-label", "value-c"),
					Sources: []mergedSeriesSource{
						{Ingester: "zone-c-ingester-1", SeriesIndex: 0},
					},
				},
			},
		},
	}

	implementations := map[string]func([]ingesterSeries) []mergedSeries{
		"naive": naiveMergeAndSortSeriesSets,
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			for implementationName, implementationFunc := range implementations {
				t.Run(implementationName, func(t *testing.T) {
					actual := implementationFunc(testCase.seriesSets)
					require.Lenf(t, actual, len(testCase.expected), "should be same length as %v", testCase.expected)

					for i := 0; i < len(actual); i++ {
						actualSeries := actual[i]
						expectedSeries := testCase.expected[i]

						require.Equal(t, expectedSeries.Labels, actualSeries.Labels)

						// We don't care about the order.
						require.ElementsMatch(t, expectedSeries.Sources, actualSeries.Sources, "series %v", actualSeries.Labels.String())
					}
				})
			}
		})
	}
}

// Equivalent of current naive implementation
func naiveMergeAndSortSeriesSets(ingesters []ingesterSeries) []mergedSeries {
	hashToStreamingSeries := map[string]mergedSeries{}

	for _, ingester := range ingesters {
		for seriesIndex, seriesLabels := range ingester.Series {
			key := client.LabelsToKeyString(seriesLabels)
			series, exists := hashToStreamingSeries[key]

			if !exists {
				series = mergedSeries{
					Labels:  seriesLabels,
					Sources: make([]mergedSeriesSource, 0, 3), // TODO: take capacity from number of zones
				}
			}

			series.Sources = append(series.Sources, mergedSeriesSource{
				SeriesIndex: seriesIndex,
				Ingester:    ingester.IngesterName,
			})

			hashToStreamingSeries[key] = series
		}
	}

	allSeries := make([]mergedSeries, 0, len(hashToStreamingSeries))

	for _, s := range hashToStreamingSeries {
		allSeries = append(allSeries, s)
	}

	// Sort the series, just like NewConcreteSeriesSet does
	sort.Sort(bySeriesLabels(allSeries))

	return allSeries
}

// Equivalent of StreamingSeries
type mergedSeries struct {
	Labels  labels.Labels
	Sources []mergedSeriesSource
}

// Equivalent of StreamingSeriesSource
type mergedSeriesSource struct {
	Ingester    string
	SeriesIndex int
}

type ingesterSeries struct {
	IngesterName string
	Series       []labels.Labels
}

func generateSeriesSets(ingestersPerZone int, zones int, seriesPerIngester int) []ingesterSeries {
	seriesPerZone := ingestersPerZone * seriesPerIngester
	zoneSeries := make([]labels.Labels, seriesPerZone)

	for seriesIdx := 0; seriesIdx < seriesPerZone; seriesIdx++ {
		zoneSeries[seriesIdx] = labels.FromStrings("the-label", strconv.Itoa(seriesIdx))
	}

	seriesSets := make([]ingesterSeries, 0, zones*ingestersPerZone)

	for zone := 1; zone <= zones; zone++ {
		rand.Shuffle(len(zoneSeries), func(i, j int) { zoneSeries[i], zoneSeries[j] = zoneSeries[j], zoneSeries[i] })

		for ingester := 1; ingester <= ingestersPerZone; ingester++ {
			ingesterName := fmt.Sprintf("zone-%v-ingester-%v", zone, ingester)
			series := zoneSeries[(ingester-1)*seriesPerIngester : ingester*seriesPerIngester]
			sort.Sort(byLabels(series))

			seriesSets = append(seriesSets, ingesterSeries{IngesterName: ingesterName, Series: series})
		}
	}

	return seriesSets
}

type byLabels []labels.Labels

func (b byLabels) Len() int           { return len(b) }
func (b byLabels) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byLabels) Less(i, j int) bool { return labels.Compare(b[i], b[j]) < 0 }

type bySeriesLabels []mergedSeries

func (b bySeriesLabels) Len() int           { return len(b) }
func (b bySeriesLabels) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b bySeriesLabels) Less(i, j int) bool { return labels.Compare(b[i].Labels, b[j].Labels) < 0 }
