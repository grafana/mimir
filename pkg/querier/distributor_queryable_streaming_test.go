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
)

func BenchmarkMergingAndSortingSeries(b *testing.B) {
	for _, ingestersPerZone := range []int{1, 2, 4, 10, 100} {
		for _, zones := range []int{1, 2, 3} {
			for _, seriesPerIngester := range []int{1, 10, 100, 1000, 10000} {
				seriesSets := generateSeriesSets(ingestersPerZone, zones, seriesPerIngester)

				b.Run(fmt.Sprintf("%v ingesters per zone, %v zones, %v series per ingester", ingestersPerZone, zones, seriesPerIngester), func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						MergeSeriesChunkStreams(seriesSets, zones)
					}
				})
			}
		}
	}
}

func TestMergingAndSortingSeries(t *testing.T) {
	ingester1 := &SeriesChunksStreamReader{}
	ingester2 := &SeriesChunksStreamReader{}
	ingester3 := &SeriesChunksStreamReader{}

	testCases := map[string]struct {
		seriesSets []SeriesChunksStream
		expected   []StreamingSeries
	}{
		"no ingesters": {
			seriesSets: []SeriesChunksStream{},
			expected:   []StreamingSeries{},
		},
		"single ingester, no series": {
			seriesSets: []SeriesChunksStream{
				{StreamReader: ingester1, Series: []labels.Labels{}},
			},
			expected: []StreamingSeries{},
		},
		"single ingester, single series": {
			seriesSets: []SeriesChunksStream{
				{StreamReader: ingester1, Series: []labels.Labels{labels.FromStrings("some-label", "some-value")}},
			},
			expected: []StreamingSeries{
				{
					Labels: labels.FromStrings("some-label", "some-value"),
					Sources: []StreamingSeriesSource{
						{StreamReader: ingester1, SeriesIndex: 0},
					},
				},
			},
		},
		"multiple ingesters, each with single series": {
			seriesSets: []SeriesChunksStream{
				{StreamReader: ingester1, Series: []labels.Labels{labels.FromStrings("some-label", "some-value")}},
				{StreamReader: ingester2, Series: []labels.Labels{labels.FromStrings("some-label", "some-value")}},
				{StreamReader: ingester3, Series: []labels.Labels{labels.FromStrings("some-label", "some-value")}},
			},
			expected: []StreamingSeries{
				{
					Labels: labels.FromStrings("some-label", "some-value"),
					Sources: []StreamingSeriesSource{
						{StreamReader: ingester1, SeriesIndex: 0},
						{StreamReader: ingester2, SeriesIndex: 0},
						{StreamReader: ingester3, SeriesIndex: 0},
					},
				},
			},
		},
		"multiple ingesters, each with different series": {
			seriesSets: []SeriesChunksStream{
				{StreamReader: ingester1, Series: []labels.Labels{labels.FromStrings("some-label", "value-a")}},
				{StreamReader: ingester2, Series: []labels.Labels{labels.FromStrings("some-label", "value-b")}},
				{StreamReader: ingester3, Series: []labels.Labels{labels.FromStrings("some-label", "value-c")}},
			},
			expected: []StreamingSeries{
				{
					Labels: labels.FromStrings("some-label", "value-a"),
					Sources: []StreamingSeriesSource{
						{StreamReader: ingester1, SeriesIndex: 0},
					},
				},
				{
					Labels: labels.FromStrings("some-label", "value-b"),
					Sources: []StreamingSeriesSource{
						{StreamReader: ingester2, SeriesIndex: 0},
					},
				},
				{
					Labels: labels.FromStrings("some-label", "value-c"),
					Sources: []StreamingSeriesSource{
						{StreamReader: ingester3, SeriesIndex: 0},
					},
				},
			},
		},
		"multiple ingesters, each with different series, with earliest ingesters having last series": {
			seriesSets: []SeriesChunksStream{
				{StreamReader: ingester3, Series: []labels.Labels{labels.FromStrings("some-label", "value-c")}},
				{StreamReader: ingester2, Series: []labels.Labels{labels.FromStrings("some-label", "value-b")}},
				{StreamReader: ingester1, Series: []labels.Labels{labels.FromStrings("some-label", "value-a")}},
			},
			expected: []StreamingSeries{
				{
					Labels: labels.FromStrings("some-label", "value-a"),
					Sources: []StreamingSeriesSource{
						{StreamReader: ingester1, SeriesIndex: 0},
					},
				},
				{
					Labels: labels.FromStrings("some-label", "value-b"),
					Sources: []StreamingSeriesSource{
						{StreamReader: ingester2, SeriesIndex: 0},
					},
				},
				{
					Labels: labels.FromStrings("some-label", "value-c"),
					Sources: []StreamingSeriesSource{
						{StreamReader: ingester3, SeriesIndex: 0},
					},
				},
			},
		},
		"multiple ingesters, each with multiple series": {
			seriesSets: []SeriesChunksStream{
				{StreamReader: ingester1, Series: []labels.Labels{labels.FromStrings("label-a", "value-a"), labels.FromStrings("label-b", "value-a")}},
				{StreamReader: ingester2, Series: []labels.Labels{labels.FromStrings("label-a", "value-b"), labels.FromStrings("label-b", "value-a")}},
				{StreamReader: ingester3, Series: []labels.Labels{labels.FromStrings("label-a", "value-c"), labels.FromStrings("label-b", "value-a")}},
			},
			expected: []StreamingSeries{
				{
					Labels: labels.FromStrings("label-a", "value-a"),
					Sources: []StreamingSeriesSource{
						{StreamReader: ingester1, SeriesIndex: 0},
					},
				},
				{
					Labels: labels.FromStrings("label-a", "value-b"),
					Sources: []StreamingSeriesSource{
						{StreamReader: ingester2, SeriesIndex: 0},
					},
				},
				{
					Labels: labels.FromStrings("label-a", "value-c"),
					Sources: []StreamingSeriesSource{
						{StreamReader: ingester3, SeriesIndex: 0},
					},
				},
				{
					Labels: labels.FromStrings("label-b", "value-a"),
					Sources: []StreamingSeriesSource{
						{StreamReader: ingester1, SeriesIndex: 1},
						{StreamReader: ingester2, SeriesIndex: 1},
						{StreamReader: ingester3, SeriesIndex: 1},
					},
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			zoneCount := 1 // The exact value of this only matters for performance (it's used to pre-allocate a slice of the correct size)
			actual := MergeSeriesChunkStreams(testCase.seriesSets, zoneCount)
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
}

func generateSeriesSets(ingestersPerZone int, zones int, seriesPerIngester int) []SeriesChunksStream {
	seriesPerZone := ingestersPerZone * seriesPerIngester
	zoneSeries := make([]labels.Labels, seriesPerZone)

	for seriesIdx := 0; seriesIdx < seriesPerZone; seriesIdx++ {
		zoneSeries[seriesIdx] = labels.FromStrings("the-label", strconv.Itoa(seriesIdx))
	}

	seriesSets := make([]SeriesChunksStream, 0, zones*ingestersPerZone)

	for zone := 1; zone <= zones; zone++ {
		rand.Shuffle(len(zoneSeries), func(i, j int) { zoneSeries[i], zoneSeries[j] = zoneSeries[j], zoneSeries[i] })

		for ingester := 1; ingester <= ingestersPerZone; ingester++ {
			streamReader := &SeriesChunksStreamReader{}
			series := zoneSeries[(ingester-1)*seriesPerIngester : ingester*seriesPerIngester]
			sort.Sort(byLabels(series))

			seriesSets = append(seriesSets, SeriesChunksStream{StreamReader: streamReader, Series: series})
		}
	}

	return seriesSets
}

type byLabels []labels.Labels

func (b byLabels) Len() int           { return len(b) }
func (b byLabels) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byLabels) Less(i, j int) bool { return labels.Compare(b[i], b[j]) < 0 }
