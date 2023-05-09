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
						loserTreeMergeSeriesSets(seriesSets, zones)
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
			actual := loserTreeMergeSeriesSets(testCase.seriesSets, zoneCount)
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

// Use a loser tree to merge lists of series from each ingester.
// This assumes we add a new implementation of NewConcreteSeriesSet that doesn't try to sort the list of series again.
func loserTreeMergeSeriesSets(ingesters []SeriesChunksStream, zoneCount int) []StreamingSeries {
	tree := newSeriesChunkStreamsTree(ingesters)
	allSeries := []StreamingSeries{}

	for tree.Next() {
		nextIngester, nextSeriesFromIngester, nextSeriesIndex := tree.Winner()
		lastSeriesIndex := len(allSeries) - 1

		if len(allSeries) == 0 || labels.Compare(allSeries[lastSeriesIndex].Labels, nextSeriesFromIngester) != 0 {
			// First time we've seen this series.
			series := StreamingSeries{
				Labels: nextSeriesFromIngester,
				// Why zoneCount? We assume each series is present exactly once in each zone.
				Sources: make([]StreamingSeriesSource, 1, zoneCount),
			}

			series.Sources[0] = StreamingSeriesSource{
				StreamReader: nextIngester.StreamReader,
				SeriesIndex:  nextSeriesIndex,
			}

			allSeries = append(allSeries, series)
		} else {
			// We've seen this series before.
			allSeries[lastSeriesIndex].Sources = append(allSeries[lastSeriesIndex].Sources, StreamingSeriesSource{
				StreamReader: nextIngester.StreamReader,
				SeriesIndex:  nextSeriesIndex,
			})
		}
	}

	return allSeries
}

type SeriesChunksStream struct {
	StreamReader *SeriesChunksStreamReader
	Series       []labels.Labels
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

func newSeriesChunkStreamsTree(ingesters []SeriesChunksStream) *seriesChunkStreamsTree {
	nIngesters := len(ingesters)
	t := seriesChunkStreamsTree{
		nodes: make([]seriesChunkStreamsTreeNode, nIngesters*2),
	}
	for idx, s := range ingesters {
		t.nodes[idx+nIngesters].ingester = s
		t.moveNext(idx + nIngesters) // Must call Next on each item so that At() has a value.
	}
	if nIngesters > 0 {
		t.nodes[0].index = -1 // flag to be initialized on first call to Next().
	}
	return &t
}

// A loser tree is a binary tree laid out such that nodes N and N+1 have parent N/2.
// We store M leaf nodes in positions M...2M-1, and M-1 internal nodes in positions 1..M-1.
// Node 0 is a special node, containing the winner of the contest.
type seriesChunkStreamsTree struct {
	nodes []seriesChunkStreamsTreeNode
}

type seriesChunkStreamsTreeNode struct {
	index           int                // This is the loser for all nodes except the 0th, where it is the winner.
	value           labels.Labels      // Value copied from the loser node, or winner for node 0.
	ingester        SeriesChunksStream // Only populated for leaf nodes.
	nextSeriesIndex int                // Only populated for leaf nodes.
}

func (t *seriesChunkStreamsTree) moveNext(index int) bool {
	n := &t.nodes[index]
	n.nextSeriesIndex++
	if n.nextSeriesIndex > len(n.ingester.Series) {
		n.value = nil
		n.index = -1
		return false
	}
	n.value = n.ingester.Series[n.nextSeriesIndex-1]
	return true
}

func (t *seriesChunkStreamsTree) Winner() (SeriesChunksStream, labels.Labels, int) {
	n := t.nodes[t.nodes[0].index]
	return n.ingester, n.value, n.nextSeriesIndex - 1
}

func (t *seriesChunkStreamsTree) Next() bool {
	if len(t.nodes) == 0 {
		return false
	}
	if t.nodes[0].index == -1 { // If tree has not been initialized yet, do that.
		t.initialize()
		return t.nodes[t.nodes[0].index].index != -1
	}
	if t.nodes[t.nodes[0].index].index == -1 { // already exhausted
		return false
	}
	t.moveNext(t.nodes[0].index)
	t.replayGames(t.nodes[0].index)
	return t.nodes[t.nodes[0].index].index != -1
}

func (t *seriesChunkStreamsTree) initialize() {
	winners := make([]int, len(t.nodes))
	// Initialize leaf nodes as winners to start.
	for i := len(t.nodes) / 2; i < len(t.nodes); i++ {
		winners[i] = i
	}
	for i := len(t.nodes) - 2; i > 0; i -= 2 {
		// At each stage the winners play each other, and we record the loser in the node.
		loser, winner := t.playGame(winners[i], winners[i+1])
		p := parent(i)
		t.nodes[p].index = loser
		t.nodes[p].value = t.nodes[loser].value
		winners[p] = winner
	}
	t.nodes[0].index = winners[1]
	t.nodes[0].value = t.nodes[winners[1]].value
}

// Starting at pos, re-consider all values up to the root.
func (t *seriesChunkStreamsTree) replayGames(pos int) {
	// At the start, pos is a leaf node, and is the winner at that level.
	n := parent(pos)
	for n != 0 {
		if t.less(t.nodes[n].value, t.nodes[pos].value) {
			loser := pos
			// Record pos as the loser here, and the old loser is the new winner.
			pos = t.nodes[n].index
			t.nodes[n].index = loser
			t.nodes[n].value = t.nodes[loser].value
		}
		n = parent(n)
	}
	// pos is now the winner; store it in node 0.
	t.nodes[0].index = pos
	t.nodes[0].value = t.nodes[pos].value
}

func (t *seriesChunkStreamsTree) playGame(a, b int) (loser, winner int) {
	if t.less(t.nodes[a].value, t.nodes[b].value) {
		return b, a
	}
	return a, b
}

func (t *seriesChunkStreamsTree) less(a, b labels.Labels) bool {
	if a == nil {
		return false
	}

	if b == nil {
		return true
	}

	return labels.Compare(a, b) < 0
}

func parent(i int) int { return i / 2 }
