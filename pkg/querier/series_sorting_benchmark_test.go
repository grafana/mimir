// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"container/heap"
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
						// Reset the test data.
						for i := range seriesSets {
							seriesSets[i].NextSeriesIndex = 0
						}

						loserTreeMergeSeriesSets(seriesSets, zones)
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
		"single ingester, no series": {
			seriesSets: []ingesterSeries{
				{IngesterName: "ingester-1", Series: []labels.Labels{}},
			},
			expected: []mergedSeries{},
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
		"multiple ingesters, each with multiple series": {
			seriesSets: []ingesterSeries{
				{IngesterName: "zone-a-ingester-1", Series: []labels.Labels{labels.FromStrings("label-a", "value-a"), labels.FromStrings("label-b", "value-a")}},
				{IngesterName: "zone-b-ingester-1", Series: []labels.Labels{labels.FromStrings("label-a", "value-b"), labels.FromStrings("label-b", "value-a")}},
				{IngesterName: "zone-c-ingester-1", Series: []labels.Labels{labels.FromStrings("label-a", "value-c"), labels.FromStrings("label-b", "value-a")}},
			},
			expected: []mergedSeries{
				{
					Labels: labels.FromStrings("label-a", "value-a"),
					Sources: []mergedSeriesSource{
						{Ingester: "zone-a-ingester-1", SeriesIndex: 0},
					},
				},
				{
					Labels: labels.FromStrings("label-a", "value-b"),
					Sources: []mergedSeriesSource{
						{Ingester: "zone-b-ingester-1", SeriesIndex: 0},
					},
				},
				{
					Labels: labels.FromStrings("label-a", "value-c"),
					Sources: []mergedSeriesSource{
						{Ingester: "zone-c-ingester-1", SeriesIndex: 0},
					},
				},
				{
					Labels: labels.FromStrings("label-b", "value-a"),
					Sources: []mergedSeriesSource{
						{Ingester: "zone-a-ingester-1", SeriesIndex: 1},
						{Ingester: "zone-b-ingester-1", SeriesIndex: 1},
						{Ingester: "zone-c-ingester-1", SeriesIndex: 1},
					},
				},
			},
		},
	}

	implementations := map[string]func([]ingesterSeries, int) []mergedSeries{
		"naive":      naiveMergeAndSortSeriesSets,
		"heap":       heapMergeSeriesSets,
		"loser tree": loserTreeMergeSeriesSets,
	}

	zoneCount := 1

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			for implementationName, implementationFunc := range implementations {
				t.Run(implementationName, func(t *testing.T) {
					// Reset the test data.
					for i := range testCase.seriesSets {
						testCase.seriesSets[i].NextSeriesIndex = 0
					}

					actual := implementationFunc(testCase.seriesSets, zoneCount)
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
func naiveMergeAndSortSeriesSets(ingesters []ingesterSeries, zoneCount int) []mergedSeries {
	hashToStreamingSeries := map[string]mergedSeries{}

	for _, ingester := range ingesters {
		for seriesIndex, seriesLabels := range ingester.Series {
			key := client.LabelsToKeyString(seriesLabels)
			series, exists := hashToStreamingSeries[key]

			if !exists {
				series = mergedSeries{
					Labels: seriesLabels,
					// Why zoneCount? We assume each series is present exactly one in each zone.
					Sources: make([]mergedSeriesSource, 0, zoneCount),
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

// Use a heap to merge lists of series from each ingester.
// This assumes we add a new implementation of NewConcreteSeriesSet that doesn't try to sort the list of series again.
func heapMergeSeriesSets(ingesters []ingesterSeries, zoneCount int) []mergedSeries {
	if len(ingesters) == 0 {
		return []mergedSeries{}
	}

	ingesterPointers := make([]*ingesterSeries, len(ingesters))
	for i, _ := range ingesters {
		ingesterPointers[i] = &ingesters[i]
	}

	h := ingesterPriorityQueue(ingesterPointers)
	heap.Init(&h)

	// TODO: can we guess the size of this? Or calculate it by building a map of all series' hashes?
	allSeries := []mergedSeries{}

	for {
		nextIngester := h[0]

		if len(nextIngester.Series) == nextIngester.NextSeriesIndex {
			// Ingesters with no series remaining sort last, so if we've reached an ingester with no series remaining, we are done.
			return allSeries
		}

		nextSeriesFromIngester := nextIngester.Series[nextIngester.NextSeriesIndex]
		lastSeriesIndex := len(allSeries) - 1

		if len(allSeries) == 0 || labels.Compare(allSeries[lastSeriesIndex].Labels, nextSeriesFromIngester) != 0 {
			// First time we've seen this series.
			series := mergedSeries{
				Labels: nextSeriesFromIngester,
				// Why zoneCount? We assume each series is present exactly once in each zone.
				Sources: make([]mergedSeriesSource, 1, zoneCount),
			}

			series.Sources[0] = mergedSeriesSource{
				Ingester:    nextIngester.IngesterName,
				SeriesIndex: nextIngester.NextSeriesIndex,
			}

			allSeries = append(allSeries, series)
		} else {
			// We've seen this series before.
			allSeries[lastSeriesIndex].Sources = append(allSeries[lastSeriesIndex].Sources, mergedSeriesSource{
				Ingester:    nextIngester.IngesterName,
				SeriesIndex: nextIngester.NextSeriesIndex,
			})
		}

		nextIngester.NextSeriesIndex++
		heap.Fix(&h, 0)
	}
}

// Use a loser tree to merge lists of series from each ingester.
// This assumes we add a new implementation of NewConcreteSeriesSet that doesn't try to sort the list of series again.
func loserTreeMergeSeriesSets(ingesters []ingesterSeries, zoneCount int) []mergedSeries {
	tree := NewTree(ingesters)
	allSeries := []mergedSeries{}

	for tree.Next() {
		nextIngester, nextSeriesFromIngester, nextSeriesIndex := tree.Winner()
		lastSeriesIndex := len(allSeries) - 1

		if len(allSeries) == 0 || labels.Compare(allSeries[lastSeriesIndex].Labels, nextSeriesFromIngester) != 0 {
			// First time we've seen this series.
			series := mergedSeries{
				Labels: nextSeriesFromIngester,
				// Why zoneCount? We assume each series is present exactly once in each zone.
				Sources: make([]mergedSeriesSource, 1, zoneCount),
			}

			series.Sources[0] = mergedSeriesSource{
				Ingester:    nextIngester.IngesterName,
				SeriesIndex: nextSeriesIndex,
			}

			allSeries = append(allSeries, series)
		} else {
			// We've seen this series before.
			allSeries[lastSeriesIndex].Sources = append(allSeries[lastSeriesIndex].Sources, mergedSeriesSource{
				Ingester:    nextIngester.IngesterName,
				SeriesIndex: nextSeriesIndex,
			})
		}
	}

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

	// Required only for heap sort
	NextSeriesIndex int
}

func (i *ingesterSeries) Next() bool {
	if i.NextSeriesIndex >= len(i.Series) {
		return false
	}

	i.NextSeriesIndex++
	return true
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

type ingesterPriorityQueue []*ingesterSeries

func (pq ingesterPriorityQueue) Len() int { return len(pq) }

func (pq ingesterPriorityQueue) Less(i, j int) bool {
	if len(pq[i].Series) == pq[i].NextSeriesIndex {
		return false
	}

	if len(pq[j].Series) == pq[j].NextSeriesIndex {
		return true
	}

	return labels.Compare(pq[i].Series[pq[i].NextSeriesIndex], pq[j].Series[pq[j].NextSeriesIndex]) < 0
}

func (pq ingesterPriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *ingesterPriorityQueue) Push(x any) {
	item := x.(*ingesterSeries)
	*pq = append(*pq, item)
}

func (pq *ingesterPriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

func NewTree(ingesters []ingesterSeries) *Tree {
	nIngesters := len(ingesters)
	t := Tree{
		nodes: make([]node, nIngesters*2),
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
type Tree struct {
	nodes []node
}

type node struct {
	index           int            // This is the loser for all nodes except the 0th, where it is the winner.
	value           labels.Labels  // Value copied from the loser node, or winner for node 0.
	ingester        ingesterSeries // Only populated for leaf nodes.
	nextSeriesIndex int            // Only populated for leaf nodes.
}

func (t *Tree) moveNext(index int) bool {
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

func (t *Tree) Winner() (ingesterSeries, labels.Labels, int) {
	n := t.nodes[t.nodes[0].index]
	return n.ingester, n.value, n.nextSeriesIndex - 1
}

func (t *Tree) Next() bool {
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

func (t *Tree) initialize() {
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
func (t *Tree) replayGames(pos int) {
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

func (t *Tree) playGame(a, b int) (loser, winner int) {
	if t.less(t.nodes[a].value, t.nodes[b].value) {
		return b, a
	}
	return a, b
}

func (t *Tree) less(a, b labels.Labels) bool {
	if a == nil {
		return false
	}

	if b == nil {
		return true
	}

	return labels.Compare(a, b) < 0
}

func parent(i int) int { return i / 2 }
