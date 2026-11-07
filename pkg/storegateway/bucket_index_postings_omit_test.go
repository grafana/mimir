// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/fixtures"
	"github.com/grafana/mimir/pkg/storage/indexheader"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

// requireSeriesSizeOf512Bytes fails the test with a clear message when the estimated size of a series changes,
// because the expected numbers in the tests below come from a size of 512 bytes.
func requireSeriesSizeOf512Bytes(t *testing.T) {
	t.Helper()
	require.Equal(t, 512, mimir_tsdb.EstimatedSeriesP99Size, "the expected numbers of this test assume a size of 512 bytes for each series")
}

func TestWorstCaseFetchedDataStrategy_OmitThreshold(t *testing.T) {
	requireSeriesSizeOf512Bytes(t)

	// The smallest group has one label value and 1000 series. Each series takes 4 bytes, and the posting list has 4 more bytes.
	// So the budget is 1000 series of 512 bytes each, which is 512,000 bytes.
	smallestGroup := postingGroup{
		matcher:     labels.MustNewMatcher(labels.MatchEqual, "job", "rare"),
		droppedKeys: 1,
		totalSize:   4 + 4*1000,
	}
	largerGroup := postingGroup{
		matcher:     labels.MustNewMatcher(labels.MatchEqual, "env", "prod"),
		droppedKeys: 1,
		totalSize:   4 + 4*5000,
	}
	subtractingGroup := postingGroup{
		isSubtract:  true,
		matcher:     labels.MustNewMatcher(labels.MatchNotEqual, "pod", ""),
		droppedKeys: 1,
		totalSize:   4 + 4*10,
	}

	testCases := map[string]struct {
		factor    float64
		built     []postingGroup
		labelName string
		expected  int64
	}{
		"no group built yet": {
			factor:    0.75,
			labelName: "pod",
			expected:  math.MaxInt64,
		},
		"only a subtracting group built": {
			factor:    0.75,
			built:     []postingGroup{subtractingGroup},
			labelName: "pod",
			expected:  math.MaxInt64,
		},
		"only the all postings group built": {
			factor:    0.75,
			built:     []postingGroup{{keys: []labels.Label{allPostingsKey}, totalSize: 100}},
			labelName: "pod",
			expected:  math.MaxInt64,
		},
		"the smallest group has less than one series": {
			factor:    0.75,
			built:     []postingGroup{{droppedKeys: 1, totalSize: 6}},
			labelName: "pod",
			expected:  math.MaxInt64,
		},
		"the label is __name__": {
			factor:    0.75,
			built:     []postingGroup{smallestGroup},
			labelName: model.MetricNameLabel,
			expected:  math.MaxInt64,
		},
		// The scaled size is 0.75 times the size. A size of 682,667 scales to 512,000, and a size of 682,668 scales to 512,001.
		"factor 0.75": {
			factor:    0.75,
			built:     []postingGroup{smallestGroup},
			labelName: "pod",
			expected:  682_667,
		},
		"factor 1": {
			factor:    1,
			built:     []postingGroup{smallestGroup},
			labelName: "pod",
			expected:  512_000,
		},
		// The budget alone gives 2560. A group with that size has fewer series than the smallest group.
		// So the threshold is twice the size of the series in the smallest group.
		"factor 200": {
			factor:    200,
			built:     []postingGroup{smallestGroup},
			labelName: "pod",
			expected:  2 * 4 * 1000,
		},
		"the largest factor that the strategy supports": {
			factor:    1 << 20,
			built:     []postingGroup{smallestGroup},
			labelName: "pod",
			expected:  2 * 4 * 1000,
		},
		// With a larger factor, selectPostings overflows for the size of a large posting group.
		"factor above the largest factor that the strategy supports": {
			factor:    1<<20 + 1,
			built:     []postingGroup{smallestGroup},
			labelName: "pod",
			expected:  math.MaxInt64,
		},
		"factor so small that no size is above the budget": {
			factor:    1e-18,
			built:     []postingGroup{smallestGroup},
			labelName: "pod",
			expected:  math.MaxInt64,
		},
		"factor zero":     {factor: 0, built: []postingGroup{smallestGroup}, labelName: "pod", expected: math.MaxInt64},
		"factor negative": {factor: -1, built: []postingGroup{smallestGroup}, labelName: "pod", expected: math.MaxInt64},
		"factor NaN":      {factor: math.NaN(), built: []postingGroup{smallestGroup}, labelName: "pod", expected: math.MaxInt64},
		"factor infinite": {factor: math.Inf(1), built: []postingGroup{smallestGroup}, labelName: "pod", expected: math.MaxInt64},
		"the smallest group is not the first or the last group built": {
			factor:    0.75,
			built:     []postingGroup{largerGroup, subtractingGroup, smallestGroup, largerGroup},
			labelName: "pod",
			expected:  682_667,
		},
		// The budget comes from the series of a group, so the posting list counts of the label values do not count as series.
		"a group with many label values": {
			factor: 1,
			built: []postingGroup{{
				matcher:     labels.MustNewMatcher(labels.MatchRegexp, "a", "b.+"),
				droppedKeys: 100,
				totalSize:   100*4 + 100*4,
			}},
			labelName: "pod",
			expected:  (100 * 4 / 4) * 512,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			strategy := worstCaseFetchedDataStrategy{postingListActualSizeFactor: tc.factor}
			assert.Equal(t, tc.expected, strategy.omitThreshold(tc.built, tc.labelName))
		})
	}
}

func TestWorstCaseFetchedDataStrategy_SmallestSizeAbove(t *testing.T) {
	// The smallest size above the budget has a scaled size above the budget, and the size before it does not.
	// The factors below 1/1024 and above 1 check that the search is exact for every factor.
	for _, factor := range []float64{0.0009, 0.001, 0.0011, 0.01, 0.1, 0.3, 0.75, 1, 1.5, 3, 1000, 1 << 20} {
		for _, budget := range []int64{512, 51_200, 512_000, 5_120_000} {
			t.Run(fmt.Sprintf("factor %v and budget %d", factor, budget), func(t *testing.T) {
				strategy := worstCaseFetchedDataStrategy{postingListActualSizeFactor: factor}

				size := strategy.smallestSizeAbove(budget)
				require.NotEqual(t, int64(math.MaxInt64), size)
				assert.Greater(t, strategy.scaledSize(size), budget)
				assert.LessOrEqual(t, strategy.scaledSize(size-1), budget)
			})
		}
	}
}

func TestWorstCaseFetchedDataStrategy_ScaledSize(t *testing.T) {
	assert.Equal(t, int64(0), worstCaseFetchedDataStrategy{postingListActualSizeFactor: 0.75}.scaledSize(0))
	assert.Equal(t, int64(75), worstCaseFetchedDataStrategy{postingListActualSizeFactor: 0.75}.scaledSize(100))
	assert.Equal(t, int64(1000), worstCaseFetchedDataStrategy{postingListActualSizeFactor: 10}.scaledSize(100))

	// A product that does not fit in an int64 gives the largest int64.
	assert.Equal(t, int64(math.MaxInt64), worstCaseFetchedDataStrategy{postingListActualSizeFactor: 1e300}.scaledSize(1))
	assert.Equal(t, int64(math.MaxInt64), worstCaseFetchedDataStrategy{postingListActualSizeFactor: 1}.scaledSize(math.MaxInt64))
}

// selectionRecord is what a recording strategy saw.
type selectionRecord struct {
	selected, omitted []string
	// stoppedSelected is the number of groups with stoppedEarly that selectPostings selected.
	stoppedSelected int
	// stoppedInput is the number of groups with stoppedEarly that selectPostings got.
	stoppedInput int
}

func (r *selectionRecord) record(groups, selected, omitted []postingGroup) {
	name := func(g postingGroup) string {
		if g.matcher == nil {
			return "ALL_POSTINGS"
		}
		return g.matcher.String()
	}
	for _, g := range groups {
		if g.stoppedEarly {
			r.stoppedInput++
		}
	}
	for _, g := range selected {
		r.selected = append(r.selected, name(g))
		if g.stoppedEarly {
			r.stoppedSelected++
		}
	}
	for _, g := range omitted {
		r.omitted = append(r.omitted, name(g))
	}
}

// stoppingRecordingStrategy records the selection of a worstCaseFetchedDataStrategy, and it reports its omit threshold.
type stoppingRecordingStrategy struct {
	worstCaseFetchedDataStrategy
	record *selectionRecord
}

func (s stoppingRecordingStrategy) selectPostings(groups []postingGroup) (selected, omitted []postingGroup) {
	// selectPostings sorts the slice that it gets, so keep a copy of the groups for the record.
	input := slices.Clone(groups)
	selected, omitted = s.worstCaseFetchedDataStrategy.selectPostings(groups)
	s.record.record(input, selected, omitted)
	return selected, omitted
}

// nonStoppingRecordingStrategy records the selection of a strategy, and it hides the omit threshold of the strategy.
type nonStoppingRecordingStrategy struct {
	postingsSelectionStrategy
	record *selectionRecord
}

func (s nonStoppingRecordingStrategy) selectPostings(groups []postingGroup) (selected, omitted []postingGroup) {
	input := slices.Clone(groups)
	selected, omitted = s.postingsSelectionStrategy.selectPostings(groups)
	s.record.record(input, selected, omitted)
	return selected, omitted
}

// TestWorstCaseFetchedDataStrategy_OmitThresholdKeepsTheSelection checks that selectPostings makes the same choice
// for a group with its real size and for the same group that stopped at the omit threshold.
func TestWorstCaseFetchedDataStrategy_OmitThresholdKeepsTheSelection(t *testing.T) {
	rnd := rand.New(rand.NewSource(1))

	// randomGroup returns a group of label values, where each label value has between 1 and maxSeries series.
	randomGroup := func(name string, maxLabelValues, maxSeries int) postingGroup {
		labelValues := 1 + rnd.Intn(maxLabelValues)
		group := postingGroup{
			isSubtract:  rnd.Intn(5) == 0,
			matcher:     labels.MustNewMatcher(labels.MatchEqual, name, "x"),
			droppedKeys: labelValues,
		}
		for i := 0; i < labelValues; i++ {
			group.totalSize += 4 + 4*int64(1+rnd.Intn(maxSeries))
		}
		return group
	}
	matcherNames := func(groups []postingGroup) []string {
		var names []string
		for _, g := range groups {
			names = append(names, g.matcher.Name)
		}
		return names
	}

	var checked, checkedWithTie int
	for i := 0; i < 5000; i++ {
		strategy := worstCaseFetchedDataStrategy{postingListActualSizeFactor: []float64{0.1, 0.5, 0.75, 1, 2, 10, 100, 1000}[rnd.Intn(8)]}

		// The groups that toPostingGroups builds before the lazy group, and the groups that it builds after it.
		// Together with the lazy group, they can be more than 12 groups, which is the size where the sort stops being an insertion sort.
		var built, later []postingGroup
		for j, n := 0, 1+rnd.Intn(8); j < n; j++ {
			built = append(built, randomGroup(fmt.Sprintf("built%d", j), 30, 200))
		}
		for j, n := 0, rnd.Intn(8); j < n; j++ {
			later = append(later, randomGroup(fmt.Sprintf("later%d", j), 30, 200))
		}

		// The lazy group has label values with between 1 and maxSeries series.
		maxSeries := []int{1, 5, 50, 500}[rnd.Intn(4)]
		lazyMatcher := labels.MustNewMatcher(labels.MatchRegexp, "lazy", "x.+")
		lazyIsSubtract := rnd.Intn(5) == 0
		lists := make([]int64, 1+rnd.Intn(3000))
		var lazyTotalSize int64
		for j := range lists {
			lists[j] = 4 + 4*int64(1+rnd.Intn(maxSeries))
			lazyTotalSize += lists[j]
		}

		threshold := strategy.omitThreshold(built, "lazy")
		if threshold == math.MaxInt64 || lazyTotalSize <= threshold {
			continue
		}

		// The scan of the lazy group stops at the first label value that makes the total size pass the threshold.
		var stoppedTotalSize int64
		var stoppedCount int
		for _, list := range lists {
			stoppedCount++
			stoppedTotalSize += list
			if stoppedTotalSize > threshold {
				break
			}
		}

		// A later group can have the size of the stopped group or the real size of the lazy group.
		// Groups with equal sizes keep the order in which toPostingGroups built them.
		var tie bool
		for j := range later {
			switch rnd.Intn(6) {
			case 0:
				later[j].totalSize, tie = stoppedTotalSize, true
			case 1:
				later[j].totalSize, tie = lazyTotalSize, true
			}
		}

		full := postingGroup{isSubtract: lazyIsSubtract, matcher: lazyMatcher, droppedKeys: len(lists), totalSize: lazyTotalSize}
		stopped := postingGroup{isSubtract: lazyIsSubtract, matcher: lazyMatcher, droppedKeys: stoppedCount, totalSize: stoppedTotalSize, stoppedEarly: true}

		selection := func(lazy postingGroup) (selected, omitted []string) {
			groups := slices.Concat(built, []postingGroup{lazy}, later)
			s, o := strategy.selectPostings(groups)
			return matcherNames(s), matcherNames(o)
		}
		selectedFull, omittedFull := selection(full)
		selectedStopped, omittedStopped := selection(stopped)

		require.ElementsMatch(t, selectedFull, selectedStopped, "iteration %d", i)
		require.ElementsMatch(t, omittedFull, omittedStopped, "iteration %d", i)
		require.Contains(t, omittedStopped, "lazy", "iteration %d", i)
		checked++
		if tie {
			checkedWithTie++
		}
	}

	// Many iterations must check a stopped group, and some of them must have equal sizes, so that the test is not vacuous.
	require.Greater(t, checked, 500)
	require.Greater(t, checkedWithTie, 100)
}

// TestWorstCaseFetchedDataStrategy_GroupsWithEqualSizesKeepTheBuildOrder uses groups with equal sizes.
// A sort that is not stable selects other groups with the stopped size of the lazy group than with its real size.
func TestWorstCaseFetchedDataStrategy_GroupsWithEqualSizesKeepTheBuildOrder(t *testing.T) {
	group := func(name string, size int64) postingGroup {
		return postingGroup{matcher: labels.MustNewMatcher(labels.MatchEqual, name, "x"), droppedKeys: 1, totalSize: size}
	}

	// The smallest group has one series, so the budget is 512 bytes with the factor 1, and the omit threshold is 512 bytes.
	strategy := worstCaseFetchedDataStrategy{postingListActualSizeFactor: 1}
	built := []postingGroup{
		group("a", 8), group("g00", 600), group("g01", 200), group("g02", 200), group("g03", 700), group("g04", 800),
		group("g05", 256), group("g06", 200), group("g07", 800), group("g08", 200),
	}
	later := []postingGroup{
		group("g09", 128), group("g10", 700), group("g11", 128), group("g12", 256), group("g13", 200), group("g14", 200),
		group("g15", 600), group("g16", 700), group("g17", 600),
	}
	require.Equal(t, int64(512), strategy.omitThreshold(built, "lazy"))

	// The scan of the lazy group stops at 520 bytes, and its real size is 960 bytes. Both sizes are above the threshold.
	for name, size := range map[string]int64{"stopped size": 520, "real size": 960} {
		t.Run(name, func(t *testing.T) {
			lazy := group("lazy", size)
			lazy.stoppedEarly = size == 520

			selected, _ := strategy.selectPostings(slices.Concat(built, []postingGroup{lazy}, later))

			// Selection takes the groups with 128 and 200 bytes in the order in which toPostingGroups built them.
			var names []string
			for _, g := range selected {
				names = append(names, g.matcher.Name)
			}
			assert.Equal(t, []string{"a", "g09", "g11", "g01"}, names)
		})
	}
}

func TestLabelValuesPostingsStrategy_DoesNotReportAnOmitThreshold(t *testing.T) {
	// The label values API can select every group, so the groups that the inner strategy omits still need their keys.
	// blockLabelValues passes a pointer, so check both the value and the pointer.
	inner := worstCaseFetchedDataStrategy{postingListActualSizeFactor: 1}
	for name, strategy := range map[string]postingsSelectionStrategy{
		"value":   labelValuesPostingsStrategy{matchersStrategy: inner},
		"pointer": &labelValuesPostingsStrategy{matchersStrategy: inner},
	} {
		t.Run(name, func(t *testing.T) {
			_, ok := strategy.(omitThresholdStrategy)
			assert.False(t, ok)
		})
	}

	var strategy postingsSelectionStrategy = inner
	_, ok := strategy.(omitThresholdStrategy)
	assert.True(t, ok)
}

func TestBlockLabelValues_DoesNotStopTheScanOfTheMatchers(t *testing.T) {
	const (
		pods       = 5_000
		rareSeries = 10
	)
	testBlock := fixtures.SetupTestBlock(t, appendHighCardinalitySeries(pods, rareSeries, "pod"))
	b := testBlockToBucketBlock(t, testBlock)()
	t.Cleanup(func() { assert.NoError(t, b.indexHeaderReader.Close()) })

	var visited int
	b.indexHeaderReader = &interceptedIndexReader{
		Reader:              b.indexHeaderReader,
		onLabelValueVisited: func(string) { visited++ },
	}

	// For a query of the series API, the scan of the pod label stops, because the group of job="rare" is much smaller.
	// The label values API can select every group, so its scan reads every label value.
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "job", "rare"),
		labels.MustNewMatcher(labels.MatchRegexp, "pod", "pod-.+"),
	}
	values, err := blockLabelValues(t.Context(), b, worstCaseFetchedDataStrategy{postingListActualSizeFactor: 0.75}, 5000, "job", matchers, log.NewNopLogger(), newSafeQueryStats())
	require.NoError(t, err)
	require.Equal(t, []string{"rare"}, values)
	require.Equal(t, pods, visited)
}

func TestToPostingGroups_StopsTheScanAtTheOmitThreshold(t *testing.T) {
	requireSeriesSizeOf512Bytes(t)

	const (
		pods       = 5_000
		rareSeries = 10
	)
	testBlock := fixtures.SetupTestBlock(t, appendHighCardinalitySeries(pods, rareSeries, "pod"))
	metricNameBlock := fixtures.SetupTestBlock(t, appendHighCardinalitySeries(pods, rareSeries, model.MetricNameLabel))

	jobRare := labels.MustNewMatcher(labels.MatchEqual, "job", "rare")
	podPrefix := labels.MustNewMatcher(labels.MatchRegexp, "pod", "pod-.+")
	podWithoutPrefix := labels.MustNewMatcher(labels.MatchRegexp, "pod", ".*-.+")
	podNotEmpty := labels.MustNewMatcher(labels.MatchNotEqual, "pod", "")
	metricNamePrefix := labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, "pod-.+")

	headerConfigs := map[string]indexheader.Config{
		"index-header v1": testIndexHeaderV1Config(),
		"index-header v2": indexHeaderBucketReaderConfig(),
	}

	for headerName, headerConfig := range headerConfigs {
		newBlock := func(t *testing.T, block *fixtures.BucketTestBlock) *bucketBlock {
			b := testBlockToBucketBlock(t, block, headerConfig)()
			t.Cleanup(func() { assert.NoError(t, b.indexHeaderReader.Close()) })
			return b
		}
		strategy := worstCaseFetchedDataStrategy{postingListActualSizeFactor: 0.75}

		// The job group has 10 series. Its posting list takes 4 + 4*10 bytes, so the budget is 10 series of 512 bytes.
		// A total size of 6827 scales to 5120, which is the budget, and 6828 scales above it.
		// Each pod has one series, so its posting list takes 8 bytes. The first total size above 6827 is 854*8.
		const (
			expectedThreshold = 6827
			expectedStopped   = 854
		)

		t.Run(headerName+"/the group is much larger than the smallest group", func(t *testing.T) {
			b := newBlock(t, testBlock)

			groups, err := toPostingGroups(t.Context(), []*labels.Matcher{jobRare, podPrefix}, b.indexHeaderReader, strategy)
			require.NoError(t, err)
			require.Len(t, groups, 2)

			require.Equal(t, jobRare, groups[0].matcher)
			require.False(t, groups[0].stoppedEarly)
			require.Len(t, groups[0].keys, 1)
			require.Equal(t, int64(expectedThreshold), strategy.omitThreshold(groups[:1], "pod"))

			podGroup := groups[1]
			require.Equal(t, podPrefix, podGroup.matcher)
			require.True(t, podGroup.stoppedEarly)
			require.False(t, podGroup.sizeEstimated)
			require.Nil(t, podGroup.keys)
			require.Nil(t, podGroup.offsets)
			require.Equal(t, expectedStopped, podGroup.droppedKeys)
			require.Equal(t, int64(expectedStopped*8), podGroup.totalSize)
		})

		t.Run(headerName+"/the matcher has no prefix", func(t *testing.T) {
			b := newBlock(t, testBlock)

			groups, err := toPostingGroups(t.Context(), []*labels.Matcher{jobRare, podWithoutPrefix}, b.indexHeaderReader, strategy)
			require.NoError(t, err)
			require.Len(t, groups, 2)
			require.True(t, groups[1].stoppedEarly)
			require.Equal(t, expectedStopped, groups[1].droppedKeys)
		})

		t.Run(headerName+"/the matcher selects every label value, and the index-header gives no size estimate", func(t *testing.T) {
			b := newBlock(t, testBlock)
			b.indexHeaderReader = &interceptedIndexReader{Reader: b.indexHeaderReader, disableSizeEstimate: true}

			groups, err := toPostingGroups(t.Context(), []*labels.Matcher{jobRare, podNotEmpty}, b.indexHeaderReader, strategy)
			require.NoError(t, err)
			require.Len(t, groups, 2)
			require.True(t, groups[1].stoppedEarly)
			require.False(t, groups[1].sizeEstimated)
			require.Equal(t, expectedStopped, groups[1].droppedKeys)
		})

		t.Run(headerName+"/the strategy does not report an omit threshold", func(t *testing.T) {
			b := newBlock(t, testBlock)

			groups, err := toPostingGroups(t.Context(), []*labels.Matcher{jobRare, podPrefix}, b.indexHeaderReader, selectAllStrategy{})
			require.NoError(t, err)
			require.Len(t, groups, 2)
			require.False(t, groups[1].stoppedEarly)
			require.Len(t, groups[1].keys, pods)
			require.Len(t, groups[1].offsets, pods)
			require.Equal(t, int64(pods*8), groups[1].totalSize)
		})

		t.Run(headerName+"/no intersecting group is built before the lazy group", func(t *testing.T) {
			b := newBlock(t, testBlock)

			groups, err := toPostingGroups(t.Context(), []*labels.Matcher{podPrefix}, b.indexHeaderReader, strategy)
			require.NoError(t, err)
			require.Len(t, groups, 1)
			require.False(t, groups[0].stoppedEarly)
			require.Len(t, groups[0].keys, pods)
		})

		t.Run(headerName+"/a subtracting lazy group stops too", func(t *testing.T) {
			b := newBlock(t, testBlock)

			// The matcher matches the empty string, so its group subtracts the label values that the inverse matcher selects.
			podNotPrefix := labels.MustNewMatcher(labels.MatchNotRegexp, "pod", "pod-.+")
			groups, err := toPostingGroups(t.Context(), []*labels.Matcher{jobRare, podNotPrefix}, b.indexHeaderReader, strategy)
			require.NoError(t, err)
			require.Len(t, groups, 2)
			require.True(t, groups[1].isSubtract)
			require.True(t, groups[1].stoppedEarly)
			require.Nil(t, groups[1].keys)
			require.Equal(t, expectedStopped, groups[1].droppedKeys)
		})

		t.Run(headerName+"/the group of __name__ keeps its keys", func(t *testing.T) {
			b := newBlock(t, metricNameBlock)

			groups, err := toPostingGroups(t.Context(), []*labels.Matcher{jobRare, metricNamePrefix}, b.indexHeaderReader, strategy)
			require.NoError(t, err)
			require.Len(t, groups, 2)
			require.False(t, groups[1].stoppedEarly)
			require.Len(t, groups[1].keys, pods)
		})
	}
}

// hidingOmitThreshold wraps a strategy and hides its omitThreshold method, so toPostingGroups never stops a scan.
type hidingOmitThreshold struct {
	postingsSelectionStrategy
}

func TestExpandedPostings_StoppedScanGivesTheSameResult(t *testing.T) {
	const (
		pods       = 5_000
		rareSeries = 10
	)
	testBlock := fixtures.SetupTestBlock(t, appendHighCardinalitySeries(pods, rareSeries, "pod"))

	jobRare := labels.MustNewMatcher(labels.MatchEqual, "job", "rare")
	jobCommon := labels.MustNewMatcher(labels.MatchEqual, "job", "common")
	podPrefix := labels.MustNewMatcher(labels.MatchRegexp, "pod", "pod-.+")
	podPrefixZero := labels.MustNewMatcher(labels.MatchRegexp, "pod", "pod-0.+")
	podNotPrefix := labels.MustNewMatcher(labels.MatchNotRegexp, "pod", "pod-.+")

	testCases := map[string]struct {
		matchers []*labels.Matcher
		// expectedVisited is the number of label values that the scans visit with the early stop.
		expectedVisited int
		expectedSeries  int
		expectedPending []*labels.Matcher
	}{
		"selection omits the group of the regular expression": {
			matchers:        []*labels.Matcher{jobRare, podPrefix},
			expectedVisited: 854,
			expectedSeries:  rareSeries,
			expectedPending: []*labels.Matcher{podPrefix},
		},
		"selection omits two groups of the regular expressions": {
			matchers:        []*labels.Matcher{jobRare, podPrefix, podPrefixZero},
			expectedVisited: 2 * 854,
			expectedSeries:  rareSeries,
			expectedPending: []*labels.Matcher{podPrefix, podPrefixZero},
		},
		"selection omits the subtracting group": {
			matchers:        []*labels.Matcher{jobRare, podNotPrefix},
			expectedVisited: 854,
			expectedSeries:  rareSeries,
			expectedPending: []*labels.Matcher{podNotPrefix},
		},
		// The group of the regular expression is much smaller than the budget of the common series, so no scan stops.
		"selection selects the group of the regular expression": {
			matchers:        []*labels.Matcher{jobCommon, podPrefix},
			expectedVisited: pods,
			expectedSeries:  pods - rareSeries,
		},
		"selection selects the only group": {
			matchers:        []*labels.Matcher{podPrefix},
			expectedVisited: pods,
			expectedSeries:  pods,
		},
	}

	headerConfigs := map[string]indexheader.Config{
		"index-header v1": testIndexHeaderV1Config(),
		"index-header v2": indexHeaderBucketReaderConfig(),
	}

	// expandedPostings runs ExpandedPostings on a new block, and it counts the label values that the scans visit.
	expandedPostings := func(t *testing.T, headerConfig indexheader.Config, strategy postingsSelectionStrategy, matchers []*labels.Matcher) (refs []storage.SeriesRef, pending []string, visited int) {
		b := testBlockToBucketBlock(t, testBlock, headerConfig)()
		t.Cleanup(func() { assert.NoError(t, b.indexHeaderReader.Close()) })
		b.indexHeaderReader = &interceptedIndexReader{
			Reader:              b.indexHeaderReader,
			onLabelValueVisited: func(string) { visited++ },
		}

		indexr := b.indexReader(strategy)
		t.Cleanup(func() { assert.NoError(t, indexr.Close()) })
		refs, pendingMatchers, err := indexr.ExpandedPostings(t.Context(), matchers, newSafeQueryStats())
		require.NoError(t, err)
		for _, m := range pendingMatchers {
			pending = append(pending, m.String())
		}
		return refs, pending, visited
	}

	for headerName, headerConfig := range headerConfigs {
		for name, tc := range testCases {
			t.Run(headerName+"/"+name, func(t *testing.T) {
				strategy := worstCaseFetchedDataStrategy{postingListActualSizeFactor: 0.75}

				// Without the omit threshold, every scan visits every label value.
				expectedRefs, expectedPending, visitedWithoutStop := expandedPostings(t, headerConfig, hidingOmitThreshold{strategy}, tc.matchers)
				require.Len(t, expectedRefs, tc.expectedSeries)
				require.Equal(t, pods*countLazyMatchers(tc.matchers), visitedWithoutStop)

				refs, pending, visited := expandedPostings(t, headerConfig, strategy, tc.matchers)
				require.Equal(t, tc.expectedVisited, visited)
				require.Equal(t, expectedRefs, refs)
				// The order of the pending matchers can differ, because a stopped group has a smaller size.
				require.ElementsMatch(t, expectedPending, pending)
				require.Len(t, pending, len(tc.expectedPending))
				for _, m := range tc.expectedPending {
					require.Contains(t, pending, m.String())
				}
			})
		}
	}
}

// countLazyMatchers returns the number of matchers on the pod label.
func countLazyMatchers(matchers []*labels.Matcher) (count int) {
	for _, m := range matchers {
		if m.Name == "pod" {
			count++
		}
	}
	return count
}

// stoppingSelectAllStrategy selects all groups, and it reports a fixed omit threshold.
type stoppingSelectAllStrategy struct {
	selectAllStrategy
	threshold int64
}

func (s stoppingSelectAllStrategy) omitThreshold([]postingGroup, string) int64 { return s.threshold }

func TestExpandedPostings_ScansAStoppedGroupAgainWhenSelectionSelectsIt(t *testing.T) {
	const (
		pods       = 5_000
		rareSeries = 10
	)
	testBlock := fixtures.SetupTestBlock(t, appendHighCardinalitySeries(pods, rareSeries, "pod"))
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "job", "rare"),
		labels.MustNewMatcher(labels.MatchRegexp, "pod", "pod-.+"),
	}

	for name, headerConfig := range map[string]indexheader.Config{
		"index-header v1": testIndexHeaderV1Config(),
		"index-header v2": indexHeaderBucketReaderConfig(),
	} {
		t.Run(name, func(t *testing.T) {
			b := testBlockToBucketBlock(t, testBlock, headerConfig)()
			t.Cleanup(func() { assert.NoError(t, b.indexHeaderReader.Close()) })

			var logs bytes.Buffer
			b.logger = log.NewLogfmtLogger(&logs)

			var scans, visited int
			b.indexHeaderReader = &interceptedIndexReader{
				Reader: b.indexHeaderReader,
				onLabelValuesOffsetsCalled: func(name string) error {
					if name == "pod" {
						scans++
					}
					return nil
				},
				onLabelValueVisited: func(string) { visited++ },
			}

			// The scan stops at the first total size above the threshold of 104 bytes.
			// Each label value takes 8 bytes, so 13 label values reach 104 bytes, and the 14th label value passes it.
			// The strategy selects every group.
			indexr := b.indexReader(stoppingSelectAllStrategy{threshold: 104})
			t.Cleanup(func() { assert.NoError(t, indexr.Close()) })
			refs, pending, err := indexr.ExpandedPostings(t.Context(), matchers, newSafeQueryStats())
			require.NoError(t, err)

			// The scan after selection reads every label value, so the result equals the result of a strategy that never stops.
			require.Len(t, refs, rareSeries)
			require.Empty(t, pending)
			require.Equal(t, 2, scans)
			require.Equal(t, 14+pods, visited)

			// A stopped group that selection selects is not expected, so the block logs a warning.
			require.Contains(t, logs.String(), "level=warn")
			require.Contains(t, logs.String(), "scan posting group again after the scan stopped early")
		})
	}
}

// TestExpandedPostings_RandomQueriesGiveTheSameResultWithTheEarlyStop compares random queries with and without the early stop.
// The queries have lazy matchers, matchers of __name__, subtracting matchers and matchers of different sizes.
func TestExpandedPostings_RandomQueriesGiveTheSameResultWithTheEarlyStop(t *testing.T) {
	const series = 3_000

	testBlock := fixtures.SetupTestBlock(t, func(tb testing.TB, appenderFactory func() storage.AppenderV2) {
		app := appenderFactory()
		for i := 0; i < series; i++ {
			job := "common"
			switch {
			case i%300 == 0:
				job = "rare"
			case i%30 == 1:
				job = "mid"
			}
			builder := labels.NewScratchBuilder(6)
			builder.Add(model.MetricNameLabel, fmt.Sprintf("m%d", i%3))
			builder.Add("job", job)
			builder.Add("zone", fmt.Sprintf("z%d", i%3))
			builder.Add("ns", fmt.Sprintf("ns-%d", i%50))
			builder.Add("host", fmt.Sprintf("host-%d", i%500))
			builder.Add("pod", fmt.Sprintf("pod-%05d", i))
			builder.Sort()
			_, err := app.Append(0, builder.Labels(), 0, 0, 0, nil, nil, storage.AOptions{})
			require.NoError(tb, err)
		}
		require.NoError(tb, app.Commit())
	})

	m := labels.MustNewMatcher
	matcherPool := []*labels.Matcher{
		m(labels.MatchEqual, "job", "rare"),
		m(labels.MatchEqual, "job", "mid"),
		m(labels.MatchEqual, "job", "common"),
		m(labels.MatchRegexp, "job", "rar.+"),
		m(labels.MatchRegexp, "job", "(rare|mid)"),
		m(labels.MatchNotEqual, "job", "common"),
		m(labels.MatchEqual, "zone", "z1"),
		m(labels.MatchRegexp, "zone", "z.+"),
		m(labels.MatchEqual, model.MetricNameLabel, "m1"),
		m(labels.MatchRegexp, model.MetricNameLabel, "m.+"),
		m(labels.MatchNotEqual, model.MetricNameLabel, "m0"),
		m(labels.MatchRegexp, "pod", "pod-.+"),
		m(labels.MatchRegexp, "pod", "pod-0.+"),
		m(labels.MatchRegexp, "pod", "pod-00.*"),
		m(labels.MatchRegexp, "pod", ".+"),
		m(labels.MatchNotEqual, "pod", ""),
		m(labels.MatchNotRegexp, "pod", "pod-.+"),
		m(labels.MatchNotRegexp, "pod", "pod-1.*"),
		m(labels.MatchNotEqual, "pod", "pod-00001"),
		m(labels.MatchRegexp, "host", "host-.+"),
		m(labels.MatchRegexp, "host", "host-1.*"),
		m(labels.MatchNotRegexp, "host", "host-2.*"),
		m(labels.MatchRegexp, "ns", "ns-.+"),
		m(labels.MatchRegexp, "ns", "ns-1.*"),
		m(labels.MatchNotRegexp, "ns", "ns-.+"),
		m(labels.MatchEqual, "ns", ""),
		m(labels.MatchNotRegexp, "ns", "ns-3.*"),
		m(labels.MatchRegexp, "nolabel", ".+"),
		m(labels.MatchNotRegexp, "nolabel", ".+"),
	}
	factors := []float64{0.1, 0.5, 0.75, 1, 2, 10, 100, 1000}

	headerConfigs := map[string]indexheader.Config{
		"index-header v1": testIndexHeaderV1Config(),
		"index-header v2": indexHeaderBucketReaderConfig(),
	}

	for headerName, headerConfig := range headerConfigs {
		t.Run(headerName, func(t *testing.T) {
			b := testBlockToBucketBlock(t, testBlock, headerConfig)()
			t.Cleanup(func() { assert.NoError(t, b.indexHeaderReader.Close()) })

			expandedPostings := func(strategy postingsSelectionStrategy, matchers []*labels.Matcher) ([]storage.SeriesRef, []string) {
				indexr := b.indexReader(strategy)
				defer func() { assert.NoError(t, indexr.Close()) }()
				refs, pendingMatchers, err := indexr.ExpandedPostings(t.Context(), matchers, newSafeQueryStats())
				require.NoError(t, err)
				var pending []string
				for _, pm := range pendingMatchers {
					pending = append(pending, pm.String())
				}
				return refs, pending
			}

			rnd := rand.New(rand.NewSource(42))
			var queriesWithStoppedGroup, queriesWithResults int
			for i := 0; i < 400; i++ {
				var matchers []*labels.Matcher
				for j, n := 0, 1+rnd.Intn(5); j < n; j++ {
					matchers = append(matchers, matcherPool[rnd.Intn(len(matcherPool))])
				}
				strategy := worstCaseFetchedDataStrategy{postingListActualSizeFactor: factors[rnd.Intn(len(factors))]}
				description := fmt.Sprintf("iteration %d, factor %v, matchers %v", i, strategy.postingListActualSizeFactor, matchers)

				withoutStop, withStop := &selectionRecord{}, &selectionRecord{}
				expectedRefs, expectedPending := expandedPostings(nonStoppingRecordingStrategy{strategy, withoutStop}, matchers)
				refs, pending := expandedPostings(stoppingRecordingStrategy{strategy, withStop}, matchers)

				// The early stop changes neither the series nor the pending matchers. The order of the pending matchers can differ.
				require.Equal(t, expectedRefs, refs, description)
				require.ElementsMatch(t, expectedPending, pending, description)
				// It changes neither the groups that selection selects nor the groups that it omits.
				require.ElementsMatch(t, withoutStop.selected, withStop.selected, description)
				require.ElementsMatch(t, withoutStop.omitted, withStop.omitted, description)
				// Selection omits every stopped group, so the loop that scans again never runs.
				require.Zero(t, withStop.stoppedSelected, description)

				if withStop.stoppedInput > 0 {
					queriesWithStoppedGroup++
				}
				if len(refs) > 0 {
					queriesWithResults++
				}
			}

			// Many queries must stop a scan and return series, so that the test is not vacuous.
			require.Greater(t, queriesWithStoppedGroup, 20)
			require.Greater(t, queriesWithResults, 20)
		})
	}
}
