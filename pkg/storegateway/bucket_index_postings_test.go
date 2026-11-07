// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/fixtures"
	"github.com/grafana/mimir/pkg/storage/indexheader"
	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

func TestBigEndianPostingsCount(t *testing.T) {
	const count = 1000
	raw := make([]byte, count*4)

	for ix := 0; ix < count; ix++ {
		binary.BigEndian.PutUint32(raw[4*ix:], rand.Uint32())
	}

	p := newBigEndianPostings(raw)
	assert.Equal(t, count, p.length())

	c := 0
	for p.Next() {
		c++
	}
	assert.Equal(t, count, c)
}

func TestWorstCaseFetchedDataStrategy(t *testing.T) {
	metricNameMatcher := labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+")

	testCases := map[string]struct {
		input            []postingGroup
		expectedSelected []postingGroup
		expectedOmitted  []postingGroup
	}{
		"single posting group is selected": {
			input: []postingGroup{
				{totalSize: 128},
			},
			expectedSelected: []postingGroup{
				{totalSize: 128},
			},
		},
		"only all-postings & subtracting groups": {
			input: []postingGroup{
				{totalSize: 0 /* all-postings doesn't have a size at the moment */, keys: []labels.Label{allPostingsKey}},
				{isSubtract: true, totalSize: 128},
				{isSubtract: true, totalSize: 64 * 1024 * 1024},
			},
			expectedSelected: []postingGroup{
				{totalSize: 0, keys: []labels.Label{allPostingsKey}},
				{isSubtract: true, totalSize: 128},
				{isSubtract: true, totalSize: 64 * 1024 * 1024},
			},
		},
		"only small posting lists": {
			input: []postingGroup{
				{totalSize: 1024},
				{totalSize: 256},
				{totalSize: 128},
			},
			expectedSelected: []postingGroup{
				{totalSize: 1024},
				{totalSize: 256},
				{totalSize: 128},
			},
		},
		"two small and one large list": {
			input: []postingGroup{
				{totalSize: 64 * 1024 * 1024},
				{totalSize: 256},
				{totalSize: 128},
			},
			expectedSelected: []postingGroup{
				{totalSize: 256},
				{totalSize: 128},
			},
			expectedOmitted: []postingGroup{
				{totalSize: 64 * 1024 * 1024},
			},
		},
		"if smallest group is subtractive it's not used as min size": {
			input: []postingGroup{
				{totalSize: 64 * 1024 * 1024},
				{totalSize: 1024 * 1024},
				{isSubtract: true, totalSize: 128},
			},
			expectedSelected: []postingGroup{
				{totalSize: 64 * 1024 * 1024},
				{totalSize: 1024 * 1024},
				{isSubtract: true, totalSize: 128},
			},
		},
		"one small and two mid size lists are all selected": {
			input: []postingGroup{
				{totalSize: 128},
				{totalSize: 4 * 1024},
				{totalSize: 4 * 1024},
			},
			expectedSelected: []postingGroup{
				{totalSize: 128},
				{totalSize: 4 * 1024},
				{totalSize: 4 * 1024},
			},
		},
		"two small, one large list, one with __name__": {
			input: []postingGroup{
				{totalSize: 64 * 1024 * 1024},
				{totalSize: 64 * 1024 * 1024, keys: []labels.Label{{Name: model.MetricNameLabel, Value: "foo"}}},
				{totalSize: 256},
				{totalSize: 128},
			},
			expectedSelected: []postingGroup{
				// Even though the __name__ group is too large it is still selected
				// in order to minimize the sparseness of the selected series.
				{totalSize: 64 * 1024 * 1024, keys: []labels.Label{{Name: model.MetricNameLabel, Value: "foo"}}},
				{totalSize: 256},
				{totalSize: 128},
			},
			expectedOmitted: []postingGroup{
				{totalSize: 64 * 1024 * 1024},
			},
		},
		"two small, one large list, one without keys for __name__": {
			input: []postingGroup{
				{totalSize: 64 * 1024 * 1024},
				{totalSize: 64 * 1024 * 1024, matcher: metricNameMatcher, droppedKeys: 1000, sizeEstimated: true},
				{totalSize: 256},
				{totalSize: 128},
			},
			expectedSelected: []postingGroup{
				// A group without keys is selected for the same reason as a group with keys.
				// The matcher of the group holds the label name __name__.
				{totalSize: 64 * 1024 * 1024, matcher: metricNameMatcher, droppedKeys: 1000, sizeEstimated: true},
				{totalSize: 256},
				{totalSize: 128},
			},
			expectedOmitted: []postingGroup{
				{totalSize: 64 * 1024 * 1024},
			},
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			actualSelected, actualOmitted := worstCaseFetchedDataStrategy{1.0}.selectPostings(testCase.input)
			assert.ElementsMatch(t, testCase.expectedSelected, actualSelected)
			assert.ElementsMatch(t, testCase.expectedOmitted, actualOmitted)
		})
	}
}

func TestLabelValuesPostingsStrategy(t *testing.T) {
	testCases := map[string]struct {
		postingLists           []streamindex.PostingListOffset
		input                  []postingGroup
		expectedSelected       []postingGroup
		expectedOmitted        []postingGroup
		expectedToPreferSeries bool
	}{
		"posting lists are shortcuttable and per-value lists are very large": {
			postingLists: []streamindex.PostingListOffset{
				{Off: index.Range{Start: 0, End: 1024 * 1024}},
			},
			input: []postingGroup{
				{totalSize: 128},
				{totalSize: 256},
				{totalSize: 1024 * 1024},
			},
			expectedSelected: []postingGroup{
				{totalSize: 128},
				{totalSize: 256},
			},
			expectedOmitted: []postingGroup{
				{totalSize: 1024 * 1024},
			},
			expectedToPreferSeries: true,
		},
		"posting lists are small enough to not be able to do shortcuts, but per-value lists are very large": {
			postingLists: []streamindex.PostingListOffset{
				{Off: index.Range{Start: 0, End: 1024 * 1024}},
			},
			input: []postingGroup{
				{totalSize: 128},
				{totalSize: 256},
				{totalSize: 1024},
			},
			expectedSelected: []postingGroup{
				{totalSize: 128},
				{totalSize: 256},
				{totalSize: 1024},
			},
			expectedOmitted:        nil,
			expectedToPreferSeries: true,
		},
		"posting lists are shortcuttable, but per-value posting list is much smaller than series, so we prefer per-value postings": {
			postingLists: []streamindex.PostingListOffset{
				{Off: index.Range{Start: 0, End: 1024}},
			},
			input: []postingGroup{
				{totalSize: 4 * 1024 * 1024},
				{totalSize: 33 * 1024},
				{totalSize: 33 * 1024},
			},
			expectedSelected: []postingGroup{
				{totalSize: 4 * 1024 * 1024},
				{totalSize: 33 * 1024},
				{totalSize: 33 * 1024},
			},
			expectedOmitted:        nil,
			expectedToPreferSeries: false,
		},
		// A group without keys has only a size and a count of label values. The strategy decides with the size, as for any other group.
		"posting groups without keys are omitted when they are very large": {
			postingLists: []streamindex.PostingListOffset{
				{Off: index.Range{Start: 0, End: 1024 * 1024}},
			},
			input: []postingGroup{
				{totalSize: 128},
				{totalSize: 1024 * 1024, droppedKeys: 20_000, sizeEstimated: true},
			},
			expectedSelected: []postingGroup{
				{totalSize: 128},
			},
			expectedOmitted: []postingGroup{
				{totalSize: 1024 * 1024, droppedKeys: 20_000, sizeEstimated: true},
			},
			expectedToPreferSeries: true,
		},
		"posting groups without keys are selected when per-value posting lists are much smaller than series": {
			postingLists: []streamindex.PostingListOffset{
				{Off: index.Range{Start: 0, End: 1024}},
			},
			input: []postingGroup{
				{totalSize: 4 * 1024 * 1024},
				{totalSize: 33 * 1024, droppedKeys: 1, sizeEstimated: true},
				{totalSize: 33 * 1024, droppedKeys: 1, sizeEstimated: true},
			},
			expectedSelected: []postingGroup{
				{totalSize: 4 * 1024 * 1024},
				{totalSize: 33 * 1024, droppedKeys: 1, sizeEstimated: true},
				{totalSize: 33 * 1024, droppedKeys: 1, sizeEstimated: true},
			},
			expectedOmitted:        nil,
			expectedToPreferSeries: false,
		},
		// Without the size of the group without keys, the strategy would think that all groups are small and select all of them.
		"the size of a group without keys counts in the decision": {
			postingLists: []streamindex.PostingListOffset{
				{Off: index.Range{Start: 0, End: 1000}},
			},
			input: []postingGroup{
				{totalSize: 100},
				{totalSize: 20_000, droppedKeys: 1, sizeEstimated: true},
			},
			expectedSelected: []postingGroup{
				{totalSize: 100},
			},
			expectedOmitted: []postingGroup{
				{totalSize: 20_000, droppedKeys: 1, sizeEstimated: true},
			},
			expectedToPreferSeries: false,
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			strategy := labelValuesPostingsStrategy{
				matchersStrategy: worstCaseFetchedDataStrategy{1},
				allLabelValues:   testCase.postingLists,
			}
			actualSelected, actualOmitted := strategy.selectPostings(testCase.input)
			assert.ElementsMatch(t, testCase.expectedSelected, actualSelected)
			assert.ElementsMatch(t, testCase.expectedOmitted, actualOmitted)

			// The posting values don't matter. Only their size does.
			actualPreferSeries := strategy.preferSeriesToPostings(make([]storage.SeriesRef, numSeriesInSmallestIntersectingPostingGroup(actualSelected)))
			assert.Equal(t, testCase.expectedToPreferSeries, actualPreferSeries)
		})
	}
}

func TestToRawPostingGroup_RegexpWithEmptyAlternative(t *testing.T) {
	// This test verifies that toRawPostingGroup correctly handles MatchRegexp
	// matchers where the regex includes an empty-string alternative (e.g. "|val1|val2").
	// Such matchers match the empty string, which means they should also match
	// series where the label is completely absent. This requires a subtracting
	// posting group (allPostings minus non-matching), NOT an intersecting one
	// (which only looks up specific label values and misses absent labels).
	//
	// Background: the binary operation exclude-hints optimization generates matchers
	// like service=~"|checkout|payments" for labels that are absent from some LHS
	// series. The empty alternative ensures RHS series without that label are included.
	// If toRawPostingGroup creates an intersecting group, it looks up postings for
	// {service,""} (explicit empty — rarely exists in the index) but NOT the implicit
	// "no service label" postings, causing series without the label to be incorrectly
	// filtered out.

	testCases := map[string]struct {
		matcher          *labels.Matcher
		expectIsSubtract bool
		expectIsLazy     bool
	}{
		"MatchRegexp with empty alternative should use subtraction": {
			matcher:          labels.MustNewMatcher(labels.MatchRegexp, "service", "|checkout|payments"),
			expectIsSubtract: true,
			expectIsLazy:     true,
		},
		"MatchRegexp without empty alternative should use intersection": {
			matcher:          labels.MustNewMatcher(labels.MatchRegexp, "service", "checkout|payments"),
			expectIsSubtract: false,
			expectIsLazy:     false,
		},
		"MatchRegexp with only empty value should use subtraction": {
			matcher:          labels.MustNewMatcher(labels.MatchRegexp, "service", ""),
			expectIsSubtract: true,
			expectIsLazy:     true,
		},
		"MatchNotRegexp with empty alternative should use lazy intersection": {
			matcher:          labels.MustNewMatcher(labels.MatchNotRegexp, "service", "|checkout"),
			expectIsSubtract: false,
			expectIsLazy:     true,
		},
		"MatchNotRegexp without empty alternative should use fast subtraction": {
			matcher:          labels.MustNewMatcher(labels.MatchNotRegexp, "service", "checkout|payments"),
			expectIsSubtract: true,
			expectIsLazy:     false,
		},
		"MatchRegexp with many alternatives including empty should use subtraction": {
			matcher:          labels.MustNewMatcher(labels.MatchRegexp, "node", "|host-1|host-2|host-3"),
			expectIsSubtract: true,
			expectIsLazy:     true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			group := toRawPostingGroup(tc.matcher)
			assert.Equal(t, tc.expectIsSubtract, group.isSubtract)
			assert.Equal(t, tc.expectIsLazy, group.isLazy)
		})
	}
}

func TestRawPostingGroup_ToPostingGroup_LazyGroup(t *testing.T) {
	// The label has more label values than lazyGroupInitialCapacity.
	const pods = 5_000

	testBlock := fixtures.SetupTestBlock(t, appendHighCardinalitySeries(pods, 100, "pod"))

	testCases := map[string]struct {
		matcher *labels.Matcher
		// capacityNearLength is true when the scan can reserve the number of matches in advance.
		// Then the capacity of keys and offsets exceeds their length by less than two sampling steps.
		capacityNearLength bool
		// capacityTrimmed is true when the group releases the capacity that a small result does not use.
		capacityTrimmed bool
	}{
		"not equal to empty":             {matcher: labels.MustNewMatcher(labels.MatchNotEqual, "pod", ""), capacityNearLength: true},
		"regexp for any non-empty value": {matcher: labels.MustNewMatcher(labels.MatchRegexp, "pod", ".+"), capacityNearLength: true},
		"equal to empty":                 {matcher: labels.MustNewMatcher(labels.MatchEqual, "pod", ""), capacityNearLength: true},
		"not regexp for any non-empty":   {matcher: labels.MustNewMatcher(labels.MatchNotRegexp, "pod", ".+"), capacityNearLength: true},
		// The matcher matches every label value, but the scan cannot know that in advance, so the capacity grows.
		"regexp for every value with a prefix": {matcher: labels.MustNewMatcher(labels.MatchRegexp, "pod", "pod-.+"), capacityNearLength: true},
		"regexp with a prefix":                 {matcher: labels.MustNewMatcher(labels.MatchRegexp, "pod", "pod-0001.*")},
		"not regexp with a prefix":             {matcher: labels.MustNewMatcher(labels.MatchNotRegexp, "pod", "pod-0001.*")},
		"regexp without a prefix and one match": {
			matcher:         labels.MustNewMatcher(labels.MatchRegexp, "pod", ".*0042-.*"),
			capacityTrimmed: true,
		},
		"regexp without a match": {matcher: labels.MustNewMatcher(labels.MatchRegexp, "pod", "no-such-value.+")},
		"unknown label":          {matcher: labels.MustNewMatcher(labels.MatchNotEqual, "unknown", "")},
	}

	headerConfigs := map[string]indexheader.Config{
		"index-header v1": testIndexHeaderV1Config(),
		"index-header v2": indexHeaderBucketReaderConfig(),
	}

	for headerName, headerConfig := range headerConfigs {
		t.Run(headerName, func(t *testing.T) {
			testBucketBlock := testBlockToBucketBlock(t, testBlock, headerConfig)()
			t.Cleanup(func() { assert.NoError(t, testBucketBlock.indexHeaderReader.Close()) })

			for name, tc := range testCases {
				t.Run(name, func(t *testing.T) {
					group := toRawPostingGroup(tc.matcher)
					require.True(t, group.isLazy)

					filter := tc.matcher.Matches
					if group.isSubtract {
						filter = not(filter)
					}

					// The expected label values come from the generator of the series, and the expected ranges from PostingsOffset.
					// So the expectation does not depend on the scan that toPostingGroup runs.
					var (
						expectedKeys      []labels.Label
						expectedOffsets   []index.Range
						expectedTotalSize int64
					)
					if tc.matcher.Name == "pod" {
						for i := 0; i < pods; i++ {
							value := highCardinalityValue(i)
							if !filter(value) {
								continue
							}
							rng, err := testBucketBlock.indexHeaderReader.PostingsOffset(t.Context(), "pod", value)
							require.NoError(t, err)
							expectedKeys = append(expectedKeys, labels.Label{Name: "pod", Value: value})
							expectedOffsets = append(expectedOffsets, rng)
							expectedTotalSize += rng.End - rng.Start
						}
					}

					actual, err := group.toPostingGroup(t.Context(), testBucketBlock.indexHeaderReader, math.MaxInt64)
					require.NoError(t, err)

					require.Equal(t, group.isSubtract, actual.isSubtract)
					require.Same(t, tc.matcher, actual.matcher)
					require.Equal(t, expectedTotalSize, actual.totalSize)
					if len(expectedKeys) == 0 {
						require.Empty(t, actual.keys)
						require.Empty(t, actual.offsets)
					} else {
						require.Equal(t, expectedKeys, actual.keys)
						require.Equal(t, expectedOffsets, actual.offsets)
					}

					switch {
					case tc.capacityNearLength:
						require.LessOrEqual(t, cap(actual.keys)-len(actual.keys), 2*mimir_tsdb.DefaultPostingOffsetInMemorySampling)
						require.LessOrEqual(t, cap(actual.offsets)-len(actual.offsets), 2*mimir_tsdb.DefaultPostingOffsetInMemorySampling)
					case tc.capacityTrimmed:
						require.Equal(t, len(actual.keys), cap(actual.keys))
						require.Equal(t, len(actual.offsets), cap(actual.offsets))
					default:
						require.LessOrEqual(t, cap(actual.keys), max(lazyGroupInitialCapacity, 2*len(actual.keys)))
						require.LessOrEqual(t, cap(actual.offsets), max(lazyGroupInitialCapacity, 2*len(actual.offsets)))
					}
				})
			}
		})
	}
}

func TestNumSeriesInSmallestIntersectingPostingGroup_CountsTheDroppedKeys(t *testing.T) {
	// The group has 10 posting lists with 100 series in total. Each list holds 4 bytes for its number of entries.
	groups := []postingGroup{{totalSize: 10*4 + 100*4, droppedKeys: 10, sizeEstimated: true}}
	assert.Equal(t, int64(100), numSeriesInSmallestIntersectingPostingGroup(groups))
}

func TestRawPostingGroup_SelectsEveryLabelValue(t *testing.T) {
	testCases := []struct {
		matcher  *labels.Matcher
		expected bool
	}{
		{matcher: labels.MustNewMatcher(labels.MatchNotEqual, "l", ""), expected: true},
		{matcher: labels.MustNewMatcher(labels.MatchRegexp, "l", ".+"), expected: true},
		{matcher: labels.MustNewMatcher(labels.MatchEqual, "l", ""), expected: true},
		{matcher: labels.MustNewMatcher(labels.MatchNotRegexp, "l", ".+"), expected: true},
		// The matchers below do not select every label value. A scan must find the label values that they select.
		{matcher: labels.MustNewMatcher(labels.MatchRegexp, "l", ".*"), expected: false},
		{matcher: labels.MustNewMatcher(labels.MatchNotRegexp, "l", ".*"), expected: false},
		{matcher: labels.MustNewMatcher(labels.MatchRegexp, "l", "a.+"), expected: false},
		{matcher: labels.MustNewMatcher(labels.MatchNotEqual, "l", "x"), expected: false},
		{matcher: labels.MustNewMatcher(labels.MatchEqual, "l", "x"), expected: false},
		{matcher: labels.MustNewMatcher(labels.MatchRegexp, "l", "a|b"), expected: false},
	}

	for _, tc := range testCases {
		t.Run(tc.matcher.String(), func(t *testing.T) {
			assert.Equal(t, tc.expected, toRawPostingGroup(tc.matcher).selectsEveryLabelValue())
		})
	}
}

func TestExpandedPostings_WholeLabelMatchersUseTheSizeEstimate(t *testing.T) {
	const (
		pods       = 5_000
		rareSeries = 10
	)
	testBlock := fixtures.SetupTestBlock(t, appendHighCardinalitySeries(pods, rareSeries, "pod"))

	jobRare := labels.MustNewMatcher(labels.MatchEqual, "job", "rare")
	jobCommon := labels.MustNewMatcher(labels.MatchEqual, "job", "common")
	podNotEmpty := labels.MustNewMatcher(labels.MatchNotEqual, "pod", "")
	podAnyValue := labels.MustNewMatcher(labels.MatchRegexp, "pod", ".+")
	// These two matchers select the series that have no pod label. Their groups subtract the label values of the pod label.
	podEmpty := labels.MustNewMatcher(labels.MatchEqual, "pod", "")
	podNotAnyValue := labels.MustNewMatcher(labels.MatchNotRegexp, "pod", ".+")

	testCases := map[string]struct {
		matchers []*labels.Matcher
		// expectedScans is the number of scans of the pod label with the size estimate.
		expectedScans int
		// expectedScansWithoutEstimate is the number of scans of the pod label without the size estimate. It defaults to 1.
		expectedScansWithoutEstimate int
		// expectedEstimates is the number of calls for a size estimate of the pod label. It defaults to 1.
		expectedEstimates int
		expectedSeries    int
		expectedPending   []*labels.Matcher
	}{
		"selection omits the group of the label with many label values": {
			matchers:        []*labels.Matcher{jobRare, podNotEmpty},
			expectedScans:   0,
			expectedSeries:  rareSeries,
			expectedPending: []*labels.Matcher{podNotEmpty},
		},
		"selection omits the group of the regexp for any value": {
			matchers:        []*labels.Matcher{jobRare, podAnyValue},
			expectedScans:   0,
			expectedSeries:  rareSeries,
			expectedPending: []*labels.Matcher{podAnyValue},
		},
		"selection selects the only group": {
			matchers:       []*labels.Matcher{podNotEmpty},
			expectedScans:  1,
			expectedSeries: pods,
		},
		"selection selects the group next to a larger group": {
			matchers:       []*labels.Matcher{jobCommon, podNotEmpty},
			expectedScans:  1,
			expectedSeries: pods - rareSeries,
		},
		"selection omits the subtracting group of the equal to empty matcher": {
			matchers:        []*labels.Matcher{jobRare, podEmpty},
			expectedScans:   0,
			expectedSeries:  rareSeries,
			expectedPending: []*labels.Matcher{podEmpty},
		},
		"selection omits the subtracting group of the not regexp matcher": {
			matchers:        []*labels.Matcher{jobRare, podNotAnyValue},
			expectedScans:   0,
			expectedSeries:  rareSeries,
			expectedPending: []*labels.Matcher{podNotAnyValue},
		},
		"selection selects the subtracting group next to a larger group": {
			matchers:       []*labels.Matcher{jobCommon, podEmpty},
			expectedScans:  1,
			expectedSeries: 0,
		},
		// Selection selects both groups. The scan runs for each of them, as it does without the size estimate.
		"selection selects two groups of the same label": {
			matchers:                     []*labels.Matcher{podNotEmpty, podAnyValue},
			expectedScans:                2,
			expectedScansWithoutEstimate: 2,
			expectedEstimates:            2,
			expectedSeries:               pods,
		},
	}

	headerConfigs := map[string]indexheader.Config{
		"index-header v1": testIndexHeaderV1Config(),
		"index-header v2": indexHeaderBucketReaderConfig(),
	}

	// expandedPostings runs ExpandedPostings on a new block, and it counts the scans and the estimates per label.
	expandedPostings := func(t *testing.T, headerConfig indexheader.Config, disableSizeEstimate bool, matchers []*labels.Matcher) (refs []storage.SeriesRef, pending []*labels.Matcher, scans, estimates map[string]int) {
		scans, estimates = map[string]int{}, map[string]int{}

		testBucketBlock := testBlockToBucketBlock(t, testBlock, headerConfig)()
		t.Cleanup(func() { assert.NoError(t, testBucketBlock.indexHeaderReader.Close()) })
		testBucketBlock.indexHeaderReader = &interceptedIndexReader{
			Reader:                          testBucketBlock.indexHeaderReader,
			onLabelValuesOffsetsCalled:      func(name string) error { scans[name]++; return nil },
			onLabelValuesSizeEstimateCalled: func(name string) error { estimates[name]++; return nil },
			disableSizeEstimate:             disableSizeEstimate,
		}

		indexr := testBucketBlock.indexReader(worstCaseFetchedDataStrategy{postingListActualSizeFactor: 0.75})
		t.Cleanup(func() { assert.NoError(t, indexr.Close()) })
		refs, pending, err := indexr.ExpandedPostings(t.Context(), matchers, newSafeQueryStats())
		require.NoError(t, err)
		return refs, pending, scans, estimates
	}

	for headerName, headerConfig := range headerConfigs {
		for name, tc := range testCases {
			t.Run(headerName+"/"+name, func(t *testing.T) {
				// Without the size estimate, the scan runs while toPostingGroups builds the group.
				expectedRefs, expectedPending, scans, _ := expandedPostings(t, headerConfig, true, tc.matchers)
				require.Len(t, expectedRefs, tc.expectedSeries)
				require.Equal(t, tc.expectedPending, expectedPending)
				require.Equal(t, cmp.Or(tc.expectedScansWithoutEstimate, 1), scans["pod"])

				refs, pending, scans, estimates := expandedPostings(t, headerConfig, false, tc.matchers)
				require.Equal(t, cmp.Or(tc.expectedEstimates, 1), estimates["pod"])
				require.Equal(t, tc.expectedScans, scans["pod"])
				require.Equal(t, expectedRefs, refs)
				require.Equal(t, expectedPending, pending)
			})
		}
	}
}

func TestExpandedPostings_WholeLabelMatcherOnMetricNameIsScannedWhenSelectionKeepsIt(t *testing.T) {
	const (
		metrics    = 5_000
		rareSeries = 10
	)
	testBlock := fixtures.SetupTestBlock(t, appendHighCardinalitySeries(metrics, rareSeries, model.MetricNameLabel))

	// Selection omits the group of __name__, because the job group is much smaller.
	// Then selection selects the group of __name__ again, because the series are sorted by __name__.
	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+"),
		labels.MustNewMatcher(labels.MatchEqual, "job", "rare"),
	}

	for _, disableSizeEstimate := range []bool{true, false} {
		t.Run(fmt.Sprintf("disableSizeEstimate=%t", disableSizeEstimate), func(t *testing.T) {
			testBucketBlock := testBlockToBucketBlock(t, testBlock)()
			t.Cleanup(func() { assert.NoError(t, testBucketBlock.indexHeaderReader.Close()) })

			var events []string
			testBucketBlock.indexHeaderReader = &interceptedIndexReader{
				Reader:                          testBucketBlock.indexHeaderReader,
				onLabelValuesOffsetsCalled:      func(name string) error { events = append(events, "scan "+name); return nil },
				onLabelValuesSizeEstimateCalled: func(name string) error { events = append(events, "estimate "+name); return nil },
				disableSizeEstimate:             disableSizeEstimate,
			}

			strategy := selectionRecorder{
				postingsSelectionStrategy: worstCaseFetchedDataStrategy{postingListActualSizeFactor: 0.75},
				onSelect:                  func() { events = append(events, "select") },
			}
			indexr := testBucketBlock.indexReader(strategy)
			t.Cleanup(func() { assert.NoError(t, indexr.Close()) })
			refs, pending, err := indexr.ExpandedPostings(t.Context(), matchers, newSafeQueryStats())
			require.NoError(t, err)

			require.Len(t, refs, rareSeries)
			require.Empty(t, pending)
			// With the size estimate, the scan of the label runs after selection. Without it, the scan runs before selection.
			if disableSizeEstimate {
				require.Equal(t, []string{"estimate __name__", "scan __name__", "select"}, events)
			} else {
				require.Equal(t, []string{"estimate __name__", "select", "scan __name__"}, events)
			}
		})
	}
}

// selectionRecorder is a postingsSelectionStrategy that calls onSelect before it delegates to the wrapped strategy.
type selectionRecorder struct {
	postingsSelectionStrategy
	onSelect func()
}

func (s selectionRecorder) selectPostings(groups []postingGroup) (selected, omitted []postingGroup) {
	s.onSelect()
	return s.postingsSelectionStrategy.selectPostings(groups)
}

func TestToPostingGroups_WholeLabelMatchersUseTheSizeEstimateForEveryLabelSize(t *testing.T) {
	testBlock := fixtures.SetupTestBlock(t, appendHighCardinalitySeries(2_000, 10, "pod"), appendSeriesWithRegion(3))
	b := testBlockToBucketBlock(t, testBlock)()
	t.Cleanup(func() { assert.NoError(t, b.indexHeaderReader.Close()) })

	testCases := map[string]struct {
		matcher   *labels.Matcher
		estimated bool
		// exact is true when the estimate must equal the result of the scan.
		exact bool
	}{
		// One sparse offset stands for exactly one label value, so the estimate is exact.
		"label with one label value": {
			matcher:   labels.MustNewMatcher(labels.MatchNotEqual, "region", ""),
			estimated: true,
			exact:     true,
		},
		"label with a few label values": {
			matcher:   labels.MustNewMatcher(labels.MatchNotEqual, "job", ""),
			estimated: true,
		},
		"label with many label values": {
			matcher:   labels.MustNewMatcher(labels.MatchNotEqual, "pod", ""),
			estimated: true,
		},
		"matcher that does not select every label value": {
			matcher:   labels.MustNewMatcher(labels.MatchRegexp, "pod", "pod-.+"),
			estimated: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			groups, err := toPostingGroups(t.Context(), []*labels.Matcher{tc.matcher}, b.indexHeaderReader, selectAllStrategy{})
			require.NoError(t, err)
			require.Len(t, groups, 1)
			group := groups[0]
			require.Equal(t, tc.estimated, group.sizeEstimated)

			exact, err := b.indexHeaderReader.LabelValuesOffsets(t.Context(), tc.matcher.Name, "", nil)
			require.NoError(t, err)
			var exactTotalSize int64
			for _, v := range exact {
				exactTotalSize += v.Off.End - v.Off.Start
			}

			if !tc.estimated {
				require.Len(t, group.keys, len(exact))
				require.Equal(t, exactTotalSize, group.totalSize)
				return
			}

			// The estimate has no keys and no offsets. Its count and size stay close to the exact values.
			maxCountError := float64(mimir_tsdb.DefaultPostingOffsetInMemorySampling / 2)
			if tc.exact {
				maxCountError = 0
			}
			require.Empty(t, group.keys)
			require.Empty(t, group.offsets)
			require.InDelta(t, len(exact), group.keyCount(), maxCountError)
			require.InDelta(t, exactTotalSize, group.totalSize, 8*maxCountError)
		})
	}
}

func TestExpandedPostings_WholeLabelMatcherReturnsTheErrors(t *testing.T) {
	testBlock := fixtures.SetupTestBlock(t, appendHighCardinalitySeries(5_000, 10, "pod"))
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "pod", "")}

	t.Run("the size estimate fails", func(t *testing.T) {
		b := testBlockToBucketBlock(t, testBlock)()
		t.Cleanup(func() { assert.NoError(t, b.indexHeaderReader.Close()) })

		var scans int
		b.indexHeaderReader = &interceptedIndexReader{
			Reader:                          b.indexHeaderReader,
			onLabelValuesSizeEstimateCalled: func(string) error { return context.DeadlineExceeded },
			onLabelValuesOffsetsCalled:      func(string) error { scans++; return nil },
		}
		b.indexCache = cacheNotExpectingToStoreExpandedPostings{t: t}

		indexr := b.indexReader(selectAllStrategy{})
		t.Cleanup(func() { assert.NoError(t, indexr.Close()) })
		_, _, err := indexr.ExpandedPostings(t.Context(), matchers, newSafeQueryStats())
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Zero(t, scans)
	})

	t.Run("the scan after selection fails", func(t *testing.T) {
		b := testBlockToBucketBlock(t, testBlock)()
		t.Cleanup(func() { assert.NoError(t, b.indexHeaderReader.Close()) })

		var estimates int
		b.indexHeaderReader = &interceptedIndexReader{
			Reader:                          b.indexHeaderReader,
			onLabelValuesSizeEstimateCalled: func(string) error { estimates++; return nil },
			onLabelValuesOffsetsCalled:      func(string) error { return context.Canceled },
		}
		b.indexCache = cacheNotExpectingToStoreExpandedPostings{t: t}

		indexr := b.indexReader(selectAllStrategy{})
		t.Cleanup(func() { assert.NoError(t, indexr.Close()) })
		_, _, err := indexr.ExpandedPostings(t.Context(), matchers, newSafeQueryStats())
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, estimates)
	})
}

func TestExpandedPostings_SubtractingWholeLabelMatchersKeepTheSeriesWithoutTheLabel(t *testing.T) {
	const (
		pods       = 5_000
		withoutPod = 7
	)
	testBlock := fixtures.SetupTestBlock(t, appendHighCardinalitySeries(pods, 10, "pod"), appendSeriesWithoutPod(withoutPod))

	testCases := map[string]struct {
		matcher        *labels.Matcher
		expectedSeries int
	}{
		`pod=""`:    {matcher: labels.MustNewMatcher(labels.MatchEqual, "pod", ""), expectedSeries: withoutPod},
		`pod!~".+"`: {matcher: labels.MustNewMatcher(labels.MatchNotRegexp, "pod", ".+"), expectedSeries: withoutPod},
		`pod!=""`:   {matcher: labels.MustNewMatcher(labels.MatchNotEqual, "pod", ""), expectedSeries: pods},
		`pod=~".+"`: {matcher: labels.MustNewMatcher(labels.MatchRegexp, "pod", ".+"), expectedSeries: pods},
	}

	for name, tc := range testCases {
		for _, disableSizeEstimate := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/disableSizeEstimate=%t", name, disableSizeEstimate), func(t *testing.T) {
				b := testBlockToBucketBlock(t, testBlock)()
				t.Cleanup(func() { assert.NoError(t, b.indexHeaderReader.Close()) })

				var estimates int
				b.indexHeaderReader = &interceptedIndexReader{
					Reader:                          b.indexHeaderReader,
					onLabelValuesSizeEstimateCalled: func(string) error { estimates++; return nil },
					disableSizeEstimate:             disableSizeEstimate,
				}

				indexr := b.indexReader(worstCaseFetchedDataStrategy{postingListActualSizeFactor: 0.75})
				t.Cleanup(func() { assert.NoError(t, indexr.Close()) })
				refs, pending, err := indexr.ExpandedPostings(t.Context(), []*labels.Matcher{tc.matcher}, newSafeQueryStats())
				require.NoError(t, err)
				require.Empty(t, pending)
				require.Len(t, refs, tc.expectedSeries)
				// Each case has one matcher that selects every label value of the pod label.
				require.Equal(t, 1, estimates)
			})
		}
	}
}
