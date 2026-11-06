// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"encoding/binary"
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

	testBlock := fixtures.SetupTestBlock(t, appendHighCardinalitySeries(pods, 100))

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
							value := highCardinalityPod(i)
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

					actual, err := group.toPostingGroup(t.Context(), testBucketBlock.indexHeaderReader)
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
