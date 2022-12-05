package storegateway

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestLoadingSeriesChunkRefsSetIterator(t *testing.T) {
	newTestBlock := prepareTestBlock(test.NewTB(t), -1, func(t testing.TB, appender storage.Appender) {
		for i := 0; i < 100; i++ {
			_, err := appender.Append(0, labels.FromStrings("l1", fmt.Sprintf("v%d", i)), int64(i*10), 0)
			assert.NoError(t, err)
		}
		assert.NoError(t, appender.Commit())
	})

	testCases := map[string]struct {
		shard        *sharding.ShardSelector
		matchers     []*labels.Matcher
		seriesHasher mockSeriesHasher
		skipChunks   bool
		minT, maxT   int64
		batchSize    int

		expectedSets []seriesChunkRefsSet
	}{
		"loads one batch": {
			minT:      0,
			maxT:      10000,
			batchSize: 100,
			matchers:  []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v[1-4]")},
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{{minTime: 10, maxTime: 10, ref: 26}}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{{minTime: 20, maxTime: 20, ref: 234}}},
					{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{{minTime: 30, maxTime: 30, ref: 442}}},
					{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{{minTime: 40, maxTime: 40, ref: 650}}},
				}},
			},
		},
		"loads multiple batches": {
			minT:      0,
			maxT:      10000,
			batchSize: 2,
			matchers:  []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v[1-4]")},
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{{minTime: 10, maxTime: 10, ref: 26}}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{{minTime: 20, maxTime: 20, ref: 234}}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{{minTime: 30, maxTime: 30, ref: 442}}},
					{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{{minTime: 40, maxTime: 40, ref: 650}}},
				}},
			},
		},
		"skips chunks": {
			skipChunks: true,
			minT:       0,
			maxT:       40,
			batchSize:  100,
			matchers:   []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v[1-4]")},
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1")},
					{lset: labels.FromStrings("l1", "v2")},
					{lset: labels.FromStrings("l1", "v3")},
					{lset: labels.FromStrings("l1", "v4")},
				}},
			},
		},
		"doesn't return series if they are outside of minT/maxT": {
			minT:         20,
			maxT:         30,
			batchSize:    100,
			matchers:     []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v1")},
			expectedSets: []seriesChunkRefsSet{},
		},
		"omits empty batches because they fall outside of minT/maxT": {
			minT:      30,
			maxT:      40,
			batchSize: 2,
			matchers:  []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v[1-4]")},
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{{minTime: 30, maxTime: 30, ref: 442}}},
					{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{{minTime: 40, maxTime: 40, ref: 650}}},
				}},
			},
		},
		"returns no batches when no series are owned by shard": {
			shard:        &sharding.ShardSelector{ShardIndex: 1, ShardCount: 2},
			minT:         0,
			maxT:         40,
			batchSize:    2,
			matchers:     []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v[1-4]")},
			expectedSets: []seriesChunkRefsSet{},
		},
		"returns only series that are owned by shard": {
			seriesHasher: mockSeriesHasher{
				hashes: map[string]uint64{`{l1="v3"}`: 1},
			},
			shard:     &sharding.ShardSelector{ShardIndex: 1, ShardCount: 2},
			minT:      0,
			maxT:      40,
			batchSize: 2,
			matchers:  []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v[1-4]")},
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{{minTime: 30, maxTime: 30, ref: 442}}},
				}},
			},
		},
		"ignores mixT/maxT when skipping chunks": {
			minT:       0,
			maxT:       10,
			skipChunks: true,
			batchSize:  4,
			matchers:   []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v[1-4]")},
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1")},
					{lset: labels.FromStrings("l1", "v2")},
					{lset: labels.FromStrings("l1", "v3")},
					{lset: labels.FromStrings("l1", "v4")},
				}},
			},
		},
	}

	for testName, testCase := range testCases {
		testName, testCase := testName, testCase
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			// Setup
			block := newTestBlock()
			indexr := block.indexReader()
			postings, err := indexr.ExpandedPostings(context.Background(), testCase.matchers, newSafeQueryStats())
			require.NoError(t, err)
			postingsIterator := newPostingsSetsIterator(
				postings,
				testCase.batchSize,
			)
			inflatedSeriesIterator := newLoadingSeriesChunkRefsSetIterator(
				context.Background(),
				postingsIterator,
				indexr,
				newSafeQueryStats(),
				block.meta,
				testCase.shard,
				testCase.seriesHasher,
				testCase.skipChunks,
				testCase.minT,
				testCase.maxT,
			)

			// Tests
			sets := readAllSeriesChunkRefsSet(inflatedSeriesIterator)
			assert.NoError(t, inflatedSeriesIterator.Err())
			if !assert.Len(t, sets, len(testCase.expectedSets)) {
				return
			}

			for i, actualSet := range sets {
				expectedSet := testCase.expectedSets[i]
				if !assert.Equalf(t, expectedSet.len(), actualSet.len(), "%d", i) {
					continue
				}
				for j, actualSeries := range actualSet.series {
					expectedSeries := expectedSet.series[j]
					assert.Truef(t, labels.Equal(actualSeries.lset, expectedSeries.lset), "%d, %d: expected labels %s got %s", i, j, expectedSeries.lset, actualSeries.lset)
					if !assert.Lenf(t, actualSeries.chunks, len(expectedSeries.chunks), "%d, %d", i, j) {
						continue
					}
					for k, actualChunk := range actualSeries.chunks {
						expectedChunk := expectedSeries.chunks[k]
						assert.Equalf(t, expectedChunk.maxTime, actualChunk.maxTime, "%d, %d, %d", i, j, k)
						assert.Equalf(t, expectedChunk.minTime, actualChunk.minTime, "%d, %d, %d", i, j, k)
						assert.Equalf(t, int(expectedChunk.ref), int(actualChunk.ref), "%d, %d, %d", i, j, k)
						assert.Equalf(t, block.meta.ULID, actualChunk.blockID, "%d, %d, %d", i, j, k)
					}
				}
			}
		})
	}
}

type mockSeriesHasher struct {
	hashes map[string]uint64
}

func (a mockSeriesHasher) Hash(seriesID storage.SeriesRef, lset labels.Labels, stats *queryStats) uint64 {
	return a.hashes[lset.String()]
}
