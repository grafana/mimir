// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/indexheader"
	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestWorstCaseStreamedStrategy(t *testing.T) {
	ctx := context.Background()
	incidentBlockDir := createTestHighCardinalityBlock(t)

	t.Run(`pod!="" parks mid-measure, before materializing keys`, func(t *testing.T) {
		reader := openIndexHeaderReaderForBlock(t, incidentBlockDir)
		src := newCountingOffsetsSource(newIndexHeaderPostingOffsetsSource(reader))

		ms := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric"),
			labels.MustNewMatcher(labels.MatchNotEqual, "pod", ""),
		}
		groups, err := newPostingGroupStreams(ctx, ms, src, nil)
		require.NoError(t, err)

		strategy := newWorstCaseStreamedStrategy(0.75)
		selected, pending, err := strategy.selectPostings(ctx, groups)
		require.NoError(t, err)

		require.Len(t, selected, 1)

		selectedGroups, err := resolvePostingGroupsFromStream(ctx, selected)
		require.NoError(t, err)
		require.Equal(t, `__name__="test_metric"`, selectedGroups[0].matcher.String())
		require.Equal(t, []labels.Label{{Name: "__name__", Value: "test_metric"}}, selectedGroups[0].keys)
		require.Len(t, selectedGroups[0].offsets, 1)
		require.Equal(t, int64(404), selectedGroups[0].totalSize)

		require.Len(t, pending, 1)
		require.Equal(t, `pod!=""`, pending[0].matcher().String())

		// The pod group parked during its measure pass: it pulled a prefix of
		// the 30100 pod values and was never re-walked to collect its keys.
		require.Equal(t, 1, src.itersOpened["pod"])
		require.Positive(t, src.pulled["pod"])
		require.Less(t, src.pulled["pod"], 30100)
	})

	t.Run("estimate parks the group before any index scan", func(t *testing.T) {
		reader := openIndexHeaderReaderForBlock(t, incidentBlockDir)
		src := newCountingOffsetsSource(newIndexHeaderPostingOffsetsSource(reader))
		est := fixedEstimator{sizes: map[string]int64{"pod": 1 << 20}}

		ms := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric"),
			labels.MustNewMatcher(labels.MatchNotEqual, "pod", ""),
		}
		groups, err := newPostingGroupStreams(ctx, ms, src, est)
		require.NoError(t, err)

		strategy := newWorstCaseStreamedStrategy(0.75)
		selected, pending, err := strategy.selectPostings(ctx, groups)
		require.NoError(t, err)

		require.Len(t, selected, 1)
		require.Len(t, pending, 1)
		require.Equal(t, `pod!=""`, pending[0].matcher().String())
		require.Equal(t, 0, src.itersOpened["pod"])
		require.Equal(t, 0, src.pulled["pod"])
	})

	t.Run("lazy group with no values empties the request", func(t *testing.T) {
		reader := openIndexHeaderReaderForBlock(t, incidentBlockDir)
		src := newCountingOffsetsSource(newIndexHeaderPostingOffsetsSource(reader))

		ms := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric"),
			labels.MustNewMatcher(labels.MatchNotEqual, "missing_label", ""),
		}
		groups, err := newPostingGroupStreams(ctx, ms, src, nil)
		require.NoError(t, err)

		selected, pending, err := newWorstCaseStreamedStrategy(0.75).selectPostings(ctx, groups)
		require.NoError(t, err)
		require.Empty(t, selected)
		require.Empty(t, pending)
	})

	t.Run("only-lazy request still materializes its single group", func(t *testing.T) {
		reader := openIndexHeaderReaderForBlock(t, incidentBlockDir)
		src := newCountingOffsetsSource(newIndexHeaderPostingOffsetsSource(reader))

		ms := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchNotEqual, "pod", ""),
		}
		groups, err := newPostingGroupStreams(ctx, ms, src, nil)
		require.NoError(t, err)

		selected, pending, err := newWorstCaseStreamedStrategy(0.75).selectPostings(ctx, groups)
		require.NoError(t, err)

		require.Len(t, selected, 1)
		require.Empty(t, pending)

		selectedGroups, err := resolvePostingGroupsFromStream(ctx, selected)
		require.NoError(t, err)
		require.Equal(t, "pod", selectedGroups[0].matcher.Name)
		require.Len(t, selectedGroups[0].keys, 30100)
		require.Equal(t, int64(240800), selectedGroups[0].totalSize)

		// One iterator for the measure pass, one for the materialize re-walk:
		// the group is the request's only intersecting group, so the strategy
		// resolves it unconditionally, like the batch strategy does.
		require.Equal(t, 2, src.itersOpened["pod"])
	})

	t.Run("example blocks: same decisions as the batch strategy", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			blockDir string
			ms       []*labels.Matcher
		}{
			{
				name:     "storegateway fixture",
				blockDir: filepath.Join("fixtures", "test-query-block-with-ooo-chunks", "01HNWXEBE3GSYWM2BX7FPFDEKT"),
				ms: []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "__name__", "test"),
					labels.MustNewMatcher(labels.MatchNotEqual, "case", ""),
				},
			},
			{
				name:     "index-header test data",
				blockDir: filepath.Join("..", "storage", "indexheader", "testdata", "index_format_v2"),
				ms: []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "__name__", "continuous_app_metric0"),
					labels.MustNewMatcher(labels.MatchNotEqual, "__blockgen_target__", ""),
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				reader := openIndexHeaderReaderForBlock(t, tc.blockDir)

				batchGroups, err := toPostingGroups(ctx, tc.ms, reader)
				require.NoError(t, err)
				batchSelected, batchOmitted := worstCaseFetchedDataStrategy{postingListActualSizeFactor: 0.75}.selectPostings(batchGroups)

				groups, err := newPostingGroupStreams(ctx, tc.ms, newIndexHeaderPostingOffsetsSource(reader), nil)
				require.NoError(t, err)
				selected, pending, err := newWorstCaseStreamedStrategy(0.75).selectPostings(ctx, groups)
				require.NoError(t, err)

				require.Equal(t, matcherStringsOfBatchGroups(batchSelected), matcherStringsOfResolvedGroups(selected))
				require.Equal(t, matcherStringsOfBatchGroups(batchOmitted), matcherStringsOfResolvedGroups(pending))
			})
		}
	})
}

// createTestHighCardinalityBlock writes a test block, shaped like a high-cardinality block.
// A queried metric with few series next to filler series that inflate the pod label to one value per series.
func createTestHighCardinalityBlock(t *testing.T) string {
	const (
		testSeries   = 100
		fillerSeries = 30000
	)
	series := make([]labels.Labels, 0, testSeries+fillerSeries)
	for i := range testSeries {
		series = append(series, labels.FromStrings("__name__", "test_metric", "pod", fmt.Sprintf("pod-%04d", i)))
	}
	for i := range fillerSeries {
		series = append(series, labels.FromStrings("__name__", fmt.Sprintf("filler_metric_%05d", i), "pod", fmt.Sprintf("filler-pod-%05d", i)))
	}

	dir := t.TempDir()
	id, err := block.CreateBlock(context.Background(), dir, series, 10, 0, 100, labels.EmptyLabels())
	require.NoError(t, err)
	return filepath.Join(dir, id.String())
}

func openIndexHeaderReaderForBlock(t *testing.T, blockDir string) indexheader.Reader {
	ctx := context.Background()

	meta, err := block.ReadMetaFromDir(blockDir)
	require.NoError(t, err)

	bucketDir := t.TempDir()
	test.Copy(t, blockDir, filepath.Join(bucketDir, meta.ULID.String()))

	ubkt, err := filesystem.NewBucket(bucketDir)
	require.NoError(t, err)
	bkt := objstore.WithNoopInstr(ubkt)
	t.Cleanup(func() {
		require.NoError(t, ubkt.Close())
		require.NoError(t, bkt.Close())
	})

	reader, err := indexheader.NewStreamBinaryReader(
		ctx, meta.ULID, bkt, t.TempDir(),
		indexheader.Config{Version: indexheader.BinaryFormatV1}, 32,
		log.NewNopLogger(), indexheader.NewStreamBinaryReaderMetrics(nil),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })
	return reader
}

type countingOffsetsSource struct {
	inner       postingOffsetsSource
	itersOpened map[string]int
	pulled      map[string]int
}

func newCountingOffsetsSource(inner postingOffsetsSource) *countingOffsetsSource {
	return &countingOffsetsSource{
		inner:       inner,
		itersOpened: map[string]int{},
		pulled:      map[string]int{},
	}
}

func (s *countingOffsetsSource) PostingsOffset(ctx context.Context, name, value string) (index.Range, bool, error) {
	return s.inner.PostingsOffset(ctx, name, value)
}

func (s *countingOffsetsSource) LabelValuesOffsetsIter(ctx context.Context, name, prefix string, filter func(string) bool) (postingListOffsetsIter, error) {
	iter, err := s.inner.LabelValuesOffsetsIter(ctx, name, prefix, filter)
	if err != nil {
		return nil, err
	}
	s.itersOpened[name]++
	return &countingOffsetsIter{inner: iter, src: s, name: name}, nil
}

type countingOffsetsIter struct {
	inner postingListOffsetsIter
	src   *countingOffsetsSource
	name  string
}

func (it *countingOffsetsIter) Next() bool {
	if !it.inner.Next() {
		return false
	}
	it.src.pulled[it.name]++
	return true
}

func (it *countingOffsetsIter) At() streamindex.PostingListOffset {
	return it.inner.At()
}

func (it *countingOffsetsIter) Err() error {
	return it.inner.Err()
}

func (it *countingOffsetsIter) Close() error {
	return it.inner.Close()
}

type fixedEstimator struct {
	sizes map[string]int64
}

func (e fixedEstimator) estimatedPostingListSize(_ context.Context, m *labels.Matcher, _ bool) int64 {
	if s, ok := e.sizes[m.Name]; ok {
		return s
	}
	return -1
}

func matcherStringsOfBatchGroups(groups []postingGroup) []string {
	out := make([]string, 0, len(groups))
	for _, g := range groups {
		if g.matcher == nil {
			// The all-postings group has no matcher and never appears in a
			// pending set.
			continue
		}
		out = append(out, g.matcher.String())
	}
	slices.Sort(out)
	return out
}

func matcherStringsOfResolvedGroups(groups []postingGroupStream) []string {
	out := make([]string, 0, len(groups))
	for _, g := range groups {
		out = append(out, g.matcher().String())
	}
	slices.Sort(out)
	return out
}
