// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"
	"math"
	"strconv"
	"testing"

	"github.com/alecthomas/units"
	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/stretchr/testify/require"
)

const countMinEpsilon = 0.005

// mockIndexReader is a simplified in-memory index reader implementation for testing
type mockIndexReader struct {
	memPostings          *index.MemPostings
	series               map[storage.SeriesRef]labels.Labels
	seriesHashCache      *hashcache.SeriesHashCache
	blockSeriesHashCache *hashcache.BlockSeriesHashCache
}

func (p *mockIndexReader) Symbols() index.StringIter {
	panic("mockIndexReader doesn't implement Symbols()")
}

func (p *mockIndexReader) SortedLabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, error) {
	panic("mockIndexReader doesn't implement SortedLabelValues()")
}

func (p *mockIndexReader) LabelValues(_ context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, error) {
	if len(matchers) > 0 {
		panic("mockIndexReader doesn't implement LabelValues() with matchers")
	}
	return p.memPostings.LabelValues(context.Background(), name, hints), nil
}

func (p *mockIndexReader) Postings(ctx context.Context, name string, values ...string) (index.Postings, error) {
	return p.memPostings.Postings(ctx, name, values...), nil
}

func (p *mockIndexReader) PostingsForLabelMatching(context.Context, string, func(value string) bool) index.Postings {
	panic("mockIndexReader doesn't implement PostingsForLabelMatching()")
}

func (p *mockIndexReader) PostingsForAllLabelValues(ctx context.Context, name string) index.Postings {
	return p.memPostings.PostingsForAllLabelValues(ctx, name)
}

func (p *mockIndexReader) PostingsForMatchers(context.Context, bool, ...*labels.Matcher) (index.Postings, error) {
	panic("mockIndexReader doesn't implement PostingsForMatchers()")
}

func (p *mockIndexReader) SortedPostings(index.Postings) index.Postings {
	panic("mockIndexReader doesn't implement SortedPostings()")
}

func (p *mockIndexReader) ShardedPostings(postings index.Postings, shardIndex, shardCount uint64) index.Postings {
	var shardedRefs []storage.SeriesRef
	bufLbls := labels.ScratchBuilder{}

	for postings.Next() {
		id := postings.At()

		var (
			hash uint64
			ok   bool
		)

		// Check if the hash is cached
		if p.blockSeriesHashCache != nil {
			hash, ok = p.blockSeriesHashCache.Fetch(id)
		}

		if !ok {
			// Get the series labels to compute hash
			if ls, exists := p.series[id]; exists {
				hash = labels.StableHash(ls)
			} else {
				// Fallback: generate labels based on series ID for consistent hashing
				bufLbls.Reset()
				bufLbls.Add(model.MetricNameLabel, strconv.Itoa(int(id)))
				hash = labels.StableHash(bufLbls.Labels())
			}

			// Store in cache if available
			if p.blockSeriesHashCache != nil {
				p.blockSeriesHashCache.Store(id, hash)
			}
		}

		// Check if the series belong to the shard
		if hash%shardCount == shardIndex {
			shardedRefs = append(shardedRefs, id)
		}
	}
	return index.NewListPostings(shardedRefs)
}

func (p *mockIndexReader) Series(storage.SeriesRef, *labels.ScratchBuilder, *[]chunks.Meta) error {
	panic("mockIndexReader doesn't implement Series()")
}

func (p *mockIndexReader) LabelNames(_ context.Context, matchers ...*labels.Matcher) ([]string, error) {
	if len(matchers) > 0 {
		panic("mockIndexReader doesn't implement LabelNames() with matchers")
	}
	labelNameSet := make(map[string]struct{})
	for _, ls := range p.series {
		ls.Range(func(label labels.Label) {
			if label.Name == "" {
				return
			}
			labelNameSet[label.Name] = struct{}{}
		})
	}

	labelNames := make([]string, 0, len(labelNameSet))
	for name := range labelNameSet {
		labelNames = append(labelNames, name)
	}

	return labelNames, nil
}

func (p *mockIndexReader) LabelValueFor(context.Context, storage.SeriesRef, string) (string, error) {
	panic("mockIndexReader doesn't implement LabelValueFor()")
}

func (p *mockIndexReader) LabelValuesFor(index.Postings, string) storage.LabelValues {
	panic("mockIndexReader doesn't implement LabelValuesFor()")
}

func (p *mockIndexReader) LabelValuesExcluding(index.Postings, string) storage.LabelValues {
	panic("mockIndexReader doesn't implement LabelValuesExcluding()")
}

func (p *mockIndexReader) LabelNamesFor(context.Context, index.Postings) ([]string, error) {
	panic("mockIndexReader doesn't implement LabelNamesFor()")
}

func (p *mockIndexReader) IndexLookupPlanner() index.LookupPlanner {
	panic("mockIndexReader doesn't implement IndexLookupPlanner()")
}

func (p *mockIndexReader) Close() error {
	return nil
}

func (p *mockIndexReader) Index() (tsdb.IndexReader, error) {
	return p, nil
}

func (p *mockIndexReader) Chunks() (tsdb.ChunkReader, error) {
	panic("mockIndexReader doesn't implement Chunks()")
}

func (p *mockIndexReader) Tombstones() (tombstones.Reader, error) {
	panic("mockIndexReader doesn't implement Tombstones()")
}

func (p *mockIndexReader) Meta() tsdb.BlockMeta {
	return tsdb.BlockMeta{
		ULID: ulid.MustNew(123, nil),
	}
}

func (p *mockIndexReader) Size() int64 {
	panic("mockIndexReader doesn't implement Size()")
}

func newMockIndexReader() *mockIndexReader {
	seriesHashCache := hashcache.NewSeriesHashCache(uint64(350 * units.Mebibyte))
	return &mockIndexReader{
		memPostings:          index.NewMemPostings(),
		series:               make(map[storage.SeriesRef]labels.Labels),
		seriesHashCache:      seriesHashCache,
		blockSeriesHashCache: seriesHashCache.GetBlockCache("0"),
	}
}

func (p *mockIndexReader) add(ref storage.SeriesRef, lset labels.Labels) {
	p.series[ref] = lset
	p.memPostings.Add(ref, lset)
}

// TestLabelsValuesSketches_LabelName tests getting cardinality and value counts for label names,
// but not specific values for a given label name, for small-count cases.
func TestLabelsValuesSketches_LabelName(t *testing.T) {
	type expectedValuesForLabelName struct {
		labelName   string
		cardinality uint64
		valuesCount uint64
	}
	tests := []struct {
		name                        string
		expectedValuesForLabelNames []expectedValuesForLabelName
		seriesRefToLabels           map[storage.SeriesRef]labels.Labels
	}{
		{
			name:                        "empty postings should return no values",
			seriesRefToLabels:           map[storage.SeriesRef]labels.Labels{},
			expectedValuesForLabelNames: []expectedValuesForLabelName{{"", 0, 0}},
		},
		{
			name:                        "one series with empty label name should return no values",
			seriesRefToLabels:           map[storage.SeriesRef]labels.Labels{1: labels.FromStrings(index.AllPostingsKey())},
			expectedValuesForLabelNames: []expectedValuesForLabelName{{"", 0, 0}},
		},
		{
			name:                        "empty label value contributes",
			seriesRefToLabels:           map[storage.SeriesRef]labels.Labels{1: labels.FromStrings("test", "")},
			expectedValuesForLabelNames: []expectedValuesForLabelName{{"test", 1, 1}},
		},
		{
			name: "multiple label names on a single series",
			seriesRefToLabels: map[storage.SeriesRef]labels.Labels{
				1: labels.FromStrings("label1", "value1", "label2", "value2"),
			},
			expectedValuesForLabelNames: []expectedValuesForLabelName{
				{"label1", 1, 1},
				{"label2", 1, 1},
			},
		},
		{
			name: "multiple series with the same label name and value",
			seriesRefToLabels: map[storage.SeriesRef]labels.Labels{
				1: labels.FromStrings("label1", "value1"),
				2: labels.FromStrings("label1", "value1"),
			},
			expectedValuesForLabelNames: []expectedValuesForLabelName{
				{"label1", 2, 1},
			},
		},
		{
			name: "multiple series with the same label name and different values",
			seriesRefToLabels: map[storage.SeriesRef]labels.Labels{
				1: labels.FromStrings("label1", "value1"),
				2: labels.FromStrings("label1", "value2"),
			},
			expectedValuesForLabelNames: []expectedValuesForLabelName{
				{"label1", 2, 2},
			},
		},
		{
			name: "multiple series with overlapping label names",
			seriesRefToLabels: map[storage.SeriesRef]labels.Labels{
				1: labels.FromStrings("label1", "value1", "label2", "value1"),
				2: labels.FromStrings("label2", "value2"),
			},
			expectedValuesForLabelNames: []expectedValuesForLabelName{
				{"label1", 1, 1},
				{"label2", 2, 2},
			},
		},
		{
			name: "multiple series with non-overlapping label names",
			seriesRefToLabels: map[storage.SeriesRef]labels.Labels{
				1: labels.FromStrings("label1", "value1", "label3", "value1"),
				2: labels.FromStrings("label2", "value2"),
			},
			expectedValuesForLabelNames: []expectedValuesForLabelName{
				{"label1", 1, 1},
				{"label2", 1, 1},
				{"label3", 1, 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newMockIndexReader()
			for seriesRef, ls := range tt.seriesRefToLabels {
				p.add(seriesRef, ls)
			}
			gen := NewStatisticsGenerator(log.NewNopLogger())
			sketches, err := gen.Stats(p.Meta(), p, DefaultLabelCardinalityForSmallerSketch, DefaultLabelCardinalityForLargerSketch)
			require.NoError(t, err)
			ctx := context.Background()

			for _, ev := range tt.expectedValuesForLabelNames {
				valuesCount := sketches.LabelValuesCount(ctx, ev.labelName)
				valuesCard := sketches.LabelValuesCardinality(ctx, ev.labelName)

				require.Equal(t, ev.valuesCount, valuesCount)
				require.Equal(t, ev.cardinality, valuesCard)
			}
		})
	}
}

// TestLabelsValuesSketches_LabelValue tests the cardinality calculation when looking at specific label values,
// for small-count cases.
func TestLabelsValuesSketches_LabelValue(t *testing.T) {
	type expectedValuesForLabelNameValues struct {
		labelName   string
		labelValues []string
		cardinality uint64
	}
	tests := []struct {
		name                        string
		expectedValuesForLabelNames []expectedValuesForLabelNameValues
		seriesRefToLabels           map[storage.SeriesRef]labels.Labels
	}{
		{
			name:              "empty label value should match",
			seriesRefToLabels: map[storage.SeriesRef]labels.Labels{1: labels.FromStrings("test", "")},
			expectedValuesForLabelNames: []expectedValuesForLabelNameValues{
				{"test", []string{""}, 1},
			},
		},
		{
			name: "multiple series with matching label values",
			seriesRefToLabels: map[storage.SeriesRef]labels.Labels{
				1: labels.FromStrings("label1", "value1"),
				2: labels.FromStrings("label1", "value1"),
			},
			expectedValuesForLabelNames: []expectedValuesForLabelNameValues{
				{"label1", []string{"value1"}, 2},
				{"label1", []string{""}, 0},
			},
		},
		{
			name: "multiple series with non-matching label values",
			seriesRefToLabels: map[storage.SeriesRef]labels.Labels{
				1: labels.FromStrings("label1", "value1"),
				2: labels.FromStrings("label1", "value2"),
			},
			expectedValuesForLabelNames: []expectedValuesForLabelNameValues{
				{"label1", []string{"value1"}, 1},
				{"label1", []string{"value2"}, 1},
				{"label1", []string{"value1", "value2"}, 2},
				{"label2", []string{"value1", "value2"}, 0},
			},
		},
		{
			name: "multiple series with non-matching label names",
			seriesRefToLabels: map[storage.SeriesRef]labels.Labels{
				1: labels.FromStrings("label1", "value1"),
				2: labels.FromStrings("label2", "value2"),
				3: labels.FromStrings("label2", "value1"),
			},
			expectedValuesForLabelNames: []expectedValuesForLabelNameValues{
				{"label1", []string{"value1"}, 1},
				{"label2", []string{"value1"}, 1},
				{"label2", []string{"value2"}, 1},
				{"label2", []string{"value1", "value2"}, 2},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newMockIndexReader()
			for seriesRef, ls := range tt.seriesRefToLabels {
				p.add(seriesRef, ls)
			}
			gen := NewStatisticsGenerator(log.NewNopLogger())
			sketches, err := gen.Stats(p.Meta(), p, DefaultLabelCardinalityForSmallerSketch, DefaultLabelCardinalityForLargerSketch)
			require.NoError(t, err)
			ctx := context.Background()

			for _, ev := range tt.expectedValuesForLabelNames {
				valuesCard := sketches.LabelValuesCardinality(ctx, ev.labelName, ev.labelValues...)
				require.Equal(t, ev.cardinality, valuesCard)
			}
		})
	}
}

// TestLabelName_ComparisonAcrossLabelNames tests comparison across multiple count-min sketches.
// The count-min sketch epsilon is set by number of label name values such that a series with a relatively
// low number of label name values would produce a sketch with a higher relative-accuracy factor -- that is, a smaller width.
// It requires that the estimated cardinality for all high-cardinality values for a label name are greater than
// the estimated cardinality for all low-cardinality values of a different label name.
// Limitation: This does not test against label names with a different number of series across label names.
func TestLabelName_ComparisonAcrossLabelNames(t *testing.T) {
	lowCardLabel := "low_card"
	highCardLabel := "high_card"
	tests := []struct {
		name                  string
		numLowCardSeries      int
		numHighCardSeries     int
		largerSketchThreshold uint64
	}{
		{
			name:                  "labels would have same epsilon with default",
			numLowCardSeries:      1e3,
			numHighCardSeries:     5e5,
			largerSketchThreshold: DefaultLabelCardinalityForLargerSketch,
		},
		{
			name:                  "labels would have different epsilon with default",
			numLowCardSeries:      1e3,
			numHighCardSeries:     1e6,
			largerSketchThreshold: DefaultLabelCardinalityForLargerSketch,
		},
		{
			name:                  "labels would have high epsilon with custom setting",
			numLowCardSeries:      1e3,
			numHighCardSeries:     1e5,
			largerSketchThreshold: 2e5,
		},
		{
			name:                  "labels would have low epsilon with custom setting",
			numLowCardSeries:      1e3,
			numHighCardSeries:     1e5,
			largerSketchThreshold: 1e2,
		},
		{
			name:                  "labels would have different epsilon with custom setting",
			numLowCardSeries:      1e3,
			numHighCardSeries:     1e5,
			largerSketchThreshold: 1e4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newMockIndexReader()
			require.Less(t, tt.numLowCardSeries, tt.numHighCardSeries)

			// Populate postings
			for i := 0; i < tt.numHighCardSeries; i++ {
				var ls labels.Labels
				if i < tt.numLowCardSeries {
					ls = labels.FromStrings(lowCardLabel, strconv.Itoa(i), highCardLabel, strconv.Itoa(i))
				} else {
					ls = labels.FromStrings(highCardLabel, strconv.Itoa(i))
				}
				p.add(storage.SeriesRef(i), ls)
			}

			ctx := context.Background()
			gen := NewStatisticsGenerator(log.NewNopLogger())
			s, err := gen.Stats(p.Meta(), p, DefaultLabelCardinalityForSmallerSketch, tt.largerSketchThreshold)
			require.NoError(t, err)

			lowCard := s.LabelValuesCardinality(ctx, lowCardLabel)
			require.Equal(t, uint64(tt.numLowCardSeries), s.LabelValuesCount(ctx, lowCardLabel))
			require.Equal(t, uint64(tt.numLowCardSeries), lowCard, "low card: %d, expected: %d", lowCard, tt.numLowCardSeries)

			highCard := s.LabelValuesCardinality(ctx, highCardLabel)
			require.Equal(t, uint64(tt.numHighCardSeries), s.LabelValuesCount(ctx, highCardLabel))
			require.Equal(t, uint64(tt.numHighCardSeries), highCard)

			require.Greater(t, highCard, lowCard)
		})
	}
}

// TestLabelName_ManySeries tests the accuracy of label value sketches at high volume.
// It evenly distributes 6M series across 1k labels,
// and expects the result to be within 30k (i.e., 0.5% or countMinEpsilon of 6M) of 6000 (6M / 1k).
func TestLabelName_ManySeries(t *testing.T) {
	labelName := "test_label"
	p := newMockIndexReader()
	numSeries := int(6e6)
	numLabelValues := int(1e3)
	for i := 0; i < numSeries; i++ {
		ls := labels.FromStrings(labelName, strconv.Itoa(i%numLabelValues))
		p.add(storage.SeriesRef(i), ls)
	}

	ctx := context.Background()
	gen := NewStatisticsGenerator(log.NewNopLogger())
	s, err := gen.Stats(p.Meta(), p, DefaultLabelCardinalityForSmallerSketch, DefaultLabelCardinalityForLargerSketch)
	require.NoError(t, err)

	require.Equal(t, uint64(numLabelValues), s.LabelValuesCount(ctx, labelName))
	require.Equal(t, uint64(numSeries), s.LabelValuesCardinality(ctx, labelName))

	for i := 0; i < numLabelValues; i++ {
		// The cardinality for every label should be within epsilon of the total number of series to the expected cardinality.
		// Technically, it should be within epsilon of the total increments seen by the count-min sketch,
		// but that's more opaque to understand. The total increments seen will always be equal or greater than the number of series.
		require.InDeltaf(t, uint64(numSeries/numLabelValues), s.LabelValuesCardinality(ctx, labelName, strconv.Itoa(i)),
			float64(numSeries)*countMinEpsilon,
			"Cardinality for label %d is not within %d of expected", i, float64(numSeries)*countMinEpsilon,
		)
	}
}

// TestLabelName_NonUniformValueDistribution tests that for a given label, if one value is much lower-cardinality
// than all others, the resulting count-min sketch reflects that difference in order of magnitude against all
// higher-cardinality label values.
func TestLabelName_NonUniformValueDistribution(t *testing.T) {
	labelName := "test_label"
	numSeries := int(6e6)
	lowCard := 10
	lowCardValue := "low"
	numHighOccurrenceValues := int(1e3)

	p := newMockIndexReader()
	require.Less(t, lowCard+numHighOccurrenceValues, numSeries)

	for i := 0; i < numSeries-lowCard; i++ {
		ls := labels.FromStrings(labelName, strconv.Itoa(i%numHighOccurrenceValues))
		p.add(storage.SeriesRef(i), ls)
	}

	// cardinality of "low" value will be 10
	for i := numSeries - lowCard; i < numSeries; i++ {
		ls := labels.FromStrings(labelName, lowCardValue)
		p.add(storage.SeriesRef(i), ls)
	}

	ctx := context.Background()
	gen := NewStatisticsGenerator(log.NewNopLogger())
	s, err := gen.Stats(p.Meta(), p, DefaultLabelCardinalityForSmallerSketch, DefaultLabelCardinalityForLargerSketch)
	require.NoError(t, err)

	lowValCard := s.LabelValuesCardinality(ctx, labelName, lowCardValue)

	// The cardinality of every other value should be ≥6000. We care about these values being correct in magnitude,
	// i.e., floor(log(highOccurrenceCardinality) / log(lowOccurrenceCardinality)) should be consistent every time.
	// We add a little margin since the margin of error (30k) is enough to push us one power up, but not two.
	for i := 0; i < numHighOccurrenceValues; i++ {
		card := s.LabelValuesCardinality(ctx, labelName, strconv.Itoa(i))
		mag := math.Log(float64(card)) / math.Log(float64(lowValCard))
		require.GreaterOrEqual(t, int(math.Floor(mag)), 3)
		require.Less(t, int(math.Floor(mag)), 5)
	}
}
