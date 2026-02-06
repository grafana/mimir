// SPDX-License-Identifier: AGPL-3.0-only

package selectors

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type Selector struct {
	Queryable            storage.Queryable
	TimeRange            types.QueryTimeRange
	Timestamp            *int64 // Milliseconds since Unix epoch, only set if selector uses @ modifier (eg. metric{...} @ 123)
	Offset               int64  // In milliseconds
	Matchers             types.Matchers
	EagerLoad            bool // If true, Select() call is made when Prepare() is called. This is used by query-frontends when evaluating shardable queries so that all selectors are evaluated in parallel.
	SkipHistogramBuckets bool

	ExpressionPosition posrange.PositionRange

	// Set for instant vector selectors, otherwise 0.
	LookbackDelta time.Duration

	// Set for range vector selectors, otherwise 0.
	Range time.Duration

	// When these range selector modifiers are used the start/end timestamps are adjusted to query for a larger range of points.
	// It's the responsibility of the caller to apply any modifications to the returned samples for these modifiers.
	Anchored bool
	Smoothed bool

	// When the Smoothed range modifier is used this flag can be set to request that interpolated boundary values compensate for counter resets.
	// For instance this is used for rate and increase functions wrapping a smoothed selector.
	// This flag has no effect unless Smoothed is set to true.
	CounterAware bool

	// Pass the set of labels required for the query to the Querier to avoid transferring labels that aren't needed
	// back and forth between the querier and storage layer (ingesters, store-gateways). The storage layer may also
	// apply further optimizations based on this information.
	ProjectionInclude bool
	ProjectionLabels  []string

	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	deduplicator limiter.SeriesDeduplicator
	querier      storage.Querier
	seriesSet    storage.SeriesSet
	series       *seriesList

	seriesIdx int
}

func (s *Selector) Prepare(ctx context.Context, _ *types.PrepareParams) error {
	// Create a per-selector deduplicator for MQE queries. Each selector in a query
	// independently tracks and deduplicates its series. For example, in `foo + foo`,
	// each binary operand has its own deduplicator to ensure accurate per-selector
	// memory accounting.
	s.deduplicator = limiter.NewSeriesDeduplicator()
	if s.EagerLoad {
		return s.loadSeriesSet(ctx, s.Matchers)
	}

	return nil
}

func (s *Selector) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	defer func() {
		// Release our reference to the series set so it can be garbage collected as soon as possible.
		s.seriesSet = nil
	}()

	if s.series != nil {
		return nil, errors.New("should not call Selector.SeriesMetadata() multiple times")
	}

	if !s.EagerLoad {
		if err := s.loadSeriesSet(ctx, s.mergeMatchers(s.Matchers, matchers)); err != nil {
			return nil, err
		}
	}

	s.series = newSeriesList(s.MemoryConsumptionTracker)

	for s.seriesSet.Next() {
		series := s.seriesSet.At()

		if s.SkipHistogramBuckets {
			series = &skipHistogramBucketsSeries{series}
		}

		s.series.Add(series)
	}

	metadata, err := s.series.ToSeriesMetadata()
	if err != nil {
		return nil, err
	}

	return metadata, s.seriesSet.Err()
}

func (s *Selector) mergeMatchers(m1, m2 types.Matchers) types.Matchers {
	if m1 == nil {
		return m2
	}

	if m2 == nil {
		return m1
	}

	unique := make(map[types.Matcher]struct{})
	for _, m := range m1 {
		unique[m] = struct{}{}
	}
	for _, m := range m2 {
		unique[m] = struct{}{}
	}

	out := make([]types.Matcher, 0, len(unique))
	for m := range unique {
		out = append(out, m)
	}

	return out
}

func (s *Selector) loadSeriesSet(ctx context.Context, matchers types.Matchers) error {
	if s.seriesSet != nil {
		return errors.New("should not call Selector.loadSeriesSet() multiple times")
	}

	startTimestamp, endTimestamp := ComputeQueriedTimeRange(s.TimeRange, s.Timestamp, s.Range, s.Offset, s.LookbackDelta, s.Anchored, s.Smoothed)

	hints := &storage.SelectHints{
		Start: startTimestamp,
		End:   endTimestamp,
		Step:  s.TimeRange.IntervalMilliseconds,
		Range: s.Range.Milliseconds(),

		// Mimir Queriers don't use projection hints for anything at time of writing.
		ProjectionInclude: s.ProjectionInclude,
		ProjectionLabels:  s.ProjectionLabels,

		// Mimir doesn't use Grouping or By, so there's no need to include them here.
		//
		// Mimir does use Func to determine if it's a /series request, but this doesn't go
		// through the PromQL engine, so we don't need to include it here either.
		//
		// Mimir does use ShardCount, ShardIndex and DisableTrimming, but not at this level:
		// ShardCount and ShardIndex are set by ingesters and store-gateways when a sharding
		// label matcher is present, and ingesters set DisableTrimming to true.
	}

	// Convert our operator type matchers to Prometheus matchers. This parses any regular
	// expressions contained in the matchers but this should never fail because they are
	// parsed when the query is initially parsed.
	promMatchers, err := matchers.ToPrometheusType()
	if err != nil {
		return err
	}

	s.querier, err = s.Queryable.Querier(startTimestamp, endTimestamp)
	if err != nil {
		return err
	}

	// Add the per-selector deduplicator to context before Select(). This ensures that
	// as series are fetched from queriers, labels are deduplicated and memory is tracked
	// at this selector's scope.
	ctx = limiter.AddSeriesDeduplicatorToContext(ctx, s.deduplicator)

	s.seriesSet = s.querier.Select(ctx, true, hints, promMatchers...)
	return nil
}

func ComputeQueriedTimeRange(timeRange types.QueryTimeRange, timestamp *int64, selectorRange time.Duration, offset int64, lookbackDelta time.Duration, anchored bool, smoothed bool) (int64, int64) {
	startTimestamp := timeRange.StartT
	endTimestamp := timeRange.EndT

	if timestamp != nil {
		// Timestamp from @ modifier takes precedence over query evaluation timestamp.
		startTimestamp = *timestamp
		endTimestamp = *timestamp
	}

	// Apply lookback delta, range and offset after adjusting for timestamp from @ modifier.
	rangeMilliseconds := selectorRange.Milliseconds()
	startTimestamp = startTimestamp - lookbackDelta.Milliseconds() - rangeMilliseconds - offset + 1 // +1 to exclude samples on the lower boundary of the range (queriers work with closed intervals, we use left-open).
	endTimestamp = endTimestamp - offset

	if smoothed {
		endTimestamp += lookbackDelta.Milliseconds()
	}

	return startTimestamp, endTimestamp
}

func (s *Selector) Next(ctx context.Context, existing chunkenc.Iterator) (chunkenc.Iterator, error) {
	if s.series.Len() == 0 {
		return nil, types.EOS
	}

	// Only check for cancellation every 128 series. This avoids a (relatively) expensive check on every iteration, but aborts
	// queries quickly enough when cancelled. Note that we purposefully check for cancellation before incrementing the series
	// index so that we check for cancellation at least once for all selectors.
	// See https://github.com/prometheus/prometheus/pull/14118 for more explanation of why we use 128 (rather than say 100).
	if s.seriesIdx%128 == 0 && ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}

	s.seriesIdx++
	return s.series.Pop().Iterator(existing), nil
}

func (s *Selector) Close() {
	if s.series != nil {
		s.series.Close()
		s.series = nil
	}

	if s.querier != nil {
		_ = s.querier.Close()
		s.querier = nil
	}

	s.seriesSet = nil
}

// seriesList is a FIFO queue of storage.Series.
//
// It is implemented as a linked list of slices of storage.Series, to allow O(1) insertion
// without the memory overhead of a linked list node per storage.Series.
type seriesList struct {
	currentSeriesBatch        *seriesBatch
	seriesIndexInCurrentBatch int

	lastSeriesBatch *seriesBatch

	length                   int
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

func newSeriesList(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *seriesList {
	firstBatch := getSeriesBatch()

	return &seriesList{
		currentSeriesBatch:       firstBatch,
		lastSeriesBatch:          firstBatch,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
}

// Add adds s to the end of this seriesList.
func (l *seriesList) Add(s storage.Series) {
	if len(l.lastSeriesBatch.series) == cap(l.lastSeriesBatch.series) {
		nextBatch := getSeriesBatch()
		l.lastSeriesBatch.next = nextBatch
		l.lastSeriesBatch = nextBatch
	}

	l.lastSeriesBatch.series = append(l.lastSeriesBatch.series, s)
	l.length++
}

// Len returns the total number of series ever added to this seriesList.
func (l *seriesList) Len() int {
	return l.length
}

// ToSeriesMetadata returns a SeriesMetadata value for each series added to this seriesList.
//
// Calling ToSeriesMetadata after calling Pop may return an incomplete list.
func (l *seriesList) ToSeriesMetadata() ([]types.SeriesMetadata, error) {
	metadata, err := types.SeriesMetadataSlicePool.Get(l.length, l.memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	batch := l.currentSeriesBatch

	for batch != nil {
		for _, s := range batch.series {
			metadata, err = types.AppendSeriesMetadata(l.memoryConsumptionTracker, metadata, types.SeriesMetadata{Labels: s.Labels()})
			if err != nil {
				return nil, err
			}
		}

		batch = batch.next
	}

	return metadata, nil
}

// Pop returns the next series from the head of this seriesList, and advances
// to the next item in this seriesList.
func (l *seriesList) Pop() storage.Series {
	if l.currentSeriesBatch == nil || len(l.currentSeriesBatch.series) == 0 {
		panic("no more series to pop")
	}

	s := l.currentSeriesBatch.series[l.seriesIndexInCurrentBatch]
	l.seriesIndexInCurrentBatch++

	if l.seriesIndexInCurrentBatch == len(l.currentSeriesBatch.series) {
		b := l.currentSeriesBatch
		l.currentSeriesBatch = l.currentSeriesBatch.next
		putSeriesBatch(b)
		l.seriesIndexInCurrentBatch = 0
	}

	return s
}

// Close releases resources associated with this seriesList.
func (l *seriesList) Close() {
	for l.currentSeriesBatch != nil {
		b := l.currentSeriesBatch
		l.currentSeriesBatch = l.currentSeriesBatch.next
		putSeriesBatch(b)
	}

	l.lastSeriesBatch = nil // Should have been put back in the pool as part of the loop above.
}

type seriesBatch struct {
	series []storage.Series
	next   *seriesBatch
}

// There's not too much science behind this number: this is based on the batch size used for chunks streaming.
const seriesBatchSize = 256

var seriesBatchPool = sync.Pool{New: func() any {
	return &seriesBatch{
		series: make([]storage.Series, 0, seriesBatchSize),
		next:   nil,
	}
}}

func getSeriesBatch() *seriesBatch {
	return seriesBatchPool.Get().(*seriesBatch)
}

func putSeriesBatch(b *seriesBatch) {
	b.series = b.series[:0]
	b.next = nil
	seriesBatchPool.Put(b)
}

type skipHistogramBucketsSeries struct {
	series storage.Series
}

func (s *skipHistogramBucketsSeries) Labels() labels.Labels {
	return s.series.Labels()
}

func (s *skipHistogramBucketsSeries) Iterator(iterator chunkenc.Iterator) chunkenc.Iterator {
	// Try to reuse the iterator if we can.
	if statsIterator, ok := iterator.(*promql.HistogramStatsIterator); ok {
		statsIterator.Reset(s.series.Iterator(statsIterator.Iterator))
		return statsIterator
	}

	return promql.NewHistogramStatsIterator(s.series.Iterator(iterator))
}
