// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package functions

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	promts "github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/rangevectorsplitting/cache"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// FunctionOverRangeVectorSplit performs range vector function calculation with range splitting and intermediate result
// caching.
// T is the type of intermediate result produced by the function's generate step.
type FunctionOverRangeVectorSplit[T any] struct {
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
	FuncId                   Function
	FuncDef                  FunctionOverRangeVectorDefinition
	Annotations              *annotations.Annotations

	metricNames                 *operators.MetricNames
	enableDelayedNameRemoval    bool
	expressionPosition          posrange.PositionRange
	innerNodeExpressionPosition posrange.PositionRange

	emitAnnotationFunc   types.EmitAnnotationFunc
	seriesValidationFunc RangeVectorSeriesValidationFunction

	cache *cache.Cache[T]

	innerNode      planning.Node
	materializer   *planning.Materializer
	queryTimeRange types.QueryTimeRange
	innerCacheKey  string
	splitRanges    []Range

	generateFunc SplitGenerateFunc[T]
	combineFunc  SplitCombineFunc[T]

	splits []Split[T]
	// seriesToSplits is ordered the same way as SeriesMetadata
	seriesToSplits   [][]SplitSeries
	currentSeriesIdx int

	metadataConsumed bool
	fullyEvaluated   bool
	finalized        bool

	logger     log.Logger
	cacheStats *cache.CacheStats

	prepareStart        time.Time
	prepareEnd          time.Time
	seriesMetadataStart time.Time
	seriesMetadataEnd   time.Time
	finalizeStart       time.Time
	finalizeEnd         time.Time
}

var _ types.InstantVectorOperator = (*FunctionOverRangeVectorSplit[any])(nil)

func NewSplittingFunctionOverRangeVector[T any](
	innerNode planning.Node,
	materializer *planning.Materializer,
	timeRange types.QueryTimeRange,
	ranges []Range,
	innerCacheKey string,
	cacheFactory *cache.CacheFactory,
	funcId Function,
	funcDef FunctionOverRangeVectorDefinition,
	generateFunc SplitGenerateFunc[T],
	combineFunc SplitCombineFunc[T],
	codec cache.SplitCodec[T],
	expressionPosition posrange.PositionRange,
	annotations *annotations.Annotations,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	enableDelayedNameRemoval bool,
	logger log.Logger,
) (*FunctionOverRangeVectorSplit[T], error) {
	if !timeRange.IsInstant {
		return nil, fmt.Errorf("FunctionOverRangeVectorSplit only supports instant queries")
	}

	innerNodeExpressionPosition, err := innerNode.ExpressionPosition()
	if err != nil {
		return nil, fmt.Errorf("failed to get inner node expression position: %w", err)
	}

	o := &FunctionOverRangeVectorSplit[T]{
		innerNode:                   innerNode,
		materializer:                materializer,
		queryTimeRange:              timeRange,
		splitRanges:                 ranges,
		innerCacheKey:               innerCacheKey,
		cache:                       cache.NewCache(cacheFactory, codec),
		FuncId:                      funcId,
		FuncDef:                     funcDef,
		generateFunc:                generateFunc,
		combineFunc:                 combineFunc,
		Annotations:                 annotations,
		MemoryConsumptionTracker:    memoryConsumptionTracker,
		expressionPosition:          expressionPosition,
		innerNodeExpressionPosition: innerNodeExpressionPosition,
		enableDelayedNameRemoval:    enableDelayedNameRemoval,
		logger:                      logger,
		cacheStats:                  &cache.CacheStats{},
	}

	if funcDef.SeriesValidationFuncFactory != nil {
		o.seriesValidationFunc = funcDef.SeriesValidationFuncFactory()
	}

	if funcDef.NeedsSeriesNamesForAnnotations {
		o.metricNames = &operators.MetricNames{}
	}

	o.emitAnnotationFunc = o.emitAnnotation // This is an optimisation to avoid creating the EmitAnnotationFunc instance on every usage.

	return o, nil
}

func (m *FunctionOverRangeVectorSplit[T]) ExpressionPosition() posrange.PositionRange {
	return m.expressionPosition
}

func (m *FunctionOverRangeVectorSplit[T]) Prepare(ctx context.Context, params *types.PrepareParams) error {
	m.prepareStart = time.Now()
	defer func() {
		m.prepareEnd = time.Now()
	}()

	stats.FromContext(ctx).AddSplitRangeVectors(1)

	var err error
	m.splits, err = m.createSplits(ctx)
	if err != nil {
		return err
	}
	for _, split := range m.splits {
		err = split.Prepare(ctx, params)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *FunctionOverRangeVectorSplit[T]) AfterPrepare(ctx context.Context) error {
	for _, split := range m.splits {
		if err := split.AfterPrepare(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (m *FunctionOverRangeVectorSplit[T]) createSplits(ctx context.Context) ([]Split[T], error) {
	var splits []Split[T]
	var currentUncachedStart int64
	var currentUncachedRanges []Range

	// closeSplits is a helper to close all previously created splits in case of error.
	closeSplits := func() {
		for _, s := range splits {
			s.Close()
		}
	}

	for _, splitRange := range m.splitRanges {
		if splitRange.Cacheable {
			metadata, annotations, results, found, err := m.cache.Get(ctx, int32(m.FuncId), m.innerCacheKey, splitRange.Start, splitRange.End, m.cacheStats)
			if err != nil {
				closeSplits()
				return nil, err
			}

			if found {
				if len(currentUncachedRanges) >= 1 {
					lastRange := currentUncachedRanges[len(currentUncachedRanges)-1]
					operator, err := m.materializeOperatorForTimeRange(currentUncachedStart, lastRange.End)
					if err != nil {
						closeSplits()
						return nil, err
					}

					split, err := NewUncachedSplit(ctx, currentUncachedRanges, operator, m)
					if err != nil {
						operator.Close()
						closeSplits()
						return nil, err
					}

					splits = append(splits, split)
					currentUncachedRanges = nil
				}

				splits = append(splits, NewCachedSplit(metadata, annotations, results, m))
				continue
			}
		}

		if len(currentUncachedRanges) == 0 {
			currentUncachedStart = splitRange.Start
			currentUncachedRanges = []Range{splitRange}
		} else {
			currentUncachedRanges = append(currentUncachedRanges, splitRange)
		}
	}

	if len(currentUncachedRanges) >= 1 {
		lastRange := currentUncachedRanges[len(currentUncachedRanges)-1]
		operator, err := m.materializeOperatorForTimeRange(currentUncachedStart, lastRange.End)
		if err != nil {
			closeSplits()
			return nil, err
		}

		split, err := NewUncachedSplit(ctx, currentUncachedRanges, operator, m)
		if err != nil {
			operator.Close()
			closeSplits()
			return nil, err
		}

		splits = append(splits, split)
	}

	return splits, nil
}

func (m *FunctionOverRangeVectorSplit[T]) materializeOperatorForTimeRange(start int64, end int64) (types.RangeVectorOperator, error) {
	subRange := time.Duration(end-start) * time.Millisecond

	overrideTimeParams := planning.RangeParams{
		IsSet: true,

		Range: subRange,
		// The offset and timestamp are cleared
		Offset:       0,
		HasTimestamp: false,
	}

	splitTimeRange := types.NewInstantQueryTimeRange(promts.Time(end))

	op, err := m.materializer.ConvertNodeToOperatorWithSubRange(m.innerNode, splitTimeRange, overrideTimeParams)
	if err != nil {
		return nil, err
	}

	innerOperator, ok := op.(types.RangeVectorOperator)
	if !ok {
		return nil, fmt.Errorf("error materializing subnode: expected RangeVectorOperator, got %T", op)
	}

	return innerOperator, nil
}

func (m *FunctionOverRangeVectorSplit[T]) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	if m.metadataConsumed {
		return nil, fmt.Errorf("SeriesMetadata() called multiple times on FunctionOverRangeVectorSplit")
	}

	m.seriesMetadataStart = time.Now()
	defer func() {
		m.seriesMetadataEnd = time.Now()
	}()

	var err error
	var metadata []types.SeriesMetadata
	metadata, m.seriesToSplits, err = m.mergeSplitsMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	if m.metricNames != nil {
		m.metricNames.CaptureMetricNames(metadata)
	}

	m.metadataConsumed = true

	if m.FuncDef.SeriesMetadataFunction.Func != nil {
		return m.FuncDef.SeriesMetadataFunction.Func(metadata, m.MemoryConsumptionTracker, m.enableDelayedNameRemoval)
	}
	return metadata, nil
}

func (m *FunctionOverRangeVectorSplit[T]) mergeSplitsMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, [][]SplitSeries, error) {
	if len(m.splits) == 0 {
		return nil, nil, nil
	}

	seriesMap := make(map[string]int)
	var seriesToSplits [][]SplitSeries

	labelBytes := make([]byte, 0, 1024)

	// Reuse split 0's metadata as base instead of copying.
	mergedMetadata, err := m.splits[0].SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, nil, err
	}
	for splitLocalIdx, serieMetadata := range mergedMetadata {
		labelBytes = serieMetadata.Labels.Bytes(labelBytes)
		key := string(labelBytes)

		// Storage guarantees unique series, so each label set appears only once.
		seriesMap[key] = splitLocalIdx
		seriesToSplits = append(seriesToSplits, []SplitSeries{{0, splitLocalIdx}})
		m.splits[0].AppendMergedSeriesIndex(splitLocalIdx, splitLocalIdx)
	}

	for splitIdx := 1; splitIdx < len(m.splits); splitIdx++ {
		split := m.splits[splitIdx]
		splitMetadata, err := split.SeriesMetadata(ctx, matchers)
		if err != nil {
			return nil, nil, err
		}

		for splitLocalIdx, serieMetadata := range splitMetadata {
			labelBytes = serieMetadata.Labels.Bytes(labelBytes)
			key := string(labelBytes)

			mergedIdx, exists := seriesMap[key]
			if !exists {
				mergedIdx = len(mergedMetadata)
				seriesMap[key] = mergedIdx
				mergedMetadata, err = types.SeriesMetadataSlicePool.AppendToSlice(mergedMetadata, m.MemoryConsumptionTracker, serieMetadata)
				if err != nil {
					return nil, nil, err
				}
				seriesToSplits = append(seriesToSplits, nil)
			} else {
				// We don't need the metadata value anymore if it's a duplicate series
				m.MemoryConsumptionTracker.DecreaseMemoryConsumptionForLabels(serieMetadata.Labels)
			}
			seriesToSplits[mergedIdx] = append(seriesToSplits[mergedIdx], SplitSeries{
				SplitIdx:      splitIdx,
				SplitLocalIdx: splitLocalIdx,
			})
			m.splits[splitIdx].AppendMergedSeriesIndex(splitLocalIdx, mergedIdx)

			labelBytes = labelBytes[:0]
		}

		// Clear elements in metadata before putting back in pool, since element decrease is already accounted for.
		clear(splitMetadata)
		types.SeriesMetadataSlicePool.Put(&splitMetadata, m.MemoryConsumptionTracker)
	}

	return mergedMetadata, seriesToSplits, nil
}

func (m *FunctionOverRangeVectorSplit[T]) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if m.currentSeriesIdx >= len(m.seriesToSplits) {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	splitSeriesList := m.seriesToSplits[m.currentSeriesIdx]

	var pieces []T
	for _, splitSeries := range splitSeriesList {
		results, err := m.splits[splitSeries.SplitIdx].GetResultsAt(ctx, splitSeries.SplitLocalIdx)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}
		pieces = append(pieces, results...)
	}

	rangeStart := m.splitRanges[0].Start
	rangeEnd := m.splitRanges[len(m.splitRanges)-1].End

	f, hasFloat, h, err := m.combineFunc(pieces, nil, rangeStart, rangeEnd, m.emitAnnotationFunc, m.MemoryConsumptionTracker)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	data := types.InstantVectorSeriesData{}

	stepT := m.queryTimeRange.StartT

	if hasFloat {
		data.Floats, err = types.FPointSlicePool.Get(1, m.MemoryConsumptionTracker)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}
		data.Floats = append(data.Floats, promql.FPoint{T: stepT, F: f})
	}
	if h != nil {
		data.Histograms, err = types.HPointSlicePool.Get(1, m.MemoryConsumptionTracker)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}
		data.Histograms = append(data.Histograms, promql.HPoint{T: stepT, H: h})
	}

	// Validation after single step, won't work for range queries if we supported them for splitting.
	if m.seriesValidationFunc != nil {
		var metricName string
		if m.metricNames != nil {
			metricName = m.metricNames.GetMetricNameForSeries(m.currentSeriesIdx)
		}
		m.seriesValidationFunc(data, metricName, m.emitAnnotationFunc)
	}

	if m.currentSeriesIdx == len(m.seriesToSplits)-1 {
		m.fullyEvaluated = true
	}

	m.currentSeriesIdx++
	return data, nil
}

func (m *FunctionOverRangeVectorSplit[T]) emitAnnotation(generator types.AnnotationGenerator) {
	var metricName string
	if m.metricNames != nil {
		metricName = m.metricNames.GetMetricNameForSeries(m.currentSeriesIdx)
	}
	m.Annotations.Add(generator(metricName, m.innerNodeExpressionPosition))
}

func (m *FunctionOverRangeVectorSplit[T]) Finalize(ctx context.Context) error {
	// Only cache if we haven't tried caching already, and if all the series were processed.
	if m.finalized || !m.fullyEvaluated {
		return nil
	}

	m.finalizeStart = time.Now()

	logger := spanlogger.FromContext(ctx, m.logger)

	var cachedSplitCount, uncachedSplitCount, uncachedRangeCount, cachedRangeCount int
	for _, split := range m.splits {
		if split.IsCached() {
			cachedSplitCount++
			cachedRangeCount += split.RangeCount()
		} else {
			uncachedSplitCount++
			uncachedRangeCount += split.RangeCount()
		}
	}

	for _, split := range m.splits {
		if err := split.Finalize(ctx); err != nil {
			return err
		}
	}

	m.finalizeEnd = time.Now()

	// Logging stats at info level while feature is experimental and being tested.
	// TODO: reduce log level to debug and remove overly detailed stats when feature is mature.
	level.Info(logger).Log(
		"msg", "range vector splitting stats",
		"function", m.FuncId.PromQLName(),
		"inner_cache_key", m.innerCacheKey,
		"query_start_ms", m.queryTimeRange.StartT,
		"query_end_ms", m.queryTimeRange.EndT,
		"inner_describe", m.innerNode.Describe(),
		"splits_total", len(m.splits),
		"splits_cached", cachedSplitCount,
		"splits_uncached", uncachedSplitCount,
		"ranges_total", uncachedRangeCount+cachedRangeCount,
		"ranges_cached", cachedRangeCount,
		"ranges_uncached", uncachedRangeCount,
		"cache_entries_read", m.cacheStats.ReadEntries,
		"cache_entries_written", m.cacheStats.WrittenEntries,
		"max_series_per_entry", m.cacheStats.MaxSeries,
		"min_series_per_entry", m.cacheStats.MinSeries,
		"total_series_across_entries", m.cacheStats.TotalSeries,
		"max_bytes_per_entry", m.cacheStats.MaxBytes,
		"min_bytes_per_entry", m.cacheStats.MinBytes,
		"total_cache_bytes", m.cacheStats.TotalBytes,
		"prepare_duration", m.prepareEnd.Sub(m.prepareStart),
		"series_metadata_duration", m.seriesMetadataEnd.Sub(m.seriesMetadataStart),
		"metadata_end_to_finalize_start_duration", m.finalizeStart.Sub(m.seriesMetadataEnd),
		"finalize_duration", m.finalizeEnd.Sub(m.finalizeStart),
		"total_duration", m.finalizeEnd.Sub(m.prepareStart),
	)

	m.finalized = true
	return nil
}

func (m *FunctionOverRangeVectorSplit[T]) Close() {
	for _, split := range m.splits {
		split.Close()
	}
}

type Split[T any] interface {
	Prepare(ctx context.Context, params *types.PrepareParams) error
	AfterPrepare(ctx context.Context) error
	// SeriesMetadata returns the metadata for the split. It is expected to only be called once. The caller is expected
	// to put the metadata slice and metadata back in the pool.
	SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error)
	GetResultsAt(ctx context.Context, idx int) ([]T, error)
	// AppendMergedSeriesIndex records the mapping from a split-local series index to the parent's merged series index.
	// This is used to make sure annotations emitted when generating the result for an uncached split reference the
	// correct metric name.
	AppendMergedSeriesIndex(splitLocalIdx int, mergedIdx int)
	Finalize(ctx context.Context) error
	Close()
	IsCached() bool
	RangeCount() int
}

type SplitSeries struct {
	SplitIdx      int
	SplitLocalIdx int
}

type CachedSplit[T any] struct {
	metadata    []mimirpb.Metric
	annotations []cache.Annotation
	results     []T

	parent *FunctionOverRangeVectorSplit[T]
}

func (c *CachedSplit[T]) RangeCount() int {
	return 1
}

func NewCachedSplit[T any](
	metadata []mimirpb.Metric,
	annotations []cache.Annotation,
	results []T,
	parent *FunctionOverRangeVectorSplit[T],
) *CachedSplit[T] {
	return &CachedSplit[T]{
		metadata:    metadata,
		annotations: annotations,
		results:     results,
		parent:      parent,
	}
}

func (p *CachedSplit[T]) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return nil
}

func (p *CachedSplit[T]) AfterPrepare(ctx context.Context) error {
	return nil
}

func (c *CachedSplit[T]) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	seriesMetadata, err := types.SeriesMetadataSlicePool.Get(len(c.metadata), c.parent.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}
	seriesMetadata = seriesMetadata[:len(c.metadata)]

	for i, proto := range c.metadata {
		seriesMetadata[i].Labels = mimirpb.FromLabelAdaptersToLabels(proto.Labels)
		if err := c.parent.MemoryConsumptionTracker.IncreaseMemoryConsumptionForLabels(seriesMetadata[i].Labels); err != nil {
			return nil, err
		}
	}

	return seriesMetadata, nil
}

func (c *CachedSplit[T]) GetResultsAt(_ context.Context, idx int) ([]T, error) {
	if idx >= len(c.results) {
		return nil, fmt.Errorf("index %d out of range for %d results", idx, len(c.results))
	}
	return []T{c.results[idx]}, nil
}

func (c *CachedSplit[T]) Finalize(ctx context.Context) error {
	// Emit cached annotations
	for _, ann := range c.annotations {
		var wrappedErr error
		if ann.Type == cache.INFO {
			wrappedErr = fmt.Errorf("%w: %s", annotations.PromQLInfo, ann.Message)
		} else {
			wrappedErr = fmt.Errorf("%w: %s", annotations.PromQLWarning, ann.Message)
		}
		c.parent.Annotations.Add(wrappedErr)
	}
	return nil
}

func (c *CachedSplit[T]) Close() {
}

func (c *CachedSplit[T]) AppendMergedSeriesIndex(_, _ int) {}

func (c *CachedSplit[T]) IsCached() bool {
	return true
}

type UncachedSplit[T any] struct {
	ranges   []Range
	operator types.RangeVectorOperator

	parent *FunctionOverRangeVectorSplit[T]

	// Data to cache
	rangeResults [][]T
	// Annotations are put into a map for deduping
	rangeAnnotations    []map[cache.Annotation]struct{}
	serializedMetadata  []byte
	seriesMetadataCount int

	// localToMergedIdx maps split-local series index to the parent's merged series index.
	// Used by emitAndCaptureAnnotation to look up the correct metric name when generating results.
	localToMergedIdx      []int
	currentLocalSeriesIdx int

	finalized    bool
	resultGetter *ResultGetter[T]
}

func (p *UncachedSplit[T]) RangeCount() int {
	return len(p.ranges)
}

func NewUncachedSplit[T any](
	ctx context.Context,
	ranges []Range,
	operator types.RangeVectorOperator,
	parent *FunctionOverRangeVectorSplit[T],
) (*UncachedSplit[T], error) {
	rangeResults := make([][]T, len(ranges))
	rangeAnnotations := make([]map[cache.Annotation]struct{}, len(ranges))
	for i := range ranges {
		rangeAnnotations[i] = make(map[cache.Annotation]struct{})
	}

	return &UncachedSplit[T]{
		ranges:           ranges,
		operator:         operator,
		parent:           parent,
		rangeResults:     rangeResults,
		rangeAnnotations: rangeAnnotations,
		finalized:        false,
	}, nil
}

func (p *UncachedSplit[T]) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return p.operator.Prepare(ctx, params)
}

func (p *UncachedSplit[T]) AfterPrepare(ctx context.Context) error {
	return p.operator.AfterPrepare(ctx)
}

func (p *UncachedSplit[T]) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	seriesMetadata, err := p.operator.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	// Defensively serialize series metadata now for later caching. The label adapters created below share
	// string memory with the returned seriesMetadata; serializing copies all bytes into an independent buffer
	// so we don't retain references into memory owned by the caller.
	toCache := cache.CachedSeriesMetadata{
		Series: make([]mimirpb.Metric, len(seriesMetadata)),
	}
	for i, sm := range seriesMetadata {
		toCache.Series[i] = mimirpb.Metric{
			Labels: mimirpb.FromLabelsToLabelAdapters(sm.Labels),
		}
	}
	p.serializedMetadata, err = toCache.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshaling series metadata for cache: %w", err)
	}
	p.seriesMetadataCount = len(seriesMetadata)
	p.localToMergedIdx = make([]int, len(seriesMetadata))

	p.resultGetter = NewResultGetter(p.NextSeries)

	return seriesMetadata, nil
}

func (p *UncachedSplit[T]) GetResultsAt(ctx context.Context, idx int) ([]T, error) {
	return p.resultGetter.GetResultsAtIdx(ctx, idx)
}

func (p *UncachedSplit[T]) NextSeries(ctx context.Context) ([]T, error) {
	localIdx := p.currentLocalSeriesIdx
	p.currentLocalSeriesIdx++

	if err := p.operator.NextSeries(ctx); err != nil {
		return nil, err
	}
	step, err := p.operator.NextStepSamples(ctx)
	if err != nil {
		return nil, err
	}
	results := make([]T, len(p.ranges))
	var previousSubStep *types.RangeVectorStepData
	for rangeIdx, splitRange := range p.ranges {
		var rangeStep *types.RangeVectorStepData
		rangeStep, err = step.SubStep(splitRange.Start, splitRange.End, previousSubStep)
		if err != nil {
			return nil, err
		}
		previousSubStep = rangeStep

		capturingEmitAnnotation := func(generator types.AnnotationGenerator) {
			p.emitAndCaptureAnnotation(rangeIdx, localIdx, generator)
		}

		result, err := p.parent.generateFunc(rangeStep, nil, capturingEmitAnnotation, p.parent.MemoryConsumptionTracker)
		if err != nil {
			return nil, err
		}
		results[rangeIdx] = result

		p.rangeResults[rangeIdx] = append(p.rangeResults[rangeIdx], result)
	}
	return results, nil
}

func (p *UncachedSplit[T]) emitAndCaptureAnnotation(rangeIdx int, localSeriesIdx int, generator types.AnnotationGenerator) {
	var metricName string
	if p.parent.metricNames != nil {
		mergedIdx := p.localToMergedIdx[localSeriesIdx]
		metricName = p.parent.metricNames.GetMetricNameForSeries(mergedIdx)
	}
	annotationErr := generator(metricName, p.parent.innerNodeExpressionPosition)
	p.parent.Annotations.Add(annotationErr)

	var annotationType cache.AnnotationType
	var messageWithoutPrefix string

	// Strip the sentinel error prefix so we can re-wrap it cleanly on replay
	errMsg := annotationErr.Error()
	if errors.Is(annotationErr, annotations.PromQLInfo) {
		annotationType = cache.INFO
		messageWithoutPrefix, _ = strings.CutPrefix(errMsg, annotations.PromQLInfo.Error()+": ")
	} else {
		annotationType = cache.WARNING
		messageWithoutPrefix, _ = strings.CutPrefix(errMsg, annotations.PromQLWarning.Error()+": ")
	}

	annotation := cache.Annotation{
		Type:    annotationType,
		Message: messageWithoutPrefix,
	}

	p.rangeAnnotations[rangeIdx][annotation] = struct{}{}
}

func (p *UncachedSplit[T]) Finalize(ctx context.Context) error {
	if p.finalized {
		return nil
	}

	for rangeIdx, splitRange := range p.ranges {
		if !splitRange.Cacheable {
			continue
		}

		annotationsMap := p.rangeAnnotations[rangeIdx]
		annotations := make([]cache.Annotation, 0, len(annotationsMap))
		for ann := range annotationsMap {
			annotations = append(annotations, ann)
		}

		results := p.rangeResults[rangeIdx]

		if err := p.parent.cache.Set(
			ctx,
			int32(p.parent.FuncId),
			p.parent.innerCacheKey,
			splitRange.Start,
			splitRange.End,
			p.serializedMetadata,
			p.seriesMetadataCount,
			annotations,
			results,
			p.parent.cacheStats,
		); err != nil {
			return err
		}
	}

	p.finalized = true
	return nil
}

func (p *UncachedSplit[T]) Close() {
	if p.operator != nil {
		p.operator.Close()
	}
}

func (p *UncachedSplit[T]) AppendMergedSeriesIndex(splitLocalIdx int, mergedIdx int) {
	p.localToMergedIdx[splitLocalIdx] = mergedIdx
}

func (p *UncachedSplit[T]) IsCached() bool {
	return false
}

type ResultGetter[T any] struct {
	resultBuffer   map[int][]T
	nextSeriesIdx  int
	nextSeriesFunc func(ctx context.Context) ([]T, error)
}

func NewResultGetter[T any](nextSeriesFunc func(ctx context.Context) ([]T, error)) *ResultGetter[T] {
	return &ResultGetter[T]{
		resultBuffer:   make(map[int][]T),
		nextSeriesIdx:  0,
		nextSeriesFunc: nextSeriesFunc,
	}
}

func (p *ResultGetter[T]) GetResultsAtIdx(ctx context.Context, splitSeriesIdx int) ([]T, error) {
	// if series is buffered
	if splitSeriesIdx < p.nextSeriesIdx {
		result, ok := p.resultBuffer[splitSeriesIdx]
		if !ok {
			return nil, fmt.Errorf("could not find buffered result for index %d, nextSeriesIdx %d", splitSeriesIdx, p.nextSeriesIdx)
		}
		// a series should only be read once
		delete(p.resultBuffer, splitSeriesIdx)
		return result, nil
	}

	// buffer series results before the requested series
	for p.nextSeriesIdx < splitSeriesIdx {
		results, err := p.nextSeriesFunc(ctx)
		if err != nil {
			return nil, err
		}
		p.resultBuffer[p.nextSeriesIdx] = results
		p.nextSeriesIdx++
	}

	result, err := p.nextSeriesFunc(ctx)
	if err != nil {
		return nil, err
	}
	p.nextSeriesIdx++
	return result, nil
}
