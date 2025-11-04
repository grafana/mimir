// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package functions

import (
	"context"
	"errors"
	"time"

	"github.com/grafana/mimir/pkg/streamingpromql/cache"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/prometheus/prometheus/model/histogram"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// FunctionOverRangeVector performs a rate calculation over a range vector.
type FunctionOverRangeVector struct {
	Inner                    types.RangeVectorOperator
	ScalarArgs               []types.ScalarOperator
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
	Func                     FunctionOverRangeVectorDefinition

	Annotations *annotations.Annotations

	scalarArgsData []types.ScalarData

	metricNames        *operators.MetricNames
	currentSeriesIndex int

	timeRange                types.QueryTimeRange
	enableDelayedNameRemoval bool

	expressionPosition   posrange.PositionRange
	emitAnnotationFunc   types.EmitAnnotationFunc
	seriesValidationFunc RangeVectorSeriesValidationFunction

	// Intermediate result caching
	// TODO: set these properly during materialization
	InnerNode           planning.Node
	intermediateResults []IntermediateResultBlock
	// seriesToIRRefs is in the same order as metadata returned from []SeriesMetadata
	// If there is uncached data, it will be the first element in the slice, all other refs will be ordered by time
	seriesToIRRefs [][]IRSeriesRef
	IRCache        *cache.IntermediateResultTenantCache
	stepMover      *StepMover
	metadata       []types.SeriesMetadata
}

type IntermediateResultBlock struct {
	loadedFromCache bool
	startTimestamp  int64
	duration        time.Duration
	ir              cache.IntermediateResultBlock
}

var _ types.InstantVectorOperator = &FunctionOverRangeVector{}

func NewFunctionOverRangeVector(
	inner types.RangeVectorOperator,
	scalarArgs []types.ScalarOperator,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	f FunctionOverRangeVectorDefinition,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
	timeRange types.QueryTimeRange,
	enableDelayedNameRemoval bool,
) *FunctionOverRangeVector {
	o := &FunctionOverRangeVector{
		Inner:                    inner,
		ScalarArgs:               scalarArgs,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		Func:                     f,
		Annotations:              annotations,
		expressionPosition:       expressionPosition,
		timeRange:                timeRange,
		enableDelayedNameRemoval: enableDelayedNameRemoval,
	}

	if f.SeriesValidationFuncFactory != nil {
		o.seriesValidationFunc = f.SeriesValidationFuncFactory()
	}

	if f.NeedsSeriesNamesForAnnotations {
		o.metricNames = &operators.MetricNames{}
	}

	o.emitAnnotationFunc = o.emitAnnotation // This is an optimisation to avoid creating the EmitAnnotationFunc instance on every usage.

	return o
}

func (m *FunctionOverRangeVector) ExpressionPosition() posrange.PositionRange {
	return m.expressionPosition
}

func (m *FunctionOverRangeVector) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	if err := m.processScalarArgs(ctx); err != nil {
		return nil, err
	}

	// Initialize step mover for cached path (where we don't call Inner.NextStepSamples)
	if m.stepMover == nil {
		params := m.Inner.StepCalculationParams()
		m.stepMover = NewStepMover(m.timeRange, params)
	}

	var metadata []types.SeriesMetadata
	var err error
	if m.splittable() {
		m.metadata, m.seriesToIRRefs, err = m.seriesMetadataWithIntermediate(ctx, matchers)
		if err != nil {
			return nil, err
		}
		metadata = m.metadata
	} else {
		metadata, err = m.Inner.SeriesMetadata(ctx, matchers)
		if err != nil {
			return nil, err
		}
	}

	if m.metricNames != nil {
		m.metricNames.CaptureMetricNames(metadata)
	}

	if m.Func.SeriesMetadataFunction.Func != nil {
		return m.Func.SeriesMetadataFunction.Func(metadata, m.MemoryConsumptionTracker, m.enableDelayedNameRemoval)
	}

	return metadata, nil
}

func (m *FunctionOverRangeVector) processScalarArgs(ctx context.Context) error {
	if len(m.ScalarArgs) == 0 {
		return nil
	}

	m.scalarArgsData = make([]types.ScalarData, 0, len(m.ScalarArgs))

	for _, arg := range m.ScalarArgs {
		d, err := arg.GetValues(ctx)
		if err != nil {
			return err
		}
		m.scalarArgsData = append(m.scalarArgsData, d)
	}

	return nil
}

// StepMover keeps track of the step for a range query
// used by intermediate result caching to keep track of steps
type StepMover struct {
	nextStepT         int64
	endT              int64
	intervalMs        int64
	rangeMilliseconds int64
	offset            int64
	timestamp         *int64
}

func NewStepMover(timeRange types.QueryTimeRange, params types.StepCalculationParams) *StepMover {
	return &StepMover{
		nextStepT:         timeRange.StartT,
		endT:              timeRange.EndT,
		intervalMs:        timeRange.IntervalMilliseconds,
		rangeMilliseconds: params.RangeMilliseconds,
		offset:            params.Offset,
		timestamp:         params.Timestamp,
	}
}

func (s *StepMover) NextStep() (*types.RangeVectorStepData, error) {
	if s.nextStepT > s.endT {
		return nil, types.EOS
	}

	stepT := s.nextStepT
	rangeEnd := stepT
	s.nextStepT += s.intervalMs

	if s.timestamp != nil {
		// Timestamp from @ modifier takes precedence over query evaluation timestamp.
		rangeEnd = *s.timestamp
	}

	// Apply offset after adjusting for timestamp from @ modifier.
	rangeEnd = rangeEnd - s.offset
	rangeStart := rangeEnd - s.rangeMilliseconds

	return &types.RangeVectorStepData{
		StepT:      stepT,
		RangeStart: rangeStart,
		RangeEnd:   rangeEnd,
	}, nil
}

func (m *FunctionOverRangeVector) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	// Decide whether we need to move to the next uncached series
	// Also load inner series if necessary
	var cachedResult []IRSeriesRef
	var hasUncached bool
	if m.splittable() {
		if m.currentSeriesIndex >= len(m.seriesToIRRefs) {
			// Write to cache
			for i := range m.intermediateResults {
				// Any irs that were not loaded from cache should be filled and ready to be written to cache
				if !m.intermediateResults[i].loadedFromCache {
					m.IRCache.Set(m.Func.Name, cache.CacheKey(m.InnerNode), m.intermediateResults[i].startTimestamp, m.intermediateResults[i].duration, m.intermediateResults[i].ir)
				}
			}
			return types.InstantVectorSeriesData{}, types.EOS
		}
		result := m.seriesToIRRefs[m.currentSeriesIndex]

		// If the ordered series groups has an uncached component, move to the next inner series
		// The uncached component is always the first elem in the slice
		if result[0].SeriesIdx == UncachedSeriesRef {
			hasUncached = true
			if err := m.Inner.NextSeries(ctx); err != nil {
				return types.InstantVectorSeriesData{}, err
			}
			cachedResult = result[1:] // ignore uncached result
		} else {
			cachedResult = result
		}
	} else {
		if err := m.Inner.NextSeries(ctx); err != nil {
			return types.InstantVectorSeriesData{}, err
		}
	}

	defer func() {
		m.currentSeriesIndex++
	}()

	data := types.InstantVectorSeriesData{}

	var pieces []cache.IntermediateResult
	var stepMover *StepMover
	if m.splittable() {
		numBlocks := int((m.timeRange.EndT - m.timeRange.StartT) / m.splitInterval().Milliseconds())
		pieces = make([]cache.IntermediateResult, numBlocks+2) // add two for head and tail
		stepMover = NewStepMover(m.timeRange, m.Inner.StepCalculationParams())
	}

	for {
		var f float64
		var hasFloat bool
		var h *histogram.FloatHistogram
		var err error
		var step *types.RangeVectorStepData

		if !m.splittable() {
			step, err = m.Inner.NextStepSamples(ctx)
			// nolint:errorlint // errors.Is introduces a performance overhead, and NextStepSamples is guaranteed to return exactly EOS, never a wrapped error.
			if err == types.EOS {
				if m.seriesValidationFunc != nil {
					m.seriesValidationFunc(data, m.metricNames.GetMetricNameForSeries(m.currentSeriesIndex), m.emitAnnotationFunc)
				}
				return data, nil
			} else if err != nil {
				return types.InstantVectorSeriesData{}, err
			}
			f, hasFloat, h, err = m.Func.StepFunc(step, m.scalarArgsData, m.timeRange, m.emitAnnotationFunc, m.MemoryConsumptionTracker)
			if err != nil {
				return types.InstantVectorSeriesData{}, err
			}
		} else {
			step, err = stepMover.NextStep()
			// nolint:errorlint // errors.Is introduces a performance overhead, and NextStepSamples is guaranteed to return exactly EOS, never a wrapped error.
			if err == types.EOS {
				if m.seriesValidationFunc != nil {
					m.seriesValidationFunc(data, m.metricNames.GetMetricNameForSeries(m.currentSeriesIndex), m.emitAnnotationFunc)
				}
				return data, nil
			} else if err != nil {
				return types.InstantVectorSeriesData{}, err
			}

			if hasUncached {
				step, err = m.Inner.NextStepSamples(ctx)
				// nolint:errorlint // errors.Is introduces a performance overhead, and NextStepSamples is guaranteed to return exactly EOS, never a wrapped error.
				if err == types.EOS {
					// We should never get here, the stepmover would have detected the EOS
					if m.seriesValidationFunc != nil {
						m.seriesValidationFunc(data, m.metricNames.GetMetricNameForSeries(m.currentSeriesIndex), m.emitAnnotationFunc)
					}
					return data, nil
				} else if err != nil {
					return types.InstantVectorSeriesData{}, err
				}
			}

			blockLengthMs := m.splitInterval().Milliseconds()
			cachedStart := ((step.RangeStart / blockLengthMs) + 1) * blockLengthMs
			cachedEnd := (step.RangeEnd / blockLengthMs) * blockLengthMs

			updatedStep := splitStep(step, step.RangeStart, cachedStart, m.MemoryConsumptionTracker)
			headPiece, err := m.Func.GenerateFunc(
				updatedStep,
				m.scalarArgsData,
				m.emitAnnotationFunc,
				m.MemoryConsumptionTracker)
			if err != nil {
				return types.InstantVectorSeriesData{}, err
			}
			pieces[0] = headPiece
			piecesIdx := 1
			cachedIdx := 0
			// calculate missing pieces in the middle (or use cached version)
			for ; cachedStart < cachedEnd; cachedStart += blockLengthMs {
				// find the cached piece/series that corresponds to this value
				for ; cachedIdx < len(cachedResult); cachedIdx++ {
					if int64(m.intermediateResults[cachedResult[cachedIdx].IRBlockIdx].ir.StartTimestampMs) < cachedStart {
						continue
					}

					if int64(m.intermediateResults[cachedResult[cachedIdx].IRBlockIdx].ir.StartTimestampMs) == cachedStart {
						break
					}
					return types.InstantVectorSeriesData{}, errors.New("can't find cached block")
				}
				if m.intermediateResults[cachedResult[cachedIdx].IRBlockIdx].ir.Version != -1 {
					pieces[piecesIdx] = m.intermediateResults[cachedResult[cachedIdx].IRBlockIdx].ir.Results[cachedResult[cachedIdx].SeriesIdx]
				} else {
					updatedStep = splitStep(step, cachedStart, cachedStart+m.splitInterval().Milliseconds(), m.MemoryConsumptionTracker)
					// TODO: this can be reused for later steps for the same series - we should make pieces wrap around to avoid recalculating when not necessary
					pieces[piecesIdx], err = m.Func.GenerateFunc(
						updatedStep,
						m.scalarArgsData, m.emitAnnotationFunc, m.MemoryConsumptionTracker)
					if err != nil {
						return types.InstantVectorSeriesData{}, err
					}
					m.intermediateResults[cachedResult[cachedIdx].IRBlockIdx].ir.Series = append(m.intermediateResults[cachedResult[cachedIdx].IRBlockIdx].ir.Series, m.metadata[m.currentSeriesIndex])
					m.intermediateResults[cachedResult[cachedIdx].IRBlockIdx].ir.Results = append(m.intermediateResults[cachedResult[cachedIdx].IRBlockIdx].ir.Results, pieces[piecesIdx])

				}
				piecesIdx++
			}

			// calculate tail piece
			updatedStep = splitStep(step, cachedEnd, step.RangeEnd, m.MemoryConsumptionTracker)
			tailPiece, err := m.Func.GenerateFunc(
				updatedStep,
				m.scalarArgsData, m.emitAnnotationFunc, m.MemoryConsumptionTracker)
			if err != nil {
				return types.InstantVectorSeriesData{}, err
			}
			pieces[piecesIdx] = tailPiece

			f, hasFloat, h, err = m.Func.CombineFunc(pieces, m.emitAnnotationFunc, m.MemoryConsumptionTracker)
			if err != nil {
				return types.InstantVectorSeriesData{}, err
			}
		}

		if hasFloat {
			if data.Floats == nil {
				// Only get FPoint slice once we are sure we have float points.
				// This potentially over-allocates as some points may be histograms, but this is expected to be rare.

				remainingStepCount := m.timeRange.StepCount - int(m.timeRange.PointIndex(step.StepT)) // Only get a slice for the number of points remaining in the query range.
				data.Floats, err = types.FPointSlicePool.Get(remainingStepCount, m.MemoryConsumptionTracker)
				if err != nil {
					return types.InstantVectorSeriesData{}, err
				}
			}
			data.Floats = append(data.Floats, promql.FPoint{T: step.StepT, F: f})
		}
		if h != nil {
			if data.Histograms == nil {
				// Only get HPoint slice once we are sure we have histogram points.
				// This potentially over-allocates as some points may be floats, but this is expected to be rare.

				remainingStepCount := m.timeRange.StepCount - int(m.timeRange.PointIndex(step.StepT)) // Only get a slice for the number of points remaining in the query range.
				data.Histograms, err = types.HPointSlicePool.Get(remainingStepCount, m.MemoryConsumptionTracker)
				if err != nil {
					return types.InstantVectorSeriesData{}, err
				}
			}
			data.Histograms = append(data.Histograms, promql.HPoint{T: step.StepT, H: h})
		}
	}
}

// TODO: splitStep currently copies samples to new ring buffers, this should be modified to reuse the current ring buffer instead
func splitStep(step *types.RangeVectorStepData, start int64, end int64, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *types.RangeVectorStepData {
	emptyFloatView := &types.FPointRingBufferView{}
	emptyHistView := &types.HPointRingBufferView{}

	newStep := &types.RangeVectorStepData{
		StepT:      step.StepT,
		RangeStart: start,
		RangeEnd:   end,
		Floats:     emptyFloatView, // Initialize to empty view, not nil
		Histograms: emptyHistView,  // Initialize to empty view, not nil
	}

	// Filter floats to only include points in the range (start, end]
	// RangeStart is exclusive, RangeEnd is inclusive
	if step.Floats != nil && step.Floats.Any() {
		newStep.Floats = filterFloatsView(step.Floats, start, end, memoryConsumptionTracker)
	}

	// Filter histograms to only include points in the range (start, end]
	if step.Histograms != nil && step.Histograms.Any() {
		newStep.Histograms = filterHistogramsView(step.Histograms, start, end, memoryConsumptionTracker)
	}

	return newStep
}

func filterFloatsView(view *types.FPointRingBufferView, start int64, end int64, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *types.FPointRingBufferView {
	if !view.Any() {
		return &types.FPointRingBufferView{}
	}

	// Get all points from the view
	head, tail := view.UnsafePoints()

	// Count points in the range (start, end]
	// RangeStart is exclusive, RangeEnd is inclusive
	count := 0
	firstInRange := -1

	allPoints := append(head, tail...)
	for i, p := range allPoints {
		if p.T > start && p.T <= end {
			if firstInRange == -1 {
				firstInRange = i
			}
			count++
		}
	}

	if count == 0 {
		return &types.FPointRingBufferView{}
	}

	// If all points are in range, return the original view
	if firstInRange == 0 && count == len(allPoints) {
		return view
	}

	// Create a new buffer and populate it with filtered points
	// Note: This creates a new buffer which needs to be managed/closed by the caller
	// This is a temporary solution - ideally we'd have a way to create offset views
	buffer := types.NewFPointRingBuffer(memoryConsumptionTracker)
	for i := firstInRange; i < firstInRange+count; i++ {
		if err := buffer.Append(allPoints[i]); err != nil {
			// If we can't append, return empty view
			return &types.FPointRingBufferView{}
		}
	}

	return buffer.ViewUntilSearchingBackwards(end, nil)
}

func filterHistogramsView(view *types.HPointRingBufferView, start int64, end int64, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *types.HPointRingBufferView {
	if !view.Any() {
		return &types.HPointRingBufferView{}
	}

	// Get all points from the view
	head, tail := view.UnsafePoints()

	// Count points in the range (start, end]
	// RangeStart is exclusive, RangeEnd is inclusive
	count := 0
	firstInRange := -1

	allPoints := append(head, tail...)
	for i, p := range allPoints {
		if p.T > start && p.T <= end {
			if firstInRange == -1 {
				firstInRange = i
			}
			count++
		}
	}

	if count == 0 {
		return &types.HPointRingBufferView{}
	}

	// If all points are in range, return the original view
	if firstInRange == 0 && count == len(allPoints) {
		return view
	}

	// Create a new buffer and populate it with filtered points
	buffer := types.NewHPointRingBuffer(memoryConsumptionTracker)
	for i := firstInRange; i < firstInRange+count; i++ {
		if err := buffer.Append(allPoints[i]); err != nil {
			// If we can't append, return empty view
			return &types.HPointRingBufferView{}
		}
	}

	return buffer.ViewUntilSearchingBackwards(end, nil)
}

func (m *FunctionOverRangeVector) emitAnnotation(generator types.AnnotationGenerator) {
	metricName := m.metricNames.GetMetricNameForSeries(m.currentSeriesIndex)
	pos := m.Inner.ExpressionPosition()

	if m.Func.UseFirstArgumentPositionForAnnotations {
		pos = m.ScalarArgs[0].ExpressionPosition()
	}

	m.Annotations.Add(generator(metricName, pos))
}

func (m *FunctionOverRangeVector) Prepare(ctx context.Context, params *types.PrepareParams) error {
	// Load results from cache if using cache
	// Is this the best place to load cache?
	if m.splittable() {
		m.intermediateResults = m.CreateIRBlocks()

		// If no complete blocks can be cached, disable caching by clearing the cache reference
		if len(m.intermediateResults) == 0 {
			// DEBUG: log why we're disabling caching
			// fmt.Printf("DEBUG Prepare: No complete blocks, disabling caching\n")
			m.IRCache = nil // This will make splittable() return false
		} else {
			for i := range m.intermediateResults {
				b, found := m.IRCache.Get(m.Func.Name, cache.CacheKey(m.InnerNode), m.intermediateResults[i].startTimestamp, m.intermediateResults[i].duration)
				if found {
					m.intermediateResults[i].ir = b
					m.intermediateResults[i].loadedFromCache = true
				} else {
					m.intermediateResults[i].ir = cache.IntermediateResultBlock{
						Version:          -1, // -1 means not found in cache i guess
						StartTimestampMs: int(m.intermediateResults[i].startTimestamp),
						DurationMs:       int(m.intermediateResults[i].duration.Milliseconds()),
						Series:           nil,
						Results:          nil,
					}
				}
			}
		}
		//TODO: merge uncached blocks?
	}

	err := m.Inner.Prepare(ctx, params)
	if err != nil {
		return err
	}

	for _, sa := range m.ScalarArgs {
		err := sa.Prepare(ctx, params)
		if err != nil {
			return err
		}
	}

	return nil
}

// CreateIRBlocks creates intermediate result blocks for caching.
// It divides the query's data range into fixed-size blocks based on splitInterval().
// The head and tail blocks that might be partial are not cached.
func (m *FunctionOverRangeVector) CreateIRBlocks() []IntermediateResultBlock {
	params := m.Inner.StepCalculationParams()
	
	// Calculate the actual data range that will be queried
	// For the first step:
	firstStepT := m.timeRange.StartT
	firstRangeEnd := firstStepT
	if params.Timestamp != nil {
		firstRangeEnd = *params.Timestamp
	}
	firstRangeEnd = firstRangeEnd - params.Offset
	earliestDataT := firstRangeEnd - params.RangeMilliseconds
	
	// For the last step:
	lastStepT := m.timeRange.EndT
	lastRangeEnd := lastStepT
	if params.Timestamp != nil {
		lastRangeEnd = *params.Timestamp
	}
	lastRangeEnd = lastRangeEnd - params.Offset
	latestDataT := lastRangeEnd
	
	blockLengthMs := m.splitInterval().Milliseconds()
	
	// Find the first complete block boundary (aligned to splitInterval)
	// This is the first block boundary >= earliestDataT
	firstBlockStart := ((earliestDataT / blockLengthMs) + 1) * blockLengthMs
	
	// Find the last complete block boundary (aligned to splitInterval)
	// This is the last block boundary <= latestDataT
	lastBlockEnd := (latestDataT / blockLengthMs) * blockLengthMs
	
	// DEBUG logging
	fmt.Printf("DEBUG CreateIRBlocks: range=%dms, earliest=%d, latest=%d, firstBlock=%d, lastBlock=%d, blockLen=%d\n",
		params.RangeMilliseconds, earliestDataT, latestDataT, firstBlockStart, lastBlockEnd, blockLengthMs)
	
	// If there are no complete blocks, return empty
	if firstBlockStart >= lastBlockEnd {
		return nil
	}

	// Calculate number of complete blocks
	numBlocks := int((lastBlockEnd - firstBlockStart) / blockLengthMs)
	if numBlocks <= 0 {
		return nil
	}

	blocks := make([]IntermediateResultBlock, numBlocks)
	blockStart := firstBlockStart

	for i := 0; i < numBlocks; i++ {
		blocks[i] = IntermediateResultBlock{
			loadedFromCache: false,
			startTimestamp:  blockStart,
			duration:        m.splitInterval(),
		}
		blockStart += blockLengthMs
	}

	return blocks
}

// splittable returns true if this query can benefit from intermediate result caching.
func (m *FunctionOverRangeVector) splittable() bool {
	if m.Func.GenerateFunc == nil || m.IRCache == nil || m.InnerNode == nil {
		return false
	}

	// Only support caching for range vector selectors (MatrixSelector nodes).
	// CacheKey() will panic for other node types.
	if m.InnerNode.NodeType() != planning.NODE_TYPE_MATRIX_SELECTOR {
		return false
	}

	// Also check if we'd actually have any complete blocks to cache.
	// CreateIRBlocks() might return zero blocks if the query range is too short.
	// We only want to use caching if there would be at least one complete block.
	if m.intermediateResults == nil {
		// Not initialized yet, can't determine
		return true
	}
	return len(m.intermediateResults) > 0
}

func (m *FunctionOverRangeVector) splitInterval() time.Duration {
	// TODO: should depend on query
	return time.Hour * 2
}

func (m *FunctionOverRangeVector) Finalize(ctx context.Context) error {
	err := m.Inner.Finalize(ctx)
	if err != nil {
		return err
	}

	for _, sa := range m.ScalarArgs {
		err := sa.Finalize(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *FunctionOverRangeVector) Close() {
	m.Inner.Close()

	for _, d := range m.scalarArgsData {
		types.FPointSlicePool.Put(&d.Samples, m.MemoryConsumptionTracker)
	}

	m.scalarArgsData = nil
}

type IRSeriesRef struct {
	IRBlockIdx int
	// SeriesIdx is the index of the series in the intermediate result block
	SeriesIdx int
}

const UncachedSeriesRef = -1

// This code is mostly copied from deduplicate_and_merge.go
// Merges the series metadata from uncached samples with the cached pieces
// TODO: can we guarantee series are sorted in lexiographical order? or a stable order? I don't think so, e.g. if there's a subquery with sort(), then different time ranges might return values in different orders
//
//	we might just have to filter out cases where that happens (while sort() only works on instant queries, if we might make multiple instant queries over different time ranges)
//	sort_by_label() (experimental) might have similar effects though in this case the order should be the same (assuming a stable sort), it just can't be assumed to be lexicographical wrt all series
func (m *FunctionOverRangeVector) seriesMetadataWithIntermediate(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, [][]IRSeriesRef, error) {
	// Why use a string, rather than the labels hash as a key here? This avoids any issues with hash collisions.
	labelsToRefUncached := map[string][]IRSeriesRef{}

	// Why 1024 bytes? It's what labels.Labels.String() uses as a buffer size, so we use that as a sensible starting point too.
	labelBytes := make([]byte, 0, 1024)

	// Currently loads metadata for entire time range
	uncachedMetadata, err := m.Inner.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, nil, err
	}

	for seriesIdx, series := range uncachedMetadata {
		labelBytes = series.Labels.Bytes(labelBytes)
		g, groupExists := labelsToRefUncached[string(labelBytes)]
		if !groupExists {
			labelsToRefUncached[string(labelBytes)] = []IRSeriesRef{{UncachedSeriesRef, seriesIdx}}
		} else {
			labelsToRefUncached[string(labelBytes)] = append(g, IRSeriesRef{UncachedSeriesRef, seriesIdx})
		}
	}

	// separate map for cache-only (as we need the uncached parts to be ordered as they are returned, as iterating through nextseries() for the uncached will be in order of their metadata)
	labelsToRefCachedOnly := make(map[string][]IRSeriesRef)

	for irIdx, result := range m.intermediateResults {
		for seriesIdx, series := range result.ir.Series {
			labelBytes = series.Labels.Bytes(labelBytes)
			g, groupExists := labelsToRefUncached[string(labelBytes)]
			if !groupExists {
				cg, cgroupExists := labelsToRefCachedOnly[string(labelBytes)]
				if !cgroupExists {
					labelsToRefCachedOnly[string(labelBytes)] = []IRSeriesRef{{irIdx, seriesIdx}}
				} else {
					labelsToRefCachedOnly[string(labelBytes)] = append(cg, IRSeriesRef{irIdx, seriesIdx})
				}
			} else {
				labelsToRefUncached[string(labelBytes)] = append(g, IRSeriesRef{irIdx, seriesIdx})
			}
		}
	}

	seriesToBlockRefs := make([][]IRSeriesRef, 0, len(labelsToRefUncached)+len(labelsToRefCachedOnly))

	// order by uncached metadata (as Inner.NextSeries() is called in defined order)
	// TODO: if multiple uncached calls, they might give different orders. This might make this approach untenable.
	for _, metadata := range uncachedMetadata {
		labelBytes = metadata.Labels.Bytes(labelBytes)
		seriesToBlockRefs = append(seriesToBlockRefs, labelsToRefUncached[string(labelBytes)])
	}

	// Add any cache-only groups to the end of the list
	// TODO: deterministic order
	for _, metadata := range labelsToRefCachedOnly {
		seriesToBlockRefs = append(seriesToBlockRefs, metadata)
	}

	outputMetadata, err := types.SeriesMetadataSlicePool.Get(len(seriesToBlockRefs), m.MemoryConsumptionTracker)

	if err != nil {
		return nil, nil, err
	}

	for _, blockRefs := range seriesToBlockRefs {
		first := blockRefs[0]
		if first.IRBlockIdx == UncachedSeriesRef {
			outputMetadata, err = types.AppendSeriesMetadata(m.MemoryConsumptionTracker, outputMetadata, uncachedMetadata[first.SeriesIdx])
		} else {
			outputMetadata, err = types.AppendSeriesMetadata(m.MemoryConsumptionTracker, outputMetadata, m.intermediateResults[first.IRBlockIdx].ir.Series[first.SeriesIdx])
		}

		if err != nil {
			return nil, nil, err
		}
	}

	return outputMetadata, seriesToBlockRefs, nil
}
