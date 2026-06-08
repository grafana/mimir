// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// RangeVectorDuplicationBuffer buffers the results of an inner operator that is used by multiple consuming operators.
//
// RangeVectorDuplicationBuffer is not thread-safe.
type RangeVectorDuplicationBuffer struct {
	Inner                    types.RangeVectorOperator
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	timeRange types.QueryTimeRange
	logger    log.Logger

	seriesMetadataCount int
	seriesMetadata      []types.SeriesMetadata

	// The series and step index of the last step returned from NextStepSamples.
	// Used to release this step (if not needed) on the next call to NextSeries or NextStepSamples.
	lastReturnedSeriesIndex      int
	lastReturnedStepSamplesIndex int

	// The series and step index of the last step read from Inner.
	lastNextSeriesCallIndex      int
	lastNextStepSamplesCallIndex int

	consumers []*RangeVectorDuplicationConsumer
	buffer    *rangeVectorSeriesDataRingBuffer

	// Multiple RangeVectorDuplicationConsumers will call RangeVectorDuplicationBuffer.Prepare() and AfterPrepare(), so this ensures idempotency.
	prepareCalled      bool
	afterPrepareCalled bool

	stats       *types.OperatorEvaluationStats
	annotations annotations.Annotations
}

func NewRangeVectorDuplicationBuffer(inner types.RangeVectorOperator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, timeRange types.QueryTimeRange, logger log.Logger) *RangeVectorDuplicationBuffer {
	return &RangeVectorDuplicationBuffer{
		Inner:                        inner,
		MemoryConsumptionTracker:     memoryConsumptionTracker,
		timeRange:                    timeRange,
		logger:                       logger,
		lastReturnedSeriesIndex:      -1,
		lastReturnedStepSamplesIndex: -1,
		lastNextSeriesCallIndex:      -1,
		lastNextStepSamplesCallIndex: -1,
		buffer:                       newRangeVectorSeriesDataRingBuffer(timeRange.StepCount, memoryConsumptionTracker),
	}
}

func (b *RangeVectorDuplicationBuffer) AddConsumer() *RangeVectorDuplicationConsumer {
	consumer := &RangeVectorDuplicationConsumer{
		Buffer:                       b,
		currentUnfilteredSeriesIndex: -1,
	}

	b.consumers = append(b.consumers, consumer)

	return consumer
}

func (b *RangeVectorDuplicationBuffer) SeriesMetadata(ctx context.Context, matchers types.Matchers, consumer *RangeVectorDuplicationConsumer) ([]types.SeriesMetadata, error) {
	if b.seriesMetadataCount == 0 {
		// Haven't loaded series metadata yet, load it now.

		if len(b.consumers) == 1 {
			// If we have any filters for this consumer, we might as well pass them down to the selector.
			for _, filter := range consumer.subset.filters {
				matchers = append(matchers, types.NewMatcherFromPrometheusType(filter))
			}
		} else {
			// Ignore the matchers passed at runtime if we have multiple consumers:
			// this operator is being used for multiple parts of the query and
			// the matchers may filter out results needed for other consumers.
			matchers = nil
		}

		var err error
		b.seriesMetadata, err = b.Inner.SeriesMetadata(ctx, matchers)
		if err != nil {
			return nil, err
		}

		// Populate the filter bitmap for each consumer now.
		// We'll produce a series metadata slice for the current consumer below - if we create all of them upfront, then we have to hold them
		// in memory for longer.
		for _, consumer := range b.consumers {
			nextUnfilteredSeriesIndex, err := consumer.subset.computeFilterBitmap(b.seriesMetadata, b.MemoryConsumptionTracker)
			if err != nil {
				return nil, err
			}

			consumer.nextUnfilteredSeriesIndex = nextUnfilteredSeriesIndex
		}
	}

	b.seriesMetadataCount++
	isLastConsumer := b.seriesMetadataCount == len(b.consumers)

	filteredSeries, err := applyFiltering(b.seriesMetadata, consumer.subset, isLastConsumer, b.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	if isLastConsumer {
		b.seriesMetadata = nil
	}

	return filteredSeries, nil
}

func (b *RangeVectorDuplicationBuffer) NextSeries(consumer *RangeVectorDuplicationConsumer) error {
	if consumer.finishedReadingCalled {
		return fmt.Errorf("consumer %p has already had FinishedReading called, can't advance to next series", consumer)
	}

	thisSeriesIndex := consumer.nextUnfilteredSeriesIndex
	consumer.currentUnfilteredSeriesIndex = thisSeriesIndex
	consumer.advanceToNextUnfilteredSeries()
	consumer.currentSeriesStepIndex = -1
	b.releaseLastReadSamples()

	// Note that we deliberately don't call NextSeries here to simplify the logic: instead,
	// we'll call it just before we call NextStepSamples.

	return nil
}

func (b *RangeVectorDuplicationBuffer) bufferUpToAndIncluding(ctx context.Context, desiredSeriesIndex int, desiredStepIndex int) (bufferedRangeVectorStepData, error) {
	var lastStepData bufferedRangeVectorStepData

	for b.lastNextSeriesCallIndex < desiredSeriesIndex || (b.lastNextSeriesCallIndex == desiredSeriesIndex && b.lastNextStepSamplesCallIndex < desiredStepIndex) {
		if b.lastNextStepSamplesCallIndex == b.timeRange.StepCount-1 || b.lastNextSeriesCallIndex == -1 {
			// We just read the last step of the current series, or we haven't read anything yet. We need to move to the next series first.
			if err := b.Inner.NextSeries(ctx); err != nil {
				return bufferedRangeVectorStepData{}, err
			}

			b.lastNextStepSamplesCallIndex = -1
			b.lastNextSeriesCallIndex++
			if !b.anyConsumerWillReadSeries(b.lastNextSeriesCallIndex, nil) {
				// No consumer needs this series, so we can skip to the next one.
				b.lastNextStepSamplesCallIndex = b.timeRange.StepCount - 1
				continue
			}
		}

		stepData, err := b.Inner.NextStepSamples(ctx)
		if err != nil {
			return bufferedRangeVectorStepData{}, err
		}

		combinedStepData, err := b.combineStepData(stepData, b.lastNextSeriesCallIndex)
		if err != nil {
			return bufferedRangeVectorStepData{}, err
		}

		b.lastNextStepSamplesCallIndex++
		b.buffer.Append(combinedStepData, b.lastNextSeriesCallIndex, b.lastNextStepSamplesCallIndex)
		lastStepData = combinedStepData
	}

	return lastStepData, nil
}

// combineStepData copies stepData into buffers owned by this struct (either shared per series or unique
// to each step).
func (b *RangeVectorDuplicationBuffer) combineStepData(stepData *types.RangeVectorStepData, seriesIndex int) (bufferedRangeVectorStepData, error) {
	// anchored/smoothed modifiers cause extra data to be included in the view for a particular
	// step beyond the range start/end and may include synthetic points. For simplicity in this
	// case, we don't bother trying to use a shared buffer for all steps and instead fall back
	// to duplicating the data for each step.
	if stepData.Anchored || stepData.Smoothed {
		cloned, err := cloneStepData(stepData)
		if err != nil {
			return bufferedRangeVectorStepData{}, err
		}

		return cloned, nil
	}

	// When not using anchored/smoothed modifiers, we can use a single buffer for all points in a
	// particular series. This minimizes duplicated points when the range selector for each step
	// overlaps with the range selector of the previous step.
	return b.mergeStepData(stepData, seriesIndex)
}

func (b *RangeVectorDuplicationBuffer) anyConsumerWillReadSeries(unfilteredSeriesIndex int, ignore *RangeVectorDuplicationConsumer) bool {
	for _, consumer := range b.consumers {
		if consumer == ignore {
			continue
		}

		if consumer.hasReadAllStepsForCurrentSeries() && consumer.nextUnfilteredSeriesIndex > unfilteredSeriesIndex {
			continue
		}

		if !consumer.hasReadAllStepsForCurrentSeries() && consumer.currentUnfilteredSeriesIndex > unfilteredSeriesIndex {
			continue
		}

		if consumer.shouldReturnUnfilteredSeries(unfilteredSeriesIndex) {
			return true
		}
	}

	return false
}

func (b *RangeVectorDuplicationBuffer) anyConsumerWillReadStep(unfilteredSeriesIndex int, stepIndex int) bool {
	for _, consumer := range b.consumers {
		if consumer.currentUnfilteredSeriesIndex > unfilteredSeriesIndex {
			// Consumer is already on a later series.
			continue
		}

		if consumer.currentUnfilteredSeriesIndex == unfilteredSeriesIndex && consumer.currentSeriesStepIndex >= stepIndex {
			// Consumer is already on a later step.
			continue
		}

		if consumer.shouldReturnUnfilteredSeries(unfilteredSeriesIndex) {
			// Consumer is not ahead of the desired step, and still needs to return this series.
			return true
		}
	}

	return false
}

func (b *RangeVectorDuplicationBuffer) releaseLastReadSamples() {
	if b.lastReturnedSeriesIndex == -1 {
		return
	}

	// Release the previously read series, if we can.
	if !b.anyConsumerWillReadStep(b.lastReturnedSeriesIndex, b.lastReturnedStepSamplesIndex) {
		if d, present := b.buffer.RemoveIfPresent(b.lastReturnedSeriesIndex, b.lastReturnedStepSamplesIndex); present {
			d.Close()
		}
	}

	b.lastReturnedSeriesIndex = -1
	b.lastReturnedStepSamplesIndex = -1
}

func (b *RangeVectorDuplicationBuffer) NextStepSamples(ctx context.Context, consumer *RangeVectorDuplicationConsumer) (*types.RangeVectorStepData, error) {
	seriesIdx := consumer.currentUnfilteredSeriesIndex
	consumer.currentSeriesStepIndex++
	stepIdx := consumer.currentSeriesStepIndex

	if stepIdx >= b.timeRange.StepCount {
		return nil, types.EOS
	}

	// Release the previous step if all consumers are done with it.
	b.releaseLastReadSamples()
	b.lastReturnedSeriesIndex = seriesIdx
	b.lastReturnedStepSamplesIndex = stepIdx

	// Check if the series is already buffered (e.g., another consumer triggered buffering first).
	if d, ok := b.buffer.GetIfPresent(seriesIdx, stepIdx); ok {
		// We can't remove the step data from the buffer now even if this is the last consumer for this series -
		// we'll do this in the next call to NextSeries so that we can return the cloned sample ring buffers to their pools.
		return d.stepData, nil
	}

	// Step is not in the buffer yet. Check if any other consumer will read this series.
	isLastConsumerOfThisSeries := !b.anyConsumerWillReadSeries(seriesIdx, consumer)

	if isLastConsumerOfThisSeries {
		// This step isn't present in the buffer, and no other consumer needs the series.
		// Don't bother buffering this series, but buffer anything before this one that other consumers might need,
		// then read and return the data directly.
		if seriesIdx > 0 {
			if _, err := b.bufferUpToAndIncluding(ctx, seriesIdx-1, b.timeRange.StepCount-1); err != nil {
				return nil, err
			}
		}

		for b.lastNextSeriesCallIndex < seriesIdx {
			if err := b.Inner.NextSeries(ctx); err != nil {
				return nil, err
			}

			b.lastNextSeriesCallIndex++
			b.lastNextStepSamplesCallIndex = -1
		}

		stepData, err := b.Inner.NextStepSamples(ctx)
		if err != nil {
			return nil, err
		}

		b.lastNextStepSamplesCallIndex++

		return stepData, nil
	}

	// This step isn't present in the buffer, and other consumers might need it.
	// Buffer all steps up to and including this one, then return it.
	d, err := b.bufferUpToAndIncluding(ctx, seriesIdx, stepIdx)
	if err != nil {
		return nil, err
	}

	return d.stepData, nil
}

func (b *RangeVectorDuplicationBuffer) CloseConsumer(consumer *RangeVectorDuplicationConsumer) {
	if consumer.closed {
		// We've already closed this consumer, nothing more to do.
		return
	}

	b.releaseBufferedData(consumer)
	consumer.closed = true

	if b.allConsumersClosed() {
		b.Inner.Close()

		if b.stats != nil {
			b.stats.Close()
			b.stats = nil
		}
	}
}

func (b *RangeVectorDuplicationBuffer) releaseBufferedData(consumer *RangeVectorDuplicationConsumer) {
	if consumer.finishedReadingCalled {
		return
	}

	consumer.finishedReadingCalled = true

	defer consumer.subset.close(b.MemoryConsumptionTracker)

	if b.allConsumersFinishedReading() {
		b.releaseAllBufferedData()
		return
	}

	if b.buffer.Size() == 0 {
		return
	}

	// Remove any buffered series that are no longer needed because they were only being retained for the consumer
	// that was just closed.
	earliestSeriesIndexStillToReturn := b.earliestSeriesIndexStillToReturn()
	allOpenConsumersHaveNoFilters := b.allOpenConsumersHaveNoFilters()
	lastIndexToCheck := b.lastNextSeriesCallIndex

	if allOpenConsumersHaveNoFilters {
		// If all open consumers have no filters, then we only need to check for buffered series to discard up to
		// the lagging open consumer, as at least one open consumer will need every buffered series after that.
		lastIndexToCheck = earliestSeriesIndexStillToReturn - 1
	}

	if consumer.currentUnfilteredSeriesIndex == -1 {
		consumer.currentUnfilteredSeriesIndex = 0
	}

	for b.buffer.Size() > 0 && consumer.currentUnfilteredSeriesIndex <= lastIndexToCheck {
		thisSeriesIndex := consumer.currentUnfilteredSeriesIndex

		// Advance currentUnfilteredSeriesIndex now so that the anyConsumerWillRead call below ignores this consumer.
		consumer.currentUnfilteredSeriesIndex++

		if thisSeriesIndex < earliestSeriesIndexStillToReturn {
			// We know no consumer needs this series, as all consumers are past it. Remove it if it's buffered.
			b.buffer.RemoveAllStepsForSeriesIfPresent(thisSeriesIndex)
		} else {
			// It's possible no consumer needs this series, but we'll have to check if it matches the filters on any open consumer first.

			if consumer.subset.unfilteredSeriesBitmap != nil && !consumer.subset.unfilteredSeriesBitmap[thisSeriesIndex] {
				// This consumer has filters but doesn't need this series, so this series would not have been buffered for this consumer.
				// So we don't need to check if any other consumer needs it - either no consumer needs the series, or a consumer needs it,
				// but either way, the fact this consumer is now closed doesn't change anything.
				continue
			}

			if !b.anyConsumerWillReadSeries(thisSeriesIndex, nil) {
				b.buffer.RemoveAllStepsForSeriesIfPresent(thisSeriesIndex)
			}
		}
	}
}

func (b *RangeVectorDuplicationBuffer) releaseAllBufferedData() {
	types.SeriesMetadataSlicePool.Put(&b.seriesMetadata, b.MemoryConsumptionTracker)
	b.buffer.Clear()
}

func (b *RangeVectorDuplicationBuffer) earliestSeriesIndexStillToReturn() int {
	idx := math.MaxInt

	for _, consumer := range b.consumers {
		if consumer.finishedReadingCalled {
			continue
		}

		currentSeriesIndex := consumer.currentUnfilteredSeriesIndex

		if consumer.hasReadAllStepsForCurrentSeries() {
			currentSeriesIndex = consumer.nextUnfilteredSeriesIndex
		}

		idx = min(idx, currentSeriesIndex)
	}

	return idx
}

func (b *RangeVectorDuplicationBuffer) allOpenConsumersHaveNoFilters() bool {
	for _, consumer := range b.consumers {
		if consumer.finishedReadingCalled {
			continue
		}

		if consumer.subset.applicable() {
			return false
		}
	}

	return true
}

func (b *RangeVectorDuplicationBuffer) allConsumersClosed() bool {
	for _, consumer := range b.consumers {
		if !consumer.closed {
			return false
		}
	}

	return true
}

func (b *RangeVectorDuplicationBuffer) Prepare(ctx context.Context, params *types.PrepareParams) error {
	if b.prepareCalled {
		return nil
	}

	b.prepareCalled = true
	return b.Inner.Prepare(ctx, params)
}

func (b *RangeVectorDuplicationBuffer) AfterPrepare(ctx context.Context) error {
	if b.afterPrepareCalled {
		return nil
	}

	b.afterPrepareCalled = true
	return b.Inner.AfterPrepare(ctx)
}

func (b *RangeVectorDuplicationBuffer) FinishedReading(ctx context.Context, consumer *RangeVectorDuplicationConsumer) error {
	if consumer.finishedReadingCalled {
		return nil
	}

	b.releaseBufferedData(consumer)

	if !b.allConsumersFinishedReading() {
		return nil
	}

	return b.Inner.FinishedReading(ctx)
}

func (b *RangeVectorDuplicationBuffer) allConsumersFinishedReading() bool {
	for _, consumer := range b.consumers {
		if !consumer.finishedReadingCalled {
			return false
		}
	}

	return true
}

func (b *RangeVectorDuplicationBuffer) Finalize(ctx context.Context, consumer *RangeVectorDuplicationConsumer) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	if !b.allConsumersFinishedReading() {
		return nil, nil, errors.New("RangeVectorDuplicationBuffer: cannot finalize when one or more consumers have not had FinishedReading called")
	}

	if consumer.finalized {
		return nil, nil, errors.New("RangeVectorDuplicationBuffer: cannot finalize the same consumer twice")
	}

	if b.stats == nil {
		var err error
		b.stats, b.annotations, err = b.Inner.Finalize(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	consumer.finalized = true
	stats := b.stats
	annos := b.annotations

	if b.allConsumersFinalized() {
		// Last consumer, return stats without cloning, and clear references to existing stats and annotations.
		b.stats = nil
		b.annotations = nil
	} else {
		var err error
		stats, err = stats.Clone() // FIXME: this is wasteful, we could just clone the subset needed
		if err != nil {
			return nil, nil, err
		}

		annos = types.CloneAnnotations(b.annotations)
	}

	if consumer.subset.applicable() {
		// If the inner operator was remotely executed on a querier that does not report stats or subset stats,
		// then the subset at the requested index won't be present.
		//
		// For simplicity during upgrades, and for consistency with remote execution's behaviour during the same circumstances,
		// we return an empty set of stats.
		if !stats.HasSubsets() {
			stats.Close()

			level.Warn(b.logger).Log("msg", "RangeVectorDuplicationBuffer expected subset statistics, but none were present, so returning empty set of statistics. This is expected during an upgrade from queriers without stats support to those with stats support, but a bug otherwise.")
			emptyStats, err := types.NewOperatorEvaluationStats(ctx, b.timeRange, b.MemoryConsumptionTracker, 0)
			return emptyStats, annos, err
		}

		stats.UseSubset(consumer.subset.subsetIndex)
	} else {
		stats.RemoveAllSubsets()
	}

	return stats, annos, nil
}

func (b *RangeVectorDuplicationBuffer) allConsumersFinalized() bool {
	for _, consumer := range b.consumers {
		if !consumer.finalized {
			return false
		}
	}

	return true
}

// cloneStepData creates a copy of stepData including dedicated underlying buffers.
func cloneStepData(stepData *types.RangeVectorStepData) (bufferedRangeVectorStepData, error) {
	cloned := bufferedRangeVectorStepData{
		stepData: &types.RangeVectorStepData{
			StepT:                stepData.StepT,
			RangeStart:           stepData.RangeStart,
			RangeEnd:             stepData.RangeEnd,
			Smoothed:             stepData.Smoothed,
			Anchored:             stepData.Anchored,
			MixedInExtendedRange: stepData.MixedInExtendedRange,
		},
	}

	var err error
	cloned.stepData.Floats, cloned.floatData, err = stepData.Floats.Clone()
	if err != nil {
		return bufferedRangeVectorStepData{}, err
	}

	cloned.stepData.Histograms, cloned.histogramData, err = stepData.Histograms.Clone()
	if err != nil {
		return bufferedRangeVectorStepData{}, err
	}

	return cloned, nil
}

// mergeStepData adds all unique points from stepData to the shared floats and histograms buffers.
// The dedicated buffers in bufferedRangeVectorStepData will be nil since the shared buffers are used.
func (b *RangeVectorDuplicationBuffer) mergeStepData(stepData *types.RangeVectorStepData, seriesIndex int) (bufferedRangeVectorStepData, error) {
	if stepData.Anchored || stepData.Smoothed {
		return bufferedRangeVectorStepData{}, fmt.Errorf("cannot use shared FPoint and HPoint buffers with anchored/smoothed modifiers (this is a bug)")
	}

	var (
		lastF      promql.FPoint
		lastH      promql.HPoint
		floats     *types.FPointRingBuffer
		histograms *types.HPointRingBuffer
	)

	if stepData.Floats.Any() {
		floats = b.buffer.GetOrCreateFloatBuffer(seriesIndex)
		if floats.Count() > 0 {
			lastF = floats.Last()
		}
	}

	if stepData.Histograms.Any() {
		histograms = b.buffer.GetOrCreateHistogramBuffer(seriesIndex)
		if histograms.Count() > 0 {
			lastH = histograms.Last()
		}
	}

	headF, tailF := stepData.Floats.UnsafePoints()
	for _, section := range [][]promql.FPoint{headF, tailF} {
		for _, p := range section {
			if floats != nil && (floats.Count() == 0 || p.T > lastF.T) {
				_, err := floats.Append(promql.FPoint{T: p.T, F: p.F})
				if err != nil {
					return bufferedRangeVectorStepData{}, err
				}
			}
		}
	}

	headH, tailH := stepData.Histograms.UnsafePoints()
	for _, section := range [][]promql.HPoint{headH, tailH} {
		for _, p := range section {
			if histograms != nil && (histograms.Count() == 0 || p.T > lastH.T) {
				_, err := histograms.Append(promql.HPoint{T: p.T, H: p.H.Copy()})
				if err != nil {
					return bufferedRangeVectorStepData{}, err
				}
			}
		}
	}

	var merged types.RangeVectorStepData
	merged.Smoothed = stepData.Smoothed
	merged.Anchored = stepData.Anchored
	merged.RangeStart = stepData.RangeStart
	merged.RangeEnd = stepData.RangeEnd
	merged.StepT = stepData.StepT

	if floats != nil {
		merged.Floats = floats.ViewBetweenSearchingBackwards(stepData.RangeStart, stepData.RangeEnd, nil)
	} else {
		merged.Floats = &types.FPointRingBufferView{}
	}

	if histograms != nil {
		merged.Histograms = histograms.ViewBetweenSearchingBackwards(stepData.RangeStart, stepData.RangeEnd, nil)
	} else {
		merged.Histograms = &types.HPointRingBufferView{}
	}

	return bufferedRangeVectorStepData{stepData: &merged}, nil
}

type bufferedRangeVectorStepData struct {
	// stepData float and histogram views for this step along with its associated time range.
	stepData *types.RangeVectorStepData

	// floatData is an optional buffer for float data specific to only this step. Only used
	// with anchored/smoothed modifiers, otherwise a shared per-series buffer is used.
	floatData *types.FPointRingBuffer

	// histogramData is an optional buffer for histogram data specific to only this step. Only
	// used with anchored/smoothed modifiers, otherwise a shared per-series buffer is used.
	histogramData *types.HPointRingBuffer
}

func (r bufferedRangeVectorStepData) Close() {
	if r.floatData != nil {
		r.floatData.Close()
	}

	if r.histogramData != nil {
		r.histogramData.Close()
	}
}

type RangeVectorDuplicationConsumer struct {
	Buffer *RangeVectorDuplicationBuffer

	subset subset

	currentUnfilteredSeriesIndex int // -1 means the consumer hasn't advanced to the first series yet.
	nextUnfilteredSeriesIndex    int
	currentSeriesStepIndex       int // -1 means the consumer hasn't called NextStepSamples for the current series yet.
	finishedReadingCalled        bool
	closed                       bool
	finalized                    bool
}

var _ types.RangeVectorOperator = &RangeVectorDuplicationConsumer{}

func (d *RangeVectorDuplicationConsumer) SetFilters(filters []*labels.Matcher, subsetIndex int) {
	d.subset = subset{
		filters:     filters,
		subsetIndex: subsetIndex,
	}
}

func (d *RangeVectorDuplicationConsumer) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	return d.Buffer.SeriesMetadata(ctx, matchers, d)
}

func (d *RangeVectorDuplicationConsumer) shouldReturnUnfilteredSeries(unfilteredSeriesIndex int) bool {
	if d.finishedReadingCalled {
		return false
	}

	if !d.subset.applicable() {
		return true
	}

	return d.subset.unfilteredSeriesBitmap[unfilteredSeriesIndex]
}

func (d *RangeVectorDuplicationConsumer) advanceToNextUnfilteredSeries() {
	d.nextUnfilteredSeriesIndex++

	for d.nextUnfilteredSeriesIndex < len(d.subset.unfilteredSeriesBitmap) && !d.shouldReturnUnfilteredSeries(d.nextUnfilteredSeriesIndex) {
		d.nextUnfilteredSeriesIndex++
	}
}

func (d *RangeVectorDuplicationConsumer) hasReadAllStepsForCurrentSeries() bool {
	return d.finishedReadingCalled || d.currentSeriesStepIndex >= d.Buffer.timeRange.StepCount-1
}

func (d *RangeVectorDuplicationConsumer) NextSeries(_ context.Context) error {
	return d.Buffer.NextSeries(d)
}

func (d *RangeVectorDuplicationConsumer) NextStepSamples(ctx context.Context) (*types.RangeVectorStepData, error) {
	return d.Buffer.NextStepSamples(ctx, d)
}

func (d *RangeVectorDuplicationConsumer) ExpressionPosition() posrange.PositionRange {
	return d.Buffer.Inner.ExpressionPosition()
}

func (d *RangeVectorDuplicationConsumer) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return d.Buffer.Prepare(ctx, params)
}

func (d *RangeVectorDuplicationConsumer) AfterPrepare(ctx context.Context) error {
	return d.Buffer.AfterPrepare(ctx)
}

func (d *RangeVectorDuplicationConsumer) FinishedReading(ctx context.Context) error {
	return d.Buffer.FinishedReading(ctx, d)
}

func (d *RangeVectorDuplicationConsumer) Finalize(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	return d.Buffer.Finalize(ctx, d)
}

func (d *RangeVectorDuplicationConsumer) Close() {
	d.Buffer.CloseConsumer(d)
}

type rangeVectorSeriesDataRingBuffer struct {
	seriesStepData      *SeriesDataRingBuffer[bufferedRangeVectorStepData]
	seriesFloatData     *SeriesDataRingBuffer[*types.FPointRingBuffer]
	seriesHistogramData *SeriesDataRingBuffer[*types.HPointRingBuffer]
	stepCount           int

	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

func newRangeVectorSeriesDataRingBuffer(stepCount int, memory *limiter.MemoryConsumptionTracker) *rangeVectorSeriesDataRingBuffer {
	return &rangeVectorSeriesDataRingBuffer{
		seriesStepData:           &SeriesDataRingBuffer[bufferedRangeVectorStepData]{},
		seriesFloatData:          &SeriesDataRingBuffer[*types.FPointRingBuffer]{},
		seriesHistogramData:      &SeriesDataRingBuffer[*types.HPointRingBuffer]{},
		stepCount:                stepCount,
		memoryConsumptionTracker: memory,
	}
}

func (b *rangeVectorSeriesDataRingBuffer) elementIndexFor(seriesIndex int, stepIndex int) int {
	return seriesIndex*b.stepCount + stepIndex
}

func (b *rangeVectorSeriesDataRingBuffer) Append(data bufferedRangeVectorStepData, seriesIndex int, stepIndex int) {
	b.seriesStepData.Append(data, b.elementIndexFor(seriesIndex, stepIndex))
}

func (b *rangeVectorSeriesDataRingBuffer) GetOrCreateFloatBuffer(seriesIndex int) *types.FPointRingBuffer {
	fpoints, present := b.seriesFloatData.GetIfPresent(seriesIndex)
	if !present {
		fpoints = types.NewFPointRingBuffer(b.memoryConsumptionTracker)
		b.seriesFloatData.Append(fpoints, seriesIndex)
	}

	return fpoints
}

func (b *rangeVectorSeriesDataRingBuffer) GetOrCreateHistogramBuffer(seriesIndex int) *types.HPointRingBuffer {
	hpoints, present := b.seriesHistogramData.GetIfPresent(seriesIndex)
	if !present {
		hpoints = types.NewHPointRingBuffer(b.memoryConsumptionTracker)
		b.seriesHistogramData.Append(hpoints, seriesIndex)
	}

	return hpoints
}

func (b *rangeVectorSeriesDataRingBuffer) GetIfPresent(seriesIndex int, stepIndex int) (bufferedRangeVectorStepData, bool) {
	return b.seriesStepData.GetIfPresent(b.elementIndexFor(seriesIndex, stepIndex))
}

func (b *rangeVectorSeriesDataRingBuffer) RemoveAllStepsForSeriesIfPresent(seriesIndex int) {
	for stepIdx := range b.stepCount {
		if d, present := b.RemoveIfPresent(seriesIndex, stepIdx); present {
			d.Close()
		}
	}

	if d, present := b.seriesFloatData.RemoveIfPresent(seriesIndex); present {
		d.Close()
	}

	if d, present := b.seriesHistogramData.RemoveIfPresent(seriesIndex); present {
		d.Close()
	}
}

func (b *rangeVectorSeriesDataRingBuffer) RemoveIfPresent(seriesIndex int, stepIndex int) (bufferedRangeVectorStepData, bool) {
	return b.seriesStepData.RemoveIfPresent(b.elementIndexFor(seriesIndex, stepIndex))
}

func (b *rangeVectorSeriesDataRingBuffer) RemoveFirst() bufferedRangeVectorStepData {
	return b.seriesStepData.RemoveFirst()
}

func (b *rangeVectorSeriesDataRingBuffer) Size() int {
	return b.seriesStepData.Size()
}

func (b *rangeVectorSeriesDataRingBuffer) Clear() {
	for b.seriesStepData.Size() > 0 {
		d := b.seriesStepData.RemoveFirst()
		d.Close()
	}

	for b.seriesFloatData.Size() > 0 {
		d := b.seriesFloatData.RemoveFirst()
		d.Close()
	}

	for b.seriesHistogramData.Size() > 0 {
		d := b.seriesHistogramData.RemoveFirst()
		d.Close()
	}
}
