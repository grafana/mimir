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
	"github.com/prometheus/prometheus/promql/parser/posrange"

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

	stats *types.OperatorEvaluationStats
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
		buffer:                       newRangeVectorSeriesDataRingBuffer(timeRange.StepCount),
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
	if consumer.finalized {
		return fmt.Errorf("consumer %p is already finalized, can't advance to next series", consumer)
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

		b.lastNextStepSamplesCallIndex++
		clonedData, err := cloneStepData(stepData)
		if err != nil {
			return bufferedRangeVectorStepData{}, err
		}

		b.buffer.Append(clonedData, b.lastNextSeriesCallIndex, b.lastNextStepSamplesCallIndex)
		lastStepData = clonedData
	}

	return lastStepData, nil
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
	if consumer.finalized {
		return
	}

	consumer.finalized = true

	defer consumer.subset.close(b.MemoryConsumptionTracker)

	if b.allConsumersFinalized() {
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

	for b.buffer.Size() > 0 {
		d := b.buffer.RemoveFirst()
		d.Close()
	}
}

func (b *RangeVectorDuplicationBuffer) earliestSeriesIndexStillToReturn() int {
	idx := math.MaxInt

	for _, consumer := range b.consumers {
		if consumer.finalized {
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
		if consumer.finalized {
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

func (b *RangeVectorDuplicationBuffer) Finalize(ctx context.Context, consumer *RangeVectorDuplicationConsumer) error {
	if consumer.finalized {
		return nil
	}

	b.releaseBufferedData(consumer)

	if !b.allConsumersFinalized() {
		return nil
	}

	return b.Inner.Finalize(ctx)
}

func (b *RangeVectorDuplicationBuffer) allConsumersFinalized() bool {
	for _, consumer := range b.consumers {
		if !consumer.finalized {
			return false
		}
	}

	return true
}

func (b *RangeVectorDuplicationBuffer) QueryStats(ctx context.Context, consumer *RangeVectorDuplicationConsumer) (*types.OperatorEvaluationStats, error) {
	if !b.allConsumersFinalized() {
		return nil, errors.New("RangeVectorDuplicationBuffer: cannot get stats when one or more consumers are not finalized")
	}

	if consumer.hasReadStats {
		return nil, errors.New("RangeVectorDuplicationBuffer: cannot get stats twice for the same consumer")
	}

	if b.stats == nil {
		var err error
		b.stats, err = b.Inner.Stats(ctx)
		if err != nil {
			return nil, err
		}
	}

	consumer.hasReadStats = true
	stats := b.stats

	if b.allConsumersHaveReadStats() {
		// Last consumer, return stats without cloning, and clear reference to existing stats.
		b.stats = nil
	} else {
		var err error
		stats, err = stats.Clone() // FIXME: this is wasteful, we could just clone the subset needed
		if err != nil {
			return nil, err
		}
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
			return types.NewOperatorEvaluationStats(b.timeRange, b.MemoryConsumptionTracker, 0)
		}

		stats.UseSubset(consumer.subset.subsetIndex)
	} else {
		stats.RemoveAllSubsets()
	}

	return stats, nil
}

func (b *RangeVectorDuplicationBuffer) allConsumersHaveReadStats() bool {
	for _, consumer := range b.consumers {
		if !consumer.hasReadStats {
			return false
		}
	}

	return true
}

type bufferedRangeVectorStepData struct {
	stepData        *types.RangeVectorStepData
	floatBuffer     *types.FPointRingBuffer
	histogramBuffer *types.HPointRingBuffer
}

func cloneStepData(stepData *types.RangeVectorStepData) (bufferedRangeVectorStepData, error) {
	buffered := bufferedRangeVectorStepData{
		stepData: &types.RangeVectorStepData{
			StepT:      stepData.StepT,
			RangeStart: stepData.RangeStart,
			RangeEnd:   stepData.RangeEnd,
			Smoothed:   stepData.Smoothed,
			Anchored:   stepData.Anchored,
		},
	}

	var err error

	buffered.stepData.Floats, buffered.floatBuffer, err = stepData.Floats.Clone()
	if err != nil {
		return bufferedRangeVectorStepData{}, err
	}

	buffered.stepData.Histograms, buffered.histogramBuffer, err = stepData.Histograms.Clone()
	if err != nil {
		return bufferedRangeVectorStepData{}, err
	}

	return buffered, nil
}

func (d bufferedRangeVectorStepData) Close() {
	if d.floatBuffer != nil {
		d.floatBuffer.Close()
	}

	if d.histogramBuffer != nil {
		d.histogramBuffer.Close()
	}
}

type RangeVectorDuplicationConsumer struct {
	Buffer *RangeVectorDuplicationBuffer

	subset subset

	currentUnfilteredSeriesIndex int // -1 means the consumer hasn't advanced to the first series yet.
	nextUnfilteredSeriesIndex    int
	currentSeriesStepIndex       int // -1 means the consumer hasn't called NextStepSamples for the current series yet.
	finalized                    bool
	closed                       bool
	hasReadStats                 bool
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
	if d.finalized {
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
	return d.finalized || d.currentSeriesStepIndex >= d.Buffer.timeRange.StepCount-1
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

func (d *RangeVectorDuplicationConsumer) Finalize(ctx context.Context) error {
	return d.Buffer.Finalize(ctx, d)
}

func (d *RangeVectorDuplicationConsumer) Stats(ctx context.Context) (*types.OperatorEvaluationStats, error) {
	return d.Buffer.QueryStats(ctx, d)
}

func (d *RangeVectorDuplicationConsumer) Close() {
	d.Buffer.CloseConsumer(d)
}

type rangeVectorSeriesDataRingBuffer struct {
	buffer    *SeriesDataRingBuffer[bufferedRangeVectorStepData]
	stepCount int
}

func newRangeVectorSeriesDataRingBuffer(stepCount int) *rangeVectorSeriesDataRingBuffer {
	return &rangeVectorSeriesDataRingBuffer{
		buffer:    &SeriesDataRingBuffer[bufferedRangeVectorStepData]{},
		stepCount: stepCount,
	}
}

func (b *rangeVectorSeriesDataRingBuffer) elementIndexFor(seriesIndex int, stepIndex int) int {
	return seriesIndex*b.stepCount + stepIndex
}

func (b *rangeVectorSeriesDataRingBuffer) Append(data bufferedRangeVectorStepData, seriesIndex int, stepIndex int) {
	b.buffer.Append(data, b.elementIndexFor(seriesIndex, stepIndex))
}

func (b *rangeVectorSeriesDataRingBuffer) GetIfPresent(seriesIndex int, stepIndex int) (bufferedRangeVectorStepData, bool) {
	return b.buffer.GetIfPresent(b.elementIndexFor(seriesIndex, stepIndex))
}

func (b *rangeVectorSeriesDataRingBuffer) RemoveAllStepsForSeriesIfPresent(seriesIndex int) {
	for stepIdx := range b.stepCount {
		if d, ok := b.RemoveIfPresent(seriesIndex, stepIdx); ok {
			d.Close()
		}
	}
}

func (b *rangeVectorSeriesDataRingBuffer) RemoveIfPresent(seriesIndex int, stepIndex int) (bufferedRangeVectorStepData, bool) {
	return b.buffer.RemoveIfPresent(b.elementIndexFor(seriesIndex, stepIndex))
}

func (b *rangeVectorSeriesDataRingBuffer) RemoveFirst() bufferedRangeVectorStepData {
	return b.buffer.RemoveFirst()
}

func (b *rangeVectorSeriesDataRingBuffer) Size() int {
	return b.buffer.Size()
}
