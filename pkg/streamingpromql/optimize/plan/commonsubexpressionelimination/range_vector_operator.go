// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"context"
	"fmt"
	"math"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// RangeVectorDuplicationBuffer buffers the results of an inner operator that is used by multiple consuming operators.
//
// RangeVectorDuplicationBuffer is not thread-safe, and only supports instant queries.
type RangeVectorDuplicationBuffer struct {
	Inner                    types.RangeVectorOperator
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	seriesMetadataCount int
	seriesMetadata      []types.SeriesMetadata

	lastNextStepSamplesCallIndex int
	lastReadSeriesIndex          int
	consumers                    []*RangeVectorDuplicationConsumer
	buffer                       *SeriesDataRingBuffer[bufferedRangeVectorStepData]

	// Multiple RangeVectorDuplicationConsumers will call RangeVectorDuplicationBuffer.Prepare() and AfterPrepare(), so this ensures idempotency.
	prepareCalled      bool
	afterPrepareCalled bool
}

func NewRangeVectorDuplicationBuffer(inner types.RangeVectorOperator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *RangeVectorDuplicationBuffer {
	return &RangeVectorDuplicationBuffer{
		Inner:                        inner,
		MemoryConsumptionTracker:     memoryConsumptionTracker,
		lastNextStepSamplesCallIndex: -1,
		lastReadSeriesIndex:          -1,
		buffer:                       &SeriesDataRingBuffer[bufferedRangeVectorStepData]{},
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

		if len(b.consumers) > 1 {
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
			nextUnfilteredSeriesIndex, unfilteredSeriesBitmap, filteredSeriesCount, err := computeFilterBitmap(b.seriesMetadata, consumer.filters, b.MemoryConsumptionTracker)
			if err != nil {
				return nil, err
			}

			consumer.unfilteredSeriesBitmap = unfilteredSeriesBitmap
			consumer.nextUnfilteredSeriesIndex = nextUnfilteredSeriesIndex
			consumer.filteredSeriesCount = filteredSeriesCount
		}
	}

	b.seriesMetadataCount++
	isLastConsumer := b.seriesMetadataCount == len(b.consumers)

	filteredSeries, err := applyFiltering(b.seriesMetadata, consumer.unfilteredSeriesBitmap, consumer.filteredSeriesCount, isLastConsumer, b.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	if isLastConsumer {
		b.seriesMetadata = nil
	}

	return filteredSeries, nil
}

func (b *RangeVectorDuplicationBuffer) NextSeries(consumer *RangeVectorDuplicationConsumer) error {
	if consumer.closed {
		return fmt.Errorf("consumer %p is already closed, can't advance to next series", consumer)
	}

	thisSeriesIndex := consumer.nextUnfilteredSeriesIndex
	consumer.currentUnfilteredSeriesIndex = thisSeriesIndex
	consumer.advanceToNextUnfilteredSeries()
	consumer.hasReadCurrentSeriesSamples = false
	b.releaseLastReadSamples()

	// Note that we deliberately don't call NextSeries here to simplify the logic: instead,
	// we'll call it just before we call NextStepSamples.

	return nil
}

func (b *RangeVectorDuplicationBuffer) bufferUpToAndIncluding(ctx context.Context, desiredSeriesIndex int) (bufferedRangeVectorStepData, error) {
	var lastStepData bufferedRangeVectorStepData

	for b.lastNextStepSamplesCallIndex < desiredSeriesIndex {
		if err := b.Inner.NextSeries(ctx); err != nil {
			return bufferedRangeVectorStepData{}, err
		}

		b.lastNextStepSamplesCallIndex++
		currentSeriesIndex := b.lastNextStepSamplesCallIndex
		if b.anyConsumerWillRead(currentSeriesIndex) {
			var err error
			if lastStepData, err = b.cacheNextStepSamples(ctx); err != nil {
				return bufferedRangeVectorStepData{}, err
			}
		}
	}

	return lastStepData, nil
}

func (b *RangeVectorDuplicationBuffer) cacheNextStepSamples(ctx context.Context) (bufferedRangeVectorStepData, error) {
	stepData, err := b.Inner.NextStepSamples(ctx)
	if err != nil {
		return bufferedRangeVectorStepData{}, err
	}

	clonedData, err := cloneStepData(stepData)
	if err != nil {
		return bufferedRangeVectorStepData{}, err
	}

	b.buffer.Append(clonedData, b.lastNextStepSamplesCallIndex)

	return clonedData, nil
}

func (b *RangeVectorDuplicationBuffer) anyConsumerWillRead(unfilteredSeriesIndex int) bool {
	for _, consumer := range b.consumers {
		if consumer.hasReadCurrentSeriesSamples && consumer.nextUnfilteredSeriesIndex > unfilteredSeriesIndex {
			continue
		}

		if !consumer.hasReadCurrentSeriesSamples && consumer.currentUnfilteredSeriesIndex > unfilteredSeriesIndex {
			continue
		}

		if consumer.shouldReturnUnfilteredSeries(unfilteredSeriesIndex) {
			return true
		}
	}

	return false
}

func (b *RangeVectorDuplicationBuffer) releaseLastReadSamples() {
	if b.lastReadSeriesIndex == -1 {
		return
	}

	// Release the previously read series, if we can.
	if !b.anyConsumerWillRead(b.lastReadSeriesIndex) {
		if d, present := b.buffer.RemoveIfPresent(b.lastReadSeriesIndex); present {
			d.Close()
		}
	}

	b.lastReadSeriesIndex = -1
}

func (b *RangeVectorDuplicationBuffer) NextStepSamples(ctx context.Context, consumer *RangeVectorDuplicationConsumer) (*types.RangeVectorStepData, error) {
	if consumer.hasReadCurrentSeriesSamples {
		return nil, types.EOS
	}

	b.releaseLastReadSamples()
	b.lastReadSeriesIndex = consumer.currentUnfilteredSeriesIndex

	consumer.hasReadCurrentSeriesSamples = true

	if d, ok := b.buffer.GetIfPresent(consumer.currentUnfilteredSeriesIndex); ok {
		// We can't remove the step data from the buffer now even if this is the last consumer for this series -
		// we'll do this in the next call to NextSeries so that we can return the cloned sample ring buffers to their pools.
		return d.stepData, nil
	}

	isLastConsumerOfThisSeries := !b.anyConsumerWillRead(consumer.currentUnfilteredSeriesIndex)
	if isLastConsumerOfThisSeries {
		// This series isn't present in the buffer, and no other consumer needs it.
		// Don't bother buffering this series, but buffer anything before this one that other consumers might need,
		// then read and return the data directly.
		if _, err := b.bufferUpToAndIncluding(ctx, consumer.currentUnfilteredSeriesIndex-1); err != nil {
			return nil, err
		}

		b.lastNextStepSamplesCallIndex++
		err := b.Inner.NextSeries(ctx)
		if err != nil {
			return nil, err
		}

		stepData, err := b.Inner.NextStepSamples(ctx)
		if err != nil {
			return nil, err
		}

		return stepData, nil
	}

	// This series isn't present in the buffer, and other consumers might need it.
	// Buffer everything up to and including this series, then return it.
	d, err := b.bufferUpToAndIncluding(ctx, consumer.currentUnfilteredSeriesIndex)
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

	consumer.closed = true

	if b.allConsumersClosed() {
		b.close()
		return
	}

	if b.buffer.Size() == 0 {
		return
	}

	// Remove any buffered series that are no longer needed because they were only being retained for the consumer
	// that was just closed.
	consumer.hasReadCurrentSeriesSamples = true
	earliestSeriesIndexStillToReturn := b.earliestSeriesIndexStillToReturn()
	allOpenConsumersHaveNoFilters := b.allOpenConsumersHaveNoFilters()
	lastIndexToCheck := b.lastNextStepSamplesCallIndex

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
			if d, ok := b.buffer.RemoveIfPresent(thisSeriesIndex); ok {
				d.Close()
			}
		} else {
			// It's possible no consumer needs this series, but we'll have to check if it matches the filters on any open consumer first.

			if consumer.unfilteredSeriesBitmap != nil && !consumer.unfilteredSeriesBitmap[thisSeriesIndex] {
				// This consumer has filters but doesn't need this series, so this series would not have been buffered for this consumer.
				// So we don't need to check if any other consumer needs it - either no consumer needs the series, or a consumer needs it,
				// but either way, the fact this consumer is now closed doesn't change anything.
				continue
			}

			if !b.anyConsumerWillRead(thisSeriesIndex) {
				if d, ok := b.buffer.RemoveIfPresent(thisSeriesIndex); ok {
					d.Close()
				}
			}
		}
	}
}

func (b *RangeVectorDuplicationBuffer) earliestSeriesIndexStillToReturn() int {
	idx := math.MaxInt

	for _, consumer := range b.consumers {
		if consumer.closed {
			continue
		}

		currentSeriesIndex := consumer.currentUnfilteredSeriesIndex

		if consumer.hasReadCurrentSeriesSamples {
			currentSeriesIndex = consumer.nextUnfilteredSeriesIndex
		}

		idx = min(idx, currentSeriesIndex)
	}

	return idx
}

func (b *RangeVectorDuplicationBuffer) allOpenConsumersHaveNoFilters() bool {
	for _, consumer := range b.consumers {
		if consumer.closed {
			continue
		}

		if len(consumer.filters) > 0 {
			return false
		}
	}

	return true
}

func (b *RangeVectorDuplicationBuffer) close() {
	types.SeriesMetadataSlicePool.Put(&b.seriesMetadata, b.MemoryConsumptionTracker)

	for b.buffer.Size() > 0 {
		d := b.buffer.RemoveFirst()
		d.Close()
	}

	b.buffer = nil

	b.Inner.Close()
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

	consumer.finalized = true

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

	filters []*labels.Matcher

	currentUnfilteredSeriesIndex int // -1 means the consumer hasn't advanced to the first series yet.
	nextUnfilteredSeriesIndex    int
	hasReadCurrentSeriesSamples  bool
	finalized                    bool
	closed                       bool

	// unfilteredSeriesBitmap contains one entry per unfiltered input series, where true indicates that it passes this consumer's filters.
	// If this consumer has no filters, this is nil.
	unfilteredSeriesBitmap []bool
	filteredSeriesCount    int
}

var _ types.RangeVectorOperator = &RangeVectorDuplicationConsumer{}

func (d *RangeVectorDuplicationConsumer) SetFilters(filters []*labels.Matcher) {
	d.filters = filters
}

func (d *RangeVectorDuplicationConsumer) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	return d.Buffer.SeriesMetadata(ctx, matchers, d)
}

func (d *RangeVectorDuplicationConsumer) shouldReturnUnfilteredSeries(unfilteredSeriesIndex int) bool {
	if d.closed {
		return false
	}

	if len(d.filters) == 0 {
		return true
	}

	return d.unfilteredSeriesBitmap[unfilteredSeriesIndex]
}

func (d *RangeVectorDuplicationConsumer) advanceToNextUnfilteredSeries() {
	d.nextUnfilteredSeriesIndex++

	for d.nextUnfilteredSeriesIndex < len(d.unfilteredSeriesBitmap) && !d.shouldReturnUnfilteredSeries(d.nextUnfilteredSeriesIndex) {
		d.nextUnfilteredSeriesIndex++
	}
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

func (d *RangeVectorDuplicationConsumer) Close() {
	d.Buffer.CloseConsumer(d)
	types.BoolSlicePool.Put(&d.unfilteredSeriesBitmap, d.Buffer.MemoryConsumptionTracker)
}
