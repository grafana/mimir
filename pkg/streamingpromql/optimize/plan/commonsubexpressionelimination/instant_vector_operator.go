// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"context"
	"math"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// InstantVectorDuplicationBuffer buffers the results of an inner operator that is used by multiple consuming operators.
//
// InstantVectorDuplicationBuffer is not thread-safe.
type InstantVectorDuplicationBuffer struct {
	Inner                    types.InstantVectorOperator
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	seriesMetadataCount int
	seriesMetadata      []types.SeriesMetadata

	consumers            []*InstantVectorDuplicationConsumer
	nextInnerSeriesIndex int
	buffer               *SeriesDataRingBuffer[types.InstantVectorSeriesData]

	// Multiple InstantVectorDuplicationConsumers will call InstantVectorDuplicationBuffer.Prepare() and AfterPrepare(), so this ensures idempotency.
	prepareCalled      bool
	afterPrepareCalled bool
}

func NewInstantVectorDuplicationBuffer(inner types.InstantVectorOperator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *InstantVectorDuplicationBuffer {
	return &InstantVectorDuplicationBuffer{
		Inner:                    inner,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		buffer:                   &SeriesDataRingBuffer[types.InstantVectorSeriesData]{},
	}
}

func (b *InstantVectorDuplicationBuffer) AddConsumer() *InstantVectorDuplicationConsumer {
	consumer := &InstantVectorDuplicationConsumer{
		Buffer: b,
	}

	b.consumers = append(b.consumers, consumer)
	return consumer
}

func (b *InstantVectorDuplicationBuffer) SeriesMetadata(ctx context.Context, consumer *InstantVectorDuplicationConsumer) ([]types.SeriesMetadata, error) {
	if b.seriesMetadataCount == 0 {
		// Haven't loaded series metadata yet, load it now.
		var err error
		// Note that we are ignoring the matchers passed at runtime and not passing them to the inner
		// operator. This is because this operator is being used for multiple parts of the query and
		// the matchers may filter out results needed for other uses of this operator.
		b.seriesMetadata, err = b.Inner.SeriesMetadata(ctx, nil)
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

func (b *InstantVectorDuplicationBuffer) NextSeries(ctx context.Context, consumer *InstantVectorDuplicationConsumer) (types.InstantVectorSeriesData, error) {
	thisSeriesIndex := consumer.nextUnfilteredSeriesIndex
	consumer.advanceToNextUnfilteredSeries()
	isLastConsumerOfThisSeries := !b.anyConsumerWillRead(thisSeriesIndex)

	for b.nextInnerSeriesIndex <= thisSeriesIndex {
		currentSeriesIndex := b.nextInnerSeriesIndex
		b.nextInnerSeriesIndex++
		d, err := b.Inner.NextSeries(ctx)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		if currentSeriesIndex == thisSeriesIndex {
			// We've just read the series we're looking for.
			if isLastConsumerOfThisSeries {
				// We don't need to buffer this series, return it as-is.
				return d, nil
			}

			// Another consumer will read this series, so buffer it and return a cloned copy.
			b.buffer.Append(d, currentSeriesIndex)
			return d.Clone(b.MemoryConsumptionTracker)
		}

		if b.anyConsumerWillRead(currentSeriesIndex) {
			b.buffer.Append(d, currentSeriesIndex)
		} else {
			types.PutInstantVectorSeriesData(d, b.MemoryConsumptionTracker)
		}
	}

	if isLastConsumerOfThisSeries {
		d := b.buffer.Remove(thisSeriesIndex)
		return d, nil
	}

	d := b.buffer.Get(thisSeriesIndex)
	return d.Clone(b.MemoryConsumptionTracker)
}

func (b *InstantVectorDuplicationBuffer) anyConsumerWillRead(unfilteredSeriesIndex int) bool {
	for _, consumer := range b.consumers {
		if consumer.nextUnfilteredSeriesIndex > unfilteredSeriesIndex {
			// Already ahead of this series.
			continue
		}

		if consumer.shouldReturnUnfilteredSeries(unfilteredSeriesIndex) {
			return true
		}
	}

	return false
}

func (b *InstantVectorDuplicationBuffer) CloseConsumer(consumer *InstantVectorDuplicationConsumer) {
	if consumer.closed {
		// We've already closed this consumer, nothing more to do.
		return
	}

	consumer.closed = true

	// Remove any buffered series that are no longer needed because they were only being retained for the consumer
	// that was just closed.
	earliestSeriesIndexStillToReturn := b.earliestSeriesIndexStillToReturn()

	if earliestSeriesIndexStillToReturn == math.MaxInt {
		// All other consumers are already closed. Close everything.
		b.close()
		return
	}

	if b.buffer.Size() == 0 {
		return
	}

	allOpenConsumersHaveNoFilters := b.allOpenConsumersHaveNoFilters()
	lastIndexToCheck := b.nextInnerSeriesIndex - 1

	if allOpenConsumersHaveNoFilters {
		// If all open consumers have no filters, then we only need to check for buffered series to discard up to
		// the lagging open consumer, as at least one open consumer will need every buffered series after that.
		lastIndexToCheck = earliestSeriesIndexStillToReturn - 1
	}

	for b.buffer.Size() > 0 && consumer.nextUnfilteredSeriesIndex <= lastIndexToCheck {
		thisSeriesIndex := consumer.nextUnfilteredSeriesIndex

		// Advance nextUnfilteredSeriesIndex now so that the anyConsumerWillRead call below ignores this consumer.
		consumer.nextUnfilteredSeriesIndex++

		if thisSeriesIndex < earliestSeriesIndexStillToReturn {
			// We know no consumer needs this series, as all consumers are past it. Remove it if it's buffered.
			if d, ok := b.buffer.RemoveIfPresent(thisSeriesIndex); ok {
				types.PutInstantVectorSeriesData(d, b.MemoryConsumptionTracker)
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
					types.PutInstantVectorSeriesData(d, b.MemoryConsumptionTracker)
				}
			}
		}
	}
}

func (b *InstantVectorDuplicationBuffer) earliestSeriesIndexStillToReturn() int {
	idx := math.MaxInt
	for _, consumer := range b.consumers {
		if consumer.closed {
			continue
		}

		idx = min(idx, consumer.nextUnfilteredSeriesIndex)
	}

	return idx
}

func (b *InstantVectorDuplicationBuffer) allOpenConsumersHaveNoFilters() bool {
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

func (b *InstantVectorDuplicationBuffer) Prepare(ctx context.Context, params *types.PrepareParams) error {
	if b.prepareCalled {
		return nil
	}

	b.prepareCalled = true
	return b.Inner.Prepare(ctx, params)
}

func (b *InstantVectorDuplicationBuffer) AfterPrepare(ctx context.Context) error {
	if b.afterPrepareCalled {
		return nil
	}

	b.afterPrepareCalled = true
	return b.Inner.AfterPrepare(ctx)
}

func (b *InstantVectorDuplicationBuffer) Finalize(ctx context.Context, consumer *InstantVectorDuplicationConsumer) error {
	if consumer.finalized {
		return nil
	}

	consumer.finalized = true

	if !b.allConsumersFinalized() {
		return nil
	}

	return b.Inner.Finalize(ctx)
}

func (b *InstantVectorDuplicationBuffer) allConsumersFinalized() bool {
	for _, consumer := range b.consumers {
		if !consumer.finalized {
			return false
		}
	}

	return true
}

func (b *InstantVectorDuplicationBuffer) close() {
	types.SeriesMetadataSlicePool.Put(&b.seriesMetadata, b.MemoryConsumptionTracker)

	for b.buffer.Size() > 0 {
		types.PutInstantVectorSeriesData(b.buffer.RemoveFirst(), b.MemoryConsumptionTracker)
	}

	b.buffer = nil

	b.Inner.Close()
}

type InstantVectorDuplicationConsumer struct {
	Buffer *InstantVectorDuplicationBuffer

	filters []*labels.Matcher

	// unfilteredSeriesBitmap contains one entry per unfiltered input series, where true indicates that it passes this consumer's filters.
	// If this consumer has no filters, this is nil.
	unfilteredSeriesBitmap []bool
	filteredSeriesCount    int

	nextUnfilteredSeriesIndex int
	closed                    bool
	finalized                 bool
}

var _ types.InstantVectorOperator = &InstantVectorDuplicationConsumer{}

func (d *InstantVectorDuplicationConsumer) SetFilters(filters []*labels.Matcher) {
	d.filters = filters
}

func computeFilterBitmap(unfilteredSeries []types.SeriesMetadata, filters []*labels.Matcher, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (nextUnfilteredSeriesIndex int, unfilteredSeriesBitmap []bool, filteredSeriesCount int, err error) {
	if len(filters) == 0 {
		return 0, nil, len(unfilteredSeries), nil
	}

	unfilteredSeriesBitmap, err = types.BoolSlicePool.Get(len(unfilteredSeries), memoryConsumptionTracker)
	if err != nil {
		return 0, nil, 0, err
	}

	nextUnfilteredSeriesIndex = len(unfilteredSeries)
	unfilteredSeriesBitmap = unfilteredSeriesBitmap[:len(unfilteredSeries)]

	for unfilteredSeriesIndex, series := range unfilteredSeries {
		if !matchesSeries(filters, series.Labels) {
			continue
		}

		unfilteredSeriesBitmap[unfilteredSeriesIndex] = true
		filteredSeriesCount++

		if nextUnfilteredSeriesIndex == len(unfilteredSeries) {
			// First unfiltered series that matches.
			nextUnfilteredSeriesIndex = unfilteredSeriesIndex
		}
	}

	return nextUnfilteredSeriesIndex, unfilteredSeriesBitmap, filteredSeriesCount, nil
}

func applyFiltering(unfilteredSeries []types.SeriesMetadata, bitmap []bool, filteredSeriesCount int, canReuseSlice bool, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) ([]types.SeriesMetadata, error) {
	if bitmap == nil {
		// Fast path: no filters to apply.

		if canReuseSlice {
			return unfilteredSeries, nil
		}

		// Return a copy of the original series metadata.
		// This is a shallow copy, which is sufficient while we're using stringlabels for labels.Labels given these are immutable.
		metadata, err := types.SeriesMetadataSlicePool.Get(len(unfilteredSeries), memoryConsumptionTracker)
		if err != nil {
			return nil, err
		}

		metadata, err = types.AppendSeriesMetadata(memoryConsumptionTracker, metadata, unfilteredSeries...)
		if err != nil {
			return nil, err
		}

		return metadata, nil
	}

	// Try to reuse the original slice, if we can.
	var filteredSeries []types.SeriesMetadata

	if canReuseSlice {
		filteredSeries = unfilteredSeries[:0]
	} else {
		var err error
		filteredSeries, err = types.SeriesMetadataSlicePool.Get(filteredSeriesCount, memoryConsumptionTracker)
		if err != nil {
			return nil, err
		}
	}

	for idx, matchesFilter := range bitmap {
		if !matchesFilter {
			continue
		}

		// If we're reusing the original slice, then we need to decrease the memory consumption estimate for the existing series
		// we're about to replace in the slice.
		if canReuseSlice {
			memoryConsumptionTracker.DecreaseMemoryConsumptionForLabels(unfilteredSeries[len(filteredSeries)].Labels)
		}

		var err error
		filteredSeries, err = types.AppendSeriesMetadata(memoryConsumptionTracker, filteredSeries, unfilteredSeries[idx])
		if err != nil {
			return nil, err
		}
	}

	if canReuseSlice {
		// If we reused the original slice, zero out the remaining elements, and adjust the memory consumption estimate to match.
		for idx, series := range unfilteredSeries[len(filteredSeries):] {
			unfilteredSeries[len(filteredSeries)+idx] = types.SeriesMetadata{}
			memoryConsumptionTracker.DecreaseMemoryConsumptionForLabels(series.Labels)
		}
	}

	return filteredSeries, nil
}

func (d *InstantVectorDuplicationConsumer) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	return d.Buffer.SeriesMetadata(ctx, d)
}

func (d *InstantVectorDuplicationConsumer) shouldReturnUnfilteredSeries(unfilteredSeriesIndex int) bool {
	if d.closed {
		// Closed.
		return false
	}

	if len(d.filters) == 0 {
		return true
	}

	return d.unfilteredSeriesBitmap[unfilteredSeriesIndex]
}

func (d *InstantVectorDuplicationConsumer) advanceToNextUnfilteredSeries() {
	d.nextUnfilteredSeriesIndex++

	for d.nextUnfilteredSeriesIndex < len(d.unfilteredSeriesBitmap) && !d.shouldReturnUnfilteredSeries(d.nextUnfilteredSeriesIndex) {
		d.nextUnfilteredSeriesIndex++
	}
}

func (d *InstantVectorDuplicationConsumer) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	return d.Buffer.NextSeries(ctx, d)
}

func (d *InstantVectorDuplicationConsumer) ExpressionPosition() posrange.PositionRange {
	return d.Buffer.Inner.ExpressionPosition()
}

func (d *InstantVectorDuplicationConsumer) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return d.Buffer.Prepare(ctx, params)
}

func (d *InstantVectorDuplicationConsumer) AfterPrepare(ctx context.Context) error {
	return d.Buffer.AfterPrepare(ctx)
}

func (d *InstantVectorDuplicationConsumer) Finalize(ctx context.Context) error {
	return d.Buffer.Finalize(ctx, d)
}

func (d *InstantVectorDuplicationConsumer) Close() {
	d.Buffer.CloseConsumer(d)
	types.BoolSlicePool.Put(&d.unfilteredSeriesBitmap, d.Buffer.MemoryConsumptionTracker)
}

func matchesSeries(filters []*labels.Matcher, series labels.Labels) bool {
	for _, filter := range filters {
		if !filter.Matches(series.Get(filter.Name)) {
			return false
		}
	}

	return true
}
