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
	consumerIndex := len(b.consumers)
	consumer := &InstantVectorDuplicationConsumer{
		Buffer:        b,
		consumerIndex: consumerIndex,
	}

	b.consumers = append(b.consumers, consumer)
	return consumer
}

func (b *InstantVectorDuplicationBuffer) SeriesMetadata(ctx context.Context, consumerIndex int) ([]types.SeriesMetadata, error) {
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
	}

	b.seriesMetadataCount++
	isLastConsumer := b.seriesMetadataCount == len(b.consumers)

	filteredSeries, err := b.consumers[consumerIndex].applyFiltering(b.seriesMetadata, isLastConsumer)
	if err != nil {
		return nil, err
	}

	if isLastConsumer {
		b.seriesMetadata = nil
	}

	return filteredSeries, nil
}

func (b *InstantVectorDuplicationBuffer) NextSeries(ctx context.Context, consumerIndex int) (types.InstantVectorSeriesData, error) {
	consumer := b.consumers[consumerIndex]
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

func (b *InstantVectorDuplicationBuffer) CloseConsumer(consumerIndex int) {
	consumer := b.consumers[consumerIndex]
	if consumer.nextUnfilteredSeriesIndex == -1 {
		// We've already closed this consumer, nothing more to do.
		return
	}

	lowestNextSeriesIndexOfOtherConsumers := math.MaxInt
	for otherConsumerIndex, otherConsumer := range b.consumers {
		if consumerIndex == otherConsumerIndex {
			continue
		}

		if otherConsumer.nextUnfilteredSeriesIndex == -1 {
			// Already closed.
			continue
		}

		lowestNextSeriesIndexOfOtherConsumers = min(lowestNextSeriesIndexOfOtherConsumers, otherConsumer.nextUnfilteredSeriesIndex)
	}

	if lowestNextSeriesIndexOfOtherConsumers == math.MaxInt {
		// All other consumers are already closed. Close everything.
		consumer.nextUnfilteredSeriesIndex = -1
		b.close()
		return
	}

	// If this consumer was the lagging consumer, free any data that was being buffered for it.
	for consumer.nextUnfilteredSeriesIndex < lowestNextSeriesIndexOfOtherConsumers {
		seriesIdx := consumer.nextUnfilteredSeriesIndex

		// Only try to remove the buffered series if it was actually buffered (we might not have stored it if an error occurred reading the series).
		if b.buffer.IsPresent(seriesIdx) {
			d := b.buffer.Remove(seriesIdx)
			types.PutInstantVectorSeriesData(d, b.MemoryConsumptionTracker)
		}

		consumer.nextUnfilteredSeriesIndex++
	}

	consumer.nextUnfilteredSeriesIndex = -1
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

func (b *InstantVectorDuplicationBuffer) Finalize(ctx context.Context, consumerIndex int) error {
	consumer := b.consumers[consumerIndex]

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
	consumerIndex          int

	nextUnfilteredSeriesIndex int // -1 if this consumer is closed.
	finalized                 bool
}

var _ types.InstantVectorOperator = &InstantVectorDuplicationConsumer{}

func (d *InstantVectorDuplicationConsumer) SetFilters(filters []*labels.Matcher) {
	d.filters = filters
}

func (d *InstantVectorDuplicationConsumer) applyFiltering(unfilteredSeries []types.SeriesMetadata, canReuseSlice bool) ([]types.SeriesMetadata, error) {
	if len(d.filters) == 0 {
		d.nextUnfilteredSeriesIndex = 0

		if canReuseSlice {
			return unfilteredSeries, nil
		}

		// Return a copy of the original series metadata.
		// This is a shallow copy, which is sufficient while we're using stringlabels for labels.Labels given these are immutable.
		metadata, err := types.SeriesMetadataSlicePool.Get(len(unfilteredSeries), d.Buffer.MemoryConsumptionTracker)
		if err != nil {
			return nil, err
		}

		return types.AppendSeriesMetadata(d.Buffer.MemoryConsumptionTracker, metadata, unfilteredSeries...)
	}

	var filteredSeries []types.SeriesMetadata

	// Try to reuse the original slice, if we can.
	if canReuseSlice {
		filteredSeries = unfilteredSeries[:0]
	} else {
		var err error
		filteredSeries, err = types.SeriesMetadataSlicePool.Get(len(unfilteredSeries), d.Buffer.MemoryConsumptionTracker)
		if err != nil {
			return nil, err
		}
	}

	var err error
	d.unfilteredSeriesBitmap, err = types.BoolSlicePool.Get(len(unfilteredSeries), d.Buffer.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	d.nextUnfilteredSeriesIndex = len(unfilteredSeries)
	d.unfilteredSeriesBitmap = d.unfilteredSeriesBitmap[:len(unfilteredSeries)]

	for unfilteredSeriesIndex, series := range unfilteredSeries {
		if !matchesSeries(d.filters, series.Labels) {
			continue
		}

		d.unfilteredSeriesBitmap[unfilteredSeriesIndex] = true

		if d.nextUnfilteredSeriesIndex == len(unfilteredSeries) {
			// First unfiltered series that matches.
			d.nextUnfilteredSeriesIndex = unfilteredSeriesIndex
		}

		// If we're reusing the original slice, then we need to decrease the memory consumption estimate for the existing series
		// we're about to replace in the slice.
		if canReuseSlice {
			d.Buffer.MemoryConsumptionTracker.DecreaseMemoryConsumptionForLabels(unfilteredSeries[len(filteredSeries)].Labels)
		}

		filteredSeries, err = types.AppendSeriesMetadata(d.Buffer.MemoryConsumptionTracker, filteredSeries, series)
		if err != nil {
			return nil, err
		}
	}

	if canReuseSlice {
		// If we reused the original slice, zero out the remaining elements, and adjust the memory consumption estimate to match.
		for idx, series := range unfilteredSeries[len(filteredSeries):] {
			unfilteredSeries[len(filteredSeries)+idx] = types.SeriesMetadata{}
			d.Buffer.MemoryConsumptionTracker.DecreaseMemoryConsumptionForLabels(series.Labels)
		}
	}

	return filteredSeries, nil
}

func (d *InstantVectorDuplicationConsumer) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	return d.Buffer.SeriesMetadata(ctx, d.consumerIndex)
}

func (d *InstantVectorDuplicationConsumer) shouldReturnUnfilteredSeries(unfilteredSeriesIndex int) bool {
	if d.nextUnfilteredSeriesIndex == -1 {
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
	return d.Buffer.NextSeries(ctx, d.consumerIndex)
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
	return d.Buffer.Finalize(ctx, d.consumerIndex)
}

func (d *InstantVectorDuplicationConsumer) Close() {
	d.Buffer.CloseConsumer(d.consumerIndex)
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
