// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"context"
	"errors"
	"math"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
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

	timeRange types.QueryTimeRange
	logger    log.Logger

	seriesMetadataCount int
	seriesMetadata      []types.SeriesMetadata

	consumers            []*InstantVectorDuplicationConsumer
	nextInnerSeriesIndex int
	buffer               *SeriesDataRingBuffer[types.InstantVectorSeriesData]

	// Multiple InstantVectorDuplicationConsumers will call InstantVectorDuplicationBuffer.Prepare() and AfterPrepare(), so this ensures idempotency.
	prepareCalled      bool
	afterPrepareCalled bool

	stats *types.OperatorEvaluationStats
}

func NewInstantVectorDuplicationBuffer(inner types.InstantVectorOperator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, timeRange types.QueryTimeRange, logger log.Logger) *InstantVectorDuplicationBuffer {
	return &InstantVectorDuplicationBuffer{
		Inner:                    inner,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		timeRange:                timeRange,
		logger:                   logger,
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

func (b *InstantVectorDuplicationBuffer) SeriesMetadata(ctx context.Context, matchers types.Matchers, consumer *InstantVectorDuplicationConsumer) ([]types.SeriesMetadata, error) {
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

func (b *InstantVectorDuplicationBuffer) releaseBufferedData(consumer *InstantVectorDuplicationConsumer) {
	if consumer.finalized {
		return
	}

	consumer.finalized = true

	defer consumer.subset.close(b.MemoryConsumptionTracker)

	// Remove any buffered series that are no longer needed because they were only being retained for the consumer
	// that was just closed.
	earliestSeriesIndexStillToReturn := b.earliestSeriesIndexStillToReturn()

	if earliestSeriesIndexStillToReturn == math.MaxInt {
		// All other consumers are already finalized. Clean up everything.
		b.releaseAllBufferedData()
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

			if consumer.subset.unfilteredSeriesBitmap != nil && !consumer.subset.unfilteredSeriesBitmap[thisSeriesIndex] {
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

func (b *InstantVectorDuplicationBuffer) releaseAllBufferedData() {
	types.SeriesMetadataSlicePool.Put(&b.seriesMetadata, b.MemoryConsumptionTracker)

	for b.buffer.Size() > 0 {
		types.PutInstantVectorSeriesData(b.buffer.RemoveFirst(), b.MemoryConsumptionTracker)
	}
}

func (b *InstantVectorDuplicationBuffer) earliestSeriesIndexStillToReturn() int {
	idx := math.MaxInt
	for _, consumer := range b.consumers {
		if consumer.finalized {
			continue
		}

		idx = min(idx, consumer.nextUnfilteredSeriesIndex)
	}

	return idx
}

func (b *InstantVectorDuplicationBuffer) allOpenConsumersHaveNoFilters() bool {
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

	b.releaseBufferedData(consumer)

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

func (b *InstantVectorDuplicationBuffer) allConsumersClosed() bool {
	for _, consumer := range b.consumers {
		if !consumer.closed {
			return false
		}
	}

	return true
}

func (b *InstantVectorDuplicationBuffer) QueryStats(ctx context.Context, consumer *InstantVectorDuplicationConsumer) (*types.OperatorEvaluationStats, error) {
	if !b.allConsumersFinalized() {
		return nil, errors.New("InstantVectorDuplicationBuffer: cannot get stats when one or more consumers are not finalized")
	}

	if consumer.hasReadStats {
		return nil, errors.New("InstantVectorDuplicationBuffer: cannot get stats twice for the same consumer")
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

			level.Warn(b.logger).Log("msg", "InstantVectorDuplicationBuffer expected subset statistics, but none were present, so returning empty set of statistics. This is expected during an upgrade from queriers without stats support to those with stats support, but a bug otherwise.")
			return types.NewOperatorEvaluationStats(ctx, b.timeRange, b.MemoryConsumptionTracker, 0)
		}

		stats.UseSubset(consumer.subset.subsetIndex)
	} else {
		stats.RemoveAllSubsets()
	}

	return stats, nil
}

func (b *InstantVectorDuplicationBuffer) allConsumersHaveReadStats() bool {
	for _, consumer := range b.consumers {
		if !consumer.hasReadStats {
			return false
		}
	}

	return true
}

type InstantVectorDuplicationConsumer struct {
	Buffer *InstantVectorDuplicationBuffer

	subset subset

	nextUnfilteredSeriesIndex int
	closed                    bool
	finalized                 bool
	hasReadStats              bool
}

var _ types.InstantVectorOperator = &InstantVectorDuplicationConsumer{}

func (d *InstantVectorDuplicationConsumer) SetFilters(filters []*labels.Matcher, subsetIndex int) {
	d.subset = subset{
		filters:     filters,
		subsetIndex: subsetIndex,
	}
}

func (d *InstantVectorDuplicationConsumer) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	return d.Buffer.SeriesMetadata(ctx, matchers, d)
}

func (d *InstantVectorDuplicationConsumer) shouldReturnUnfilteredSeries(unfilteredSeriesIndex int) bool {
	if d.finalized {
		return false
	}

	if !d.subset.applicable() {
		return true
	}

	return d.subset.unfilteredSeriesBitmap[unfilteredSeriesIndex]
}

func (d *InstantVectorDuplicationConsumer) advanceToNextUnfilteredSeries() {
	d.nextUnfilteredSeriesIndex++

	for d.nextUnfilteredSeriesIndex < len(d.subset.unfilteredSeriesBitmap) && !d.shouldReturnUnfilteredSeries(d.nextUnfilteredSeriesIndex) {
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

func (d *InstantVectorDuplicationConsumer) Stats(ctx context.Context) (*types.OperatorEvaluationStats, error) {
	return d.Buffer.QueryStats(ctx, d)
}

func (d *InstantVectorDuplicationConsumer) Close() {
	d.Buffer.CloseConsumer(d)
}
