// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"context"
	"math"

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

	consumerCount int

	seriesMetadataCount int
	seriesMetadata      []types.SeriesMetadata

	nextSeriesIndex []int // One entry per consumer.
	buffer          *SeriesDataRingBuffer[types.InstantVectorSeriesData]

	// Multiple InstantVectorDuplicationConsumers will call InstantVectorDuplicationBuffer.Prepare(), so this ensures idempotency.
	prepared bool
}

func NewInstantVectorDuplicationBuffer(inner types.InstantVectorOperator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *InstantVectorDuplicationBuffer {
	return &InstantVectorDuplicationBuffer{
		Inner:                    inner,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		buffer:                   &SeriesDataRingBuffer[types.InstantVectorSeriesData]{},
	}
}

func (b *InstantVectorDuplicationBuffer) AddConsumer() *InstantVectorDuplicationConsumer {
	consumerIndex := b.consumerCount
	b.consumerCount++
	b.nextSeriesIndex = append(b.nextSeriesIndex, 0)

	return &InstantVectorDuplicationConsumer{
		Buffer:        b,
		consumerIndex: consumerIndex,
	}
}

func (b *InstantVectorDuplicationBuffer) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	if b.seriesMetadataCount == 0 {
		// Haven't loaded series metadata yet, load it now.
		var err error
		b.seriesMetadata, err = b.Inner.SeriesMetadata(ctx)
		if err != nil {
			return nil, err
		}
	}

	b.seriesMetadataCount++

	if b.seriesMetadataCount == b.consumerCount {
		// We can safely return the original series metadata, as we're not going to return this to another consumer.
		metadata := b.seriesMetadata
		b.seriesMetadata = nil

		return metadata, nil
	}

	// Return a copy of the original series metadata.
	// This is a shallow copy, which is sufficient while we're using stringlabels for labels.Labels given these are immutable.
	metadata, err := types.SeriesMetadataSlicePool.Get(len(b.seriesMetadata), b.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	return types.AppendSeriesMetadata(b.MemoryConsumptionTracker, metadata, b.seriesMetadata...)
}

func (b *InstantVectorDuplicationBuffer) NextSeries(ctx context.Context, consumerIndex int) (types.InstantVectorSeriesData, error) {
	nextSeriesIndex := b.nextSeriesIndex[consumerIndex]
	isLastConsumerOfThisSeries := b.checkIfAllOtherConsumersAreAheadOf(consumerIndex)
	b.nextSeriesIndex[consumerIndex]++

	buffered := b.buffer.IsPresent(nextSeriesIndex)
	if buffered {
		if isLastConsumerOfThisSeries {
			// We can safely return the series as-is, as we're not going to return this to another consumer.
			d := b.buffer.Remove(nextSeriesIndex)
			return d, nil
		}

		d := b.buffer.Get(nextSeriesIndex)
		return d.Clone(b.MemoryConsumptionTracker)
	}

	d, err := b.Inner.NextSeries(ctx)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	// Only bother storing the data if another consumer needs it.
	if isLastConsumerOfThisSeries {
		return d, nil
	}

	b.buffer.Append(d, nextSeriesIndex)
	return d.Clone(b.MemoryConsumptionTracker)
}

func (b *InstantVectorDuplicationBuffer) checkIfAllOtherConsumersAreAheadOf(consumerIndex int) bool {
	thisConsumerPosition := b.nextSeriesIndex[consumerIndex]

	for otherConsumerIndex, otherConsumerPosition := range b.nextSeriesIndex {
		if otherConsumerIndex == consumerIndex {
			continue
		}

		if otherConsumerPosition == -1 {
			// This consumer is closed.
			continue
		}

		if otherConsumerPosition <= thisConsumerPosition {
			return false
		}
	}

	return true
}

func (b *InstantVectorDuplicationBuffer) CloseConsumer(consumerIndex int) {
	if b.nextSeriesIndex[consumerIndex] == -1 {
		// We've already closed this consumer, nothing more to do.
		return
	}

	lowestNextSeriesIndexOfOtherConsumers := math.MaxInt
	for otherConsumerIndex, nextSeriesIndex := range b.nextSeriesIndex {
		if consumerIndex == otherConsumerIndex {
			continue
		}

		if nextSeriesIndex == -1 {
			// Already closed.
			continue
		}

		lowestNextSeriesIndexOfOtherConsumers = min(lowestNextSeriesIndexOfOtherConsumers, nextSeriesIndex)
	}

	if lowestNextSeriesIndexOfOtherConsumers == math.MaxInt {
		// All other consumers are already closed. Close everything.
		b.nextSeriesIndex[consumerIndex] = -1
		b.close()
		return
	}

	// If this consumer was the lagging consumer, free any data that was being buffered for it.
	for b.nextSeriesIndex[consumerIndex] < lowestNextSeriesIndexOfOtherConsumers {
		seriesIdx := b.nextSeriesIndex[consumerIndex]

		// Only try to remove the buffered series if it was actually buffered (we might not have stored it if an error occurred reading the series).
		if b.buffer.IsPresent(seriesIdx) {
			d := b.buffer.Remove(seriesIdx)
			types.PutInstantVectorSeriesData(d, b.MemoryConsumptionTracker)
		}

		b.nextSeriesIndex[consumerIndex]++
	}

	b.nextSeriesIndex[consumerIndex] = -1
}

func (b *InstantVectorDuplicationBuffer) Prepare(ctx context.Context, params *types.PrepareParams) error {
	if b.prepared {
		return nil
	}
	if err := b.Inner.Prepare(ctx, params); err != nil {
		return err
	}
	b.prepared = true
	return nil
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

	consumerIndex int
}

var _ types.InstantVectorOperator = &InstantVectorDuplicationConsumer{}

func (d *InstantVectorDuplicationConsumer) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	return d.Buffer.SeriesMetadata(ctx)
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

func (d *InstantVectorDuplicationConsumer) Close() {
	d.Buffer.CloseConsumer(d.consumerIndex)
}
