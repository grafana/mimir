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

	seriesMetadataCount int
	seriesMetadata      []types.SeriesMetadata

	consumers []*instantVectorConsumerState
	buffer    *SeriesDataRingBuffer[types.InstantVectorSeriesData]

	// Multiple InstantVectorDuplicationConsumers will call InstantVectorDuplicationBuffer.Prepare(), so this ensures idempotency.
	prepared bool
}

type instantVectorConsumerState struct {
	nextSeriesIndex int // -1 if this consumer is closed.
	finalized       bool
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
	b.consumers = append(b.consumers, &instantVectorConsumerState{})

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

	if b.seriesMetadataCount == len(b.consumers) {
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
	consumer := b.consumers[consumerIndex]
	nextSeriesIndex := consumer.nextSeriesIndex
	isLastConsumerOfThisSeries := b.checkIfAllOtherConsumersAreAheadOf(consumer)
	consumer.nextSeriesIndex++

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

func (b *InstantVectorDuplicationBuffer) checkIfAllOtherConsumersAreAheadOf(consumer *instantVectorConsumerState) bool {
	for _, otherConsumer := range b.consumers {
		if otherConsumer == consumer {
			continue
		}

		if otherConsumer.nextSeriesIndex == -1 {
			// This consumer is closed.
			continue
		}

		if otherConsumer.nextSeriesIndex <= consumer.nextSeriesIndex {
			return false
		}
	}

	return true
}

func (b *InstantVectorDuplicationBuffer) CloseConsumer(consumerIndex int) {
	consumer := b.consumers[consumerIndex]
	if consumer.nextSeriesIndex == -1 {
		// We've already closed this consumer, nothing more to do.
		return
	}

	lowestNextSeriesIndexOfOtherConsumers := math.MaxInt
	for otherConsumerIndex, otherConsumer := range b.consumers {
		if consumerIndex == otherConsumerIndex {
			continue
		}

		if otherConsumer.nextSeriesIndex == -1 {
			// Already closed.
			continue
		}

		lowestNextSeriesIndexOfOtherConsumers = min(lowestNextSeriesIndexOfOtherConsumers, otherConsumer.nextSeriesIndex)
	}

	if lowestNextSeriesIndexOfOtherConsumers == math.MaxInt {
		// All other consumers are already closed. Close everything.
		consumer.nextSeriesIndex = -1
		b.close()
		return
	}

	// If this consumer was the lagging consumer, free any data that was being buffered for it.
	for consumer.nextSeriesIndex < lowestNextSeriesIndexOfOtherConsumers {
		seriesIdx := consumer.nextSeriesIndex

		// Only try to remove the buffered series if it was actually buffered (we might not have stored it if an error occurred reading the series).
		if b.buffer.IsPresent(seriesIdx) {
			d := b.buffer.Remove(seriesIdx)
			types.PutInstantVectorSeriesData(d, b.MemoryConsumptionTracker)
		}

		consumer.nextSeriesIndex++
	}

	consumer.nextSeriesIndex = -1
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

func (d *InstantVectorDuplicationConsumer) Finalize(ctx context.Context) error {
	return d.Buffer.Finalize(ctx, d.consumerIndex)
}

func (d *InstantVectorDuplicationConsumer) Close() {
	d.Buffer.CloseConsumer(d.consumerIndex)
}
