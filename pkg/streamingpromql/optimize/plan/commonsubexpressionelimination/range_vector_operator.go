// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"context"
	"fmt"
	"math"

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

	lastNextSeriesCallIndex int
	consumers               []*rangeVectorConsumerState
	buffer                  *SeriesDataRingBuffer[bufferedRangeVectorStepData]

	// Multiple RangeVectorDuplicationConsumers will call RangeVectorDuplicationBuffer.Prepare() and AfterPrepare(), so this ensures idempotency.
	prepareCalled      bool
	afterPrepareCalled bool
}

type rangeVectorConsumerState struct {
	currentSeriesIndex          int // -1 means the consumer hasn't advanced to the first series yet.
	hasReadCurrentSeriesSamples bool
	finalized                   bool
	closed                      bool
}

func NewRangeVectorDuplicationBuffer(inner types.RangeVectorOperator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *RangeVectorDuplicationBuffer {
	return &RangeVectorDuplicationBuffer{
		Inner:                    inner,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		lastNextSeriesCallIndex:  -1,
		buffer:                   &SeriesDataRingBuffer[bufferedRangeVectorStepData]{},
	}
}

func (b *RangeVectorDuplicationBuffer) AddConsumer() *RangeVectorDuplicationConsumer {
	consumerIndex := len(b.consumers)
	b.consumers = append(b.consumers, &rangeVectorConsumerState{
		currentSeriesIndex: -1,
	})

	return &RangeVectorDuplicationConsumer{
		Buffer:        b,
		consumerIndex: consumerIndex,
	}
}

func (b *RangeVectorDuplicationBuffer) SeriesMetadata(ctx context.Context, _ types.Matchers) ([]types.SeriesMetadata, error) {
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

func (b *RangeVectorDuplicationBuffer) NextSeries(ctx context.Context, consumerIndex int) error {
	consumer := b.consumers[consumerIndex]
	if consumer.closed {
		return fmt.Errorf("consumer %d is already closed, can't advance to next series", consumerIndex)
	}

	consumer.currentSeriesIndex++
	consumer.hasReadCurrentSeriesSamples = false
	b.releaseUnneededBufferedData()

	if b.lastNextSeriesCallIndex < consumer.currentSeriesIndex {
		// If this consumer is the leading one, then call NextSeries on the inner operator now.
		b.lastNextSeriesCallIndex++
		if err := b.Inner.NextSeries(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (b *RangeVectorDuplicationBuffer) releaseUnneededBufferedData() {
	if b.buffer.seriesCount == 0 {
		return
	}

	earliestNeededSeriesIndex := b.earliestNeededSeriesIndex()

	for b.buffer.seriesCount > 0 && b.buffer.firstSeriesIndex < earliestNeededSeriesIndex {
		d := b.buffer.RemoveFirst()
		d.Close()
	}
}

func (b *RangeVectorDuplicationBuffer) earliestNeededSeriesIndex() int {
	idx := math.MaxInt

	for _, consumer := range b.consumers {
		if consumer.closed {
			continue
		}

		currentSeriesIndex := consumer.currentSeriesIndex

		if consumer.hasReadCurrentSeriesSamples {
			currentSeriesIndex++
		}

		idx = min(idx, currentSeriesIndex)
	}

	return idx
}

func (b *RangeVectorDuplicationBuffer) checkIfAllOtherConsumersAreAheadOf(consumerIndex int) bool {
	thisConsumerPosition := b.consumers[consumerIndex].currentSeriesIndex

	for otherConsumerIndex, otherConsumer := range b.consumers {
		if otherConsumerIndex == consumerIndex {
			continue
		}

		if otherConsumer.closed {
			continue
		}

		if otherConsumer.currentSeriesIndex <= thisConsumerPosition {
			return false
		}
	}

	return true
}

func (b *RangeVectorDuplicationBuffer) NextStepSamples(ctx context.Context, consumerIndex int) (*types.RangeVectorStepData, error) {
	consumer := b.consumers[consumerIndex]

	if consumer.hasReadCurrentSeriesSamples {
		return nil, types.EOS
	}

	consumer.hasReadCurrentSeriesSamples = true

	if b.buffer.IsPresent(consumer.currentSeriesIndex) {
		// We can't remove the step data from the buffer now if this is the last consumer for this series -
		// we'll do this in the next call to NextSeries so that we can return the cloned sample ring buffers to their pools.
		return b.buffer.Get(consumer.currentSeriesIndex).stepData, nil
	}

	isLastConsumerOfThisSeries := b.checkIfAllOtherConsumersAreAheadOf(consumerIndex)
	stepData, err := b.Inner.NextStepSamples(ctx)
	if err != nil {
		return nil, err
	}

	if isLastConsumerOfThisSeries {
		// There's no need to buffer this series' data.
		return stepData, nil
	}

	// Clone the step data, so that the inner operator can mutate the ring buffer on the next NextStepSamples call.
	clonedData, err := cloneStepData(stepData)
	if err != nil {
		return nil, err
	}

	b.buffer.Append(clonedData, consumer.currentSeriesIndex)

	return clonedData.stepData, nil
}

func (b *RangeVectorDuplicationBuffer) CloseConsumer(consumerIndex int) {
	consumer := b.consumers[consumerIndex]
	if consumer.closed {
		// We've already closed this consumer, nothing more to do.
		return
	}

	consumer.closed = true

	if b.allConsumersClosed() {
		b.close()
	} else {
		b.releaseUnneededBufferedData()
	}
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

func (b *RangeVectorDuplicationBuffer) Finalize(ctx context.Context, consumerIndex int) error {
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

	consumerIndex int
}

var _ types.RangeVectorOperator = &RangeVectorDuplicationConsumer{}

func (d *RangeVectorDuplicationConsumer) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	return d.Buffer.SeriesMetadata(ctx, matchers)
}

func (d *RangeVectorDuplicationConsumer) NextSeries(ctx context.Context) error {
	return d.Buffer.NextSeries(ctx, d.consumerIndex)
}

func (d *RangeVectorDuplicationConsumer) NextStepSamples(ctx context.Context) (*types.RangeVectorStepData, error) {
	return d.Buffer.NextStepSamples(ctx, d.consumerIndex)
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
	return d.Buffer.Finalize(ctx, d.consumerIndex)
}

func (d *RangeVectorDuplicationConsumer) Close() {
	d.Buffer.CloseConsumer(d.consumerIndex)
}
