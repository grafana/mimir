// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"context"
	"fmt"
	"math"
	"time"

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

	consumerCount int

	seriesMetadataCount int
	seriesMetadata      []types.SeriesMetadata

	lastNextSeriesCallIndex     int
	currentSeriesIndex          []int  // One entry per consumer. -1 means the consumer hasn't advanced to the first series yet, -2 means the consumer is closed.
	hasReadCurrentSeriesSamples []bool // One entry per consumer.
	buffer                      *SeriesDataRingBuffer[bufferedRangeVectorStepData]

	// Multiple RangeVectorDuplicationConsumers will call InstantVectorDuplicationBuffer.Prepare(), so this ensures idempotency.
	prepared bool
}

const consumerClosed int = -2

func NewRangeVectorDuplicationBuffer(inner types.RangeVectorOperator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *RangeVectorDuplicationBuffer {
	return &RangeVectorDuplicationBuffer{
		Inner:                    inner,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		lastNextSeriesCallIndex:  -1,
		buffer:                   &SeriesDataRingBuffer[bufferedRangeVectorStepData]{},
	}
}

func (b *RangeVectorDuplicationBuffer) AddConsumer() *RangeVectorDuplicationConsumer {
	consumerIndex := b.consumerCount
	b.consumerCount++
	b.currentSeriesIndex = append(b.currentSeriesIndex, -1)
	b.hasReadCurrentSeriesSamples = append(b.hasReadCurrentSeriesSamples, true)

	return &RangeVectorDuplicationConsumer{
		Buffer:        b,
		consumerIndex: consumerIndex,
	}
}

func (b *RangeVectorDuplicationBuffer) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
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

func (b *RangeVectorDuplicationBuffer) NextSeries(ctx context.Context, consumerIndex int) error {
	currentSeriesIndex := b.currentSeriesIndex[consumerIndex]
	if currentSeriesIndex == consumerClosed {
		return fmt.Errorf("consumer %d is already closed, can't advance to next series", consumerIndex)
	}

	b.currentSeriesIndex[consumerIndex]++
	b.hasReadCurrentSeriesSamples[consumerIndex] = false
	b.releaseUnneededBufferedData()

	if b.lastNextSeriesCallIndex < b.currentSeriesIndex[consumerIndex] {
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

	for consumerIndex, currentSeriesIndex := range b.currentSeriesIndex {
		if currentSeriesIndex == consumerClosed {
			continue
		}

		if b.hasReadCurrentSeriesSamples[consumerIndex] {
			currentSeriesIndex++
		}

		idx = min(idx, currentSeriesIndex)
	}

	return idx
}

func (b *RangeVectorDuplicationBuffer) checkIfAllOtherConsumersAreAheadOf(consumerIndex int) bool {
	thisConsumerPosition := b.currentSeriesIndex[consumerIndex]

	for otherConsumerIndex, otherConsumerPosition := range b.currentSeriesIndex {
		if otherConsumerIndex == consumerIndex {
			continue
		}

		if otherConsumerPosition == consumerClosed {
			continue
		}

		if otherConsumerPosition <= thisConsumerPosition {
			return false
		}
	}

	return true
}

func (b *RangeVectorDuplicationBuffer) NextStepSamples(consumerIndex int) (*types.RangeVectorStepData, error) {
	if b.hasReadCurrentSeriesSamples[consumerIndex] {
		return nil, types.EOS
	}

	currentSeriesIndex := b.currentSeriesIndex[consumerIndex]
	b.hasReadCurrentSeriesSamples[consumerIndex] = true

	if b.buffer.IsPresent(currentSeriesIndex) {
		// We can't remove the step data from the buffer now if this is the last consumer for this series -
		// we'll do this in the next call to NextSeries so that we can return the cloned sample ring buffers to their pools.
		return b.buffer.Get(currentSeriesIndex).stepData, nil
	}

	isLastConsumerOfThisSeries := b.checkIfAllOtherConsumersAreAheadOf(consumerIndex)
	stepData, err := b.Inner.NextStepSamples()
	if err != nil {
		return nil, err
	}

	if isLastConsumerOfThisSeries {
		// There's no need to buffer this series' data.
		return stepData, nil
	}

	// Clone the step data, so that the inner operator can mutate the ring buffer on the next NextStepSamples call.
	clonedData, err := cloneStepData(stepData, b.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	b.buffer.Append(clonedData, currentSeriesIndex)

	return clonedData.stepData, nil
}

func (b *RangeVectorDuplicationBuffer) CloseConsumer(consumerIndex int) {
	if b.currentSeriesIndex[consumerIndex] == consumerClosed {
		// We've already closed this consumer, nothing more to do.
		return
	}

	b.currentSeriesIndex[consumerIndex] = consumerClosed

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
	for _, currentIndex := range b.currentSeriesIndex {
		if currentIndex != consumerClosed {
			return false
		}
	}

	return true
}

func (b *RangeVectorDuplicationBuffer) Prepare(ctx context.Context, params *types.PrepareParams) error {
	if b.prepared {
		return nil
	}
	if err := b.Inner.Prepare(ctx, params); err != nil {
		return err
	}
	b.prepared = true
	return nil
}

type bufferedRangeVectorStepData struct {
	stepData        *types.RangeVectorStepData
	floatBuffer     *types.FPointRingBuffer
	histogramBuffer *types.HPointRingBuffer
}

func cloneStepData(stepData *types.RangeVectorStepData, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (bufferedRangeVectorStepData, error) {
	buffered := bufferedRangeVectorStepData{
		stepData: &types.RangeVectorStepData{
			StepT:      stepData.StepT,
			RangeStart: stepData.RangeStart,
			RangeEnd:   stepData.RangeEnd,
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

func (d *RangeVectorDuplicationConsumer) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	return d.Buffer.SeriesMetadata(ctx)
}

func (d *RangeVectorDuplicationConsumer) NextSeries(ctx context.Context) error {
	return d.Buffer.NextSeries(ctx, d.consumerIndex)
}

func (d *RangeVectorDuplicationConsumer) NextStepSamples() (*types.RangeVectorStepData, error) {
	return d.Buffer.NextStepSamples(d.consumerIndex)
}

func (d *RangeVectorDuplicationConsumer) StepCount() int {
	return d.Buffer.Inner.StepCount()
}

func (d *RangeVectorDuplicationConsumer) Range() time.Duration {
	return d.Buffer.Inner.Range()
}

func (d *RangeVectorDuplicationConsumer) ExpressionPosition() posrange.PositionRange {
	return d.Buffer.Inner.ExpressionPosition()
}

func (d *RangeVectorDuplicationConsumer) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return d.Buffer.Prepare(ctx, params)
}

func (d *RangeVectorDuplicationConsumer) Close() {
	d.Buffer.CloseConsumer(d.consumerIndex)
}
