// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"context"
	"errors"

	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// ScalarDuplicationBuffer buffers the result of an inner operator that is used by multiple consuming operators.
//
// Unlike instant vector and range vector operators, scalar operators produce all of their data in a single call to
// GetValues rather than streaming a series at a time. This makes ScalarDuplicationBuffer much simpler than its instant
// vector and range vector equivalents: it reads the inner operator's values exactly once and hands out an independent
// copy to each consumer.
//
// ScalarDuplicationBuffer does not support filtering: a scalar expression returns a single set of samples without labels,
// so it is not possible to filter the result. If a scalar expression is derived from a subset of another selector, this
// would be handled elsewhere, before the result is transformed to a scalar value.
//
// ScalarDuplicationBuffer is not thread-safe.
type ScalarDuplicationBuffer struct {
	Inner                    types.ScalarOperator
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	consumers []*ScalarDuplicationConsumer

	// values holds the result of the single call to Inner.GetValues, buffered until every consumer has read it.
	valuesLoaded      bool
	values            types.ScalarData
	readConsumerCount int

	// Multiple ScalarDuplicationConsumers will call ScalarDuplicationBuffer.Prepare() and AfterPrepare(), so this ensures idempotency.
	prepareCalled      bool
	afterPrepareCalled bool

	stats       *types.OperatorEvaluationStats
	annotations annotations.Annotations
}

func NewScalarDuplicationBuffer(inner types.ScalarOperator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *ScalarDuplicationBuffer {
	return &ScalarDuplicationBuffer{
		Inner:                    inner,
		MemoryConsumptionTracker: memoryConsumptionTracker,
	}
}

func (b *ScalarDuplicationBuffer) AddConsumer() *ScalarDuplicationConsumer {
	consumer := &ScalarDuplicationConsumer{
		Buffer: b,
	}

	b.consumers = append(b.consumers, consumer)
	return consumer
}

func (b *ScalarDuplicationBuffer) GetValues(ctx context.Context, consumer *ScalarDuplicationConsumer) (types.ScalarData, error) {
	if consumer.read {
		return types.ScalarData{}, errors.New("ScalarDuplicationBuffer: cannot read values for the same consumer multiple times")
	}

	if !b.valuesLoaded {
		// Haven't read the inner operator's values yet, read them now.
		values, err := b.Inner.GetValues(ctx)
		if err != nil {
			return types.ScalarData{}, err
		}

		b.values = values
		b.valuesLoaded = true
	}

	consumer.read = true
	b.readConsumerCount++

	if b.readConsumerCount == len(b.consumers) {
		// This is the last consumer to read, so we don't need to buffer the values any longer.
		// Hand over the buffered values as-is rather than returning a clone.
		values := b.values
		b.values = types.ScalarData{}
		return values, nil
	}

	// Another consumer will read these values, so return a cloned copy.
	return b.cloneValues()
}

func (b *ScalarDuplicationBuffer) cloneValues() (types.ScalarData, error) {
	samples, err := types.FPointSlicePool.Get(len(b.values.Samples), b.MemoryConsumptionTracker)
	if err != nil {
		return types.ScalarData{}, err
	}

	samples = samples[:len(b.values.Samples)]
	copy(samples, b.values.Samples) // We can do a simple copy here, as FPoints don't contain pointers.

	return types.ScalarData{Samples: samples}, nil
}

func (b *ScalarDuplicationBuffer) Prepare(ctx context.Context, params *types.PrepareParams) error {
	if b.prepareCalled {
		return nil
	}

	b.prepareCalled = true
	return b.Inner.Prepare(ctx, params)
}

func (b *ScalarDuplicationBuffer) AfterPrepare(ctx context.Context) error {
	if b.afterPrepareCalled {
		return nil
	}

	b.afterPrepareCalled = true
	return b.Inner.AfterPrepare(ctx)
}

func (b *ScalarDuplicationBuffer) FinishedReading(ctx context.Context, consumer *ScalarDuplicationConsumer) error {
	if consumer.finishedReadingCalled {
		return nil
	}

	b.releaseBufferedData(consumer)

	if !b.allConsumersFinishedReading() {
		return nil
	}

	return b.Inner.FinishedReading(ctx)
}

func (b *ScalarDuplicationBuffer) releaseBufferedData(consumer *ScalarDuplicationConsumer) {
	if consumer.finishedReadingCalled {
		return
	}

	consumer.finishedReadingCalled = true

	// All consumers share the single buffered value, so we can only release it once every consumer is done with it.
	if b.allConsumersFinishedReading() {
		types.FPointSlicePool.Put(&b.values.Samples, b.MemoryConsumptionTracker)
	}
}

func (b *ScalarDuplicationBuffer) allConsumersFinishedReading() bool {
	for _, consumer := range b.consumers {
		if !consumer.finishedReadingCalled {
			return false
		}
	}

	return true
}

func (b *ScalarDuplicationBuffer) CloseConsumer(consumer *ScalarDuplicationConsumer) {
	if consumer.closed {
		// We've already closed this consumer, nothing more to do.
		return
	}

	b.releaseBufferedData(consumer)
	consumer.closed = true

	if !b.allConsumersClosed() {
		return
	}

	b.Inner.Close()

	if b.stats != nil {
		b.stats.Close()
		b.stats = nil
	}
}

func (b *ScalarDuplicationBuffer) allConsumersClosed() bool {
	for _, consumer := range b.consumers {
		if !consumer.closed {
			return false
		}
	}

	return true
}

func (b *ScalarDuplicationBuffer) Finalize(ctx context.Context, consumer *ScalarDuplicationConsumer) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	if !b.allConsumersFinishedReading() {
		return nil, nil, errors.New("ScalarDuplicationBuffer: cannot finalize when one or more consumers have not had FinishedReading called")
	}

	if consumer.finalized {
		return nil, nil, errors.New("ScalarDuplicationBuffer: cannot finalize the same consumer twice")
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
		stats, err = stats.Clone()
		if err != nil {
			return nil, nil, err
		}

		annos = types.CloneAnnotations(b.annotations)
	}

	return stats, annos, nil
}

func (b *ScalarDuplicationBuffer) allConsumersFinalized() bool {
	for _, consumer := range b.consumers {
		if !consumer.finalized {
			return false
		}
	}

	return true
}

type ScalarDuplicationConsumer struct {
	Buffer *ScalarDuplicationBuffer

	read                  bool
	closed                bool
	finishedReadingCalled bool
	finalized             bool
}

var _ types.ScalarOperator = &ScalarDuplicationConsumer{}

func (d *ScalarDuplicationConsumer) GetValues(ctx context.Context) (types.ScalarData, error) {
	return d.Buffer.GetValues(ctx, d)
}

func (d *ScalarDuplicationConsumer) ExpressionPosition() posrange.PositionRange {
	return d.Buffer.Inner.ExpressionPosition()
}

func (d *ScalarDuplicationConsumer) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return d.Buffer.Prepare(ctx, params)
}

func (d *ScalarDuplicationConsumer) AfterPrepare(ctx context.Context) error {
	return d.Buffer.AfterPrepare(ctx)
}

func (d *ScalarDuplicationConsumer) FinishedReading(ctx context.Context) error {
	return d.Buffer.FinishedReading(ctx, d)
}

func (d *ScalarDuplicationConsumer) Finalize(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	return d.Buffer.Finalize(ctx, d)
}

func (d *ScalarDuplicationConsumer) Close() {
	d.Buffer.CloseConsumer(d)
}
