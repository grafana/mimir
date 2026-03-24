// SPDX-License-Identifier: AGPL-3.0-only

package sharding

import (
	"context"
	"errors"
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func init() {
	parser.Functions[ConcatFunction.Name] = ConcatFunction

	if err := functions.RegisterFunction(functions.FUNCTION_SHARDING_CONCAT, ConcatFunction.Name, ConcatFunction.ReturnType, concatFactory); err != nil {
		panic(err)
	}
}

func concatFactory(args []types.Operator, _ labels.Labels, params *planning.OperatorParameters, _ posrange.PositionRange, _ types.QueryTimeRange) (types.Operator, error) {
	operators := make([]types.InstantVectorOperator, 0, len(args))

	for _, arg := range args {
		instantVectorOperator, ok := arg.(types.InstantVectorOperator)
		if !ok {
			return nil, fmt.Errorf("expected InstantVectorOperator for %s, but got %T", ConcatFunction.Name, arg)
		}

		operators = append(operators, instantVectorOperator)
	}

	return NewConcat(operators, params.MemoryConsumptionTracker)
}

var ConcatFunction = &parser.Function{
	Name:       "__sharded_concat__",
	ArgTypes:   []parser.ValueType{parser.ValueTypeVector},
	Variadic:   -1,
	ReturnType: parser.ValueTypeVector,
}

type Concat struct {
	Inner                    []types.InstantVectorOperator
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	seriesCounts      []int // One entry per inner operator, value is the number of series left to read from that operator.
	nextOperatorIndex int
}

func NewConcat(inner []types.InstantVectorOperator, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (*Concat, error) {
	if len(inner) == 0 {
		return nil, errors.New("no inner operators passed to NewConcat")
	}

	return &Concat{
		Inner:                    inner,
		MemoryConsumptionTracker: memoryConsumptionTracker,
	}, nil
}

func (c *Concat) Prepare(ctx context.Context, params *types.PrepareParams) error {
	for _, o := range c.Inner {
		if err := o.Prepare(ctx, params); err != nil {
			return err
		}
	}

	return nil
}

func (c *Concat) AfterPrepare(ctx context.Context) error {
	for _, o := range c.Inner {
		if err := o.AfterPrepare(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (c *Concat) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	var err error
	c.seriesCounts, err = types.IntSlicePool.Get(len(c.Inner), c.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	results := make([][]types.SeriesMetadata, 0, len(c.Inner))
	seriesCount := 0

	for _, o := range c.Inner {
		result, err := o.SeriesMetadata(ctx, matchers)
		if err != nil {
			return nil, err
		}

		results = append(results, result)
		seriesCount += len(result)
		c.seriesCounts = append(c.seriesCounts, len(result))
	}

	combined, err := types.SeriesMetadataSlicePool.Get(seriesCount, c.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	for _, result := range results {
		combined = append(combined, result...)

		// Clear the slice, as we don't want Put to reduce the memory consumption
		// estimate based on the labels in the slice (since we're retaining those labels in
		// the combined slice).
		clear(result)
		types.SeriesMetadataSlicePool.Put(&result, c.MemoryConsumptionTracker)
	}

	return combined, nil
}

func (c *Concat) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if c.nextOperatorIndex >= len(c.Inner) {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	for c.seriesCounts[c.nextOperatorIndex] == 0 {
		c.nextOperatorIndex++

		if c.nextOperatorIndex >= len(c.Inner) {
			return types.InstantVectorSeriesData{}, types.EOS
		}
	}

	d, err := c.Inner[c.nextOperatorIndex].NextSeries(ctx)
	c.seriesCounts[c.nextOperatorIndex]--

	return d, err
}

func (c *Concat) Finalize(ctx context.Context) error {
	for _, o := range c.Inner {
		if err := o.Finalize(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (c *Concat) ExpressionPosition() posrange.PositionRange {
	return c.Inner[0].ExpressionPosition()
}

func (c *Concat) Close() {
	for _, o := range c.Inner {
		o.Close()
	}

	types.IntSlicePool.Put(&c.seriesCounts, c.MemoryConsumptionTracker)
}

var _ types.InstantVectorOperator = &Concat{}
