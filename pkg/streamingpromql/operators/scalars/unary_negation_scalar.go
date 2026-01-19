// SPDX-License-Identifier: AGPL-3.0-only

package scalars

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type UnaryNegationOfScalar struct {
	Inner              types.ScalarOperator
	expressionPosition posrange.PositionRange
}

var _ types.ScalarOperator = &UnaryNegationOfScalar{}

func NewUnaryNegationOfScalar(inner types.ScalarOperator, expressionPosition posrange.PositionRange) *UnaryNegationOfScalar {
	return &UnaryNegationOfScalar{
		Inner:              inner,
		expressionPosition: expressionPosition,
	}
}

func (u *UnaryNegationOfScalar) GetValues(ctx context.Context) (types.ScalarData, error) {
	values, err := u.Inner.GetValues(ctx)
	if err != nil {
		return types.ScalarData{}, err
	}

	for i := range values.Samples {
		values.Samples[i].F = -values.Samples[i].F
	}

	return values, nil
}

func (u *UnaryNegationOfScalar) ExpressionPosition() posrange.PositionRange {
	return u.expressionPosition
}

func (u *UnaryNegationOfScalar) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return u.Inner.Prepare(ctx, params)
}

func (u *UnaryNegationOfScalar) AfterPrepare(ctx context.Context) error {
	return u.Inner.AfterPrepare(ctx)
}

func (u *UnaryNegationOfScalar) Finalize(ctx context.Context) error {
	return u.Inner.Finalize(ctx)
}

func (u *UnaryNegationOfScalar) Close() {
	u.Inner.Close()
}
