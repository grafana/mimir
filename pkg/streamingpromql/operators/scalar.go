// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// TODO: We don't fully support Scalar's yet
type ConstantScalar struct {
	Expr *parser.NumberLiteral
}

var _ types.Operator = &ConstantScalar{}

func (s *ConstantScalar) SeriesMetadata(_ context.Context) ([]types.SeriesMetadata, error) {
	return nil, fmt.Errorf("SeriesMetadata should not be called for ConstantScalar")
}

func (s *ConstantScalar) GetFloat() float64 {
	return s.Expr.Val
}

func (s *ConstantScalar) Close() {}
