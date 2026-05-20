// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type ScalarRemoteExec struct {
	Node               planning.Node
	TimeRange          types.QueryTimeRange
	GroupEvaluator     GroupEvaluator
	Annotations        *annotations.Annotations
	expressionPosition posrange.PositionRange

	resp                  ScalarRemoteExecutionResponse
	finishedReadingCalled bool
}

var _ types.ScalarOperator = &ScalarRemoteExec{}

func (s *ScalarRemoteExec) Prepare(ctx context.Context, params *types.PrepareParams) error {
	var err error
	s.resp, err = s.GroupEvaluator.CreateScalarExecution(ctx, s.Node, s.TimeRange)
	if err != nil {
		return err
	}

	return nil
}

func (s *ScalarRemoteExec) AfterPrepare(ctx context.Context) error {
	return s.resp.Start(ctx)
}

func (s *ScalarRemoteExec) GetValues(ctx context.Context) (types.ScalarData, error) {
	v, err := s.resp.GetValues(ctx)
	if err != nil {
		return types.ScalarData{}, err
	}

	return v, nil
}

func (s *ScalarRemoteExec) FinishedReading(ctx context.Context) error {
	if s.finishedReadingCalled {
		return nil
	}

	s.finishedReadingCalled = true

	return finishedReading(ctx, s.resp, s.Annotations)
}

func (s *ScalarRemoteExec) ExpressionPosition() posrange.PositionRange {
	return s.expressionPosition
}

func (s *ScalarRemoteExec) Stats(ctx context.Context) (*types.OperatorEvaluationStats, error) {
	return s.resp.Stats(ctx)
}

func (s *ScalarRemoteExec) Close() {
	if s.resp != nil {
		s.resp.Close()
	}

	s.finishedReadingCalled = true // Don't try to call FinishedReading from a closed stream.
}
