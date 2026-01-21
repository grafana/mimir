// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type ScalarRemoteExec struct {
	QueryParameters          *planning.QueryParameters
	Node                     planning.Node
	TimeRange                types.QueryTimeRange
	RemoteExecutor           RemoteExecutor
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
	Annotations              *annotations.Annotations
	QueryStats               *types.QueryStats
	EagerLoad                bool
	expressionPosition       posrange.PositionRange

	resp      ScalarRemoteExecutionResponse
	finalized bool
}

var _ types.ScalarOperator = &ScalarRemoteExec{}

func (s *ScalarRemoteExec) Prepare(ctx context.Context, params *types.PrepareParams) error {
	var err error
	s.resp, err = s.RemoteExecutor.StartScalarExecution(ctx, s.QueryParameters, s.Node, s.TimeRange, s.MemoryConsumptionTracker, s.EagerLoad)
	return err
}

func (s *ScalarRemoteExec) AfterPrepare(ctx context.Context) error {
	return nil
}

func (s *ScalarRemoteExec) GetValues(ctx context.Context) (types.ScalarData, error) {
	v, err := s.resp.GetValues(ctx)
	if err != nil {
		return types.ScalarData{}, err
	}

	return v, nil
}

func (s *ScalarRemoteExec) Finalize(ctx context.Context) error {
	if s.finalized {
		return nil
	}

	s.finalized = true

	return finalize(ctx, s.resp, s.Annotations, s.QueryStats)
}

func (s *ScalarRemoteExec) ExpressionPosition() posrange.PositionRange {
	return s.expressionPosition
}

func (s *ScalarRemoteExec) Close() {
	if s.resp != nil {
		s.resp.Close()
	}

	s.finalized = true // Don't try to finalize from a closed stream.
}
