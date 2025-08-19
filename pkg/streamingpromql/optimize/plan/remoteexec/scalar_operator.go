// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type ScalarRemoteExec struct {
	Node           planning.Node
	TimeRange      types.QueryTimeRange
	RemoteExecutor RemoteExecutor

	resp ScalarRemoteExecutionResponse
}

func (s *ScalarRemoteExec) Prepare(ctx context.Context, params *types.PrepareParams) error {
	var err error
	s.resp, err = s.RemoteExecutor.StartScalarExecution(ctx, s.Node, s.TimeRange)
	return err
}

func (s *ScalarRemoteExec) GetValues(ctx context.Context) (types.ScalarData, error) {
	return s.resp.GetValues(ctx)
}

func (s *ScalarRemoteExec) ExpressionPosition() posrange.PositionRange {
	return s.Node.ExpressionPosition()
}

func (s *ScalarRemoteExec) Close() {
	if s.resp != nil {
		s.resp.Close()
	}
}

var _ types.ScalarOperator = &ScalarRemoteExec{}
