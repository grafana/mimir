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
	RootPlan                 *planning.QueryPlan
	Node                     planning.Node
	TimeRange                types.QueryTimeRange
	RemoteExecutor           RemoteExecutor
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
	Annotations              *annotations.Annotations
	QueryStats               *types.QueryStats

	resp ScalarRemoteExecutionResponse
}

func (s *ScalarRemoteExec) Prepare(ctx context.Context, params *types.PrepareParams) error {
	s.QueryStats = params.QueryStats

	var err error
	s.resp, err = s.RemoteExecutor.StartScalarExecution(ctx, s.RootPlan, s.Node, s.TimeRange)
	return err
}

func (s *ScalarRemoteExec) GetValues(ctx context.Context) (types.ScalarData, error) {
	v, err := s.resp.GetValues(ctx)
	if err != nil {
		return types.ScalarData{}, err
	}

	if err := s.MemoryConsumptionTracker.IncreaseMemoryConsumption(uint64(cap(v.Samples))*types.FPointSize, limiter.FPointSlices); err != nil {
		return types.ScalarData{}, err
	}

	annos, totalSamples, err := s.resp.GetEvaluationInfo(ctx)
	if err != nil {
		return types.ScalarData{}, err
	}

	s.Annotations.Merge(annos)

	if s.QueryStats != nil {
		// FIXME: once we support evaluating multiple nodes at once, only do this once per request, not once per requested node
		s.QueryStats.TotalSamples += totalSamples
	}

	return v, nil
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
