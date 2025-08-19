// SPDX-License-Identifier: AGPL-3.0-only

package v2

import (
	"context"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/remoteexec"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type RemoteExecutor struct {
	frontend *Frontend
}

var _ remoteexec.RemoteExecutor = &RemoteExecutor{}

func NewRemoteExecutor(frontend *Frontend) *RemoteExecutor {
	return &RemoteExecutor{frontend: frontend}
}

func (r *RemoteExecutor) StartScalarExecution(ctx context.Context, node planning.Node, timeRange types.QueryTimeRange) (remoteexec.ScalarRemoteExecutionResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (r *RemoteExecutor) StartInstantVectorExecution(ctx context.Context, node planning.Node, timeRange types.QueryTimeRange) (remoteexec.InstantVectorRemoteExecutionResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (r *RemoteExecutor) StartRangeVectorExecution(ctx context.Context, node planning.Node, timeRange types.QueryTimeRange) (remoteexec.RangeVectorRemoteExecutionResponse, error) {
	//TODO implement me
	panic("implement me")
}
