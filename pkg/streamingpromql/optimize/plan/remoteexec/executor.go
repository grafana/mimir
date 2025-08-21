// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type RemoteExecutor interface {
	// StartScalarExecution submits a request to remotely evaluate an expression that produces a scalar.
	StartScalarExecution(ctx context.Context, plan *planning.QueryPlan, node planning.Node, timeRange types.QueryTimeRange) (ScalarRemoteExecutionResponse, error)

	// StartInstantVectorExecution submits a request to remotely evaluate an expression that produces an instant vector.
	StartInstantVectorExecution(ctx context.Context, plan *planning.QueryPlan, node planning.Node, timeRange types.QueryTimeRange) (InstantVectorRemoteExecutionResponse, error)

	// StartRangeVectorExecution submits a request to remotely evaluate an expression that produces a range vector.
	StartRangeVectorExecution(ctx context.Context, plan *planning.QueryPlan, node planning.Node, timeRange types.QueryTimeRange) (RangeVectorRemoteExecutionResponse, error)
}

type ScalarRemoteExecutionResponse interface {
	GetValues(ctx context.Context) (types.ScalarData, error)
	Close()
}

type InstantVectorRemoteExecutionResponse interface {
	// TODO: series metadata
	// TODO: series data
	Close()
}

type RangeVectorRemoteExecutionResponse interface {
	// TODO: series metadata
	// TODO: series data
	Close()
}
