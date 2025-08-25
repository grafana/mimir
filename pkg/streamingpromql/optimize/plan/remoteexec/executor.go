// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"

	"github.com/prometheus/prometheus/util/annotations"

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
	RemoteExecutionResponse

	GetValues(ctx context.Context) (types.ScalarData, error)
}

type InstantVectorRemoteExecutionResponse interface {
	RemoteExecutionResponse
	// TODO: series metadata
	// TODO: series data
}

type RangeVectorRemoteExecutionResponse interface {
	RemoteExecutionResponse
	// TODO: series metadata
	// TODO: series data
}

type RemoteExecutionResponse interface {
	// GetEvaluationInfo returns the annotations and total number of samples read as part of the remote evaluation.
	//
	// It can only be called once the response has been read, and can only be called before Close is called.
	GetEvaluationInfo(ctx context.Context) (annotations.Annotations, int64, error)

	// Close cleans up any resources associated with this request.
	// If the request is still inflight, it is cancelled.
	Close()
}


