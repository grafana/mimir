// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"

	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type RemoteExecutor interface {
	// StartScalarExecution submits a request to remotely evaluate an expression that produces a scalar.
	StartScalarExecution(ctx context.Context, plan *planning.QueryPlan, node planning.Node, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, eagerLoad bool) (ScalarRemoteExecutionResponse, error)

	// StartInstantVectorExecution submits a request to remotely evaluate an expression that produces an instant vector.
	StartInstantVectorExecution(ctx context.Context, plan *planning.QueryPlan, node planning.Node, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, eagerLoad bool) (InstantVectorRemoteExecutionResponse, error)

	// StartRangeVectorExecution submits a request to remotely evaluate an expression that produces a range vector.
	StartRangeVectorExecution(ctx context.Context, plan *planning.QueryPlan, node planning.Node, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, eagerLoad bool) (RangeVectorRemoteExecutionResponse, error)
}

type ScalarRemoteExecutionResponse interface {
	RemoteExecutionResponse

	// GetValues returns the result of evaluating the scalar expression, or the next available error from the stream.
	GetValues(ctx context.Context) (types.ScalarData, error)
}

type InstantVectorRemoteExecutionResponse interface {
	RemoteExecutionResponse

	// GetSeriesMetadata returns the series metadata for this expression, or the next available error from the stream.
	GetSeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error)

	// GetNextSeries returns the data for the next series in the stream, or the next available error from the stream.
	GetNextSeries(ctx context.Context) (types.InstantVectorSeriesData, error)
}

type RangeVectorRemoteExecutionResponse interface {
	RemoteExecutionResponse

	// GetSeriesMetadata returns the series metadata for this expression, or the next available error from the stream.
	GetSeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error)

	// AdvanceToNextSeries advances this response to the next series.
	//
	// Note that GetNextStepSamples may return an error if AdvanceToNextSeries is called before exhausting the data available for the current series.
	AdvanceToNextSeries(ctx context.Context) error

	// GetNextStepSamples returns the data for the next step in the stream for the current series, or the next available error from the stream.
	//
	// If the next available data in the stream is not for the current series, GetNextStepSamples returns an error.
	GetNextStepSamples(ctx context.Context) (*types.RangeVectorStepData, error)
}

type RemoteExecutionResponse interface {
	// GetEvaluationInfo returns the annotations and statistics from the remote evaluation, or the next available error from the stream.
	//
	// If any unread part of the response is not the evaluation info (eg. there is unread series data), this is skipped until the evaluation info or an error is found.
	//
	// GetEvaluationInfo can only be called before Close is called.
	GetEvaluationInfo(ctx context.Context) (*annotations.Annotations, stats.Stats, error)

	// Close cleans up any resources associated with this request.
	//
	// If the request is still inflight, it is cancelled.
	Close()
}
