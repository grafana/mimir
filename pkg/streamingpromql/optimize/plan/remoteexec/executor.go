// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"

	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type GroupEvaluator interface {
	// CreateScalarExecution creates a request to remotely evaluate an expression that produces a scalar.
	CreateScalarExecution(ctx context.Context, node planning.Node, timeRange types.QueryTimeRange) (ScalarRemoteExecutionResponse, error)

	// CreateInstantVectorExecution creates a request to remotely evaluate an expression that produces an instant vector.
	CreateInstantVectorExecution(ctx context.Context, node planning.Node, timeRange types.QueryTimeRange) (InstantVectorRemoteExecutionResponse, error)

	// CreateRangeVectorExecution creates a request to remotely evaluate an expression that produces a range vector.
	CreateRangeVectorExecution(ctx context.Context, node planning.Node, timeRange types.QueryTimeRange) (RangeVectorRemoteExecutionResponse, error)
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
	// Start triggers evaluation of the request.
	//
	// Start must be called before calling any other method on the response (except Close).
	// Calling another method before calling Start may lead to unpredictable behaviour.
	Start(ctx context.Context) error

	// Finalize finishes evaluation of the request.
	//
	// If there is any unread data for this request, it is discarded.
	//
	// If this is the last request in a group, it returns the annotations and statistics from the remote evaluation, or otherwise returns an empty set of annotations
	// and statistics.
	//
	// Finalize can only be called before Close is called.
	Finalize(ctx context.Context) (*annotations.Annotations, stats.Stats, error)

	// Close cleans up any resources associated with this request.
	//
	// If there is any unread data for this request, it is discarded.
	//
	// If this is the last request in a group, any resources associated with the group itself are also cleaned up.
	//
	// It is safe to call Close multiple times on the same request.
	Close()
}
