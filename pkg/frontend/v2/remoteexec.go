// SPDX-License-Identifier: AGPL-3.0-only

package v2

import (
	"context"
	"fmt"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/querierpb"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/remoteexec"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type RemoteExecutor struct {
	frontend *Frontend
}

var _ remoteexec.RemoteExecutor = &RemoteExecutor{}

func NewRemoteExecutor(frontend *Frontend) *RemoteExecutor {
	return &RemoteExecutor{frontend: frontend}
}

func (r *RemoteExecutor) StartScalarExecution(ctx context.Context, fullPlan *planning.QueryPlan, node planning.Node, timeRange types.QueryTimeRange) (remoteexec.ScalarRemoteExecutionResponse, error) {
	logger, ctx := spanlogger.New(ctx, r.frontend.log, tracer, "RemoteExecution.Scalar")
	defer logger.Finish() // TODO: defer this until response is closed or error is returned

	stream, err := r.startExecution(ctx, fullPlan, node, timeRange)
	if err != nil {
		return nil, err
	}

	return &scalarExecutionResponse{stream}, nil
}

func (r *RemoteExecutor) StartInstantVectorExecution(ctx context.Context, fullPlan *planning.QueryPlan, node planning.Node, timeRange types.QueryTimeRange) (remoteexec.InstantVectorRemoteExecutionResponse, error) {
	logger, ctx := spanlogger.New(ctx, r.frontend.log, tracer, "RemoteExecution.InstantVector")
	defer logger.Finish() // TODO: defer this until response is closed or error is returned

	//TODO implement me
	panic("implement me")
}

func (r *RemoteExecutor) StartRangeVectorExecution(ctx context.Context, fullPlan *planning.QueryPlan, node planning.Node, timeRange types.QueryTimeRange) (remoteexec.RangeVectorRemoteExecutionResponse, error) {
	logger, ctx := spanlogger.New(ctx, r.frontend.log, tracer, "RemoteExecution.RangeVector")
	defer logger.Finish() // TODO: defer this until response is closed or error is returned

	//TODO implement me
	panic("implement me")
}

func (r *RemoteExecutor) startExecution(ctx context.Context, fullPlan *planning.QueryPlan, node planning.Node, timeRange types.QueryTimeRange) (*ProtobufResponseStream, error) {
	subsetPlan := &planning.QueryPlan{
		TimeRange:          timeRange,
		Root:               node,
		OriginalExpression: fullPlan.OriginalExpression,
	}

	encodedPlan, err := subsetPlan.ToEncodedPlan(false, true)
	if err != nil {
		return nil, err
	}

	req := &querierpb.EvaluateQueryRequest{
		Plan: *encodedPlan,
		Nodes: []querierpb.EvaluationNode{
			{NodeIndex: encodedPlan.RootNode, TimeRange: encodedPlan.TimeRange},
		},
	}

	stream, err := r.frontend.DoProtobufRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	// TODO: start buffering response stream
	return stream, nil
}

type scalarExecutionResponse struct {
	stream *ProtobufResponseStream
}

func (r *scalarExecutionResponse) GetValues(ctx context.Context) (types.ScalarData, error) {
	msg, err := r.stream.Next()
	if err != nil {
		return types.ScalarData{}, err
	}

	if err := errorFromMessage(msg); err != nil {
		return types.ScalarData{}, err
	}

	resp := msg.GetEvaluateQueryResponse()
	if resp == nil {
		return types.ScalarData{}, fmt.Errorf("expected EvaluateQueryResponse, got %T", msg.Data)
	}

	scalar := resp.GetScalarValue()
	if scalar == nil {
		return types.ScalarData{}, fmt.Errorf("expected ScalarValue, got %T", resp.Message)
	}

	// TODO: add to memory consumption estimate
	return types.ScalarData{
		Samples: mimirpb.FromSamplesToFPoints(scalar.Values),
	}, nil
}

func (r *scalarExecutionResponse) Close() {
	// TODO: cancel context started to ensure stream is cleaned up
}

func errorFromMessage(msg *frontendv2pb.QueryResultStreamRequest) error {
	e := msg.GetError()

	if e == nil {
		return nil
	}

	errorType, err := e.Type.ToPrometheusString()
	if err != nil {
		return err
	}

	return apierror.New(apierror.Type(errorType), e.Message)
}
