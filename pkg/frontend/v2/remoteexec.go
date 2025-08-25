// SPDX-License-Identifier: AGPL-3.0-only

package v2

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/util/annotations"

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
	logger, ctx := spanlogger.New(ctx, r.frontend.log, tracer, "RemoteExecution.Scalar") // This span will either be closed below or in scalarExecutionResponse.Close().

	stream, err := r.startExecution(ctx, fullPlan, node, timeRange)
	if err != nil {
		logger.Finish()
		return nil, err
	}

	return &scalarExecutionResponse{stream, logger}, nil
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
	stream     *ProtobufResponseStream
	spanLogger *spanlogger.SpanLogger
}

func (r *scalarExecutionResponse) GetValues(ctx context.Context) (types.ScalarData, error) {
	msg, err := r.stream.Next()
	if err != nil {
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

	return types.ScalarData{
		Samples: mimirpb.FromSamplesToFPoints(scalar.Values),
	}, nil
}

func (r *scalarExecutionResponse) GetEvaluationInfo(ctx context.Context) (annotations.Annotations, int64, error) {
	return readEvaluationCompleted(r.stream)
}

func (r *scalarExecutionResponse) Close() {
	r.stream.Close()
	r.spanLogger.Finish()
}

func readEvaluationCompleted(stream *ProtobufResponseStream) (annotations.Annotations, int64, error) {
	msg, err := stream.Next()
	if err != nil {
		return nil, 0, err
	}

	resp := msg.GetEvaluateQueryResponse()
	if resp == nil {
		return nil, 0, fmt.Errorf("expected EvaluateQueryResponse, got %T", msg.Data)
	}

	completion := resp.GetEvaluationCompleted()
	if completion == nil {
		return nil, 0, fmt.Errorf("expected EvaluationCompleted, got %T", resp.Message)
	}

	annos, totalSamples := decodeEvaluationCompletedMessage(completion)
	return annos, totalSamples, nil
}

func decodeEvaluationCompletedMessage(msg *querierpb.EvaluateQueryResponseEvaluationCompleted) (annotations.Annotations, int64) {
	count := len(msg.Annotations.Infos) + len(msg.Annotations.Warnings)

	annos := make(annotations.Annotations, count)
	for _, a := range msg.Annotations.Infos {
		annos.Add(newRemoteInfo(a))
	}

	for _, a := range msg.Annotations.Warnings {
		annos.Add(newRemoteWarning(a))
	}

	return annos, msg.Stats.TotalSamples
}

// Prometheus' annotations.Annotations type stores Golang error types, and checks if they
// are annotations.PromQLInfo or annotations.PromQLWarning, so this type allows us to
// create errors with arbitrary strings received from remote executors that satisfies
// this requirement.
type remoteAnnotation struct {
	msg   string
	inner error
}

func newRemoteWarning(msg string) error {
	return &remoteAnnotation{msg: msg, inner: annotations.PromQLWarning}
}

func newRemoteInfo(msg string) error {
	return &remoteAnnotation{msg: msg, inner: annotations.PromQLInfo}
}

func (r remoteAnnotation) Error() string {
	return r.msg
}

func (r remoteAnnotation) Unwrap() error {
	return r.inner
}
