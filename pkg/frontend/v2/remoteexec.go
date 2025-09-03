// SPDX-License-Identifier: AGPL-3.0-only

package v2

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/querierpb"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/remoteexec"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type RemoteExecutor struct {
	frontend *Frontend
}

var _ remoteexec.RemoteExecutor = &RemoteExecutor{}

func NewRemoteExecutor(frontend *Frontend) *RemoteExecutor {
	return &RemoteExecutor{frontend: frontend}
}

func (r *RemoteExecutor) StartScalarExecution(ctx context.Context, fullPlan *planning.QueryPlan, node planning.Node, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (remoteexec.ScalarRemoteExecutionResponse, error) {
	stream, err := r.startExecution(ctx, fullPlan, node, timeRange)
	if err != nil {
		return nil, err
	}

	return &scalarExecutionResponse{stream, memoryConsumptionTracker}, nil
}

func (r *RemoteExecutor) StartInstantVectorExecution(ctx context.Context, fullPlan *planning.QueryPlan, node planning.Node, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (remoteexec.InstantVectorRemoteExecutionResponse, error) {
	stream, err := r.startExecution(ctx, fullPlan, node, timeRange)
	if err != nil {
		return nil, err
	}

	return &instantVectorExecutionResponse{stream, memoryConsumptionTracker}, nil
}

func (r *RemoteExecutor) StartRangeVectorExecution(ctx context.Context, fullPlan *planning.QueryPlan, node planning.Node, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (remoteexec.RangeVectorRemoteExecutionResponse, error) {
	stream, err := r.startExecution(ctx, fullPlan, node, timeRange)
	if err != nil {
		return nil, err
	}

	return newRangeVectorExecutionResponse(stream, memoryConsumptionTracker), nil
}

func (r *RemoteExecutor) startExecution(ctx context.Context, fullPlan *planning.QueryPlan, node planning.Node, timeRange types.QueryTimeRange) (responseStream, error) {
	subsetPlan := &planning.QueryPlan{
		TimeRange:          timeRange,
		Root:               node,
		OriginalExpression: fullPlan.OriginalExpression,
	}

	// FIXME: need to capture the affected query components (ingester, store-gateway or both) here by calling QueriedTimeRange

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

	// FIXME: when we support query sharding in MQE, we'll need to start buffering the response stream so we avoid the deadlocking issue
	return stream, nil
}

type responseStream interface {
	Next(ctx context.Context) (*frontendv2pb.QueryResultStreamRequest, error)
	Close()
}

type scalarExecutionResponse struct {
	stream                   responseStream
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

func (r *scalarExecutionResponse) GetValues(ctx context.Context) (types.ScalarData, error) {
	resp, err := readNextEvaluateQueryResponse(ctx, r.stream)
	if err != nil {
		return types.ScalarData{}, err
	}

	scalar := resp.GetScalarValue()
	if scalar == nil {
		return types.ScalarData{}, fmt.Errorf("expected ScalarValue, got %T", resp.Message)
	}

	v := types.ScalarData{
		Samples: mimirpb.FromSamplesToFPoints(scalar.Values),
	}

	if err := accountForFPointMemoryConsumption(v.Samples, r.memoryConsumptionTracker); err != nil {
		return types.ScalarData{}, err
	}

	return v, nil
}

func (r *scalarExecutionResponse) GetEvaluationInfo(ctx context.Context) (annotations.Annotations, int64, error) {
	return readEvaluationCompleted(ctx, r.stream)
}

func (r *scalarExecutionResponse) Close() {
	r.stream.Close()
}

type instantVectorExecutionResponse struct {
	stream                   responseStream
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

func (r *instantVectorExecutionResponse) GetSeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	return readSeriesMetadata(ctx, r.stream, r.memoryConsumptionTracker)
}

func (r *instantVectorExecutionResponse) GetNextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	resp, err := readNextEvaluateQueryResponse(ctx, r.stream)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	seriesData := resp.GetInstantVectorSeriesData()
	if seriesData == nil {
		return types.InstantVectorSeriesData{}, fmt.Errorf("expected InstantVectorSeriesData, got %T", resp.Message)
	}

	mqeData := types.InstantVectorSeriesData{
		Floats:     mimirpb.FromSamplesToFPoints(seriesData.Floats),
		Histograms: mimirpb.FromHistogramsToHPoints(seriesData.Histograms),
	}

	if err := accountForFPointMemoryConsumption(mqeData.Floats, r.memoryConsumptionTracker); err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if err := accountForHPointMemoryConsumption(mqeData.Histograms, r.memoryConsumptionTracker); err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	return mqeData, nil
}

func (r *instantVectorExecutionResponse) GetEvaluationInfo(ctx context.Context) (annotations.Annotations, int64, error) {
	return readEvaluationCompleted(ctx, r.stream)
}

func (r *instantVectorExecutionResponse) Close() {
	r.stream.Close()
}

type rangeVectorExecutionResponse struct {
	stream                   responseStream
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	currentSeriesIndex       int64
	floats                   *types.FPointRingBuffer
	histograms               *types.HPointRingBuffer
	stepData                 *types.RangeVectorStepData
}

func newRangeVectorExecutionResponse(stream responseStream, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *rangeVectorExecutionResponse {
	return &rangeVectorExecutionResponse{
		stream:                   stream,
		memoryConsumptionTracker: memoryConsumptionTracker,
		currentSeriesIndex:       -1,
		floats:                   types.NewFPointRingBuffer(memoryConsumptionTracker),
		histograms:               types.NewHPointRingBuffer(memoryConsumptionTracker),
		stepData:                 &types.RangeVectorStepData{},
	}
}

func (r *rangeVectorExecutionResponse) GetSeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	return readSeriesMetadata(ctx, r.stream, r.memoryConsumptionTracker)
}

func (r *rangeVectorExecutionResponse) AdvanceToNextSeries(ctx context.Context) error {
	r.currentSeriesIndex++
	return nil
}

func (r *rangeVectorExecutionResponse) GetNextStepSamples(ctx context.Context) (*types.RangeVectorStepData, error) {
	r.floats.Release()
	r.histograms.Release()

	resp, err := readNextEvaluateQueryResponse(ctx, r.stream)
	if err != nil {
		return nil, err
	}

	data := resp.GetRangeVectorStepData()
	if data == nil {
		return nil, fmt.Errorf("expected RangeVectorStepData, got %T", resp.Message)
	}

	if data.SeriesIndex != r.currentSeriesIndex {
		return nil, fmt.Errorf("expected data for series index %v, but got data for series index %v", r.currentSeriesIndex, data.SeriesIndex)
	}

	fPoints := mimirpb.FromSamplesToFPoints(data.Floats)
	hPoints := mimirpb.FromHistogramsToHPoints(data.Histograms)

	if err := accountForFPointMemoryConsumption(fPoints, r.memoryConsumptionTracker); err != nil {
		return nil, err
	}

	if err := accountForHPointMemoryConsumption(hPoints, r.memoryConsumptionTracker); err != nil {
		return nil, err
	}

	if err := r.floats.Use(fPoints); err != nil {
		return nil, err
	}

	if err := r.histograms.Use(hPoints); err != nil {
		return nil, err
	}

	r.stepData.StepT = data.StepT
	r.stepData.RangeStart = data.RangeStart
	r.stepData.RangeEnd = data.RangeEnd
	r.stepData.Floats = r.floats.ViewUntilSearchingBackwards(data.RangeEnd, r.stepData.Floats)
	r.stepData.Histograms = r.histograms.ViewUntilSearchingBackwards(data.RangeEnd, r.stepData.Histograms)

	return r.stepData, nil
}

func (r *rangeVectorExecutionResponse) GetEvaluationInfo(ctx context.Context) (annotations.Annotations, int64, error) {
	return readEvaluationCompleted(ctx, r.stream)
}

func (r *rangeVectorExecutionResponse) Close() {
	r.floats.Close()
	r.histograms.Close()
	r.stream.Close()
}

func readNextEvaluateQueryResponse(ctx context.Context, stream responseStream) (*querierpb.EvaluateQueryResponse, error) {
	msg, err := stream.Next(ctx)
	if err != nil {
		return nil, err
	}

	resp := msg.GetEvaluateQueryResponse()
	if resp == nil {
		return nil, fmt.Errorf("expected EvaluateQueryResponse, got %T", msg.Data)
	}

	return resp, nil
}

func readSeriesMetadata(ctx context.Context, stream responseStream, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) ([]types.SeriesMetadata, error) {
	resp, err := readNextEvaluateQueryResponse(ctx, stream)
	if err != nil {
		return nil, err
	}

	seriesMetadata := resp.GetSeriesMetadata()
	if seriesMetadata == nil {
		return nil, fmt.Errorf("expected SeriesMetadata, got %T", resp.Message)
	}

	mqeSeries, err := types.SeriesMetadataSlicePool.Get(len(seriesMetadata.Series), memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	for _, s := range seriesMetadata.Series {
		mqeSeries, err = types.AppendSeriesMetadata(memoryConsumptionTracker, mqeSeries, types.SeriesMetadata{Labels: mimirpb.FromLabelAdaptersToLabels(s.Labels)})
		if err != nil {
			return nil, err
		}
	}

	return mqeSeries, nil
}

func readEvaluationCompleted(ctx context.Context, stream responseStream) (annotations.Annotations, int64, error) {
	// Keep reading the stream until we get to an evaluation completed message.
	for {
		resp, err := readNextEvaluateQueryResponse(ctx, stream)
		if err != nil {
			return nil, 0, err
		}

		completion := resp.GetEvaluationCompleted()
		if completion == nil {
			continue // Try the next message.
		}

		annos, totalSamples := decodeEvaluationCompletedMessage(completion)
		return annos, totalSamples, nil
	}
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

// Prometheus' annotations.Annotations type stores Golang error types and checks if they
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

func accountForFPointMemoryConsumption(d []promql.FPoint, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) error {
	if err := memoryConsumptionTracker.IncreaseMemoryConsumption(uint64(cap(d))*types.FPointSize, limiter.FPointSlices); err != nil {
		return err
	}

	return nil
}

func accountForHPointMemoryConsumption(d []promql.HPoint, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) error {
	if err := memoryConsumptionTracker.IncreaseMemoryConsumption(uint64(cap(d))*types.HPointSize, limiter.HPointSlices); err != nil {
		return err
	}

	return nil
}
