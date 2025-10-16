// SPDX-License-Identifier: AGPL-3.0-only

package v2

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/querierpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/remoteexec"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/pool"
)

var errMultipleEagerLoadingNextCalls = errors.New("multiple pending calls to eagerLoadingResponseStream.Next()")

type RemoteExecutor struct {
	frontend ProtobufFrontend
	cfg      Config
}

var _ remoteexec.RemoteExecutor = &RemoteExecutor{}

type ProtobufFrontend interface {
	DoProtobufRequest(ctx context.Context, req proto.Message, minT, maxT time.Time) (*ProtobufResponseStream, error)
}

func NewRemoteExecutor(frontend ProtobufFrontend, cfg Config) *RemoteExecutor {
	return &RemoteExecutor{frontend: frontend, cfg: cfg}
}

func (r *RemoteExecutor) StartScalarExecution(ctx context.Context, fullPlan *planning.QueryPlan, node planning.Node, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, enablePerStepStats bool, eagerLoad bool) (remoteexec.ScalarRemoteExecutionResponse, error) {
	stream, err := r.startExecution(ctx, fullPlan, node, timeRange, enablePerStepStats, eagerLoad, 0)
	if err != nil {
		return nil, err
	}

	return &scalarExecutionResponse{stream, memoryConsumptionTracker}, nil
}

func (r *RemoteExecutor) StartInstantVectorExecution(ctx context.Context, fullPlan *planning.QueryPlan, node planning.Node, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, enablePerStepStats bool, eagerLoad bool) (remoteexec.InstantVectorRemoteExecutionResponse, error) {
	stream, err := r.startExecution(ctx, fullPlan, node, timeRange, enablePerStepStats, eagerLoad, r.cfg.RemoteExecutionBatchSize)
	if err != nil {
		return nil, err
	}

	return newInstantVectorExecutionResponse(stream, memoryConsumptionTracker), nil
}

func (r *RemoteExecutor) StartRangeVectorExecution(ctx context.Context, fullPlan *planning.QueryPlan, node planning.Node, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, enablePerStepStats bool, eagerLoad bool) (remoteexec.RangeVectorRemoteExecutionResponse, error) {
	stream, err := r.startExecution(ctx, fullPlan, node, timeRange, enablePerStepStats, eagerLoad, 0)
	if err != nil {
		return nil, err
	}

	return newRangeVectorExecutionResponse(stream, memoryConsumptionTracker), nil
}

func (r *RemoteExecutor) startExecution(
	ctx context.Context,
	fullPlan *planning.QueryPlan,
	node planning.Node,
	timeRange types.QueryTimeRange,
	enablePerStepStats bool,
	eagerLoad bool,
	batchSize uint64,
) (responseStream, error) {
	subsetPlan := &planning.QueryPlan{
		TimeRange:          timeRange,
		Root:               node,
		OriginalExpression: fullPlan.OriginalExpression,
		Version:            fullPlan.Version,
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
		EnablePerStepStats: enablePerStepStats,
		BatchSize:          batchSize,
	}

	queriedTimeRange := node.QueriedTimeRange(timeRange, r.cfg.LookBackDelta)
	var stream responseStream
	stream, err = r.frontend.DoProtobufRequest(ctx, req, queriedTimeRange.MinT, queriedTimeRange.MaxT)
	if err != nil {
		return nil, err
	}

	if eagerLoad {
		stream = newEagerLoadingResponseStream(ctx, stream)
	}

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

func (r *scalarExecutionResponse) GetEvaluationInfo(ctx context.Context) (*annotations.Annotations, stats.Stats, error) {
	return readEvaluationCompleted(ctx, r.stream)
}

func (r *scalarExecutionResponse) Close() {
	r.stream.Close()
}

type instantVectorExecutionResponse struct {
	stream                   responseStream
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	currentBatch             []querierpb.InstantVectorSeriesData
}

func newInstantVectorExecutionResponse(stream responseStream, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *instantVectorExecutionResponse {
	return &instantVectorExecutionResponse{
		stream:                   stream,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
}

func (r *instantVectorExecutionResponse) GetSeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	return readSeriesMetadata(ctx, r.stream, r.memoryConsumptionTracker)
}

func (r *instantVectorExecutionResponse) GetNextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if len(r.currentBatch) == 0 {
		resp, err := readNextEvaluateQueryResponse(ctx, r.stream)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		data := resp.GetInstantVectorSeriesData()
		if data == nil {
			return types.InstantVectorSeriesData{}, fmt.Errorf("expected InstantVectorSeriesData, got %T", resp.Message)
		}

		r.currentBatch = data.Series

		for _, series := range r.currentBatch {
			// This isn't as expensive as it looks: FromSamplesToFPoints and FromHistogramsToHPoints directly cast the slices,
			// they don't create new slices.
			if err := accountForFPointMemoryConsumption(mimirpb.FromSamplesToFPoints(series.Floats), r.memoryConsumptionTracker); err != nil {
				return types.InstantVectorSeriesData{}, err
			}

			if err := accountForHPointMemoryConsumption(mimirpb.FromHistogramsToHPoints(series.Histograms), r.memoryConsumptionTracker); err != nil {
				return types.InstantVectorSeriesData{}, err
			}
		}
	}

	series := r.currentBatch[0]
	r.currentBatch = r.currentBatch[1:]

	mqeData := types.InstantVectorSeriesData{
		Floats:     mimirpb.FromSamplesToFPoints(series.Floats),
		Histograms: mimirpb.FromHistogramsToHPoints(series.Histograms),
	}

	return mqeData, nil
}

func (r *instantVectorExecutionResponse) GetEvaluationInfo(ctx context.Context) (*annotations.Annotations, stats.Stats, error) {
	return readEvaluationCompleted(ctx, r.stream)
}

func (r *instantVectorExecutionResponse) Close() {
	r.stream.Close()
	r.currentBatch = nil
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

func (r *rangeVectorExecutionResponse) GetEvaluationInfo(ctx context.Context) (*annotations.Annotations, stats.Stats, error) {
	return readEvaluationCompleted(ctx, r.stream)
}

func (r *rangeVectorExecutionResponse) Close() {
	r.floats.Close()
	r.histograms.Close()
	r.stream.Close()
}

var errUnexpectedEndOfStream = errors.New("expected EvaluateQueryResponse, got end of stream")

func readNextEvaluateQueryResponse(ctx context.Context, stream responseStream) (*querierpb.EvaluateQueryResponse, error) {
	msg, err := stream.Next(ctx)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, errUnexpectedEndOfStream
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
		m := types.SeriesMetadata{
			Labels:   mimirpb.FromLabelAdaptersToLabels(s.Labels),
			DropName: s.DropName,
		}
		mqeSeries, err = types.AppendSeriesMetadata(memoryConsumptionTracker, mqeSeries, m)
		if err != nil {
			return nil, err
		}
	}

	return mqeSeries, nil
}

func readEvaluationCompleted(ctx context.Context, stream responseStream) (*annotations.Annotations, stats.Stats, error) {
	// Keep reading the stream until we get to an evaluation completed message.
	for {
		resp, err := readNextEvaluateQueryResponse(ctx, stream)
		if err != nil {
			return nil, stats.Stats{}, err
		}

		completion := resp.GetEvaluationCompleted()
		if completion == nil {
			continue // Try the next message.
		}

		annos, stats := decodeEvaluationCompletedMessage(completion)
		return annos, stats, nil
	}
}

func decodeEvaluationCompletedMessage(msg *querierpb.EvaluateQueryResponseEvaluationCompleted) (*annotations.Annotations, stats.Stats) {
	count := len(msg.Annotations.Infos) + len(msg.Annotations.Warnings)

	annos := make(annotations.Annotations, count)
	for _, a := range msg.Annotations.Infos {
		annos.Add(newRemoteInfo(a))
	}

	for _, a := range msg.Annotations.Warnings {
		annos.Add(newRemoteWarning(a))
	}

	return &annos, msg.Stats
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

type eagerLoadingResponseStream struct {
	inner       responseStream
	bufferedErr error

	mtx              *sync.Mutex
	buffer           *responseStreamBuffer
	newDataAvailable chan struct{}
}

func newEagerLoadingResponseStream(ctx context.Context, inner responseStream) *eagerLoadingResponseStream {
	stream := &eagerLoadingResponseStream{
		inner:  inner,
		mtx:    &sync.Mutex{},
		buffer: &responseStreamBuffer{},
	}

	go stream.startBuffering(ctx)

	return stream
}

func (e *eagerLoadingResponseStream) startBuffering(ctx context.Context) {
	for {
		if !e.bufferOne(ctx) {
			return
		}
	}
}

func (e *eagerLoadingResponseStream) bufferOne(ctx context.Context) bool {
	msg, err := e.inner.Next(ctx)

	e.mtx.Lock()
	defer e.mtx.Unlock()
	defer e.notifyNewDataAvailable()

	e.buffer.Push(bufferedMessage{msg, err})

	return err == nil
}

// notifyNewDataAvailable unblocks a pending Next() call.
// It must only be called with e.mtx locked.
func (e *eagerLoadingResponseStream) notifyNewDataAvailable() {
	if e.newDataAvailable == nil {
		return
	}

	close(e.newDataAvailable)
	e.newDataAvailable = nil
}

func (e *eagerLoadingResponseStream) Next(ctx context.Context) (*frontendv2pb.QueryResultStreamRequest, error) {
	if e.bufferedErr != nil {
		return nil, e.bufferedErr
	}

	e.mtx.Lock()

	if e.newDataAvailable != nil {
		e.mtx.Unlock()
		return nil, errMultipleEagerLoadingNextCalls
	}

	if e.buffer.Any() {
		msg := e.buffer.Pop()
		defer e.mtx.Unlock()

		if msg.err != nil {
			e.bufferedErr = msg.err
		}

		return msg.payload, msg.err
	}

	newDataAvailable := make(chan struct{}) // We can't store this directly in e.newDataAvailable because e.newDataAvailable might be cleared by the time we wait on it below.
	e.newDataAvailable = newDataAvailable
	e.mtx.Unlock()

	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case <-newDataAvailable:
		e.mtx.Lock()
		defer e.mtx.Unlock()
		msg := e.buffer.Pop()

		if msg.err != nil {
			e.bufferedErr = msg.err
		}

		return msg.payload, msg.err
	}
}

func (e *eagerLoadingResponseStream) Close() {
	e.inner.Close() // This is expected to release any pending Next() call in startBuffering(), so we don't need to do anything to terminate it here.
}

type responseStreamBuffer struct {
	msgs       []bufferedMessage
	startIndex int
	length     int
}

type bufferedMessage struct {
	payload *frontendv2pb.QueryResultStreamRequest
	err     error
}

func (b *responseStreamBuffer) Push(msg bufferedMessage) {
	if b.length == cap(b.msgs) {
		newCap := max(len(b.msgs)*2, 1)
		newMsgs := responseMessageSlicePool.Get(newCap)
		newMsgs = newMsgs[:newCap]
		headSize := cap(b.msgs) - b.startIndex
		copy(newMsgs[0:headSize], b.msgs[b.startIndex:])
		copy(newMsgs[headSize:newCap], b.msgs[0:b.startIndex])

		clear(b.msgs)
		responseMessageSlicePool.Put(b.msgs)
		b.msgs = newMsgs
		b.startIndex = 0
	}

	newIndex := (b.startIndex + b.length) % len(b.msgs)
	b.msgs[newIndex] = msg
	b.length++
}

func (b *responseStreamBuffer) Pop() bufferedMessage {
	msg := b.msgs[b.startIndex]
	b.length--

	if b.length == 0 {
		b.startIndex = 0
	} else {
		b.startIndex = (b.startIndex + 1) % len(b.msgs)
	}

	return msg
}

func (b *responseStreamBuffer) Any() bool {
	return b.length > 0
}

// Why types.MaxExpectedSeriesPerResult as the slice size limit?
// The vast majority of these slices will be used for instant vector results, which
// will have 2 + num_series messages in the worst case. So we use our estimate of the
// maximum number of series per result to limit the size of slices in the pool.
var responseMessageSlicePool = pool.NewBucketedPool(types.MaxExpectedSeriesPerResult, func(size int) []bufferedMessage {
	return make([]bufferedMessage, 0, size)
})
