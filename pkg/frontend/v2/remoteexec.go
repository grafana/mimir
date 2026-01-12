// SPDX-License-Identifier: AGPL-3.0-only

package v2

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/bits"
	"slices"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/querierpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/remoteexec"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/pool"
)

var errMultipleEagerLoadingNextCalls = errors.New("multiple pending calls to eagerLoadingResponseStream.Next()")
var errRequestAlreadySent = errors.New("can't call enqueueEvaluation() after the request was already sent")
var errRequestNotSent = errors.New("can't call getNextMessageForStream() before the request was sent")
var errUnexpectedEndOfStream = errors.New("expected EvaluateQueryResponse, got end of stream")

var _ remoteexec.GroupEvaluator = &RemoteExecutionGroupEvaluator{}

func RegisterRemoteExecutionMaterializers(engine *streamingpromql.Engine, frontend ProtobufFrontend, cfg Config) error {
	groupEvaluatorFactory := func(eagerLoad bool, queryParameters *planning.QueryParameters, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) remoteexec.GroupEvaluator {
		return NewRemoteExecutionGroupEvaluator(frontend, cfg, eagerLoad, queryParameters, memoryConsumptionTracker)
	}

	if err := engine.RegisterNodeMaterializer(planning.NODE_TYPE_REMOTE_EXEC_GROUP, remoteexec.NewRemoteExecutionGroupMaterializer(groupEvaluatorFactory)); err != nil {
		return fmt.Errorf("unable to register remote execution group materializer: %w", err)
	}

	if err := engine.RegisterNodeMaterializer(planning.NODE_TYPE_REMOTE_EXEC_CONSUMER, remoteexec.NewRemoteExecutionConsumerMaterializer()); err != nil {
		return fmt.Errorf("unable to register remote execution consumer materializer: %w", err)
	}

	return nil
}

type ProtobufFrontend interface {
	DoProtobufRequest(ctx context.Context, req proto.Message, minT, maxT time.Time) (ResponseStream, error)
}

// A RemoteExecutionGroupEvaluator is responsible for evaluating a group of one or more query plan nodes.
//
// It receives the nodes to evaluate, prepares the request for the frontend to send to queriers, and buffers
// any received messages so they may be read by the corresponding remote execution operators in any order.
type RemoteExecutionGroupEvaluator struct {
	frontend                 ProtobufFrontend
	cfg                      Config
	eagerLoad                bool
	queryParameters          *planning.QueryParameters
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker

	stream                 ResponseStream
	nodeStreamState        []*remoteExecutionNodeStreamState
	requestSent            bool
	nodeIndexToStreamState map[int64]*remoteExecutionNodeStreamState // Only populated once request is sent.
}

type remoteExecutionNodeStreamState struct {
	node      planning.Node
	timeRange types.QueryTimeRange
	finished  bool
	buffer    *responseStreamBuffer

	// The following field is only populated once the request is sent:
	nodeIndex int64
}

// remoteExecutionNodeStreamIndex represents the index of a node stream in a group (ie. index into RemoteExecutionGroupEvaluator.nodeStreamState)
// We use a different type to make it harder to accidentally confuse it with a node index in the query plan.
type remoteExecutionNodeStreamIndex int

func NewRemoteExecutionGroupEvaluator(frontend ProtobufFrontend, cfg Config, eagerLoad bool, queryParameters *planning.QueryParameters, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *RemoteExecutionGroupEvaluator {
	return &RemoteExecutionGroupEvaluator{
		frontend:                 frontend,
		cfg:                      cfg,
		eagerLoad:                eagerLoad,
		queryParameters:          queryParameters,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
}

func (g *RemoteExecutionGroupEvaluator) enqueueEvaluation(node planning.Node, timeRange types.QueryTimeRange) (remoteExecutionNodeStreamIndex, error) {
	if g.requestSent {
		return -1, errRequestAlreadySent
	}

	g.nodeStreamState = append(g.nodeStreamState, &remoteExecutionNodeStreamState{
		node:      node,
		timeRange: timeRange,
		buffer:    &responseStreamBuffer{},
	})

	return remoteExecutionNodeStreamIndex(len(g.nodeStreamState) - 1), nil
}

func (g *RemoteExecutionGroupEvaluator) CreateScalarExecution(ctx context.Context, node planning.Node, timeRange types.QueryTimeRange) (remoteexec.ScalarRemoteExecutionResponse, error) {
	streamIdx, err := g.enqueueEvaluation(node, timeRange)
	if err != nil {
		return nil, err
	}

	return newScalarExecutionResponse(g, streamIdx, g.memoryConsumptionTracker), nil
}

func (g *RemoteExecutionGroupEvaluator) CreateInstantVectorExecution(ctx context.Context, node planning.Node, timeRange types.QueryTimeRange) (remoteexec.InstantVectorRemoteExecutionResponse, error) {
	streamIdx, err := g.enqueueEvaluation(node, timeRange)
	if err != nil {
		return nil, err
	}

	return newInstantVectorExecutionResponse(g, streamIdx, g.memoryConsumptionTracker), nil
}

func (g *RemoteExecutionGroupEvaluator) CreateRangeVectorExecution(ctx context.Context, node planning.Node, timeRange types.QueryTimeRange) (remoteexec.RangeVectorRemoteExecutionResponse, error) {
	streamIdx, err := g.enqueueEvaluation(node, timeRange)
	if err != nil {
		return nil, err
	}

	return newRangeVectorExecutionResponse(g, streamIdx, g.memoryConsumptionTracker), nil
}

func (g *RemoteExecutionGroupEvaluator) sendRequest(ctx context.Context) error {
	if g.requestSent {
		return nil
	}

	g.requestSent = true

	nodes := make([]planning.Node, 0, len(g.nodeStreamState))
	version := planning.QueryPlanVersionZero
	for _, state := range g.nodeStreamState {
		nodes = append(nodes, state.node)
		version = max(version, planning.MinimumRequiredPlanVersion(state.node))
	}

	subsetPlan := &planning.QueryPlan{
		Parameters: g.queryParameters,
		Version:    version,
	}

	encodedPlan, nodeIndices, err := subsetPlan.ToEncodedPlan(false, true, nodes...)
	if err != nil {
		return err
	}

	req := &querierpb.EvaluateQueryRequest{
		Plan:      *encodedPlan,
		Nodes:     make([]querierpb.EvaluationNode, 0, len(nodeIndices)),
		BatchSize: g.cfg.RemoteExecutionBatchSize,
	}

	overallQueriedTimeRange := planning.NoDataQueried()
	g.nodeIndexToStreamState = make(map[int64]*remoteExecutionNodeStreamState, len(g.nodeStreamState))

	for streamIdx, nodeIdx := range nodeIndices {
		state := g.nodeStreamState[streamIdx]
		state.nodeIndex = nodeIdx
		g.nodeIndexToStreamState[nodeIdx] = state
		timeRange := state.timeRange

		queriedTimeRange, err := state.node.QueriedTimeRange(timeRange, g.cfg.LookBackDelta)
		if err != nil {
			return err
		}

		overallQueriedTimeRange = overallQueriedTimeRange.Union(queriedTimeRange)

		req.Nodes = append(req.Nodes, querierpb.EvaluationNode{NodeIndex: nodeIdx, TimeRange: planning.ToEncodedTimeRange(timeRange)})
	}

	stats := stats.FromContext(ctx)
	stats.AddRemoteExecutionRequests(1)

	g.stream, err = g.frontend.DoProtobufRequest(ctx, req, overallQueriedTimeRange.MinT, overallQueriedTimeRange.MaxT)
	if err != nil {
		return err
	}

	if g.eagerLoad {
		g.stream = newEagerLoadingResponseStream(ctx, g.stream)
	}

	return nil
}

type releaseMessageFunc func()

func (g *RemoteExecutionGroupEvaluator) getNextMessageForStream(ctx context.Context, nodeStreamIndex remoteExecutionNodeStreamIndex) (*querierpb.EvaluateQueryResponse, releaseMessageFunc, error) {
	if !g.requestSent {
		return nil, nil, errRequestNotSent
	}

	nodeStreamState := g.nodeStreamState[nodeStreamIndex]
	if nodeStreamState.finished {
		return nil, nil, fmt.Errorf("can't read next message for node stream at index %v, as it is already finished", nodeStreamState.nodeIndex)
	}

	if nodeStreamState.buffer.Any() {
		b := nodeStreamState.buffer.Pop()
		if b.err != nil {
			return nil, nil, b.err
		}

		resp := b.payload.GetEvaluateQueryResponse()
		if resp == nil {
			return nil, nil, fmt.Errorf("expected EvaluateQueryResponse, got %T", b.payload.Data)
		}

		return resp, b.payload.FreeBuffer, nil
	}

	for {
		payload, err := g.readNextMessage(ctx)
		if err != nil {
			return nil, nil, err
		}

		// We don't need to check for Error messages below, these are translated to Go errors in ProtobufResponseStream.
		resp := payload.GetEvaluateQueryResponse()
		if resp == nil {
			return nil, nil, fmt.Errorf("expected EvaluateQueryResponse, got %T", payload.Data)
		}

		respNodeState, err := g.getNodeStreamState(resp)
		if err != nil {
			return nil, nil, err
		}

		if respNodeState == nodeStreamState {
			return resp, payload.FreeBuffer, nil
		}

		if !respNodeState.finished {
			respNodeState.buffer.Push(bufferedMessage{payload: payload})
		}
	}
}

func (g *RemoteExecutionGroupEvaluator) getNodeStreamState(resp *querierpb.EvaluateQueryResponse) (*remoteExecutionNodeStreamState, error) {
	var idx int64

	switch msg := resp.Message.(type) {
	case *querierpb.EvaluateQueryResponse_SeriesMetadata:
		idx = msg.SeriesMetadata.NodeIndex
	case *querierpb.EvaluateQueryResponse_StringValue:
		idx = msg.StringValue.NodeIndex
	case *querierpb.EvaluateQueryResponse_ScalarValue:
		idx = msg.ScalarValue.NodeIndex
	case *querierpb.EvaluateQueryResponse_InstantVectorSeriesData:
		idx = msg.InstantVectorSeriesData.NodeIndex
	case *querierpb.EvaluateQueryResponse_RangeVectorStepData:
		idx = msg.RangeVectorStepData.NodeIndex
	default:
		return nil, fmt.Errorf("getNodeStreamState: unexpected message type %T, this is a bug", msg)
	}

	if nodeState, ok := g.nodeIndexToStreamState[idx]; ok {
		return nodeState, nil
	}

	return nil, fmt.Errorf("received message of type %T for node with index %v, expected nodes with indices %v", resp.Message, idx, slices.Collect(maps.Keys(g.nodeIndexToStreamState)))
}

func (g *RemoteExecutionGroupEvaluator) readNextMessage(ctx context.Context) (*frontendv2pb.QueryResultStreamRequest, error) {
	msg, err := g.stream.Next(ctx)
	if err != nil {
		return nil, err
	}

	if msg == nil {
		return nil, errUnexpectedEndOfStream
	}

	return msg, nil
}

func (g *RemoteExecutionGroupEvaluator) getEvaluationCompletedForStream(ctx context.Context, nodeStreamIndex remoteExecutionNodeStreamIndex) (*annotations.Annotations, stats.Stats, error) {
	nodeState := g.nodeStreamState[nodeStreamIndex]
	if nodeState.finished {
		return nil, stats.Stats{}, fmt.Errorf("can't read evaluation completed message for node stream index %v, as it is already finished", nodeState.nodeIndex)
	}

	g.markStreamAsFinished(nodeStreamIndex)
	if !g.allNodesFinished() {
		// We'll return the actual evaluation information when the last node calls GetEvaluationInfo().
		return nil, stats.Stats{}, nil
	}

	defer g.onAllStreamsFinished()

	// Keep reading the stream until we get to an evaluation completed message, or reach the end of the stream.
	// If we reach the end of the stream, tryToReadEvaluationCompleted will return an error (because readNextMessage will return an error),
	// so we don't need to do anything special here to handle that case.
	for {
		ok, annos, stats, err := g.tryToReadEvaluationCompleted(ctx)
		if err != nil {
			return nil, stats, err
		}
		if ok {
			return annos, stats, nil
		}
	}
}

func (g *RemoteExecutionGroupEvaluator) tryToReadEvaluationCompleted(ctx context.Context) (bool, *annotations.Annotations, stats.Stats, error) {
	msg, err := g.readNextMessage(ctx)
	if err != nil {
		return false, nil, stats.Stats{}, err
	}
	defer msg.FreeBuffer()

	resp := msg.GetEvaluateQueryResponse()
	if resp == nil {
		return false, nil, stats.Stats{}, fmt.Errorf("expected EvaluateQueryResponse, got %T", msg.Data)
	}

	completion := resp.GetEvaluationCompleted()
	if completion == nil {
		return false, nil, stats.Stats{}, nil
	}

	annos, stats := decodeEvaluationCompletedMessage(completion)
	return true, annos, stats, nil
}

func (g *RemoteExecutionGroupEvaluator) allNodesFinished() bool {
	for _, state := range g.nodeStreamState {
		if !state.finished {
			return false
		}
	}

	return true
}

func (g *RemoteExecutionGroupEvaluator) closeStream(nodeStreamIndex remoteExecutionNodeStreamIndex) {
	g.markStreamAsFinished(nodeStreamIndex)

	if g.allNodesFinished() {
		g.onAllStreamsFinished()
	}
}

func (g *RemoteExecutionGroupEvaluator) markStreamAsFinished(nodeStreamIndex remoteExecutionNodeStreamIndex) {
	nodeState := g.nodeStreamState[nodeStreamIndex]
	if nodeState.finished {
		return
	}

	nodeState.finished = true
	nodeState.buffer.Close()
}

func (g *RemoteExecutionGroupEvaluator) onAllStreamsFinished() {
	g.stream.Close()
}

type ResponseStream interface {
	// Next returns the next message in the stream, or an error if the stream has ended or failed.
	Next(ctx context.Context) (*frontendv2pb.QueryResultStreamRequest, error)
	// Close closes the stream.
	Close()
}

type scalarExecutionResponse struct {
	group                    *RemoteExecutionGroupEvaluator
	nodeStreamIndex          remoteExecutionNodeStreamIndex
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	closed                   bool
}

func newScalarExecutionResponse(group *RemoteExecutionGroupEvaluator, nodeStreamIndex remoteExecutionNodeStreamIndex, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *scalarExecutionResponse {
	return &scalarExecutionResponse{
		group:                    group,
		nodeStreamIndex:          nodeStreamIndex,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
}

func (r *scalarExecutionResponse) Start(ctx context.Context) error {
	return r.group.sendRequest(ctx)
}

func (r *scalarExecutionResponse) GetValues(ctx context.Context) (types.ScalarData, error) {
	resp, releaseMessage, err := r.group.getNextMessageForStream(ctx, r.nodeStreamIndex)
	if err != nil {
		return types.ScalarData{}, err
	}

	defer releaseMessage()

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

	if v.Samples, err = ensureFPointSliceCapacityIsPowerOfTwo(v.Samples, r.memoryConsumptionTracker); err != nil {
		return types.ScalarData{}, err
	}

	return v, nil
}

func (r *scalarExecutionResponse) GetEvaluationInfo(ctx context.Context) (*annotations.Annotations, stats.Stats, error) {
	return r.group.getEvaluationCompletedForStream(ctx, r.nodeStreamIndex)
}

func (r *scalarExecutionResponse) Close() {
	if r.closed {
		return
	}

	r.group.closeStream(r.nodeStreamIndex)
	r.closed = true
}

type instantVectorExecutionResponse struct {
	group                    *RemoteExecutionGroupEvaluator
	nodeStreamIndex          remoteExecutionNodeStreamIndex
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	currentBatch             []querierpb.InstantVectorSeriesData
	closed                   bool
}

func newInstantVectorExecutionResponse(group *RemoteExecutionGroupEvaluator, nodeStreamIndex remoteExecutionNodeStreamIndex, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *instantVectorExecutionResponse {
	return &instantVectorExecutionResponse{
		group:                    group,
		nodeStreamIndex:          nodeStreamIndex,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
}

func (r *instantVectorExecutionResponse) Start(ctx context.Context) error {
	return r.group.sendRequest(ctx)
}

func (r *instantVectorExecutionResponse) GetSeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	return readSeriesMetadata(ctx, r.group, r.nodeStreamIndex, r.memoryConsumptionTracker)
}

func (r *instantVectorExecutionResponse) GetNextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if len(r.currentBatch) == 0 {
		resp, releaseMessage, err := r.group.getNextMessageForStream(ctx, r.nodeStreamIndex)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		defer releaseMessage()

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

	var err error

	if mqeData.Floats, err = ensureFPointSliceCapacityIsPowerOfTwo(mqeData.Floats, r.memoryConsumptionTracker); err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if mqeData.Histograms, err = ensureHPointSliceCapacityIsPowerOfTwo(mqeData.Histograms, r.memoryConsumptionTracker); err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	return mqeData, nil
}

func (r *instantVectorExecutionResponse) GetEvaluationInfo(ctx context.Context) (*annotations.Annotations, stats.Stats, error) {
	return r.group.getEvaluationCompletedForStream(ctx, r.nodeStreamIndex)
}

func (r *instantVectorExecutionResponse) Close() {
	if r.closed {
		return
	}

	r.group.closeStream(r.nodeStreamIndex)
	r.currentBatch = nil
	r.closed = true
}

type rangeVectorExecutionResponse struct {
	group                    *RemoteExecutionGroupEvaluator
	nodeStreamIndex          remoteExecutionNodeStreamIndex
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	currentSeriesIndex       int64
	floats                   *types.FPointRingBuffer
	histograms               *types.HPointRingBuffer
	stepData                 *types.RangeVectorStepData
	closed                   bool
}

func newRangeVectorExecutionResponse(group *RemoteExecutionGroupEvaluator, nodeStreamIndex remoteExecutionNodeStreamIndex, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *rangeVectorExecutionResponse {
	return &rangeVectorExecutionResponse{
		group:                    group,
		nodeStreamIndex:          nodeStreamIndex,
		memoryConsumptionTracker: memoryConsumptionTracker,
		currentSeriesIndex:       -1,
		floats:                   types.NewFPointRingBuffer(memoryConsumptionTracker),
		histograms:               types.NewHPointRingBuffer(memoryConsumptionTracker),
		stepData:                 &types.RangeVectorStepData{},
	}
}

func (r *rangeVectorExecutionResponse) Start(ctx context.Context) error {
	return r.group.sendRequest(ctx)
}

func (r *rangeVectorExecutionResponse) GetSeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	return readSeriesMetadata(ctx, r.group, r.nodeStreamIndex, r.memoryConsumptionTracker)
}

func (r *rangeVectorExecutionResponse) AdvanceToNextSeries(ctx context.Context) error {
	r.currentSeriesIndex++
	return nil
}

func (r *rangeVectorExecutionResponse) GetNextStepSamples(ctx context.Context) (*types.RangeVectorStepData, error) {
	r.floats.Release()
	r.histograms.Release()

	resp, releaseMessage, err := r.group.getNextMessageForStream(ctx, r.nodeStreamIndex)
	if err != nil {
		return nil, err
	}

	defer releaseMessage()

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

	if fPoints, err = ensureFPointSliceCapacityIsPowerOfTwo(fPoints, r.memoryConsumptionTracker); err != nil {
		return nil, err
	}

	if hPoints, err = ensureHPointSliceCapacityIsPowerOfTwo(hPoints, r.memoryConsumptionTracker); err != nil {
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
	return r.group.getEvaluationCompletedForStream(ctx, r.nodeStreamIndex)
}

func (r *rangeVectorExecutionResponse) Close() {
	if r.closed {
		return
	}

	r.group.closeStream(r.nodeStreamIndex)
	r.floats.Close()
	r.histograms.Close()
	r.closed = true
}

func readSeriesMetadata(ctx context.Context, group *RemoteExecutionGroupEvaluator, nodeStreamIndex remoteExecutionNodeStreamIndex, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) ([]types.SeriesMetadata, error) {
	resp, releaseMessage, err := group.getNextMessageForStream(ctx, nodeStreamIndex)
	if err != nil {
		return nil, err
	}

	// The labels in the message contain references to the underlying buffer, so we can't release it immediately.
	// The value returned by FromLabelAdaptersToLabels does not retain a reference to the underlying buffer,
	// so we can release the buffer once that method returns.
	defer releaseMessage()

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

func accountForFPointMemoryConsumption(points []promql.FPoint, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) error {
	if err := memoryConsumptionTracker.IncreaseMemoryConsumption(uint64(cap(points))*types.FPointSize, limiter.FPointSlices); err != nil {
		return err
	}

	return nil
}

func accountForHPointMemoryConsumption(points []promql.HPoint, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) error {
	if err := memoryConsumptionTracker.IncreaseMemoryConsumption(uint64(cap(points))*types.HPointSize, limiter.HPointSlices); err != nil {
		return err
	}

	return nil
}

// ensureFPointSliceCapacityIsPowerOfTwo returns d if its capacity is already a power of two, or otherwise a new slice with the same elements and a
// capacity that is a power of two.
//
// If a new slice is created, the memory consumption estimate is adjusted assuming the old slice is no longer used.
//
// This exists because many places in MQE assume that slices have come from our pools and always have a capacity that is a power of two.
// For example, the ring buffer implementations rely on the fact that slices have a capacity that is a power of two.
func ensureFPointSliceCapacityIsPowerOfTwo(points []promql.FPoint, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) ([]promql.FPoint, error) {
	if pool.IsPowerOfTwo(cap(points)) {
		return points, nil
	}

	nextPowerOfTwo := 1 << bits.Len(uint(cap(points)-1))
	newSlice, err := types.FPointSlicePool.Get(nextPowerOfTwo, memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	newSlice = newSlice[:len(points)]
	copy(newSlice, points)

	// Don't return the old slice to the pool, but update the memory consumption estimate.
	// The pool won't use it because it's not a power of two, so there's no point in calling Put() on it.
	memoryConsumptionTracker.DecreaseMemoryConsumption(uint64(cap(points))*types.FPointSize, limiter.FPointSlices)

	return newSlice, nil
}

// ensureHPointSliceCapacityIsPowerOfTwo is like ensureFPointSliceCapacityIsPowerOfTwo, but for HPoint slices.
func ensureHPointSliceCapacityIsPowerOfTwo(points []promql.HPoint, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) ([]promql.HPoint, error) {
	if pool.IsPowerOfTwo(cap(points)) {
		return points, nil
	}

	nextPowerOfTwo := 1 << bits.Len(uint(cap(points)-1))
	newSlice, err := types.HPointSlicePool.Get(nextPowerOfTwo, memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	newSlice = newSlice[:len(points)]
	copy(newSlice, points)

	// Don't return the old slice to the pool, but update the memory consumption estimate.
	// The pool won't use it because it's not a power of two, so there's no point in calling Put() on it.
	memoryConsumptionTracker.DecreaseMemoryConsumption(uint64(cap(points))*types.HPointSize, limiter.HPointSlices)

	return newSlice, nil
}

type eagerLoadingResponseStream struct {
	inner       ResponseStream
	bufferedErr error

	mtx              *sync.Mutex
	buffer           *responseStreamBuffer
	newDataAvailable chan struct{}
}

func newEagerLoadingResponseStream(ctx context.Context, inner ResponseStream) *eagerLoadingResponseStream {
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

	if e.buffer == nil {
		// The stream has been closed, don't buffer any more data.
		return false
	}

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

	if e.buffer == nil {
		e.mtx.Unlock()
		return nil, errStreamClosed
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

		// Check the buffer wasn't closed in the meantime.
		if e.buffer == nil {
			return nil, errStreamClosed
		}

		msg := e.buffer.Pop()

		if msg.err != nil {
			e.bufferedErr = msg.err
		}

		return msg.payload, msg.err
	}
}

func (e *eagerLoadingResponseStream) Close() {
	e.inner.Close() // This is expected to release any pending Next() call in startBuffering(), so we don't need to do anything to terminate it here.

	e.mtx.Lock()
	defer e.mtx.Unlock()

	if e.buffer == nil {
		// Already closed, nothing more to do.
		return
	}

	e.buffer.Close()
	e.buffer = nil
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
		newCap := max(cap(b.msgs)*2, 1)
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

func (b *responseStreamBuffer) Close() {
	b.startIndex = 0
	b.length = 0
	clear(b.msgs)
	responseMessageSlicePool.Put(b.msgs)
	b.msgs = nil
}

// Why types.MaxExpectedSeriesPerResult as the slice size limit?
// The vast majority of these slices will be used for instant vector results, which
// will have 2 + num_series messages in the worst case. So we use our estimate of the
// maximum number of series per result to limit the size of slices in the pool.
var responseMessageSlicePool = pool.NewBucketedPool(types.MaxExpectedSeriesPerResult, func(size int) []bufferedMessage {
	return make([]bufferedMessage, 0, size)
})
