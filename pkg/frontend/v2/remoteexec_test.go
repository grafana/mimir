// SPDX-License-Identifier: AGPL-3.0-only

package v2

import (
	"context"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/querierpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/remoteexec"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestScalarExecutionResponse(t *testing.T) {
	stream := &mockResponseStream{
		responses: []mockResponse{
			{
				msg: newScalarValue(
					mimirpb.Sample{TimestampMs: 1000, Value: 1.01},
					mimirpb.Sample{TimestampMs: 2000, Value: 2.01},
					mimirpb.Sample{TimestampMs: 3000, Value: 3.01},
				),
			},
		},
	}

	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")

	response := &scalarExecutionResponse{stream: stream, memoryConsumptionTracker: memoryConsumptionTracker}
	d, err := response.GetValues(ctx)
	require.NoError(t, err)

	expected := types.ScalarData{
		Samples: []promql.FPoint{
			{T: 1000, F: 1.01},
			{T: 2000, F: 2.01},
			{T: 3000, F: 3.01},
		},
	}
	require.Equal(t, expected, d)

	require.NotZero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	types.FPointSlicePool.Put(&d.Samples, memoryConsumptionTracker)
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

	response.Close()
	require.True(t, stream.closed)
}

func TestInstantVectorExecutionResponse(t *testing.T) {
	stream := &mockResponseStream{
		responses: []mockResponse{
			{
				msg: newSeriesMetadata(
					false,
					labels.FromStrings("series", "1"),
					labels.FromStrings("series", "2"),
				),
			},
			{
				msg: newInstantVectorSeriesData(
					generateFPoints(1000, 2, 0),
					generateHPoints(3000, 2, 0),
				),
			},
			{
				msg: newInstantVectorSeriesData(
					generateFPoints(1000, 2, 1),
					generateHPoints(3000, 2, 1),
				),
			},
		},
	}

	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")

	response := &instantVectorExecutionResponse{stream: stream, memoryConsumptionTracker: memoryConsumptionTracker}
	series, err := response.GetSeriesMetadata(ctx)
	require.NoError(t, err)

	expectedSeries := []types.SeriesMetadata{
		{Labels: labels.FromStrings("series", "1")},
		{Labels: labels.FromStrings("series", "2")},
	}
	require.Equal(t, expectedSeries, series)
	require.NotZero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	types.SeriesMetadataSlicePool.Put(&series, memoryConsumptionTracker)
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

	data, err := response.GetNextSeries(ctx)
	require.NoError(t, err)

	expectedData := types.InstantVectorSeriesData{
		Floats:     generateFPoints(1000, 2, 0),
		Histograms: generateHPoints(3000, 2, 0),
	}
	require.Equal(t, expectedData, data)
	require.NotZero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	types.PutInstantVectorSeriesData(data, memoryConsumptionTracker)
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

	data, err = response.GetNextSeries(ctx)
	require.NoError(t, err)

	expectedData = types.InstantVectorSeriesData{
		Floats:     generateFPoints(1000, 2, 1),
		Histograms: generateHPoints(3000, 2, 1),
	}
	require.Equal(t, expectedData, data)
	require.NotZero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	types.PutInstantVectorSeriesData(data, memoryConsumptionTracker)
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

	response.Close()
	require.True(t, stream.closed)
}

func TestInstantVectorExecutionResponse_DelayedNameRemoval(t *testing.T) {
	stream := &mockResponseStream{
		responses: []mockResponse{
			{
				msg: newSeriesMetadata(
					true,
					labels.FromStrings("series", "1"),
					labels.FromStrings("series", "2"),
				),
			},
		},
	}

	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")

	response := &instantVectorExecutionResponse{stream: stream, memoryConsumptionTracker: memoryConsumptionTracker}
	series, err := response.GetSeriesMetadata(ctx)
	require.NoError(t, err)

	expectedSeries := []types.SeriesMetadata{
		{Labels: labels.FromStrings("series", "1"), DropName: true},
		{Labels: labels.FromStrings("series", "2"), DropName: true},
	}
	require.Equal(t, expectedSeries, series)
	require.NotZero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	types.SeriesMetadataSlicePool.Put(&series, memoryConsumptionTracker)
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
}

func TestRangeVectorExecutionResponse(t *testing.T) {
	stream := &mockResponseStream{
		responses: []mockResponse{
			{
				msg: newSeriesMetadata(
					false,
					labels.FromStrings("series", "1"),
					labels.FromStrings("series", "2"),
				),
			},
			{
				msg: newRangeVectorStepData(
					0,
					1000,
					500,
					4000,
					generateFPoints(1000, 2, 0),
					generateHPoints(3000, 2, 0),
				),
			},
			{
				msg: newRangeVectorStepData(
					0,
					2000,
					1500,
					5000,
					generateFPoints(1000, 2, 1),
					generateHPoints(3000, 2, 1),
				),
			},
			{
				msg: newRangeVectorStepData(
					1,
					1000,
					500,
					4000,
					generateFPoints(1000, 2, 2),
					generateHPoints(3000, 2, 2),
				),
			},
			{
				msg: newRangeVectorStepData(
					1,
					2000,
					1500,
					5000,
					generateFPoints(1000, 2, 3),
					generateHPoints(3000, 2, 3),
				),
			},
		},
	}

	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")

	response := newRangeVectorExecutionResponse(stream, memoryConsumptionTracker)
	series, err := response.GetSeriesMetadata(ctx)
	require.NoError(t, err)

	expectedSeries := []types.SeriesMetadata{
		{Labels: labels.FromStrings("series", "1")},
		{Labels: labels.FromStrings("series", "2")},
	}
	require.Equal(t, expectedSeries, series)
	require.NotZero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	types.SeriesMetadataSlicePool.Put(&series, memoryConsumptionTracker)
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

	require.NoError(t, response.AdvanceToNextSeries(ctx))
	data, err := response.GetNextStepSamples(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1000), data.StepT)
	require.Equal(t, int64(500), data.RangeStart)
	require.Equal(t, int64(4000), data.RangeEnd)
	requireEqualFPointRingBuffer(t, data.Floats, generateFPoints(1000, 2, 0))
	requireEqualHPointRingBuffer(t, data.Histograms, generateHPoints(3000, 2, 0))
	require.NotZero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

	data, err = response.GetNextStepSamples(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2000), data.StepT)
	require.Equal(t, int64(1500), data.RangeStart)
	require.Equal(t, int64(5000), data.RangeEnd)
	requireEqualFPointRingBuffer(t, data.Floats, generateFPoints(1000, 2, 1))
	requireEqualHPointRingBuffer(t, data.Histograms, generateHPoints(3000, 2, 1))
	require.NotZero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

	require.NoError(t, response.AdvanceToNextSeries(ctx))
	data, err = response.GetNextStepSamples(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1000), data.StepT)
	require.Equal(t, int64(500), data.RangeStart)
	require.Equal(t, int64(4000), data.RangeEnd)
	requireEqualFPointRingBuffer(t, data.Floats, generateFPoints(1000, 2, 2))
	requireEqualHPointRingBuffer(t, data.Histograms, generateHPoints(3000, 2, 2))
	require.NotZero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

	data, err = response.GetNextStepSamples(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2000), data.StepT)
	require.Equal(t, int64(1500), data.RangeStart)
	require.Equal(t, int64(5000), data.RangeEnd)
	requireEqualFPointRingBuffer(t, data.Floats, generateFPoints(1000, 2, 3))
	requireEqualHPointRingBuffer(t, data.Histograms, generateHPoints(3000, 2, 3))
	require.NotZero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

	response.Close()
	require.True(t, stream.closed)
	require.Zerof(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes(), "buffers should be released when closing response, have: %v", memoryConsumptionTracker.DescribeCurrentMemoryConsumption())
}

func TestRangeVectorExecutionResponse_DelayedNameRemoval(t *testing.T) {
	stream := &mockResponseStream{
		responses: []mockResponse{
			{
				msg: newSeriesMetadata(
					true,
					labels.FromStrings("series", "1"),
					labels.FromStrings("series", "2"),
				),
			},
		},
	}

	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")

	response := newRangeVectorExecutionResponse(stream, memoryConsumptionTracker)
	series, err := response.GetSeriesMetadata(ctx)
	require.NoError(t, err)

	expectedSeries := []types.SeriesMetadata{
		{Labels: labels.FromStrings("series", "1"), DropName: true},
		{Labels: labels.FromStrings("series", "2"), DropName: true},
	}
	require.Equal(t, expectedSeries, series)
	require.NotZero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	types.SeriesMetadataSlicePool.Put(&series, memoryConsumptionTracker)
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
}

func TestRangeVectorExecutionResponse_ExpectedSeriesMismatch(t *testing.T) {
	stream := &mockResponseStream{
		responses: []mockResponse{
			{
				msg: newSeriesMetadata(
					false,
					labels.FromStrings("series", "1"),
					labels.FromStrings("series", "2"),
				),
			},
			{
				msg: newRangeVectorStepData(
					0,
					1000,
					500,
					4000,
					generateFPoints(1000, 2, 0),
					generateHPoints(3000, 2, 0),
				),
			},
			{
				msg: newRangeVectorStepData(
					0,
					2000,
					1500,
					5000,
					generateFPoints(1000, 2, 1),
					generateHPoints(3000, 2, 1),
				),
			},
		},
	}

	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")

	response := newRangeVectorExecutionResponse(stream, memoryConsumptionTracker)
	series, err := response.GetSeriesMetadata(ctx)
	require.NoError(t, err)

	expectedSeries := []types.SeriesMetadata{
		{Labels: labels.FromStrings("series", "1")},
		{Labels: labels.FromStrings("series", "2")},
	}
	require.Equal(t, expectedSeries, series)
	require.NotZero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	types.SeriesMetadataSlicePool.Put(&series, memoryConsumptionTracker)
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

	require.NoError(t, response.AdvanceToNextSeries(ctx))
	data, err := response.GetNextStepSamples(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1000), data.StepT)
	require.Equal(t, int64(500), data.RangeStart)
	require.Equal(t, int64(4000), data.RangeEnd)
	requireEqualFPointRingBuffer(t, data.Floats, generateFPoints(1000, 2, 0))
	requireEqualHPointRingBuffer(t, data.Histograms, generateHPoints(3000, 2, 0))
	require.NotZero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

	// Advance to the next series early. We should get an error when reading the next step.
	require.NoError(t, response.AdvanceToNextSeries(ctx))
	data, err = response.GetNextStepSamples(ctx)
	require.EqualError(t, err, "expected data for series index 1, but got data for series index 0")
	require.Nil(t, data)

	response.Close()
	require.True(t, stream.closed)
	require.Zerof(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes(), "buffers should be released when closing response, have: %v", memoryConsumptionTracker.DescribeCurrentMemoryConsumption())
}

func TestExecutionResponses_GetEvaluationInfo(t *testing.T) {
	responseCreators := map[string]func(stream responseStream, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) remoteexec.RemoteExecutionResponse{
		"scalar": func(stream responseStream, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) remoteexec.RemoteExecutionResponse {
			return &scalarExecutionResponse{stream, memoryConsumptionTracker}
		},
		"instant vector": func(stream responseStream, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) remoteexec.RemoteExecutionResponse {
			return &instantVectorExecutionResponse{stream, memoryConsumptionTracker}
		},
		"range vector": func(stream responseStream, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) remoteexec.RemoteExecutionResponse {
			return newRangeVectorExecutionResponse(stream, memoryConsumptionTracker)
		},
	}

	expectedError := apierror.New(apierror.TypeUnavailable, "something went wrong")
	expectedTotalSamples := uint64(1234)
	expectedWarnings := []string{"warning #1", "warning #2"}
	expectedInfos := []string{"info #1", "info #2"}

	runScenario := func(t *testing.T, expectSuccess bool, responses ...mockResponse) {
		for name, responseCreator := range responseCreators {
			t.Run(name, func(t *testing.T) {
				stream := &mockResponseStream{
					responses: responses,
				}

				ctx := context.Background()
				memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
				response := responseCreator(stream, memoryConsumptionTracker)

				annos, stats, err := response.GetEvaluationInfo(ctx)
				if expectSuccess {
					require.NoError(t, err)
					require.Equal(t, expectedTotalSamples, stats.SamplesProcessed)

					warnings, infos := annos.AsStrings("", 0, 0)
					require.ElementsMatch(t, expectedWarnings, warnings)
					require.ElementsMatch(t, expectedInfos, infos)
				} else {
					require.Equal(t, expectedError, err)
				}
			})
		}
	}

	t.Run("next message to read is an error", func(t *testing.T) {
		runScenario(t, false, mockResponse{err: expectedError})
	})

	t.Run("next message to read is EvaluationCompleted", func(t *testing.T) {
		runScenario(t, true, mockResponse{msg: newEvaluationCompleted(expectedTotalSamples, expectedWarnings, expectedInfos)})
	})

	t.Run("next message to read is not EvaluationCompleted, eventually stream returns EvaluationCompleted", func(t *testing.T) {
		runScenario(
			t,
			true,
			mockResponse{msg: newScalarValue(mimirpb.Sample{TimestampMs: 1000, Value: 2})},
			mockResponse{msg: newScalarValue(mimirpb.Sample{TimestampMs: 4000, Value: 2})},
			mockResponse{msg: newEvaluationCompleted(expectedTotalSamples, expectedWarnings, expectedInfos)},
		)
	})

	t.Run("next message to read is not EvaluationCompleted, eventually stream returns error", func(t *testing.T) {
		runScenario(
			t,
			false,
			mockResponse{msg: newScalarValue(mimirpb.Sample{TimestampMs: 1000, Value: 2})},
			mockResponse{msg: newScalarValue(mimirpb.Sample{TimestampMs: 4000, Value: 2})},
			mockResponse{err: expectedError},
		)
	})
}

type mockResponseStream struct {
	responses []mockResponse
	closed    bool
}

func (m *mockResponseStream) Next(ctx context.Context) (*frontendv2pb.QueryResultStreamRequest, error) {
	resp := m.responses[0]
	m.responses = m.responses[1:]

	return resp.msg, resp.err
}

func (m *mockResponseStream) Close() {
	m.closed = true
}

type mockResponse struct {
	msg *frontendv2pb.QueryResultStreamRequest
	err error
}

func TestDecodeEvaluationCompletedMessage(t *testing.T) {
	msg := &querierpb.EvaluateQueryResponseEvaluationCompleted{
		Annotations: querierpb.Annotations{
			Warnings: []string{
				"warning: something isn't quite right",
				"warning: something else isn't quite right",
			},
			Infos: []string{
				"info: you should know about this",
				"info: you should know about this too",
			},
		},
		Stats: stats.Stats{
			SamplesProcessed: 1234,
		},
	}

	annos, stats := decodeEvaluationCompletedMessage(msg)
	require.Equal(t, msg.Stats, stats)

	// If these tests fail, then the errors we're adding to the set of annotations likely
	// don't wrap PromQLInfo / PromQLWarning correctly.
	warnings, infos := annos.AsStrings("", 0, 0)
	require.ElementsMatch(t, []string{"warning: something isn't quite right", "warning: something else isn't quite right"}, warnings)
	require.ElementsMatch(t, []string{"info: you should know about this", "info: you should know about this too"}, infos)
}

func newScalarValue(samples ...mimirpb.Sample) *frontendv2pb.QueryResultStreamRequest {
	return &frontendv2pb.QueryResultStreamRequest{
		Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
			EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
				Message: &querierpb.EvaluateQueryResponse_ScalarValue{
					ScalarValue: &querierpb.EvaluateQueryResponseScalarValue{
						Values: samples,
					},
				},
			},
		},
	}
}

func newSeriesMetadata(dropName bool, series ...labels.Labels) *frontendv2pb.QueryResultStreamRequest {
	protoSeries := make([]querierpb.SeriesMetadata, 0, len(series))
	for _, series := range series {
		protoSeries = append(protoSeries, querierpb.SeriesMetadata{
			Labels:   mimirpb.FromLabelsToLabelAdapters(series),
			DropName: dropName,
		})
	}

	return &frontendv2pb.QueryResultStreamRequest{
		Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
			EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
				Message: &querierpb.EvaluateQueryResponse_SeriesMetadata{
					SeriesMetadata: &querierpb.EvaluateQueryResponseSeriesMetadata{
						Series: protoSeries,
					},
				},
			},
		},
	}
}

func newInstantVectorSeriesData(floats []promql.FPoint, histograms []promql.HPoint) *frontendv2pb.QueryResultStreamRequest {
	return &frontendv2pb.QueryResultStreamRequest{
		Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
			EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
				Message: &querierpb.EvaluateQueryResponse_InstantVectorSeriesData{
					InstantVectorSeriesData: &querierpb.EvaluateQueryResponseInstantVectorSeriesData{
						Floats:     mimirpb.FromFPointsToSamples(floats),
						Histograms: mimirpb.FromHPointsToHistograms(histograms),
					},
				},
			},
		},
	}
}

func newRangeVectorStepData(seriesIndex int64, stepT int64, rangeStart int64, rangeEnd int64, floats []promql.FPoint, histograms []promql.HPoint) *frontendv2pb.QueryResultStreamRequest {
	return &frontendv2pb.QueryResultStreamRequest{
		Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
			EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
				Message: &querierpb.EvaluateQueryResponse_RangeVectorStepData{
					RangeVectorStepData: &querierpb.EvaluateQueryResponseRangeVectorStepData{
						SeriesIndex: seriesIndex,
						StepT:       stepT,
						RangeStart:  rangeStart,
						RangeEnd:    rangeEnd,
						Floats:      mimirpb.FromFPointsToSamples(floats),
						Histograms:  mimirpb.FromHPointsToHistograms(histograms),
					},
				},
			},
		},
	}
}

func newEvaluationCompleted(totalSamples uint64, warnings []string, infos []string) *frontendv2pb.QueryResultStreamRequest {
	return &frontendv2pb.QueryResultStreamRequest{
		Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
			EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
				Message: &querierpb.EvaluateQueryResponse_EvaluationCompleted{
					EvaluationCompleted: &querierpb.EvaluateQueryResponseEvaluationCompleted{
						Annotations: querierpb.Annotations{
							Warnings: warnings,
							Infos:    infos,
						},
						Stats: stats.Stats{
							SamplesProcessed: totalSamples,
						},
					},
				},
			},
		},
	}
}

func generateFPoints(baseT int64, count int, offset float64) []promql.FPoint {
	points := make([]promql.FPoint, 0, count)
	for i := range count {
		points = append(points, promql.FPoint{
			T: baseT + (1000 * int64(i)),
			F: 1 + float64(i) + offset,
		})
	}

	return points
}

func generateHPoints(baseT int64, count int, offset float64) []promql.HPoint {
	points := make([]promql.HPoint, 0, count)
	for i := range count {
		points = append(points, promql.HPoint{
			T: baseT + (1000 * int64(i)),
			H: &histogram.FloatHistogram{Count: offset + float64(i)},
		})
	}

	return points
}

func requireEqualFPointRingBuffer(t *testing.T, buffer *types.FPointRingBufferView, expected []promql.FPoint) {
	require.Equal(t, len(expected), buffer.Count())

	for i := range expected {
		require.Equalf(t, buffer.PointAt(i), expected[i], "expected points at index %v to be equal", i)
	}
}

func requireEqualHPointRingBuffer(t *testing.T, buffer *types.HPointRingBufferView, expected []promql.HPoint) {
	require.Equal(t, len(expected), buffer.Count())

	for i := range expected {
		require.Equalf(t, buffer.PointAt(i), expected[i], "expected points at index %v to be equal", i)
	}
}

func TestRemoteExecutor_CorrectlyPassesQueriedTimeRange(t *testing.T) {
	frontendMock := &timeRangeCapturingFrontend{}
	cfg := Config{LookBackDelta: 7 * time.Minute}
	executor := NewRemoteExecutor(frontendMock, cfg)

	endT := time.Now().Truncate(time.Minute).UTC()
	startT := endT.Add(-time.Hour)
	timeRange := types.NewRangeQueryTimeRange(startT, endT, time.Minute)

	ctx := context.Background()
	node := &core.VectorSelector{VectorSelectorDetails: &core.VectorSelectorDetails{}}
	_, err := executor.startExecution(ctx, &planning.QueryPlan{}, node, timeRange, false, false)
	require.NoError(t, err)

	require.Equal(t, startT.Add(-cfg.LookBackDelta+time.Millisecond), frontendMock.minT)
	require.Equal(t, endT, frontendMock.maxT)
}

type timeRangeCapturingFrontend struct {
	minT time.Time
	maxT time.Time
}

func (m *timeRangeCapturingFrontend) DoProtobufRequest(ctx context.Context, req proto.Message, minT, maxT time.Time) (*ProtobufResponseStream, error) {
	m.minT = minT
	m.maxT = maxT
	return nil, nil
}
