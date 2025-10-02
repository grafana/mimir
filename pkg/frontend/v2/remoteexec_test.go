// SPDX-License-Identifier: AGPL-3.0-only

package v2

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/querierpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast/sharding"
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
	release   chan struct{}
	responses []mockResponse
	closed    bool
}

func (m *mockResponseStream) Next(ctx context.Context) (*frontendv2pb.QueryResultStreamRequest, error) {
	if m.release != nil {
		<-m.release
	}

	if len(m.responses) == 0 {
		return nil, errors.New("exhausted mock response stream")
	}

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

func TestEagerLoadingResponseStream_ShouldBufferAllMessagesEvenIfNoNextCallWaiting(t *testing.T) {
	msg1 := newStringMessage("first message")
	msg2 := newStringMessage("second message")
	msg3 := newStringMessage("third message")
	inner := &mockResponseStream{
		responses: []mockResponse{
			{msg: msg1},
			{msg: msg2},
			{msg: msg3},
		},
	}

	stream := newEagerLoadingResponseStream(context.Background(), inner)

	require.Eventually(t, func() bool {
		stream.mtx.Lock()
		defer stream.mtx.Unlock()

		return stream.buffer.length >= 3
	}, 100*time.Millisecond, 10*time.Millisecond, "buffer should be populated even if no Next() call waiting")

	receivedMessage, err := stream.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, msg1, receivedMessage)

	receivedMessage, err = stream.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, msg2, receivedMessage)

	receivedMessage, err = stream.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, msg3, receivedMessage)
}

func TestEagerLoadingResponseStream_ShouldWaitForDataIfBufferEmpty(t *testing.T) {
	msg1 := newStringMessage("first message")
	msg2 := newStringMessage("second message")
	msg3 := newStringMessage("third message")
	release := make(chan struct{})
	inner := &mockResponseStream{
		release: release, // Don't return any messages until we've had a chance to call Next() below.
		responses: []mockResponse{
			{msg: msg1},
			{msg: msg2},
			{msg: msg3},
		},
	}

	stream := newEagerLoadingResponseStream(context.Background(), inner)
	go func() {
		<-time.After(100 * time.Millisecond)
		close(release)
	}()

	receivedMessage, err := stream.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, msg1, receivedMessage)

	receivedMessage, err = stream.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, msg2, receivedMessage)

	receivedMessage, err = stream.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, msg3, receivedMessage)
}

func TestEagerLoadingResponseStream_ErrorReturnedByInnerStream(t *testing.T) {
	inner := &mockResponseStream{
		responses: []mockResponse{
			{err: errors.New("something went wrong")},
		},
	}
	stream := newEagerLoadingResponseStream(context.Background(), inner)

	msg, err := stream.Next(context.Background())
	require.EqualError(t, err, "something went wrong")
	require.Nil(t, msg)

	msg, err = stream.Next(context.Background())
	require.EqualError(t, err, "something went wrong", "error should be returned on subsequent Next() calls")
	require.Nil(t, msg)
}

func TestEagerLoadingResponseStream_AbortsNextCallOnContextCancellation(t *testing.T) {
	inner := &mockResponseStreamThatNeverReturns{release: make(chan struct{})}
	stream := newEagerLoadingResponseStream(context.Background(), inner)

	ctx, cancel := context.WithTimeoutCause(context.Background(), 20*time.Millisecond, errors.New("something has timed out"))
	defer cancel()

	msg, err := stream.Next(ctx)
	require.EqualError(t, err, "something has timed out")
	require.Nil(t, msg)

	inner.Close()
}

type mockResponseStreamThatNeverReturns struct {
	release chan struct{}
}

func (m *mockResponseStreamThatNeverReturns) Next(ctx context.Context) (*frontendv2pb.QueryResultStreamRequest, error) {
	<-m.release
	return nil, errors.New("mock response stream closed")
}

func (m *mockResponseStreamThatNeverReturns) Close() {
	close(m.release)
}

func TestEagerLoadingResponseStream_ClosesInnerStreamWhenClosed(t *testing.T) {
	inner := &mockResponseStream{}
	stream := newEagerLoadingResponseStream(context.Background(), inner)
	stream.Close()
	require.True(t, inner.closed)
}

func TestResponseStreamBuffer(t *testing.T) {
	buf := &responseStreamBuffer{}
	require.False(t, buf.Any())

	msg1 := bufferedMessage{newStringMessage("first message"), nil}
	buf.Push(msg1)
	require.True(t, buf.Any())
	require.Equal(t, msg1, buf.Pop())
	require.False(t, buf.Any())
	// Make sure the internal state has been reset correctly, so that the tests below exercise the expected behaviour.
	require.Zero(t, buf.startIndex)
	require.Zero(t, buf.length)
	require.Equal(t, 1, cap(buf.msgs))

	msg2 := bufferedMessage{newStringMessage("second message"), nil}
	msg3 := bufferedMessage{newStringMessage("third message"), nil}
	buf.Push(msg1)
	buf.Push(msg2)
	buf.Push(msg3)
	require.True(t, buf.Any())
	require.Equal(t, msg1, buf.Pop())
	require.Equal(t, msg2, buf.Pop())
	require.Equal(t, msg3, buf.Pop())
	require.False(t, buf.Any())
	// Make sure the internal state has been reset correctly, so that the tests below exercise the expected behaviour.
	require.Zero(t, buf.startIndex)
	require.Zero(t, buf.length)
	require.Equal(t, 4, cap(buf.msgs))

	// Test that appending to the buffer when the first item is not in the first index of the underlying slice behaves correctly.
	msg4 := bufferedMessage{newStringMessage("fourth message"), nil}
	buf.Push(msg1)
	buf.Push(msg2)
	buf.Push(msg3)
	buf.Push(msg4)
	require.Equal(t, 4, cap(buf.msgs))

	require.Equal(t, msg1, buf.Pop())

	msg5 := bufferedMessage{newStringMessage("fifth message"), nil}
	buf.Push(msg5)
	require.Equal(t, 4, cap(buf.msgs), "buffer should have wrapped around inside slice")
	require.Equal(t, 1, buf.startIndex, "buffer should not have shifted elements in slice")

	msg6 := bufferedMessage{newStringMessage("sixth message"), nil}
	buf.Push(msg6)
	require.Equal(t, 8, cap(buf.msgs), "slice should have been expanded")
	require.Equal(t, 0, buf.startIndex, "buffer should have shifted elements in slice")

	require.Equal(t, msg2, buf.Pop())
	require.Equal(t, msg3, buf.Pop())
	require.Equal(t, msg4, buf.Pop())
	require.Equal(t, msg5, buf.Pop())
	require.Equal(t, msg6, buf.Pop())
}

func TestMaxQueryParallelismWithRemoteExecution(t *testing.T) {
	t.Run("with sharding running outside MQE", func(t *testing.T) {
		runQueryParallelismTestCase(t, false)
	})

	t.Run("with sharding running inside MQE", func(t *testing.T) {
		runQueryParallelismTestCase(t, true)
	})
}

func runQueryParallelismTestCase(t *testing.T, enableMQESharding bool) {
	const maxQueryParallelism = int64(2)
	count := atomic.NewInt64(0)
	maxMtx := &sync.Mutex{}
	maxConcurrent := int64(0)
	ctx := user.InjectOrgID(context.Background(), "the-user")
	ctx, cancel := context.WithTimeoutCause(ctx, 20*time.Second, errors.New("parallelism test timed out: this may indicate a deadlock somewhere"))
	defer cancel()
	codec := newTestCodec()
	logger := log.NewNopLogger()
	limits := &mockLimitedParallelismLimits{maxQueryParallelism: int(maxQueryParallelism)}

	opts := streamingpromql.NewTestEngineOpts()
	planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts)
	require.NoError(t, err)
	planner.RegisterQueryPlanOptimizationPass(remoteexec.NewOptimizationPass())

	if enableMQESharding {
		planner.RegisterASTOptimizationPass(sharding.NewOptimizationPass(limits, 0, nil, logger))
	}

	engine, err := streamingpromql.NewEngine(opts, streamingpromql.NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)

	frontend, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		if msg.Type != schedulerpb.ENQUEUE {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: fmt.Sprintf("scheduler received unexpected message type: %v", msg.Type)}
		}

		go func() {
			current := count.Inc()
			maxMtx.Lock()
			maxConcurrent = max(current, maxConcurrent)
			maxMtx.Unlock()
			defer count.Dec()

			// Simulate doing some work, then send an empty response.
			time.Sleep(20 * time.Millisecond)
			sendStreamingResponse(t, f, msg.UserID, msg.QueryID, newSeriesMetadata(false), newEvaluationCompleted(0, nil, nil))
		}()

		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	cfg := Config{LookBackDelta: 7 * time.Minute}
	executor := NewRemoteExecutor(frontend, cfg)
	require.NoError(t, engine.RegisterNodeMaterializer(planning.NODE_TYPE_REMOTE_EXEC, remoteexec.NewRemoteExecutionMaterializer(executor)))

	expr, err := parser.ParseExpr("sum(foo)")
	require.NoError(t, err)
	request := querymiddleware.NewPrometheusRangeQueryRequest("/api/v1/query_range", nil, timestamp.FromTime(time.Now().Add(-time.Hour)), timestamp.FromTime(time.Now()), time.Second.Milliseconds(), 5*time.Minute, expr, querymiddleware.Options{}, nil, "")
	httpRequest, err := codec.EncodeMetricsQueryRequest(ctx, request)
	require.NoError(t, err)

	middleware := querymiddleware.MetricsQueryMiddlewareFunc(func(next querymiddleware.MetricsQueryHandler) querymiddleware.MetricsQueryHandler {
		return querymiddleware.HandlerFunc(func(c context.Context, _ querymiddleware.MetricsQueryRequest) (querymiddleware.Response, error) {
			wg := &errgroup.Group{}

			// Simulate many parts of the request being split and evaluated in parallel.
			for range maxQueryParallelism + 10 {
				wg.Go(func() error {
					_, err := next.Do(c, request)
					return err
				})
			}

			err := wg.Wait()
			return querymiddleware.NewEmptyPrometheusResponse(), err
		})
	})

	handler := querymiddleware.NewEngineQueryRequestRoundTripperHandler(engine, codec, logger)
	_, err = querymiddleware.NewLimitedParallelismRoundTripper(handler, codec, limits, middleware).RoundTrip(httpRequest)
	require.NoError(t, err)
	require.NotZero(t, maxConcurrent, "expected at least one query to be executed")
	require.LessOrEqualf(t, maxConcurrent, maxQueryParallelism, "max query parallelism %v went over the configured limit of %v", maxConcurrent, maxQueryParallelism)
}

type mockLimitedParallelismLimits struct {
	maxQueryParallelism int
}

func (m mockLimitedParallelismLimits) QueryShardingTotalShards(_ string) int {
	return m.maxQueryParallelism * 3
}

func (m mockLimitedParallelismLimits) QueryShardingMaxRegexpSizeBytes(_ string) int {
	return 0
}

func (m mockLimitedParallelismLimits) QueryShardingMaxShardedQueries(_ string) int {
	return 0
}

func (m mockLimitedParallelismLimits) CompactorSplitAndMergeShards(_ string) int {
	return 4
}

func (m mockLimitedParallelismLimits) MaxQueryParallelism(_ string) int {
	return m.maxQueryParallelism
}
