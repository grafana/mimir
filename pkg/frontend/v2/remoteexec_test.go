// SPDX-License-Identifier: AGPL-3.0-only

package v2

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
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
	"github.com/grafana/mimir/pkg/util/test"
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
	require.Equal(t, 4, cap(d.Samples), "should expand slice capacity to nearest power of two")

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

func TestInstantVectorExecutionResponse_Batching(t *testing.T) {
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
				msg: newBatchedInstantVectorSeriesData(
					querierpb.InstantVectorSeriesData{
						Floats:     mimirpb.FromFPointsToSamples(generateFPoints(1000, 2, 0)),
						Histograms: mimirpb.FromHPointsToHistograms(generateHPoints(3000, 2, 0)),
					},
					querierpb.InstantVectorSeriesData{
						Floats:     mimirpb.FromFPointsToSamples(generateFPoints(1000, 2, 1)),
						Histograms: mimirpb.FromHPointsToHistograms(generateHPoints(3000, 2, 1)),
					},
				),
			},
			{
				msg: newBatchedInstantVectorSeriesData(
					querierpb.InstantVectorSeriesData{
						Floats:     mimirpb.FromFPointsToSamples(generateFPoints(1000, 2, 2)),
						Histograms: mimirpb.FromHPointsToHistograms(generateHPoints(3000, 2, 2)),
					},
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

	data, err = response.GetNextSeries(ctx)
	require.NoError(t, err)

	expectedData = types.InstantVectorSeriesData{
		Floats:     generateFPoints(1000, 2, 1),
		Histograms: generateHPoints(3000, 2, 1),
	}
	require.Equal(t, expectedData, data)
	require.NotZero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	types.PutInstantVectorSeriesData(data, memoryConsumptionTracker)
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes(), "should not be holding previous batch in memory consumption estimate")

	data, err = response.GetNextSeries(ctx)
	require.NoError(t, err)

	expectedData = types.InstantVectorSeriesData{
		Floats:     generateFPoints(1000, 2, 2),
		Histograms: generateHPoints(3000, 2, 2),
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

func TestInstantVectorExecutionResponse_PointSliceLengthNotAPowerOfTwo(t *testing.T) {
	stream := &mockResponseStream{
		responses: []mockResponse{
			{
				msg: newSeriesMetadata(
					false,
					labels.FromStrings("series", "1"),
				),
			},
			{
				msg: newInstantVectorSeriesData(
					generateFPoints(1000, 3, 0),
					generateHPoints(4000, 9, 0),
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
	}
	require.Equal(t, expectedSeries, series)
	require.NotZero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	types.SeriesMetadataSlicePool.Put(&series, memoryConsumptionTracker)
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

	data, err := response.GetNextSeries(ctx)
	require.NoError(t, err)

	expectedData := types.InstantVectorSeriesData{
		Floats:     generateFPoints(1000, 3, 0),
		Histograms: generateHPoints(4000, 9, 0),
	}
	require.Equal(t, expectedData, data)
	require.Equal(t, 4, cap(data.Floats), "should expand slice capacity to nearest power of two")
	require.Equal(t, 16, cap(data.Histograms), "should expand slice capacity to nearest power of two")
	require.NotZero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
	types.PutInstantVectorSeriesData(data, memoryConsumptionTracker)
	require.Zero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

	response.Close()
	require.True(t, stream.closed)
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

func TestRangeVectorExecutionResponse_PointSliceLengthNotAPowerOfTwo(t *testing.T) {
	stream := &mockResponseStream{
		responses: []mockResponse{
			{
				msg: newSeriesMetadata(
					false,
					labels.FromStrings("series", "1"),
				),
			},
			{
				msg: newRangeVectorStepData(
					0,
					1000,
					500,
					6000,
					generateFPoints(1000, 3, 0),
					generateHPoints(4000, 3, 0),
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
	require.Equal(t, int64(6000), data.RangeEnd)
	requireEqualFPointRingBuffer(t, data.Floats, generateFPoints(1000, 3, 0))
	requireEqualHPointRingBuffer(t, data.Histograms, generateHPoints(4000, 3, 0))
	require.NotZero(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

	response.Close()
	require.True(t, stream.closed)
	require.Zerof(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes(), "buffers should be released when closing response, have: %v", memoryConsumptionTracker.DescribeCurrentMemoryConsumption())
}

func TestEnsureFPointSliceCapacityIsPowerOfTwo(t *testing.T) {
	testCases := map[string]struct {
		input            []promql.FPoint
		expectedCapacity int
	}{
		"empty slice": {
			input:            nil,
			expectedCapacity: 0,
		},
		"slice with length 0 and capacity 0": {
			input:            make([]promql.FPoint, 0),
			expectedCapacity: 0,
		},
		"slice with length 0 and capacity 1": {
			input:            make([]promql.FPoint, 0, 1),
			expectedCapacity: 1,
		},
		"slice with length 1 and capacity 1": {
			input:            make([]promql.FPoint, 1),
			expectedCapacity: 1,
		},
		"slice with length 0 and capacity 2": {
			input:            make([]promql.FPoint, 0, 2),
			expectedCapacity: 2,
		},
		"slice with length 1 and capacity 2": {
			input:            make([]promql.FPoint, 1, 2),
			expectedCapacity: 2,
		},
		"slice with length 2 and capacity 2": {
			input:            make([]promql.FPoint, 2),
			expectedCapacity: 2,
		},
		"slice with length 2 and capacity 3": {
			input:            make([]promql.FPoint, 2, 3),
			expectedCapacity: 4,
		},
		"slice with length 3 and capacity 3": {
			input:            make([]promql.FPoint, 3),
			expectedCapacity: 4,
		},
		"slice with length 4 and capacity 4": {
			input:            make([]promql.FPoint, 4),
			expectedCapacity: 4,
		},
		"slice with length 5 and capacity 5": {
			input:            make([]promql.FPoint, 5),
			expectedCapacity: 8,
		},
		"slice with length 6 and capacity 6": {
			input:            make([]promql.FPoint, 6),
			expectedCapacity: 8,
		},
		"slice with length 7 and capacity 7": {
			input:            make([]promql.FPoint, 7),
			expectedCapacity: 8,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			for idx := range testCase.input {
				testCase.input[idx].T = int64(idx)
				testCase.input[idx].F = float64(idx * 10)
			}

			memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
			err := memoryConsumptionTracker.IncreaseMemoryConsumption(uint64(cap(testCase.input))*types.FPointSize, limiter.FPointSlices)
			require.NoError(t, err)

			output, err := ensureFPointSliceCapacityIsPowerOfTwo(testCase.input, memoryConsumptionTracker)
			require.NoError(t, err)
			require.Len(t, output, len(testCase.input), "output length should be the same as the provided slice")
			require.Equal(t, testCase.expectedCapacity, cap(output))
			require.Equal(t, testCase.input, output, "output should contain the same elements as the provided slice")

			require.Equal(t, uint64(testCase.expectedCapacity)*types.FPointSize, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

			if cap(testCase.input) == testCase.expectedCapacity {
				require.Equal(t, uint64(testCase.expectedCapacity)*types.FPointSize, memoryConsumptionTracker.PeakEstimatedMemoryConsumptionBytes(), "should not allocate a new slice if the provided slice already has a capacity that is a power of two")
			} else {
				require.Equal(t, uint64(testCase.expectedCapacity+cap(testCase.input))*types.FPointSize, memoryConsumptionTracker.PeakEstimatedMemoryConsumptionBytes())
			}
		})
	}
}

func TestEnsureHPointSliceCapacityIsPowerOfTwo(t *testing.T) {
	testCases := map[string]struct {
		input            []promql.HPoint
		expectedCapacity int
	}{
		"empty slice": {
			input:            nil,
			expectedCapacity: 0,
		},
		"slice with length 0 and capacity 0": {
			input:            make([]promql.HPoint, 0),
			expectedCapacity: 0,
		},
		"slice with length 0 and capacity 1": {
			input:            make([]promql.HPoint, 0, 1),
			expectedCapacity: 1,
		},
		"slice with length 1 and capacity 1": {
			input:            make([]promql.HPoint, 1),
			expectedCapacity: 1,
		},
		"slice with length 0 and capacity 2": {
			input:            make([]promql.HPoint, 0, 2),
			expectedCapacity: 2,
		},
		"slice with length 1 and capacity 2": {
			input:            make([]promql.HPoint, 1, 2),
			expectedCapacity: 2,
		},
		"slice with length 2 and capacity 2": {
			input:            make([]promql.HPoint, 2),
			expectedCapacity: 2,
		},
		"slice with length 2 and capacity 3": {
			input:            make([]promql.HPoint, 2, 3),
			expectedCapacity: 4,
		},
		"slice with length 3 and capacity 3": {
			input:            make([]promql.HPoint, 3),
			expectedCapacity: 4,
		},
		"slice with length 4 and capacity 4": {
			input:            make([]promql.HPoint, 4),
			expectedCapacity: 4,
		},
		"slice with length 5 and capacity 5": {
			input:            make([]promql.HPoint, 5),
			expectedCapacity: 8,
		},
		"slice with length 6 and capacity 6": {
			input:            make([]promql.HPoint, 6),
			expectedCapacity: 8,
		},
		"slice with length 7 and capacity 7": {
			input:            make([]promql.HPoint, 7),
			expectedCapacity: 8,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			for idx := range testCase.input {
				testCase.input[idx].T = int64(idx)
				testCase.input[idx].H = &histogram.FloatHistogram{Count: float64(idx * 10)}
			}

			memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
			err := memoryConsumptionTracker.IncreaseMemoryConsumption(uint64(cap(testCase.input))*types.HPointSize, limiter.HPointSlices)
			require.NoError(t, err)

			output, err := ensureHPointSliceCapacityIsPowerOfTwo(testCase.input, memoryConsumptionTracker)
			require.NoError(t, err)
			require.Len(t, output, len(testCase.input), "output length should be the same as the provided slice")
			require.Equal(t, testCase.expectedCapacity, cap(output))
			require.Equal(t, testCase.input, output, "output should contain the same elements as the provided slice")

			require.Equal(t, uint64(testCase.expectedCapacity)*types.HPointSize, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

			if cap(testCase.input) == testCase.expectedCapacity {
				require.Equal(t, uint64(testCase.expectedCapacity)*types.HPointSize, memoryConsumptionTracker.PeakEstimatedMemoryConsumptionBytes(), "should not allocate a new slice if the provided slice already has a capacity that is a power of two")
			} else {
				require.Equal(t, uint64(testCase.expectedCapacity+cap(testCase.input))*types.HPointSize, memoryConsumptionTracker.PeakEstimatedMemoryConsumptionBytes())
			}
		})
	}
}

func TestExecutionResponses_GetEvaluationInfo(t *testing.T) {
	responseCreators := map[string]func(stream responseStream, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) remoteexec.RemoteExecutionResponse{
		"scalar": func(stream responseStream, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) remoteexec.RemoteExecutionResponse {
			return &scalarExecutionResponse{stream, memoryConsumptionTracker}
		},
		"instant vector": func(stream responseStream, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) remoteexec.RemoteExecutionResponse {
			return newInstantVectorExecutionResponse(stream, memoryConsumptionTracker)
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
						Series: []querierpb.InstantVectorSeriesData{
							{
								Floats:     mimirpb.FromFPointsToSamples(floats),
								Histograms: mimirpb.FromHPointsToHistograms(histograms),
							},
						},
					},
				},
			},
		},
	}
}

func newBatchedInstantVectorSeriesData(series ...querierpb.InstantVectorSeriesData) *frontendv2pb.QueryResultStreamRequest {
	return &frontendv2pb.QueryResultStreamRequest{
		Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
			EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
				Message: &querierpb.EvaluateQueryResponse_InstantVectorSeriesData{
					InstantVectorSeriesData: &querierpb.EvaluateQueryResponseInstantVectorSeriesData{
						Series: series,
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

func TestRemoteExecutor_CorrectlyPassesQueriedTimeRangeAndUpdatesQueryStats(t *testing.T) {
	frontendMock := &timeRangeCapturingFrontend{}
	cfg := Config{LookBackDelta: 7 * time.Minute}
	executor := NewRemoteExecutor(frontendMock, cfg)

	endT := time.Now().Truncate(time.Minute).UTC()
	startT := endT.Add(-time.Hour)
	timeRange := types.NewRangeQueryTimeRange(startT, endT, time.Minute)

	stats, ctx := stats.ContextWithEmptyStats(context.Background())
	stats.AddRemoteExecutionRequests(12)
	node := &core.VectorSelector{VectorSelectorDetails: &core.VectorSelectorDetails{}}
	_, err := executor.startExecution(ctx, &planning.QueryPlan{}, node, timeRange, false, 1)
	require.NoError(t, err)

	require.Equal(t, startT.Add(-cfg.LookBackDelta+time.Millisecond), frontendMock.minT)
	require.Equal(t, endT, frontendMock.maxT)
	require.Equal(t, uint32(13), stats.LoadRemoteExecutionRequestCount())
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

func TestRemoteExecutor_SendsQueryPlanVersion(t *testing.T) {
	frontendMock := &requestCapturingFrontendMock{}
	executor := NewRemoteExecutor(frontendMock, Config{})

	ctx := context.Background()
	timeRange := types.NewInstantQueryTimeRange(time.Now())

	node := &nodeWithOverriddenVersion{
		Node: &nodeWithOverriddenVersion{
			Node:    &core.NumberLiteral{NumberLiteralDetails: &core.NumberLiteralDetails{Value: 1234}},
			version: 55,
		},
		version: 44,
	}

	fullPlan := &planning.QueryPlan{Version: 66}

	_, err := executor.startExecution(ctx, fullPlan, node, timeRange, false, 1)
	require.NoError(t, err)

	require.NotNil(t, frontendMock.request)
	require.IsType(t, &querierpb.EvaluateQueryRequest{}, frontendMock.request)
	request := frontendMock.request.(*querierpb.EvaluateQueryRequest)
	require.Equal(t, planning.QueryPlanVersion(66), request.Plan.Version, "should set request plan version to match the original plan version")
}

type nodeWithOverriddenVersion struct {
	planning.Node
	version planning.QueryPlanVersion
}

func (n *nodeWithOverriddenVersion) MinimumRequiredPlanVersion() planning.QueryPlanVersion {
	return n.version
}

type requestCapturingFrontendMock struct {
	request proto.Message
}

func (m *requestCapturingFrontendMock) DoProtobufRequest(ctx context.Context, req proto.Message, minT, maxT time.Time) (*ProtobufResponseStream, error) {
	m.request = req
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
	release    chan struct{}
	nextCalled chan struct{}
}

func (m *mockResponseStreamThatNeverReturns) Next(ctx context.Context) (*frontendv2pb.QueryResultStreamRequest, error) {
	if m.nextCalled != nil {
		close(m.nextCalled)
	}

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

func TestEagerLoadingResponseStream_ClosedWhileBuffering(t *testing.T) {
	inner := &mockResponseStreamThatNeverReturns{
		release:    make(chan struct{}),
		nextCalled: make(chan struct{}),
	}

	stream := newEagerLoadingResponseStream(context.Background(), inner)

	// Wait for the stream to start buffering.
	<-inner.nextCalled

	// Close the stream to trigger the inner Next() call returning.
	stream.Close()

	// Make sure the buffering goroutine has exited.
	test.VerifyNoLeak(t)

	// Make sure calling Next() fails with a sensible error.
	msg, err := stream.Next(context.Background())
	require.Nil(t, msg)
	require.ErrorContains(t, err, "stream closed")
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
	planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
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

			beforeLastMessageSent := func() {
				// Ideally we'd decrement the count the moment the last message is received by the frontend, but that's not possible.
				// If we decrement it later than that, the test might incorrectly fail because the frontend might have already received the last message,
				// so the frontend sending another request is OK.
				// If we decrement it slightly early like this, there's a small risk we won't catch a case where the frontend sends too many requests
				// in parallel.
				count.Dec()
			}

			// Simulate doing some work, then send an empty response.
			time.Sleep(20 * time.Millisecond)
			err := sendStreamingResponseWithErrorCapture(f, msg.UserID, msg.QueryID, beforeLastMessageSent, newSeriesMetadata(false), newEvaluationCompleted(0, nil, nil))
			require.NoError(t, err)
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
	_, err = querymiddleware.NewLimitedParallelismRoundTripper(handler, codec, limits, true, middleware).RoundTrip(httpRequest)
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

func BenchmarkProtobufResponses(b *testing.B) {
	const batchSize = 256

	for _, seriesCount := range []int{1, 2, 10, 100, 1000, 5000} {
		b.Run(fmt.Sprintf("series count=%v", seriesCount), func(b *testing.B) {
			for _, pointCount := range []int{0, 1, 10, 100} {
				b.Run(fmt.Sprintf("point count=%v", pointCount), func(b *testing.B) {
					runProtobufResponseBenchmark(b, seriesCount, pointCount, batchSize)
				})
			}
		})
	}
}

func runProtobufResponseBenchmark(b *testing.B, seriesCount int, pointCount int, batchSize int) {
	messages := generateBenchmarkResponse(seriesCount, pointCount, batchSize)
	encodedMessages := encode(b, messages)

	scheduler := func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		if msg.Type != schedulerpb.ENQUEUE {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: fmt.Sprintf("scheduler received unexpected message type: %v", msg.Type)}
		}

		go sendStreamingResponseFromEncodedMessages(b, f, msg.UserID, msg.QueryID, encodedMessages...)

		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	}

	frontend, _ := setupFrontendWithConcurrencyAndServerOptions(b, nil, scheduler, testFrontendWorkerConcurrency, log.NewNopLogger())

	ctx := user.InjectOrgID(context.Background(), "the-user")
	ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
	req := &querierpb.EvaluateQueryRequest{}
	minT := time.Now().Add(-time.Hour)
	maxT := time.Now()

	for b.Loop() {
		resp, err := frontend.DoProtobufRequest(ctx, req, minT, maxT)
		if err != nil {
			require.NoError(b, err)
		}

		for range messages {
			_, err := resp.Next(ctx)
			if err != nil {
				require.NoError(b, err)
			}
		}

		resp.Close()
	}
}

func generateBenchmarkResponse(seriesCount int, pointCount int, batchSize int) []*frontendv2pb.QueryResultStreamRequest {
	msgs := make([]*frontendv2pb.QueryResultStreamRequest, 0, seriesCount+2)

	series := make([]labels.Labels, 0, seriesCount)
	for i := range seriesCount {
		series = append(series, labels.FromStrings(model.MetricNameLabel, "my_metric", "idx", strconv.Itoa(i)))
	}

	msgs = append(msgs, newSeriesMetadata(false, series...))

	pendingSeriesData := make([]querierpb.InstantVectorSeriesData, 0, batchSize)
	appendSeriesDataMessage := func() {
		msgs = append(msgs, &frontendv2pb.QueryResultStreamRequest{
			Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
				EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
					Message: &querierpb.EvaluateQueryResponse_InstantVectorSeriesData{
						InstantVectorSeriesData: &querierpb.EvaluateQueryResponseInstantVectorSeriesData{
							Series: pendingSeriesData,
						},
					},
				},
			},
		})

		pendingSeriesData = make([]querierpb.InstantVectorSeriesData, 0, batchSize)
	}

	for seriesIdx := range seriesCount {
		floats := make([]mimirpb.Sample, 0, pointCount)

		for pointIdx := range pointCount {
			floats = append(floats, mimirpb.Sample{
				TimestampMs: int64(pointIdx),
				Value:       float64(seriesIdx*100000 + pointIdx),
			})
		}

		pendingSeriesData = append(pendingSeriesData, querierpb.InstantVectorSeriesData{
			Floats: floats,
		})

		if len(pendingSeriesData) == batchSize {
			appendSeriesDataMessage()
		}
	}

	if len(pendingSeriesData) > 0 {
		appendSeriesDataMessage()
	}

	msgs = append(msgs, newEvaluationCompleted(uint64(seriesCount*pointCount), nil, nil))

	return msgs
}

func encode(t testing.TB, msgs []*frontendv2pb.QueryResultStreamRequest) [][]byte {
	encoded := make([][]byte, 0, len(msgs))

	for _, msg := range msgs {
		b, err := msg.Marshal()
		require.NoError(t, err)

		encoded = append(encoded, b)
	}

	return encoded
}
