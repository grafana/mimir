// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	prototypes "github.com/gogo/protobuf/types"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/querierpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestErrorTypeForError(t *testing.T) {
	testCases := map[string]struct {
		err      error
		expected mimirpb.QueryErrorType
	}{
		"generic error": {
			err:      errors.New("something went wrong"),
			expected: mimirpb.QUERY_ERROR_TYPE_EXECUTION,
		},
		"context canceled": {
			err:      context.Canceled,
			expected: mimirpb.QUERY_ERROR_TYPE_CANCELED,
		},
		"context deadline exceeded": {
			err:      context.DeadlineExceeded,
			expected: mimirpb.QUERY_ERROR_TYPE_TIMEOUT,
		},
		"storage error": {
			err:      promql.ErrStorage{Err: errors.New("could not load data")},
			expected: mimirpb.QUERY_ERROR_TYPE_INTERNAL,
		},
		"apierror.APIError": {
			err:      apierror.New(apierror.TypeNotAcceptable, "request is not acceptable"),
			expected: mimirpb.QUERY_ERROR_TYPE_NOT_ACCEPTABLE,
		},
		// These types shouldn't be emitted by MQE, but we support them for consistency.
		"query canceled error": {
			err:      promql.ErrQueryCanceled("canceled"),
			expected: mimirpb.QUERY_ERROR_TYPE_CANCELED,
		},
		"query timeout error": {
			err:      promql.ErrQueryTimeout("timed out"),
			expected: mimirpb.QUERY_ERROR_TYPE_TIMEOUT,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, testCase.expected, errorTypeForError(testCase.err))
		})

		t.Run(name+" (wrapped)", func(t *testing.T) {
			err := fmt.Errorf("something went wrong one level down: %w", testCase.err)
			require.Equal(t, testCase.expected, errorTypeForError(err))
		})
	}
}

func TestDispatcher_HandleProtobuf(t *testing.T) {
	storage := promqltest.LoadedStorage(t, `
		load 10s
			my_series{"idx"="0"} 0+1x10
			my_series{"idx"="1"} 1+2x10
			my_other_series{"idx"="0"} 2+3x10
	`)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	opts := streamingpromql.NewTestEngineOpts()
	ctx := context.Background()
	planner, err := streamingpromql.NewQueryPlanner(opts)
	require.NoError(t, err)
	engine, err := streamingpromql.NewEngine(opts, streamingpromql.NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)

	createQueryRequestForSpecificNode := func(expr string, timeRange types.QueryTimeRange, nodeIndex int64) *prototypes.Any {
		plan, err := planner.NewQueryPlan(ctx, expr, timeRange, streamingpromql.NoopPlanningObserver{})
		require.NoError(t, err)

		encodedPlan, err := plan.ToEncodedPlan(false, true)
		require.NoError(t, err)

		if nodeIndex == -1 {
			nodeIndex = encodedPlan.RootNode
		}

		body := &querierpb.EvaluateQueryRequest{
			Plan: *encodedPlan,
			Nodes: []querierpb.EvaluationNode{
				{
					TimeRange: encodedPlan.TimeRange,
					NodeIndex: nodeIndex,
				},
			},
		}

		req, err := prototypes.MarshalAny(body)
		require.NoError(t, err)
		return req
	}

	createQueryRequest := func(expr string, timeRange types.QueryTimeRange) *prototypes.Any {
		return createQueryRequestForSpecificNode(expr, timeRange, -1)
	}

	dispatcher := NewDispatcher(opts.Logger, engine, storage)
	startT := timestamp.Time(0)

	testCases := map[string]struct {
		req                      *prototypes.Any
		expectedResponseMessages []*frontendv2pb.QueryResultStreamRequest
	}{
		"unknown payload type": {
			req: &prototypes.Any{
				TypeUrl: "grafana.com/something/unknown",
			},
			expectedResponseMessages: []*frontendv2pb.QueryResultStreamRequest{
				{
					Data: &frontendv2pb.QueryResultStreamRequest_Error{
						Error: &querierpb.Error{
							Type:    mimirpb.QUERY_ERROR_TYPE_BAD_DATA,
							Message: `unknown query request type "grafana.com/something/unknown"`,
						},
					},
				},
			},
		},

		"malformed payload type": {
			req: &prototypes.Any{
				TypeUrl: "unknown",
			},
			expectedResponseMessages: []*frontendv2pb.QueryResultStreamRequest{
				{
					Data: &frontendv2pb.QueryResultStreamRequest_Error{
						Error: &querierpb.Error{
							Type:    mimirpb.QUERY_ERROR_TYPE_BAD_DATA,
							Message: `malformed query request type "unknown": message type url "unknown" is invalid`,
						},
					},
				},
			},
		},

		"query that returns an instant vector": {
			req: createQueryRequest(`my_series + 0.123`, types.NewRangeQueryTimeRange(startT, startT.Add(20*time.Second), 10*time.Second)),
			expectedResponseMessages: []*frontendv2pb.QueryResultStreamRequest{
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_SeriesMetadata{
								SeriesMetadata: &querierpb.EvaluateQueryResponseSeriesMetadata{
									NodeIndex: 3,
									Series: []querierpb.SeriesMetadata{
										{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("idx", "0"))},
										{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("idx", "1"))},
									},
								},
							},
						},
					},
				},
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_InstantVectorSeriesData{
								InstantVectorSeriesData: &querierpb.EvaluateQueryResponseInstantVectorSeriesData{
									NodeIndex: 3,
									Floats: []mimirpb.Sample{
										{TimestampMs: 0, Value: 0.123},
										{TimestampMs: 10_000, Value: 1.123},
										{TimestampMs: 20_000, Value: 2.123},
									},
								},
							},
						},
					},
				},
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_InstantVectorSeriesData{
								InstantVectorSeriesData: &querierpb.EvaluateQueryResponseInstantVectorSeriesData{
									NodeIndex: 3,
									Floats: []mimirpb.Sample{
										{TimestampMs: 0, Value: 1.123},
										{TimestampMs: 10_000, Value: 3.123},
										{TimestampMs: 20_000, Value: 5.123},
									},
								},
							},
						},
					},
				},
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_EvaluationCompleted{
								EvaluationCompleted: &querierpb.EvaluateQueryResponseEvaluationCompleted{
									Stats: querierpb.QueryStats{
										TotalSamples: 6,
									},
								},
							},
						},
					},
				},
			},
		},

		"query that returns a range vector": {
			req: createQueryRequestForSpecificNode(
				`max_over_time(my_series[11s:10s])`,
				types.NewRangeQueryTimeRange(startT, startT.Add(20*time.Second), 10*time.Second),
				1, // Evaluate the subquery expression (my_series[11s:10s])
			),
			expectedResponseMessages: []*frontendv2pb.QueryResultStreamRequest{
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_SeriesMetadata{
								SeriesMetadata: &querierpb.EvaluateQueryResponseSeriesMetadata{
									NodeIndex: 1,
									Series: []querierpb.SeriesMetadata{
										{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(labels.MetricName, "my_series", "idx", "0"))},
										{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(labels.MetricName, "my_series", "idx", "1"))},
									},
								},
							},
						},
					},
				},
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_RangeVectorStepData{
								RangeVectorStepData: &querierpb.EvaluateQueryResponseRangeVectorStepData{
									NodeIndex:   1,
									SeriesIndex: 0,
									StepT:       0,
									RangeStart:  -11_000,
									RangeEnd:    0,
									Floats: []mimirpb.Sample{
										{TimestampMs: 0, Value: 0},
									},
								},
							},
						},
					},
				},
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_RangeVectorStepData{
								RangeVectorStepData: &querierpb.EvaluateQueryResponseRangeVectorStepData{
									NodeIndex:   1,
									SeriesIndex: 0,
									StepT:       10_000,
									RangeStart:  -1_000,
									RangeEnd:    10_000,
									Floats: []mimirpb.Sample{
										{TimestampMs: 0, Value: 0},
										{TimestampMs: 10_000, Value: 1},
									},
								},
							},
						},
					},
				},
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_RangeVectorStepData{
								RangeVectorStepData: &querierpb.EvaluateQueryResponseRangeVectorStepData{
									NodeIndex:   1,
									SeriesIndex: 0,
									StepT:       20_000,
									RangeStart:  9_000,
									RangeEnd:    20_000,
									Floats: []mimirpb.Sample{
										{TimestampMs: 10_000, Value: 1},
										{TimestampMs: 20_000, Value: 2},
									},
								},
							},
						},
					},
				},
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_RangeVectorStepData{
								RangeVectorStepData: &querierpb.EvaluateQueryResponseRangeVectorStepData{
									NodeIndex:   1,
									SeriesIndex: 1,
									StepT:       0,
									RangeStart:  -11_000,
									RangeEnd:    0,
									Floats: []mimirpb.Sample{
										{TimestampMs: 0, Value: 1},
									},
								},
							},
						},
					},
				},
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_RangeVectorStepData{
								RangeVectorStepData: &querierpb.EvaluateQueryResponseRangeVectorStepData{
									NodeIndex:   1,
									SeriesIndex: 1,
									StepT:       10_000,
									RangeStart:  -1_000,
									RangeEnd:    10_000,
									Floats: []mimirpb.Sample{
										{TimestampMs: 0, Value: 1},
										{TimestampMs: 10_000, Value: 3},
									},
								},
							},
						},
					},
				},
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_RangeVectorStepData{
								RangeVectorStepData: &querierpb.EvaluateQueryResponseRangeVectorStepData{
									NodeIndex:   1,
									SeriesIndex: 1,
									StepT:       20_000,
									RangeStart:  9_000,
									RangeEnd:    20_000,
									Floats: []mimirpb.Sample{
										{TimestampMs: 10_000, Value: 3},
										{TimestampMs: 20_000, Value: 5},
									},
								},
							},
						},
					},
				},
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_EvaluationCompleted{
								EvaluationCompleted: &querierpb.EvaluateQueryResponseEvaluationCompleted{
									Stats: querierpb.QueryStats{
										TotalSamples: 10,
									},
								},
							},
						},
					},
				},
			},
		},

		"query that returns a scalar": {
			req: createQueryRequest(`time() + 0.123`, types.NewRangeQueryTimeRange(startT, startT.Add(20*time.Second), 10*time.Second)),
			expectedResponseMessages: []*frontendv2pb.QueryResultStreamRequest{
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_ScalarValue{
								ScalarValue: &querierpb.EvaluateQueryResponseScalarValue{
									NodeIndex: 2,
									Values: []mimirpb.Sample{
										{TimestampMs: 0, Value: 0.123},
										{TimestampMs: 10_000, Value: 10.123},
										{TimestampMs: 20_000, Value: 20.123},
									},
								},
							},
						},
					},
				},
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_EvaluationCompleted{
								EvaluationCompleted: &querierpb.EvaluateQueryResponseEvaluationCompleted{},
							},
						},
					},
				},
			},
		},

		"query that returns a string": {
			req: createQueryRequest(`"the string"`, types.NewInstantQueryTimeRange(startT)),
			expectedResponseMessages: []*frontendv2pb.QueryResultStreamRequest{
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_StringValue{
								StringValue: &querierpb.EvaluateQueryResponseStringValue{
									NodeIndex: 0,
									Value:     "the string",
								},
							},
						},
					},
				},
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_EvaluationCompleted{
								EvaluationCompleted: &querierpb.EvaluateQueryResponseEvaluationCompleted{},
							},
						},
					},
				},
			},
		},

		"query that returns annotations": {
			req: createQueryRequest(`sum by (idx) (rate(my_series{idx="0"}[11s])) + quantile by (idx) (2, my_series{idx="0"})`, types.NewInstantQueryTimeRange(startT.Add(30*time.Second))),
			expectedResponseMessages: []*frontendv2pb.QueryResultStreamRequest{
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_SeriesMetadata{
								SeriesMetadata: &querierpb.EvaluateQueryResponseSeriesMetadata{
									NodeIndex: 7,
									Series: []querierpb.SeriesMetadata{
										{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("idx", "0"))},
									},
								},
							},
						},
					},
				},
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_InstantVectorSeriesData{
								InstantVectorSeriesData: &querierpb.EvaluateQueryResponseInstantVectorSeriesData{
									NodeIndex: 7,
									Floats: []mimirpb.Sample{
										{TimestampMs: 30_000, Value: math.Inf(1)},
									},
								},
							},
						},
					},
				},
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_EvaluationCompleted{
								EvaluationCompleted: &querierpb.EvaluateQueryResponseEvaluationCompleted{
									Annotations: querierpb.Annotations{
										Infos:    []string{`PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "my_series" (1:20)`},
										Warnings: []string{`PromQL warning: quantile value should be between 0 and 1, got 2 (1:67)`},
									},
									Stats: querierpb.QueryStats{
										TotalSamples: 3,
									},
								},
							},
						},
					},
				},
			},
		},

		"query that fails with an error": {
			req: createQueryRequest(`abs({__name__=~"my_.*"})`, types.NewInstantQueryTimeRange(startT)),
			expectedResponseMessages: []*frontendv2pb.QueryResultStreamRequest{
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_SeriesMetadata{
								SeriesMetadata: &querierpb.EvaluateQueryResponseSeriesMetadata{
									NodeIndex: 2,
									Series: []querierpb.SeriesMetadata{
										{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("idx", "0"))},
										{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("idx", "1"))},
									},
								},
							},
						},
					},
				},
				{
					Data: &frontendv2pb.QueryResultStreamRequest_Error{
						Error: &querierpb.Error{
							Type:    mimirpb.QUERY_ERROR_TYPE_EXECUTION,
							Message: `vector cannot contain metrics with the same labelset`,
						},
					},
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			stream := &mockQueryResultStream{t: t}
			dispatcher.HandleProtobuf(ctx, testCase.req, stream)
			require.Equal(t, testCase.expectedResponseMessages, stream.messages)
		})
	}
}

type mockQueryResultStream struct {
	t        testing.TB
	messages []*frontendv2pb.QueryResultStreamRequest
}

func (m *mockQueryResultStream) Write(_ context.Context, request *frontendv2pb.QueryResultStreamRequest) error {
	// Encode and decode the messages, so that we don't retain references to any slices that are returned to a pool.
	encoded, err := proto.Marshal(request)
	require.NoError(m.t, err)

	decoded := &frontendv2pb.QueryResultStreamRequest{}
	err = proto.Unmarshal(encoded, decoded)
	require.NoError(m.t, err)

	m.messages = append(m.messages, decoded)
	return nil
}

func TestDispatcher_MQEDisabled(t *testing.T) {
	dispatcher := NewDispatcher(log.NewNopLogger(), nil, nil)

	req, err := prototypes.MarshalAny(&querierpb.EvaluateQueryRequest{})
	require.NoError(t, err)

	stream := &mockQueryResultStream{t: t}
	dispatcher.HandleProtobuf(context.Background(), req, stream)

	expected := []*frontendv2pb.QueryResultStreamRequest{
		{
			Data: &frontendv2pb.QueryResultStreamRequest_Error{
				Error: &querierpb.Error{
					Type:    mimirpb.QUERY_ERROR_TYPE_NOT_FOUND,
					Message: `MQE is not enabled on this querier`,
				},
			},
		},
	}
	require.Equal(t, expected, stream.messages)
}
