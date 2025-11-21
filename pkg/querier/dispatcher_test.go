// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	prototypes "github.com/gogo/protobuf/types"
	"github.com/grafana/dskit/server"
	"github.com/grafana/dskit/user"
	"github.com/grafana/regexp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/querierpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/propagation"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

func TestDispatcher_HandleProtobuf(t *testing.T) {
	storage := promqltest.LoadedStorage(t, `
		load 10s
			my_series{"idx"="0"} 0+1x10
			my_series{"idx"="1"} 1+2x10
			my_other_series{"idx"="0"} 2+3x10
			my_three_item_series{"idx"="0"} 3+4x10
			my_three_item_series{"idx"="1"} 4+5x10
			my_three_item_series{"idx"="2"} 5+6x10
	`)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	opts := streamingpromql.NewTestEngineOpts()
	opts.Pedantic = true
	ctx := context.Background()
	planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	engine, err := streamingpromql.NewEngine(opts, streamingpromql.NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)

	createQueryRequestForSpecificNode := func(expr string, timeRange types.QueryTimeRange, nodeIndex int64, batchSize uint64) *prototypes.Any {
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
			BatchSize: batchSize,
		}

		req, err := prototypes.MarshalAny(body)
		require.NoError(t, err)
		return req
	}

	createQueryRequest := func(expr string, timeRange types.QueryTimeRange) *prototypes.Any {
		return createQueryRequestForSpecificNode(expr, timeRange, -1, 1)
	}

	createQueryRequestWithBatchSize := func(expr string, timeRange types.QueryTimeRange, batchSize uint64) *prototypes.Any {
		return createQueryRequestForSpecificNode(expr, timeRange, -1, batchSize)
	}

	startT := timestamp.Time(0)
	expectedQueryWallTime := 2 * time.Second

	testCases := map[string]struct {
		req                                          *prototypes.Any
		dontSetTenantID                              bool
		expectedResponseMessages                     []*frontendv2pb.QueryResultStreamRequest
		expectedStatusCode                           string
		expectStorageToBeCalledWithPropagatedHeaders bool
		dontExpectQueryPlanVersionMetric             bool
	}{
		"unknown payload type": {
			req: &prototypes.Any{
				TypeUrl: "grafana.com/something/unknown",
			},
			dontExpectQueryPlanVersionMetric: true,
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
			expectedStatusCode: "ERROR_BAD_DATA",
		},

		"malformed payload type": {
			req: &prototypes.Any{
				TypeUrl: "unknown",
			},
			dontSetTenantID:                  true,
			dontExpectQueryPlanVersionMetric: true,
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
			expectedStatusCode: "ERROR_BAD_DATA",
		},

		"request without tenant ID": {
			req:                              createQueryRequest(`my_series`, types.NewInstantQueryTimeRange(startT)),
			dontSetTenantID:                  true,
			dontExpectQueryPlanVersionMetric: true,
			expectedResponseMessages: []*frontendv2pb.QueryResultStreamRequest{
				{
					Data: &frontendv2pb.QueryResultStreamRequest_Error{
						Error: &querierpb.Error{
							Type:    mimirpb.QUERY_ERROR_TYPE_BAD_DATA,
							Message: `no org id`,
						},
					},
				},
			},
			expectedStatusCode: "ERROR_BAD_DATA",
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
									Series: []querierpb.InstantVectorSeriesData{
										{
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
					},
				},
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_InstantVectorSeriesData{
								InstantVectorSeriesData: &querierpb.EvaluateQueryResponseInstantVectorSeriesData{
									NodeIndex: 3,
									Series: []querierpb.InstantVectorSeriesData{
										{
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
					},
				},
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_EvaluationCompleted{
								EvaluationCompleted: &querierpb.EvaluateQueryResponseEvaluationCompleted{
									Stats: stats.Stats{
										SamplesProcessed:   6,
										QueueTime:          3 * time.Second,
										WallTime:           expectedQueryWallTime,
										FetchedSeriesCount: 123,
										FetchedChunksCount: 456,
										FetchedChunkBytes:  789,
									},
								},
							},
						},
					},
				},
			},
			expectedStatusCode:                           "OK",
			expectStorageToBeCalledWithPropagatedHeaders: true,
		},

		"query that returns an instant vector with batching, where all series fit into one batch with space to spare": {
			req: createQueryRequestWithBatchSize(`my_three_item_series + 0.123`, types.NewRangeQueryTimeRange(startT, startT.Add(20*time.Second), 10*time.Second), 4),
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
										{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("idx", "2"))},
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
									Series: []querierpb.InstantVectorSeriesData{
										{
											Floats: []mimirpb.Sample{
												{TimestampMs: 0, Value: 3.123},
												{TimestampMs: 10_000, Value: 7.123},
												{TimestampMs: 20_000, Value: 11.123},
											},
										},
										{
											Floats: []mimirpb.Sample{
												{TimestampMs: 0, Value: 4.123},
												{TimestampMs: 10_000, Value: 9.123},
												{TimestampMs: 20_000, Value: 14.123},
											},
										},
										{
											Floats: []mimirpb.Sample{
												{TimestampMs: 0, Value: 5.123},
												{TimestampMs: 10_000, Value: 11.123},
												{TimestampMs: 20_000, Value: 17.123},
											},
										},
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
									Stats: stats.Stats{
										SamplesProcessed:   9,
										QueueTime:          3 * time.Second,
										WallTime:           expectedQueryWallTime,
										FetchedSeriesCount: 123,
										FetchedChunksCount: 456,
										FetchedChunkBytes:  789,
									},
								},
							},
						},
					},
				},
			},
			expectedStatusCode:                           "OK",
			expectStorageToBeCalledWithPropagatedHeaders: true,
		},

		"query that returns an instant vector with batching, where all series fit exactly into one batch": {
			req: createQueryRequestWithBatchSize(`my_three_item_series + 0.123`, types.NewRangeQueryTimeRange(startT, startT.Add(20*time.Second), 10*time.Second), 3),
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
										{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("idx", "2"))},
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
									Series: []querierpb.InstantVectorSeriesData{
										{
											Floats: []mimirpb.Sample{
												{TimestampMs: 0, Value: 3.123},
												{TimestampMs: 10_000, Value: 7.123},
												{TimestampMs: 20_000, Value: 11.123},
											},
										},
										{
											Floats: []mimirpb.Sample{
												{TimestampMs: 0, Value: 4.123},
												{TimestampMs: 10_000, Value: 9.123},
												{TimestampMs: 20_000, Value: 14.123},
											},
										},
										{
											Floats: []mimirpb.Sample{
												{TimestampMs: 0, Value: 5.123},
												{TimestampMs: 10_000, Value: 11.123},
												{TimestampMs: 20_000, Value: 17.123},
											},
										},
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
									Stats: stats.Stats{
										SamplesProcessed:   9,
										QueueTime:          3 * time.Second,
										WallTime:           expectedQueryWallTime,
										FetchedSeriesCount: 123,
										FetchedChunksCount: 456,
										FetchedChunkBytes:  789,
									},
								},
							},
						},
					},
				},
			},
			expectedStatusCode:                           "OK",
			expectStorageToBeCalledWithPropagatedHeaders: true,
		},

		"query that returns an instant vector with batching, where the last batch is not completely full": {
			req: createQueryRequestWithBatchSize(`my_three_item_series + 0.123`, types.NewRangeQueryTimeRange(startT, startT.Add(20*time.Second), 10*time.Second), 2),
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
										{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("idx", "2"))},
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
									Series: []querierpb.InstantVectorSeriesData{
										{
											Floats: []mimirpb.Sample{
												{TimestampMs: 0, Value: 3.123},
												{TimestampMs: 10_000, Value: 7.123},
												{TimestampMs: 20_000, Value: 11.123},
											},
										},
										{
											Floats: []mimirpb.Sample{
												{TimestampMs: 0, Value: 4.123},
												{TimestampMs: 10_000, Value: 9.123},
												{TimestampMs: 20_000, Value: 14.123},
											},
										},
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
									Series: []querierpb.InstantVectorSeriesData{
										{
											Floats: []mimirpb.Sample{
												{TimestampMs: 0, Value: 5.123},
												{TimestampMs: 10_000, Value: 11.123},
												{TimestampMs: 20_000, Value: 17.123},
											},
										},
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
									Stats: stats.Stats{
										SamplesProcessed:   9,
										QueueTime:          3 * time.Second,
										WallTime:           expectedQueryWallTime,
										FetchedSeriesCount: 123,
										FetchedChunksCount: 456,
										FetchedChunkBytes:  789,
									},
								},
							},
						},
					},
				},
			},
			expectedStatusCode:                           "OK",
			expectStorageToBeCalledWithPropagatedHeaders: true,
		},

		"query that returns a range vector": {
			req: createQueryRequestForSpecificNode(
				`max_over_time(my_series[11s:10s])`,
				types.NewRangeQueryTimeRange(startT, startT.Add(20*time.Second), 10*time.Second),
				1, // Evaluate the subquery expression (my_series[11s:10s])
				1,
			),
			expectedResponseMessages: []*frontendv2pb.QueryResultStreamRequest{
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_SeriesMetadata{
								SeriesMetadata: &querierpb.EvaluateQueryResponseSeriesMetadata{
									NodeIndex: 1,
									Series: []querierpb.SeriesMetadata{
										{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(model.MetricNameLabel, "my_series", "idx", "0"))},
										{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(model.MetricNameLabel, "my_series", "idx", "1"))},
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
									Stats: stats.Stats{
										SamplesProcessed:   6,
										QueueTime:          3 * time.Second,
										WallTime:           expectedQueryWallTime,
										FetchedSeriesCount: 123,
										FetchedChunksCount: 456,
										FetchedChunkBytes:  789,
									},
								},
							},
						},
					},
				},
			},
			expectedStatusCode:                           "OK",
			expectStorageToBeCalledWithPropagatedHeaders: true,
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
								EvaluationCompleted: &querierpb.EvaluateQueryResponseEvaluationCompleted{
									Stats: stats.Stats{
										QueueTime:          3 * time.Second,
										WallTime:           expectedQueryWallTime,
										FetchedSeriesCount: 123,
										FetchedChunksCount: 456,
										FetchedChunkBytes:  789,
									},
								},
							},
						},
					},
				},
			},
			expectedStatusCode: "OK",
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
								EvaluationCompleted: &querierpb.EvaluateQueryResponseEvaluationCompleted{
									Stats: stats.Stats{
										QueueTime:          3 * time.Second,
										WallTime:           expectedQueryWallTime,
										FetchedSeriesCount: 123,
										FetchedChunksCount: 456,
										FetchedChunkBytes:  789,
									},
								},
							},
						},
					},
				},
			},
			expectedStatusCode: "OK",
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
									Series: []querierpb.InstantVectorSeriesData{
										{
											Floats: []mimirpb.Sample{
												{TimestampMs: 30_000, Value: math.Inf(1)},
											},
										},
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
									Stats: stats.Stats{
										SamplesProcessed:   3,
										QueueTime:          3 * time.Second,
										WallTime:           expectedQueryWallTime,
										FetchedSeriesCount: 123,
										FetchedChunksCount: 456,
										FetchedChunkBytes:  789,
									},
								},
							},
						},
					},
				},
			},
			expectedStatusCode:                           "OK",
			expectStorageToBeCalledWithPropagatedHeaders: true,
		},

		"query that fails with an error": {
			req: createQueryRequest(`abs({__name__=~"(my_series|my_other_series)"})`, types.NewInstantQueryTimeRange(startT)),
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
			expectedStatusCode:                           "ERROR_EXECUTION",
			expectStorageToBeCalledWithPropagatedHeaders: true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			stats, ctx := stats.ContextWithEmptyStats(context.Background())

			// This value should be set by the querier's scheduler processor (ie. the thing that calls Dispatcher.HandleProtobuf).
			stats.QueueTime = 3 * time.Second

			// These values are populated by the ingester and store-gateway Queryable implementations, which
			// aren't used in this test, so emulate them here.
			stats.FetchedSeriesCount = 123
			stats.FetchedChunksCount = 456
			stats.FetchedChunkBytes = 789

			tenantID := ""

			if !testCase.dontSetTenantID {
				tenantID = "tenant-1"
				ctx = user.InjectOrgID(ctx, tenantID)
			}

			route, err := prototypes.AnyMessageName(testCase.req)
			if err != nil {
				route = "<invalid>"
			}

			reg, requestMetrics, serverMetrics := newMetrics()
			stream := &mockQueryResultStream{t: t, route: route, reg: reg}
			storage := &contextCapturingStorage{inner: storage}
			dispatcher := NewDispatcher(engine, storage, requestMetrics, serverMetrics, &testExtractor{}, opts.Logger)
			dispatcher.timeNow = replaceTimeNow(timestamp.Time(4000), timestamp.Time(4000).Add(expectedQueryWallTime))
			metadata := &propagation.MapCarrier{}
			metadata.Add(testExtractorHeaderName, "some-value-from-the-request")

			dispatcher.HandleProtobuf(ctx, testCase.req, metadata, stream)
			require.Equal(t, testCase.expectedResponseMessages, stream.messages)

			expectedMetrics := fmt.Sprintf(`
				# HELP cortex_inflight_requests Current number of inflight requests.
				# TYPE cortex_inflight_requests gauge
				cortex_inflight_requests{method="gRPC",route="%[1]s"} 0

				# HELP cortex_per_tenant_request_duration_seconds Time (in seconds) spent serving HTTP requests for a particular tenant.
				# TYPE cortex_per_tenant_request_duration_seconds histogram
				cortex_per_tenant_request_duration_seconds_count{method="gRPC",route="%[1]s",status_code="%[2]s",tenant="%[6]s",ws="false"} 1
				# HELP cortex_per_tenant_request_total Total count of requests for a particular tenant.
				# TYPE cortex_per_tenant_request_total counter
				cortex_per_tenant_request_total{method="gRPC",route="%[1]s",status_code="%[2]s",tenant="%[6]s",ws="false"} 1

				# HELP cortex_querier_inflight_requests Current number of inflight requests to the querier.
				# TYPE cortex_querier_inflight_requests gauge
				cortex_querier_inflight_requests{method="gRPC",route="%[1]s"} 0
				# HELP cortex_querier_request_duration_seconds Time (in seconds) spent serving HTTP requests to the querier.
				# TYPE cortex_querier_request_duration_seconds histogram
				cortex_querier_request_duration_seconds_count{method="gRPC",route="%[1]s",status_code="%[2]s",ws="false"} 1
				# HELP cortex_querier_request_message_bytes Size (in bytes) of messages received in the request to the querier.
				# TYPE cortex_querier_request_message_bytes histogram
				cortex_querier_request_message_bytes_sum{method="gRPC",route="%[1]s"} %[3]v
				cortex_querier_request_message_bytes_count{method="gRPC",route="%[1]s"} 1
				# HELP cortex_querier_response_message_bytes Size (in bytes) of messages sent in response by the querier.
				# TYPE cortex_querier_response_message_bytes histogram
				cortex_querier_response_message_bytes_sum{method="gRPC",route="%[1]s"} %[4]v
				cortex_querier_response_message_bytes_count{method="gRPC",route="%[1]s"} %[5]v

				# HELP cortex_request_duration_seconds Time (in seconds) spent serving HTTP requests.
				# TYPE cortex_request_duration_seconds histogram
				cortex_request_duration_seconds_count{method="gRPC",route="%[1]s",status_code="%[2]s",ws="false"} 1
				# HELP cortex_request_message_bytes Size (in bytes) of messages received in the request.
				# TYPE cortex_request_message_bytes histogram
				cortex_request_message_bytes_sum{method="gRPC",route="%[1]s"} %[3]v
				cortex_request_message_bytes_count{method="gRPC",route="%[1]s"} 1
				# HELP cortex_response_message_bytes Size (in bytes) of messages sent in response.
				# TYPE cortex_response_message_bytes histogram
				cortex_response_message_bytes_sum{method="gRPC",route="%[1]s"} %[4]v
				cortex_response_message_bytes_count{method="gRPC",route="%[1]s"} %[5]v
			`, route, testCase.expectedStatusCode, len(testCase.req.Value), totalResponseBytes(testCase.expectedResponseMessages), len(testCase.expectedResponseMessages), tenantID)

			metricNames := []string{
				"cortex_inflight_requests",
				"cortex_per_tenant_request_duration_seconds",
				"cortex_per_tenant_request_total",
				"cortex_querier_request_duration_seconds",
				"cortex_querier_request_message_bytes",
				"cortex_querier_response_message_bytes",
				"cortex_querier_inflight_requests",
				"cortex_request_duration_seconds",
				"cortex_request_message_bytes",
				"cortex_response_message_bytes",
			}

			requireMetricsIgnoringHistogramBucketsAndDurationSums(t, reg, expectedMetrics, metricNames...)

			if testCase.expectStorageToBeCalledWithPropagatedHeaders {
				require.NotNil(t, storage.ctx)
				require.Equal(t, "some-value-from-the-request", storage.ctx.Value(testExtractorKey))
			}

			if !testCase.dontExpectQueryPlanVersionMetric {
				req := &querierpb.EvaluateQueryRequest{}
				require.NoError(t, prototypes.UnmarshalAny(testCase.req, req))

				expectedVersion := req.Plan.Version

				expectedMetrics := fmt.Sprintf(`
					# HELP cortex_querier_received_query_plans_total Total number of query plans received by the querier.
					# TYPE cortex_querier_received_query_plans_total counter
					cortex_querier_received_query_plans_total{version="%[1]d"} 1
				`, expectedVersion)

				require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), "cortex_querier_received_query_plans_total"))
			}
		})
	}
}

func TestDispatcher_HandleProtobuf_WithDelayedNameRemovalEnabled(t *testing.T) {
	storage := promqltest.LoadedStorage(t, `
		load 1s
			some_total{"idx"="0"} 0+1x10
	`)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	opts := streamingpromql.NewTestEngineOpts()
	opts.CommonOpts.EnableDelayedNameRemoval = true
	// Disable the optimization pass, since it requires delayed name removal to be enabled.
	opts.EnableEliminateDeduplicateAndMerge = false
	ctx := context.Background()
	planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	engine, err := streamingpromql.NewEngine(opts, streamingpromql.NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)

	createQueryRequestForSpecificNode := func(expr string, timeRange types.QueryTimeRange, nodeIndex int64) *prototypes.Any {
		plan, err := planner.NewQueryPlan(ctx, expr, timeRange, streamingpromql.NoopPlanningObserver{})
		require.NoError(t, err)

		encodedPlan, err := plan.ToEncodedPlan(false, true)
		require.NoError(t, err)

		body := &querierpb.EvaluateQueryRequest{
			Plan: *encodedPlan,
			Nodes: []querierpb.EvaluationNode{
				{
					TimeRange: encodedPlan.TimeRange,
					NodeIndex: nodeIndex,
				},
			},
			BatchSize: 1,
		}

		req, err := prototypes.MarshalAny(body)
		require.NoError(t, err)
		return req
	}

	startT := timestamp.Time(0)
	expectedQueryWallTime := 3 * time.Second

	testCases := map[string]struct {
		req                      *prototypes.Any
		expectedResponseMessages []*frontendv2pb.QueryResultStreamRequest
	}{
		"inner part of query": {
			req: createQueryRequestForSpecificNode(
				`rate(some_total[5s])`,
				types.NewInstantQueryTimeRange(startT.Add(9*time.Second)),
				1, // Evaluate the rate() directly, rather than the root node, which is the deduplicate and merge operation that removes the metric name.
			),
			expectedResponseMessages: []*frontendv2pb.QueryResultStreamRequest{
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_SeriesMetadata{
								SeriesMetadata: &querierpb.EvaluateQueryResponseSeriesMetadata{
									NodeIndex: 1,
									Series: []querierpb.SeriesMetadata{
										{DropName: true, Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(model.MetricNameLabel, "some_total", "idx", "0"))},
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
									NodeIndex: 1,
									Series: []querierpb.InstantVectorSeriesData{
										{
											Floats: []mimirpb.Sample{
												{TimestampMs: 9_000, Value: 1},
											},
										},
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
									Stats: stats.Stats{
										SamplesProcessed: 5,
										WallTime:         expectedQueryWallTime,
									},
								},
							},
						},
					},
				},
			},
		},
		"root of query": {
			req: createQueryRequestForSpecificNode(
				`rate(some_total[5s])`,
				types.NewInstantQueryTimeRange(startT.Add(9*time.Second)),
				2, // The root of the query (0=selector, 1=rate(), 2=deduplicate and merge operation).
			),
			expectedResponseMessages: []*frontendv2pb.QueryResultStreamRequest{
				{
					Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
						EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
							Message: &querierpb.EvaluateQueryResponse_SeriesMetadata{
								SeriesMetadata: &querierpb.EvaluateQueryResponseSeriesMetadata{
									NodeIndex: 2,
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
									NodeIndex: 2,
									Series: []querierpb.InstantVectorSeriesData{
										{
											Floats: []mimirpb.Sample{
												{TimestampMs: 9_000, Value: 1},
											},
										},
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
									Stats: stats.Stats{
										SamplesProcessed: 5,
										WallTime:         expectedQueryWallTime,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			tenantID := "tenant-1"
			ctx = user.InjectOrgID(context.Background(), tenantID)
			_, ctx := stats.ContextWithEmptyStats(ctx)
			route, err := prototypes.AnyMessageName(testCase.req)
			require.NoError(t, err)

			reg, requestMetrics, serverMetrics := newMetrics()
			stream := &mockQueryResultStream{t: t, route: route, reg: reg}
			dispatcher := NewDispatcher(engine, storage, requestMetrics, serverMetrics, &propagation.NoopExtractor{}, opts.Logger)
			dispatcher.timeNow = replaceTimeNow(timestamp.Time(4000), timestamp.Time(4000).Add(expectedQueryWallTime))

			dispatcher.HandleProtobuf(ctx, testCase.req, propagation.MapCarrier{}, stream)
			require.Equal(t, testCase.expectedResponseMessages, stream.messages)
		})
	}
}

func totalResponseBytes(msgs []*frontendv2pb.QueryResultStreamRequest) int {
	total := 0

	for _, msg := range msgs {
		total += msg.Size()
	}

	return total
}

type mockQueryResultStream struct {
	t        testing.TB
	route    string
	reg      *prometheus.Registry
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

	// Ensure the inflight requests metric has been incremented.
	expectedMetrics := fmt.Sprintf(`
		# HELP cortex_querier_inflight_requests Current number of inflight requests to the querier.
		# TYPE cortex_querier_inflight_requests gauge
		cortex_querier_inflight_requests{method="gRPC", route="%[1]s"} 1
		# HELP cortex_inflight_requests Current number of inflight requests.
		# TYPE cortex_inflight_requests gauge
		cortex_inflight_requests{method="gRPC", route="%[1]s"} 1
	`, m.route)
	require.NoError(m.t, testutil.GatherAndCompare(m.reg, strings.NewReader(expectedMetrics), "cortex_querier_inflight_requests", "cortex_inflight_requests"))

	return nil
}

func TestDispatcher_MQEDisabled(t *testing.T) {
	reg, requestMetrics, serverMetrics := newMetrics()
	dispatcher := NewDispatcher(nil, nil, requestMetrics, serverMetrics, &propagation.NoopExtractor{}, log.NewNopLogger())

	req, err := prototypes.MarshalAny(&querierpb.EvaluateQueryRequest{})
	require.NoError(t, err)

	stream := &mockQueryResultStream{t: t, route: "querierpb.EvaluateQueryRequest", reg: reg}
	ctx := user.InjectOrgID(context.Background(), "test")
	dispatcher.HandleProtobuf(ctx, req, nil, stream)

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

func newMetrics() (*prometheus.Registry, *RequestMetrics, *server.Metrics) {
	reg := prometheus.NewPedanticRegistry()
	serverConfig := server.Config{
		Registerer:       reg,
		MetricsNamespace: "cortex",
	}

	return reg, NewRequestMetrics(reg), server.NewServerMetrics(serverConfig)
}

func requireMetricsIgnoringHistogramBucketsAndDurationSums(t *testing.T, reg *prometheus.Registry, expected string, metricNames ...string) {
	original, err := testutil.CollectAndFormat(reg, expfmt.TypeTextPlain, metricNames...)
	require.NoError(t, err)

	filter := regexp.MustCompile(`(?m)^[a-z_]+(_bucket|_duration_seconds_sum)[{ ].*$[[:space:]]+`)
	filteredText := string(filter.ReplaceAll(original, nil))

	// 'expected' is likely from a raw string and it may contain leading whitespace on each line, so remove it.
	removeLeadingWhitespace := regexp.MustCompile(`(?m)^[ \t\n]+`)
	expected = removeLeadingWhitespace.ReplaceAllString(expected, "")

	require.Equal(t, expected, filteredText)
}

func replaceTimeNow(times ...time.Time) func() time.Time {
	return func() time.Time {
		t := times[0]
		times = times[1:]
		return t
	}
}

type contextCapturingStorage struct {
	inner storage.Storage
	ctx   context.Context
}

func (s *contextCapturingStorage) Querier(mint, maxt int64) (storage.Querier, error) {
	q, err := s.inner.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return &contextCapturingQuerier{q, s}, nil
}

func (s *contextCapturingStorage) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	panic("not supported")
}

func (s *contextCapturingStorage) Appender(ctx context.Context) storage.Appender {
	panic("not supported")
}

func (s *contextCapturingStorage) StartTime() (int64, error) {
	panic("not supported")
}

func (s *contextCapturingStorage) Close() error {
	return s.inner.Close()
}

type contextCapturingQuerier struct {
	inner   storage.Querier
	storage *contextCapturingStorage
}

func (c *contextCapturingQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	c.storage.ctx = ctx
	return c.inner.Select(ctx, sortSeries, hints, matchers...)
}

func (c *contextCapturingQuerier) Close() error {
	return c.inner.Close()
}

func (c *contextCapturingQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	panic("not supported")
}

func (c *contextCapturingQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	panic("not supported")
}

type testExtractor struct{}

type testExtractorKeyType int

const testExtractorHeaderName = "The-Test-Header"
const testExtractorKey testExtractorKeyType = iota

func (p *testExtractor) ExtractFromCarrier(ctx context.Context, carrier propagation.Carrier) (context.Context, error) {
	return context.WithValue(ctx, testExtractorKey, carrier.Get(testExtractorHeaderName)), nil
}

func TestQueryResponseWriter_WriteError(t *testing.T) {
	testCases := map[string]struct {
		err             error
		expectedMessage string
		expectedType    mimirpb.QueryErrorType
	}{
		"generic error": {
			err:             errors.New("error with no type"),
			expectedMessage: "error with no type",
			expectedType:    mimirpb.QUERY_ERROR_TYPE_NOT_FOUND,
		},
		"APIError instance": {
			err:             apierror.New(apierror.TypeTooManyRequests, "error with 'too many requests' type"),
			expectedMessage: "error with 'too many requests' type",
			expectedType:    mimirpb.QUERY_ERROR_TYPE_TOO_MANY_REQUESTS,
		},
		"wrapped APIError instance": {
			err:             fmt.Errorf("wrapped: %w", apierror.New(apierror.TypeTooLargeEntry, "error with 'too large entry' type")),
			expectedMessage: "wrapped: error with 'too large entry' type",
			expectedType:    mimirpb.QUERY_ERROR_TYPE_TOO_LARGE_ENTRY,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			reg, requestMetrics, serverMetrics := newMetrics()
			stream := &mockQueryResultStream{t: t, route: "test-route", reg: reg}
			ctx := context.Background()
			writer := newQueryResponseWriter(stream, requestMetrics, serverMetrics, spanlogger.FromContext(ctx, log.NewNopLogger()))
			writer.Start("test-route", 123)

			writer.WriteError(ctx, apierror.TypeNotFound, testCase.err)

			expectedMessages := []*frontendv2pb.QueryResultStreamRequest{
				{
					Data: &frontendv2pb.QueryResultStreamRequest_Error{
						Error: &querierpb.Error{
							Type:    testCase.expectedType,
							Message: testCase.expectedMessage,
						},
					},
				},
			}

			require.Equal(t, expectedMessages, stream.messages)
		})
	}
}
