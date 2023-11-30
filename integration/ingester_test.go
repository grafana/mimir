// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
	ingesterpkg "github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestIngesterQuerying(t *testing.T) {
	query := "foobar"
	queryEnd := time.Now().Round(time.Second)
	queryStart := queryEnd.Add(-1 * time.Hour)
	queryStep := 10 * time.Minute
	timestampQuery := "timestamp(foobar)"

	timestampsAlignedToQueryStep := model.Matrix{
		{
			Metric: model.Metric{},
			Values: []model.SamplePair{
				{
					Timestamp: model.Time(queryStart.UnixMilli()),
					Value:     model.SampleValue(queryStart.Unix()),
				},
				{
					Timestamp: model.Time(queryStart.Add(queryStep).UnixMilli()),
					Value:     model.SampleValue(queryStart.Add(queryStep).Unix()),
				},
				{
					Timestamp: model.Time(queryStart.Add(queryStep * 2).UnixMilli()),
					Value:     model.SampleValue(queryStart.Add(queryStep * 2).Unix()),
				},
				{
					Timestamp: model.Time(queryStart.Add(queryStep * 3).UnixMilli()),
					Value:     model.SampleValue(queryStart.Add(queryStep * 3).Unix()),
				},
				{
					Timestamp: model.Time(queryStart.Add(queryStep * 4).UnixMilli()),
					Value:     model.SampleValue(queryStart.Add(queryStep * 4).Unix()),
				},
				{
					Timestamp: model.Time(queryStart.Add(queryStep * 5).UnixMilli()),
					Value:     model.SampleValue(queryStart.Add(queryStep * 5).Unix()),
				},
			},
		},
	}

	testCases := map[string]struct {
		inSeries                     []prompb.TimeSeries
		expectedQueryResult          model.Matrix
		expectedTimestampQueryResult model.Matrix
	}{
		"float series": {
			inSeries: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{
							Name:  "__name__",
							Value: "foobar",
						},
					},
					Samples: []prompb.Sample{
						{
							Timestamp: queryStart.UnixMilli(),
							Value:     100,
						},
						{
							Timestamp: queryStart.Add(queryStep).UnixMilli(),
							Value:     110,
						},
						{
							Timestamp: queryStart.Add(queryStep * 2).UnixMilli(),
							Value:     120,
						},
						{
							Timestamp: queryStart.Add(queryStep * 3).UnixMilli(),
							Value:     130,
						},
						{
							Timestamp: queryStart.Add(queryStep * 4).UnixMilli(),
							Value:     140,
						},
						{
							Timestamp: queryStart.Add(queryStep * 5).UnixMilli(),
							Value:     150,
						},
					},
				},
			},
			expectedQueryResult: model.Matrix{
				{
					Metric: model.Metric{"__name__": "foobar"},
					Values: []model.SamplePair{
						{
							Timestamp: model.Time(queryStart.UnixMilli()),
							Value:     model.SampleValue(100),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep).UnixMilli()),
							Value:     model.SampleValue(110),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 2).UnixMilli()),
							Value:     model.SampleValue(120),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 3).UnixMilli()),
							Value:     model.SampleValue(130),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 4).UnixMilli()),
							Value:     model.SampleValue(140),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 5).UnixMilli()),
							Value:     model.SampleValue(150),
						},
					},
				},
			},
			expectedTimestampQueryResult: timestampsAlignedToQueryStep,
		},
		"integer histogram series": {
			inSeries: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{
							Name:  "__name__",
							Value: "foobar",
						},
					},
					Histograms: []prompb.Histogram{
						remote.HistogramToHistogramProto(queryStart.UnixMilli(), test.GenerateTestHistogram(1)),
						remote.HistogramToHistogramProto(queryStart.Add(queryStep).UnixMilli(), test.GenerateTestHistogram(2)),
						remote.HistogramToHistogramProto(queryStart.Add(queryStep*2).UnixMilli(), test.GenerateTestHistogram(3)),
						remote.HistogramToHistogramProto(queryStart.Add(queryStep*3).UnixMilli(), test.GenerateTestHistogram(4)),
						remote.HistogramToHistogramProto(queryStart.Add(queryStep*4).UnixMilli(), test.GenerateTestHistogram(5)),
						remote.HistogramToHistogramProto(queryStart.Add(queryStep*5).UnixMilli(), test.GenerateTestHistogram(6)),
					},
				},
			},
			expectedQueryResult: model.Matrix{
				{
					Metric: model.Metric{"__name__": "foobar"},
					Histograms: []model.SampleHistogramPair{
						{
							Timestamp: model.Time(queryStart.UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(1),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(2),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 2).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(3),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 3).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(4),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 4).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(5),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 5).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(6),
						},
					},
				},
			},
			expectedTimestampQueryResult: timestampsAlignedToQueryStep,
		},
		"float histogram series": {
			inSeries: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{
							Name:  "__name__",
							Value: "foobar",
						},
					},
					Histograms: []prompb.Histogram{
						remote.FloatHistogramToHistogramProto(queryStart.UnixMilli(), test.GenerateTestFloatHistogram(1)),
						remote.FloatHistogramToHistogramProto(queryStart.Add(queryStep).UnixMilli(), test.GenerateTestFloatHistogram(2)),
						remote.FloatHistogramToHistogramProto(queryStart.Add(queryStep*2).UnixMilli(), test.GenerateTestFloatHistogram(3)),
						remote.FloatHistogramToHistogramProto(queryStart.Add(queryStep*3).UnixMilli(), test.GenerateTestFloatHistogram(4)),
						remote.FloatHistogramToHistogramProto(queryStart.Add(queryStep*4).UnixMilli(), test.GenerateTestFloatHistogram(5)),
						remote.FloatHistogramToHistogramProto(queryStart.Add(queryStep*5).UnixMilli(), test.GenerateTestFloatHistogram(6)),
					},
				},
			},
			expectedQueryResult: model.Matrix{
				{
					Metric: model.Metric{"__name__": "foobar"},
					Histograms: []model.SampleHistogramPair{
						{
							Timestamp: model.Time(queryStart.UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(1),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(2),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 2).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(3),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 3).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(4),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 4).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(5),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 5).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(6),
						},
					},
				},
			},
			expectedTimestampQueryResult: timestampsAlignedToQueryStep,
		},
		"series switching from float to integer histogram to float histogram": {
			inSeries: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{
							Name:  "__name__",
							Value: "foobar",
						},
					},
					Samples: []prompb.Sample{
						{
							Timestamp: queryStart.UnixMilli(),
							Value:     100,
						},
						{
							Timestamp: queryStart.Add(queryStep).UnixMilli(),
							Value:     110,
						},
					},
					Histograms: []prompb.Histogram{
						remote.HistogramToHistogramProto(queryStart.Add(queryStep*2).UnixMilli(), test.GenerateTestHistogram(3)),
						remote.HistogramToHistogramProto(queryStart.Add(queryStep*3).UnixMilli(), test.GenerateTestHistogram(4)),
						remote.FloatHistogramToHistogramProto(queryStart.Add(queryStep*4).UnixMilli(), test.GenerateTestFloatHistogram(5)),
						remote.FloatHistogramToHistogramProto(queryStart.Add(queryStep*5).UnixMilli(), test.GenerateTestFloatHistogram(6)),
					},
				},
			},
			expectedQueryResult: model.Matrix{
				{
					Metric: model.Metric{"__name__": "foobar"},
					Values: []model.SamplePair{
						{
							Timestamp: model.Time(queryStart.UnixMilli()),
							Value:     model.SampleValue(100),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep).UnixMilli()),
							Value:     model.SampleValue(110),
						},
					},
					Histograms: []model.SampleHistogramPair{
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 2).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(3),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 3).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(4),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 4).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(5),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 5).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(6),
						},
					},
				},
			},
			expectedTimestampQueryResult: timestampsAlignedToQueryStep,
		},
		"series including float and native histograms at same timestamp": {
			inSeries: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{
							Name:  "__name__",
							Value: "foobar",
						},
					},
					Samples: []prompb.Sample{
						{
							Timestamp: queryStart.UnixMilli(),
							Value:     100,
						},
						{
							Timestamp: queryStart.Add(queryStep).UnixMilli(),
							Value:     110,
						},
						{
							Timestamp: queryStart.Add(queryStep * 2).UnixMilli(),
							Value:     120,
						},
					},
					Histograms: []prompb.Histogram{
						// This first of these will fail to get appended because there's already a float sample for that timestamp.
						remote.HistogramToHistogramProto(queryStart.Add(queryStep*2).UnixMilli(), test.GenerateTestHistogram(3)),
						remote.HistogramToHistogramProto(queryStart.Add(queryStep*3).UnixMilli(), test.GenerateTestHistogram(4)),
						remote.HistogramToHistogramProto(queryStart.Add(queryStep*4).UnixMilli(), test.GenerateTestHistogram(5)),
						remote.HistogramToHistogramProto(queryStart.Add(queryStep*5).UnixMilli(), test.GenerateTestHistogram(6)),
					},
				},
			},
			expectedQueryResult: model.Matrix{
				{
					Metric: model.Metric{"__name__": "foobar"},
					Values: []model.SamplePair{
						{
							Timestamp: model.Time(queryStart.UnixMilli()),
							Value:     model.SampleValue(100),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep).UnixMilli()),
							Value:     model.SampleValue(110),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 2).UnixMilli()),
							Value:     model.SampleValue(120),
						},
					},
					Histograms: []model.SampleHistogramPair{
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 3).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(4),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 4).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(5),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 5).UnixMilli()),
							Histogram: test.GenerateTestSampleHistogram(6),
						},
					},
				},
			},
			expectedTimestampQueryResult: timestampsAlignedToQueryStep,
		},
		"float series where sample timestamps don't align with query step": {
			inSeries: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{
							Name:  "__name__",
							Value: "foobar",
						},
					},
					Samples: []prompb.Sample{
						{
							Timestamp: queryStart.Add(-2 * time.Second).UnixMilli(),
							Value:     100,
						},
						{
							Timestamp: queryStart.Add(queryStep).Add(-2 * time.Second).UnixMilli(),
							Value:     110,
						},
						{
							Timestamp: queryStart.Add(queryStep * 2).Add(-2 * time.Second).UnixMilli(),
							Value:     120,
						},
						{
							Timestamp: queryStart.Add(queryStep * 3).Add(-2 * time.Second).UnixMilli(),
							Value:     130,
						},
						{
							Timestamp: queryStart.Add(queryStep * 4).Add(-2 * time.Second).UnixMilli(),
							Value:     140,
						},
						{
							Timestamp: queryStart.Add(queryStep * 5).Add(-2 * time.Second).UnixMilli(),
							Value:     150,
						},
					},
				},
			},
			expectedQueryResult: model.Matrix{
				{
					Metric: model.Metric{"__name__": "foobar"},
					Values: []model.SamplePair{
						{
							Timestamp: model.Time(queryStart.UnixMilli()),
							Value:     model.SampleValue(100),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep).UnixMilli()),
							Value:     model.SampleValue(110),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 2).UnixMilli()),
							Value:     model.SampleValue(120),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 3).UnixMilli()),
							Value:     model.SampleValue(130),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 4).UnixMilli()),
							Value:     model.SampleValue(140),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 5).UnixMilli()),
							Value:     model.SampleValue(150),
						},
					},
				},
			},
			expectedTimestampQueryResult: model.Matrix{
				{
					Metric: model.Metric{},
					Values: []model.SamplePair{
						{
							Timestamp: model.Time(queryStart.UnixMilli()),
							Value:     model.SampleValue(queryStart.Add(-2 * time.Second).Unix()),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep).UnixMilli()),
							Value:     model.SampleValue(queryStart.Add(queryStep).Add(-2 * time.Second).Unix()),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 2).UnixMilli()),
							Value:     model.SampleValue(queryStart.Add(queryStep * 2).Add(-2 * time.Second).Unix()),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 3).UnixMilli()),
							Value:     model.SampleValue(queryStart.Add(queryStep * 3).Add(-2 * time.Second).Unix()),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 4).UnixMilli()),
							Value:     model.SampleValue(queryStart.Add(queryStep * 4).Add(-2 * time.Second).Unix()),
						},
						{
							Timestamp: model.Time(queryStart.Add(queryStep * 5).UnixMilli()),
							Value:     model.SampleValue(queryStart.Add(queryStep * 5).Add(-2 * time.Second).Unix()),
						},
					},
				},
			},
		},
		"query that returns no results": {
			// We have to push at least one sample to ensure that the tenant TSDB exists (otherwise the ingester takes a shortcut and returns early).
			inSeries: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{
							Name:  "__name__",
							Value: "not_foobar",
						},
					},
					Samples: []prompb.Sample{
						{
							Timestamp: queryStart.Add(-2 * time.Second).UnixMilli(),
							Value:     100,
						},
					},
				},
			},
			expectedQueryResult:          model.Matrix{},
			expectedTimestampQueryResult: model.Matrix{},
		},
	}

	for testName, tc := range testCases {
		t.Run(testName, func(t *testing.T) {
			for _, streamingEnabled := range []bool{true, false} {
				t.Run(fmt.Sprintf("streaming enabled: %v", streamingEnabled), func(t *testing.T) {
					s, err := e2e.NewScenario(networkName)
					require.NoError(t, err)
					defer s.Close()

					baseFlags := map[string]string{
						"-distributor.ingestion-tenant-shard-size":        "0",
						"-ingester.ring.heartbeat-period":                 "1s",
						"-querier.prefer-streaming-chunks-from-ingesters": strconv.FormatBool(streamingEnabled),
					}

					flags := mergeFlags(
						BlocksStorageFlags(),
						BlocksStorageS3Flags(),
						baseFlags,
					)

					// Start dependencies.
					consul := e2edb.NewConsul()
					minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
					require.NoError(t, s.StartAndWaitReady(consul, minio))

					// Start Mimir components.
					distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
					ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
					querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
					require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))

					// Wait until distributor has updated the ring.
					require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
						labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
						labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

					// Wait until querier has updated the ring.
					require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
						labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
						labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

					client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
					require.NoError(t, err)

					res, err := client.Push(tc.inSeries)
					require.NoError(t, err)
					require.Equal(t, http.StatusOK, res.StatusCode)

					result, err := client.QueryRange(query, queryStart, queryEnd, queryStep)
					require.NoError(t, err)
					require.Equal(t, tc.expectedQueryResult, result)

					// The PromQL engine does some special handling for the timestamp() function which previously
					// caused queries to fail when streaming chunks was enabled, so check that this regression
					// has not been reintroduced.
					result, err = client.QueryRange(timestampQuery, queryStart, queryEnd, queryStep)
					require.NoError(t, err)
					require.Equal(t, tc.expectedTimestampQueryResult, result)

					queryRequestCount := func(status string) (float64, error) {
						counts, err := querier.SumMetrics([]string{"cortex_ingester_client_request_duration_seconds"},
							e2e.WithLabelMatchers(
								labels.MustNewMatcher(labels.MatchEqual, "operation", "/cortex.Ingester/QueryStream"),
								labels.MustNewMatcher(labels.MatchRegexp, "status_code", status),
							),
							e2e.WithMetricCount,
							e2e.SkipMissingMetrics,
						)

						if err != nil {
							return 0, err
						}

						require.Len(t, counts, 1)
						return counts[0], nil
					}

					successfulQueryRequests, err := queryRequestCount("2xx")
					require.NoError(t, err)

					cancelledQueryRequests, err := queryRequestCount("cancel")
					require.NoError(t, err)

					totalQueryRequests, err := queryRequestCount(".*")
					require.NoError(t, err)

					// We expect two query requests: the first query request and the timestamp query request
					require.Equalf(t, 2.0, totalQueryRequests, "got %v query requests (%v successful, %v cancelled)", totalQueryRequests, successfulQueryRequests, cancelledQueryRequests)
					require.Equalf(t, 2.0, successfulQueryRequests, "got %v query requests (%v successful, %v cancelled)", totalQueryRequests, successfulQueryRequests, cancelledQueryRequests)
					require.Equalf(t, 0.0, cancelledQueryRequests, "got %v query requests (%v successful, %v cancelled)", totalQueryRequests, successfulQueryRequests, cancelledQueryRequests)
				})
			}
		})
	}
}

func TestIngesterQueryingWithRequestMinimization(t *testing.T) {
	for _, streamingEnabled := range []bool{true, false} {
		t.Run(fmt.Sprintf("streaming enabled: %v", streamingEnabled), func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			baseFlags := map[string]string{
				"-distributor.ingestion-tenant-shard-size":        "0",
				"-ingester.ring.heartbeat-period":                 "1s",
				"-ingester.ring.zone-awareness-enabled":           "true",
				"-ingester.ring.replication-factor":               "3",
				"-querier.minimize-ingester-requests":             "true",
				"-querier.prefer-streaming-chunks-from-ingesters": strconv.FormatBool(streamingEnabled),
			}

			flags := mergeFlags(
				BlocksStorageFlags(),
				BlocksStorageS3Flags(),
				baseFlags,
			)

			// Start dependencies.
			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(consul, minio))

			ingesterFlags := func(zone string) map[string]string {
				return mergeFlags(flags, map[string]string{
					"-ingester.ring.instance-availability-zone": zone,
				})
			}

			// Start Mimir components.
			distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
			ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-a"))
			ingester2 := e2emimir.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-b"))
			ingester3 := e2emimir.NewIngester("ingester-3", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-c"))
			querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
			require.NoError(t, s.StartAndWaitReady(distributor, ingester1, ingester2, ingester3, querier))

			// Wait until distributor and querier have updated the ring.
			for _, component := range []*e2emimir.MimirService{distributor, querier} {
				require.NoError(t, component.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
					labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
					labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))
			}

			client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
			require.NoError(t, err)

			// Push some data to the cluster.
			seriesName := "test_series"
			now := time.Now()
			series, expectedVector, _ := generateFloatSeries(seriesName, now, prompb.Label{Name: "foo", Value: "bar"})

			res, err := client.Push(series)
			require.NoError(t, err)
			require.Equal(t, 200, res.StatusCode)

			// Verify we can query the data we just pushed.
			queryResult, err := client.Query(seriesName, now)
			require.NoError(t, err)
			require.Equal(t, model.ValVector, queryResult.Type())
			require.Equal(t, expectedVector, queryResult.(model.Vector))

			// Check that we only queried two of the three ingesters.
			totalQueryRequests := 0.0

			for _, ingester := range []*e2emimir.MimirService{ingester1, ingester2, ingester3} {
				sums, err := ingester.SumMetrics(
					[]string{"cortex_request_duration_seconds"},
					e2e.WithLabelMatchers(
						labels.MustNewMatcher(labels.MatchEqual, "route", "/cortex.Ingester/QueryStream"),
						labels.MustNewMatcher(labels.MatchEqual, "status_code", "success"),
					),
					e2e.SkipMissingMetrics,
					e2e.WithMetricCount,
				)

				require.NoError(t, err)
				queryRequests := sums[0]
				require.LessOrEqual(t, queryRequests, 1.0)
				totalQueryRequests += queryRequests
			}

			require.Equal(t, 2.0, totalQueryRequests)
		})
	}
}

func TestIngesterReportGRPCStatusCodes(t *testing.T) {
	query := "foobar"
	queryEnd := time.Now().Round(time.Second)
	queryStart := queryEnd.Add(-1 * time.Hour)
	queryStep := 10 * time.Minute

	testCases := map[string]struct {
		serverReportGRPCStatusCodes         bool
		ingesterClientReportGRPCStatusCodes bool
		expectedPushStatusCode              string
		expectedQueryStatusCode             string
	}{
		"when server and ingester client do not report grpc codes, successful push and query give success and 2xx": {
			serverReportGRPCStatusCodes:         false,
			ingesterClientReportGRPCStatusCodes: false,
			expectedPushStatusCode:              "success",
			expectedQueryStatusCode:             "2xx",
		},
		"when server does not report and ingester client reports grpc codes, successful push and query give success and OK": {
			serverReportGRPCStatusCodes:         false,
			ingesterClientReportGRPCStatusCodes: true,
			expectedPushStatusCode:              "success",
			expectedQueryStatusCode:             "OK",
		},
		"when server reports and ingester client does not report grpc codes, successful push and query give OK and 2xx": {
			serverReportGRPCStatusCodes:         true,
			ingesterClientReportGRPCStatusCodes: false,
			expectedPushStatusCode:              "OK",
			expectedQueryStatusCode:             "2xx",
		},
		"when server and ingester client report grpc codes, successful push and query give OK and OK": {
			serverReportGRPCStatusCodes:         true,
			ingesterClientReportGRPCStatusCodes: true,
			expectedPushStatusCode:              "OK",
			expectedQueryStatusCode:             "OK",
		},
	}

	series := []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{
					Name:  "__name__",
					Value: "not_foobar",
				},
			},
			Samples: []prompb.Sample{
				{
					Timestamp: queryStart.Add(-2 * time.Second).UnixMilli(),
					Value:     100,
				},
			},
		},
	}
	expectedQueryResult := model.Matrix{}

	for testName, testData := range testCases {
		t.Run(testName, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			baseFlags := map[string]string{
				"-distributor.ingestion-tenant-shard-size":                            "0",
				"-ingester.ring.heartbeat-period":                                     "1s",
				"-ingester.client.report-grpc-codes-in-instrumentation-label-enabled": strconv.FormatBool(testData.ingesterClientReportGRPCStatusCodes),
				"-server.report-grpc-codes-in-instrumentation-label-enabled":          strconv.FormatBool(testData.serverReportGRPCStatusCodes),
			}

			flags := mergeFlags(
				BlocksStorageFlags(),
				BlocksStorageS3Flags(),
				baseFlags,
			)

			// Start dependencies.
			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(consul, minio))

			// Start Mimir components.
			distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
			ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
			querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
			require.NoError(t, s.StartAndWaitReady(distributor, ingester, querier))

			// Wait until distributor has updated the ring.
			require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
				labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

			// Wait until querier has updated the ring.
			require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
				labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

			client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
			require.NoError(t, err)

			res, err := client.Push(series)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, res.StatusCode)

			sums, err := ingester.SumMetrics(
				[]string{"cortex_request_duration_seconds"},
				e2e.WithLabelMatchers(
					labels.MustNewMatcher(labels.MatchEqual, "route", "/cortex.Ingester/Push"),
					labels.MustNewMatcher(labels.MatchEqual, "status_code", testData.expectedPushStatusCode),
				),
				e2e.SkipMissingMetrics,
				e2e.WithMetricCount,
			)

			require.NoError(t, err)
			pushRequests := sums[0]
			require.Equal(t, pushRequests, 1.0)

			result, err := client.QueryRange(query, queryStart, queryEnd, queryStep)
			require.NoError(t, err)
			require.Equal(t, expectedQueryResult, result)

			queryRequestCount := func(status string) (float64, error) {
				counts, err := querier.SumMetrics([]string{"cortex_ingester_client_request_duration_seconds"},
					e2e.WithLabelMatchers(
						labels.MustNewMatcher(labels.MatchEqual, "operation", "/cortex.Ingester/QueryStream"),
						labels.MustNewMatcher(labels.MatchRegexp, "status_code", status),
					),
					e2e.WithMetricCount,
					e2e.SkipMissingMetrics,
				)

				if err != nil {
					return 0, err
				}

				require.Len(t, counts, 1)
				return counts[0], nil
			}

			successfulQueryRequests, err := queryRequestCount(testData.expectedQueryStatusCode)
			require.NoError(t, err)

			cancelledQueryRequests, err := queryRequestCount("cancel")
			require.NoError(t, err)

			totalQueryRequests, err := queryRequestCount(".*")
			require.NoError(t, err)

			// We expect two query requests: the first query request and the timestamp query request
			require.Equalf(t, 1.0, totalQueryRequests, "got %v query requests (%v successful, %v cancelled)", totalQueryRequests, successfulQueryRequests, cancelledQueryRequests)
			require.Equalf(t, 1.0, successfulQueryRequests, "got %v query requests (%v successful, %v cancelled)", totalQueryRequests, successfulQueryRequests, cancelledQueryRequests)
			require.Equalf(t, 0.0, cancelledQueryRequests, "got %v query requests (%v successful, %v cancelled)", totalQueryRequests, successfulQueryRequests, cancelledQueryRequests)
		})
	}
}

func BenchmarkIngesterPush(b *testing.B) {
	const (
		series                 = 200_000
		requestsPerConcurrency = 200
		samples                = 4
		useKafka               = true
		kafkaTopic             = "t1"
	)
	allLabels, allSamples := benchmarkData(series)

	for _, concurrency := range []int{1, 2, 3, 4, 5} {
		b.Run(fmt.Sprintf("kafka=%t,series=%d,req=%d,samples=%d,concurrency=%d", useKafka, series, requestsPerConcurrency, samples, concurrency), func(b *testing.B) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(b, err)
			b.Cleanup(s.Close)

			bucketFlags := BlocksStorageS3Flags()

			// Start dependencies.
			zk := e2emimir.NewZookeeper()
			kafka := e2emimir.NewKafka(fmt.Sprintf("%s-zookeeper:%d", GetNetworkName(), zk.HTTPPort()))
			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, bucketFlags["-blocks-storage.s3.bucket-name"])
			require.NoError(b, s.StartAndWaitReady(consul, zk, minio, kafka))

			const pushTimeout = time.Minute
			baseFlags := map[string]string{
				"-blocks-storage.tsdb.head-compaction-interval": "1m",
				"-distributor.ingestion-rate-limit":             "1000000000",
				"-distributor.ingestion-tenant-shard-size":      "0",
				"-distributor.max-recv-msg-size":                "1121068200",
				"-distributor.remote-timeout":                   pushTimeout.String(),
				"-ingest-storage.enabled":                       strconv.FormatBool(useKafka),
				"-ingest-storage.kafka-address":                 fmt.Sprintf("%s-kafka:%d", GetNetworkName(), kafka.HTTPPort()),
				"-ingest-storage.kafka-topic":                   kafkaTopic,
				"-ingester.client.grpc-max-send-msg-size":       "1121068200",
				"-ingester.instance-limits.max-ingestion-rate":  "1000000000",
				"-ingester.ring.heartbeat-period":               "1s",
				"-ingester.ring.instance-id":                    "ingester-0",
				"-server.grpc-max-recv-msg-size-bytes":          "1121068200",
				"-server.grpc-max-send-msg-size-bytes":          "1121068200",
				//"-log.level":                                    "debug",
			}

			flags := mergeFlags(
				BlocksStorageFlags(),
				bucketFlags,
				baseFlags,
			)

			// Start Mimir components.
			distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
			ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)

			require.NoError(b, s.StartAndWaitReady(distributor, ingester))

			// Wait until distributor has updated the ring.
			require.NoError(b, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
				labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

			client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", userID)
			require.NoError(b, err)
			client.SetTimeout(pushTimeout)

			var (
				seriesPerReqPerConcurrency = series / requestsPerConcurrency / concurrency
			)

			pushReqs := make([][][]prompb.TimeSeries, concurrency)
			// Generate requests
			{
				startTime := time.Now().UnixMilli()
				for iter := 0; iter < b.N; iter++ {
					// Bump the timestamp on each of our test samples each time round the loop
					for j := 0; j < samples; j++ {
						for i := range allSamples {
							allSamples[i].Timestamp = startTime + int64(iter*samples+j+1)
						}

						for seriesIdx := 0; seriesIdx < len(allLabels); {
							for c := 0; c < concurrency && seriesIdx < len(allLabels); c++ {
								startI, endI := seriesIdx, min(len(allLabels), seriesIdx+seriesPerReqPerConcurrency)
								series, samples := allLabels[startI:endI], allSamples[startI:endI]
								request := make([]prompb.TimeSeries, 0, len(series))
								for i := range series {
									request = append(request, prompb.TimeSeries{Labels: series[i], Samples: samples[i : i+1]})
								}
								pushReqs[c] = append(pushReqs[c], request)
								seriesIdx += seriesPerReqPerConcurrency
							}
						}
					}
				}

				pushReqs[len(pushReqs)-1] = append(pushReqs[len(pushReqs)-1], []prompb.TimeSeries{{
					Labels:  []prompb.Label{{Value: "marker", Name: labels.MetricName}},
					Samples: []prompb.Sample{{Timestamp: time.Now().UnixMilli(), Value: ingesterpkg.FinalMessageSampleValue}},
				}})
			}

			// Start producers
			producersWg := &sync.WaitGroup{}
			{
				for i := 0; i < concurrency; i++ {
					producersWg.Add(1)
					go func(i int) {
						defer producersWg.Done()
						for _, req := range pushReqs[i] {
							resp, err := client.Push(req)
							var body []byte
							if resp != nil && resp.Body != nil {
								body, _ = io.ReadAll(resp.Body)
							}
							if assert.NoError(b, err, string(body)) {
								assert.Equal(b, http.StatusOK, resp.StatusCode, string(body))
							}
						}
					}(i)
				}
				if useKafka {
					producersWg.Wait()
				}
			}
			b.ResetTimer()
			resp, err := http.Get("http://" + ingester.HTTPEndpoint() + "/ingester/do-replay") // TODO dimitarvdimitrov control replay concurrency here
			require.NoError(b, err)
			require.Equal(b, http.StatusOK, resp.StatusCode)
			producersWg.Wait()
		})
	}
}

// Construct a set of realistic-looking samples, all with slightly different label sets
func benchmarkData(nSeries int) (allLabels [][]prompb.Label, allSamples []prompb.Sample) {
	// Real example from Kubernetes' embedded cAdvisor metrics, lightly obfuscated.
	var benchmarkLabels = []prompb.Label{
		{Name: model.MetricNameLabel, Value: "container_cpu_usage_seconds_total"},
		{Name: "beta_kubernetes_io_arch", Value: "amd64"},
		{Name: "beta_kubernetes_io_instance_type", Value: "c3.somesize"},
		{Name: "beta_kubernetes_io_os", Value: "linux"},
		{Name: "container_name", Value: "some-name"},
		{Name: "cpu", Value: "cpu01"},
		{Name: "failure_domain_beta_kubernetes_io_region", Value: "somewhere-1"},
		{Name: "failure_domain_beta_kubernetes_io_zone", Value: "somewhere-1b"},
		{Name: "id", Value: "/kubepods/burstable/pod6e91c467-e4c5-11e7-ace3-0a97ed59c75e/a3c8498918bd6866349fed5a6f8c643b77c91836427fb6327913276ebc6bde28"},
		{Name: "image", Value: "registry/organisation/name@sha256:dca3d877a80008b45d71d7edc4fd2e44c0c8c8e7102ba5cbabec63a374d1d506"},
		{Name: "instance", Value: "ip-111-11-1-11.ec2.internal"},
		{Name: "job", Value: "kubernetes-cadvisor"},
		{Name: "kubernetes_io_hostname", Value: "ip-111-11-1-11"},
		{Name: "monitor", Value: "prod"},
		{Name: "name", Value: "k8s_some-name_some-other-name-5j8s8_kube-system_6e91c467-e4c5-11e7-ace3-0a97ed59c75e_0"},
		{Name: "namespace", Value: "kube-system"},
		{Name: "pod_name", Value: "some-other-name-5j8s8"},
	}

	for j := 0; j < nSeries; j++ {
		for i := range benchmarkLabels {
			if benchmarkLabels[i].Name == "cpu" {
				benchmarkLabels[i].Value = fmt.Sprintf("cpu%02d", j)
			}
		}

		allLabels = append(allLabels, append([]prompb.Label(nil), benchmarkLabels...))
		allSamples = append(allSamples, prompb.Sample{Timestamp: 0, Value: float64(j)})
	}
	return
}
