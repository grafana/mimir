// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/querier_remote_read_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestQuerierRemoteRead(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
	)

	// Start dependencies.
	minio := e2edb.NewMinio(9000, blocksBucketName)

	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(minio, consul))

	// Start Mimir components for the write path.
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester))

	// Wait until the distributor has updated the ring.
	// The distributor should have 512 tokens for the ingester ring and 1 for the distributor ring
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))

	c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
	require.NoError(t, err)

	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(querier))

	// Wait until the querier has updated the ring.
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	runTestPushSeriesForQuerierRemoteRead(t, c, querier, "series_1", generateFloatSeries)
	runTestPushSeriesForQuerierRemoteRead(t, c, querier, "hseries_1", generateHistogramSeries)
}

func runTestPushSeriesForQuerierRemoteRead(t *testing.T, c *e2emimir.Client, querier *e2emimir.MimirService, seriesName string, genSeries generateSeriesFunc) {
	// Push a series for each user to Mimir.
	now := time.Now()

	series, expectedVectors, _ := genSeries(seriesName, now)
	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	startMs := now.Add(-1 * time.Minute)
	endMs := now.Add(time.Minute)

	client, err := e2emimir.NewClient("", querier.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)
	httpResp, resp, _, err := client.RemoteRead(seriesName, startMs, endMs)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	require.NoError(t, err)

	// Validate the returned remote read data matches what was written
	require.Len(t, resp.Timeseries, 1)
	require.Len(t, resp.Timeseries[0].Labels, 1)
	require.Equal(t, seriesName, resp.Timeseries[0].Labels[0].GetValue())
	isSeriesFloat := len(resp.Timeseries[0].Samples) == 1
	isSeriesHistogram := len(resp.Timeseries[0].Histograms) == 1
	require.Equal(t, isSeriesFloat, !isSeriesHistogram)
	if isSeriesFloat {
		require.Equal(t, int64(expectedVectors[0].Timestamp), resp.Timeseries[0].Samples[0].Timestamp)
		require.Equal(t, float64(expectedVectors[0].Value), resp.Timeseries[0].Samples[0].Value)
	} else if isSeriesHistogram {
		require.Equal(t, expectedVectors[0].Histogram, mimirpb.FromHistogramToPromHistogram(remote.HistogramProtoToHistogram(resp.Timeseries[0].Histograms[0])))
	}
}

func TestQuerierStreamingRemoteRead(t *testing.T) {
	testCases := map[string]struct {
		expectedValType chunkenc.ValueType
		floats          func(startMs, endMs int64) []prompb.Sample
		histograms      func(startMs, endMs int64) []prompb.Histogram
	}{
		"float samples": {
			expectedValType: chunkenc.ValFloat,
			floats: func(startMs, endMs int64) []prompb.Sample {
				var samples []prompb.Sample
				for i := startMs; i < endMs; i++ {
					samples = append(samples, prompb.Sample{
						Value:     rand.Float64(),
						Timestamp: i,
					})
				}
				return samples
			},
		},
		"histograms": {
			expectedValType: chunkenc.ValHistogram,
			histograms: func(startMs, endMs int64) []prompb.Histogram {
				var hists []prompb.Histogram
				for i := startMs; i < endMs; i++ {
					h := test.GenerateTestHistogram(int(i))
					hists = append(hists, remote.HistogramToHistogramProto(i, h))
				}
				return hists
			},
		},
		"float histograms": {
			expectedValType: chunkenc.ValFloatHistogram,
			histograms: func(startMs, endMs int64) []prompb.Histogram {
				var hists []prompb.Histogram
				for i := startMs; i < endMs; i++ {
					h := test.GenerateTestFloatHistogram(int(i))
					hists = append(hists, remote.FloatHistogramToHistogramProto(i, h))
				}
				return hists
			},
		},
	}

	for testName, tc := range testCases {
		t.Run(testName, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			flags := mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
				"-distributor.ingestion-rate-limit": "1048576",
				"-distributor.ingestion-burst-size": "1048576",
				"-distributor.remote-timeout":       "10s",
			})

			// Start dependencies.
			minio := e2edb.NewMinio(9000, blocksBucketName)

			consul := e2edb.NewConsul()
			require.NoError(t, s.StartAndWaitReady(minio, consul))

			// Start Mimir components for the write path.
			distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
			ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
			require.NoError(t, s.StartAndWaitReady(distributor, ingester))

			// Wait until the distributor has updated the ring.
			// The distributor should have 512 tokens for the ingester ring and 1 for the distributor ring
			require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))

			querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
			require.NoError(t, s.StartAndWaitReady(querier))

			// Wait until the querier has updated the ring.
			require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

			// Push a series to Mimir.
			now := time.Now()

			c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
			c.SetTimeout(10 * time.Second)
			require.NoError(t, err)

			// Generate the series
			startMs := now.Add(-time.Minute)
			endMs := now.Add(time.Minute)

			var samples []prompb.Sample
			if tc.floats != nil {
				samples = tc.floats(startMs.UnixMilli(), endMs.UnixMilli())
			}
			var histograms []prompb.Histogram
			if tc.histograms != nil {
				histograms = tc.histograms(startMs.UnixMilli(), endMs.UnixMilli())
			}

			var series []prompb.TimeSeries
			series = append(series, prompb.TimeSeries{
				Labels: []prompb.Label{
					{Name: labels.MetricName, Value: "series_1"},
				},
				Samples:    samples,
				Histograms: histograms,
			})

			res, err := c.Push(series)
			require.NoError(t, err)
			require.Equal(t, 200, res.StatusCode)

			client, err := e2emimir.NewClient("", querier.HTTPEndpoint(), "", "", "user-1")
			require.NoError(t, err)
			httpResp, results, _, err := client.RemoteReadChunks("series_1", startMs, endMs)
			require.Equal(t, http.StatusOK, httpResp.StatusCode)
			require.NoError(t, err)

			// Validate the returned remote read data
			sampleIdx := 0
			for _, result := range results {
				// We're only expected a single series `series_1`.
				require.Len(t, result.ChunkedSeries, 1)
				require.Equal(t, "series_1", result.ChunkedSeries[0].Labels[0].GetValue())

				for _, rawChk := range result.ChunkedSeries[0].Chunks {
					var enc chunkenc.Encoding
					switch rawChk.Type {
					case prompb.Chunk_XOR:
						enc = chunkenc.EncXOR
					case prompb.Chunk_HISTOGRAM:
						enc = chunkenc.EncHistogram
					case prompb.Chunk_FLOAT_HISTOGRAM:
						enc = chunkenc.EncFloatHistogram
					default:
						require.Fail(t, "unrecognized chunk type")
					}

					chk, err := chunkenc.FromData(enc, rawChk.Data)
					require.NoError(t, err)

					chkItr := chk.Iterator(nil)
					chkIdx := 0
					for valType := chkItr.Next(); valType != chunkenc.ValNone; valType = chkItr.Next() {
						require.Equal(t, tc.expectedValType, valType)
						switch valType {
						case chunkenc.ValFloat:
							ts, val := chkItr.At()
							require.Equal(t, samples[sampleIdx].Timestamp, ts)
							require.Equal(t, samples[sampleIdx].Value, val)
						case chunkenc.ValHistogram:
							ts, h := chkItr.AtHistogram(nil)
							require.Equal(t, histograms[sampleIdx].Timestamp, ts)

							expected := remote.HistogramProtoToHistogram(histograms[sampleIdx])
							test.RequireHistogramEqual(t, expected, h)
						case chunkenc.ValFloatHistogram:
							ts, fh := chkItr.AtFloatHistogram(nil)
							require.Equal(t, histograms[sampleIdx].Timestamp, ts)

							expected := remote.FloatHistogramProtoToFloatHistogram(histograms[sampleIdx])
							test.RequireFloatHistogramEqual(t, expected, fh)
						default:
							require.Fail(t, "unrecognized value type")
						}
						sampleIdx++
						chkIdx++
					}
				}
			}

			if samples != nil {
				require.Len(t, samples, sampleIdx)
			} else if histograms != nil {
				require.Len(t, histograms, sampleIdx)
			}
		})
	}
}
