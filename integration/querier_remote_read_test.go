// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/querier_remote_read_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"net/http"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
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
		map[string]string{
			// This test writes samples sparse in time. We don't want compaction to trigger while testing.
			"-blocks-storage.tsdb.block-ranges-period": "2h",
		},
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

	t.Run("float series", func(t *testing.T) {
		runTestPushSeriesForQuerierRemoteRead(t, c, querier, "series_1", generateFloatSeries)
	})

	t.Run("histogram series", func(t *testing.T) {
		runTestPushSeriesForQuerierRemoteRead(t, c, querier, "hseries_1", generateHistogramSeries)
	})
}

func runTestPushSeriesForQuerierRemoteRead(t *testing.T, c *e2emimir.Client, querier *e2emimir.MimirService, seriesName string, genSeries generateSeriesFunc) {
	now := time.Now()

	// Generate multiple series, sparse in time.
	series1, expectedVector1, _ := genSeries(seriesName, now.Add(-10*time.Minute))
	series2, expectedVector2, _ := genSeries(seriesName, now)

	for _, series := range [][]prompb.TimeSeries{series1, series2} {
		res, err := c.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)
	}

	tests := map[string]struct {
		query          *prompb.Query
		expectedVector model.Vector
	}{
		"remote read request without hints": {
			query: &prompb.Query{
				Matchers:         remoteReadQueryMatchersByMetricName(seriesName),
				StartTimestampMs: now.Add(-1 * time.Minute).UnixMilli(),
				EndTimestampMs:   now.Add(+1 * time.Minute).UnixMilli(),
			},
			expectedVector: expectedVector2,
		},
		"remote read request with hints time range equal to query time range": {
			query: &prompb.Query{
				Matchers:         remoteReadQueryMatchersByMetricName(seriesName),
				StartTimestampMs: now.Add(-1 * time.Minute).UnixMilli(),
				EndTimestampMs:   now.Add(+1 * time.Minute).UnixMilli(),
				Hints: &prompb.ReadHints{
					StartMs: now.Add(-1 * time.Minute).UnixMilli(),
					EndMs:   now.Add(+1 * time.Minute).UnixMilli(),
				},
			},
			expectedVector: expectedVector2,
		},
		"remote read request with hints time range different than query time range": {
			query: &prompb.Query{
				Matchers:         remoteReadQueryMatchersByMetricName(seriesName),
				StartTimestampMs: now.Add(-1 * time.Minute).UnixMilli(),
				EndTimestampMs:   now.Add(+1 * time.Minute).UnixMilli(),
				Hints: &prompb.ReadHints{
					StartMs: now.Add(-11 * time.Minute).UnixMilli(),
					EndMs:   now.Add(-9 * time.Minute).UnixMilli(),
				},
			},
			expectedVector: expectedVector1,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			client, err := e2emimir.NewClient("", querier.HTTPEndpoint(), "", "", "user-1")
			require.NoError(t, err)
			httpResp, resp, _, err := client.RemoteRead(testData.query)
			require.Equal(t, http.StatusOK, httpResp.StatusCode)
			require.NoError(t, err)

			// Validate the returned remote read data matches what was written
			require.Len(t, resp.Timeseries, 1)
			require.Len(t, resp.Timeseries[0].Labels, 1)
			require.Equal(t, seriesName, resp.Timeseries[0].Labels[0].GetValue())
			isSeriesFloat := len(resp.Timeseries[0].Samples) > 0
			isSeriesHistogram := len(resp.Timeseries[0].Histograms) > 0
			require.Equal(t, isSeriesFloat, !isSeriesHistogram)
			if isSeriesFloat {
				require.Len(t, resp.Timeseries[0].Samples, 1)
				require.Equal(t, int64(testData.expectedVector[0].Timestamp), resp.Timeseries[0].Samples[0].Timestamp)
				require.Equal(t, float64(testData.expectedVector[0].Value), resp.Timeseries[0].Samples[0].Value)
			} else if isSeriesHistogram {
				require.Len(t, resp.Timeseries[0].Histograms, 1)
				require.Equal(t, testData.expectedVector[0].Histogram, mimirpb.FromHistogramToPromHistogram(resp.Timeseries[0].Histograms[0].ToIntHistogram()))
			}
		})
	}
}

func TestQuerierStreamingRemoteRead(t *testing.T) {
	var (
		// This test runs with a fixed time so that assertions are stable. The reason is that the formula used by TSDB
		// to cut chunks has some rounding which may cause this test to be flaky if run with a random "now" time.
		now           = must(time.Parse(time.RFC3339, "2024-01-01T00:00:00Z"))
		pushedStartMs = now.Add(-time.Minute).UnixMilli()
		pushedEndMs   = now.Add(time.Minute).UnixMilli()
		pushedStepMs  = int64(100) // Simulate a high frequency scraping (10Hz).
	)

	const (
		floatMetricName          = "series_float"
		histogramMetricName      = "series_histogram"
		floatHistogramMetricName = "series_float_histogram"
	)

	tests := map[string]struct {
		valType         chunkenc.ValueType
		metricName      string
		query           *prompb.Query
		expectedStartMs int64
		expectedEndMs   int64
	}{
		"float samples, remote read request without hints": {
			valType:    chunkenc.ValFloat,
			metricName: floatMetricName,
			query: &prompb.Query{
				Matchers:         remoteReadQueryMatchersByMetricName(floatMetricName),
				StartTimestampMs: pushedStartMs,
				EndTimestampMs:   pushedEndMs,
			},
			expectedStartMs: pushedStartMs,
			expectedEndMs:   pushedEndMs,
		},
		"float samples, with hints time range equal to query time range": {
			valType:    chunkenc.ValFloat,
			metricName: floatMetricName,
			query: &prompb.Query{
				Matchers:         remoteReadQueryMatchersByMetricName(floatMetricName),
				StartTimestampMs: pushedStartMs,
				EndTimestampMs:   pushedEndMs,
				Hints: &prompb.ReadHints{
					StartMs: pushedStartMs,
					EndMs:   pushedEndMs,
				},
			},
			expectedStartMs: pushedStartMs,
			expectedEndMs:   pushedEndMs,
		},
		"float samples, with hints time range different than query time range": {
			valType:    chunkenc.ValFloat,
			metricName: floatMetricName,
			query: &prompb.Query{
				Matchers:         remoteReadQueryMatchersByMetricName(floatMetricName),
				StartTimestampMs: pushedStartMs,
				EndTimestampMs:   pushedEndMs,
				Hints: &prompb.ReadHints{
					StartMs: now.Add(5 * time.Second).UnixMilli(),
					EndMs:   now.Add(15 * time.Second).UnixMilli(),
				},
			},
			// Mimir doesn't cut returned chunks to the requested time range for performance reasons. This means
			// that we get the entire chunks in output. TSDB targets to cut chunks once every 120 samples, but
			// it's based on estimation and math is not super accurate. That's why we run this test with a fixed time.
			expectedStartMs: now.UnixMilli(),
			expectedEndMs:   now.Add(23300 * time.Millisecond).UnixMilli(),
		},
		"histograms": {
			valType:         chunkenc.ValHistogram,
			metricName:      histogramMetricName,
			query:           remoteReadQueryByMetricName(histogramMetricName, time.UnixMilli(pushedStartMs), time.UnixMilli(pushedEndMs)),
			expectedStartMs: pushedStartMs,
			expectedEndMs:   pushedEndMs,
		},
		"float histograms": {
			valType:         chunkenc.ValFloatHistogram,
			metricName:      floatHistogramMetricName,
			query:           remoteReadQueryByMetricName(floatHistogramMetricName, time.UnixMilli(pushedStartMs), time.UnixMilli(pushedEndMs)),
			expectedStartMs: pushedStartMs,
			expectedEndMs:   pushedEndMs,
		},
	}

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
		"-distributor.ingestion-rate-limit": "1048576",
		"-distributor.ingestion-burst-size": "1048576",
		"-distributor.remote-timeout":       "10s",

		// This test writes samples sparse in time. We don't want compaction to trigger while testing.
		"-blocks-storage.tsdb.block-ranges-period": "2h",

		// This test writes samples with an old timestamp.
		"-querier.query-ingesters-within": "0",
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

	c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", "user-1")
	c.SetTimeout(10 * time.Second)
	require.NoError(t, err)

	// Generate all the samples and histograms.
	var floats []prompb.Sample
	var histograms []prompb.Histogram
	var floatHistograms []prompb.Histogram

	for ts := pushedStartMs; ts < pushedEndMs; ts += pushedStepMs {
		floats = append(floats, prompb.Sample{Value: float64(ts), Timestamp: ts})
		histograms = append(histograms, prompb.FromIntHistogram(ts, test.GenerateTestHistogram(int(ts))))
		floatHistograms = append(floatHistograms, prompb.FromFloatHistogram(ts, test.GenerateTestFloatHistogram(int(ts))))
	}

	// Generate the series.
	seriesToPush := []prompb.TimeSeries{
		{
			Labels:  []prompb.Label{{Name: labels.MetricName, Value: floatMetricName}},
			Samples: floats,
		}, {
			Labels:     []prompb.Label{{Name: labels.MetricName, Value: histogramMetricName}},
			Histograms: histograms,
		}, {
			Labels:     []prompb.Label{{Name: labels.MetricName, Value: floatHistogramMetricName}},
			Histograms: floatHistograms,
		},
	}

	// Push a series to Mimir.
	res, err := c.Push(seriesToPush)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			expectedSamples := filterSamplesByTimestamp(floats, testData.expectedStartMs, testData.expectedEndMs)
			expectedHistograms := filterHistogramsByTimestamp(histograms, testData.expectedStartMs, testData.expectedEndMs)
			expectedFloatHistograms := filterHistogramsByTimestamp(floatHistograms, testData.expectedStartMs, testData.expectedEndMs)

			httpResp, results, _, err := c.RemoteReadChunks(testData.query)
			require.Equal(t, http.StatusOK, httpResp.StatusCode)
			require.NoError(t, err)

			// Validate the returned remote read data
			sampleIdx := 0
			for _, result := range results {
				// We expect only 1 series.
				require.Len(t, result.ChunkedSeries, 1)
				require.Equal(t, testData.metricName, result.ChunkedSeries[0].Labels[0].GetValue())

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
					for valType := chkItr.Next(); valType != chunkenc.ValNone; valType = chkItr.Next() {
						require.Equal(t, testData.valType, valType)
						switch valType {
						case chunkenc.ValFloat:
							ts, val := chkItr.At()
							require.Equalf(t, expectedSamples[sampleIdx].Timestamp, ts, "index: %d", sampleIdx)
							require.Equalf(t, expectedSamples[sampleIdx].Value, val, "index: %d", sampleIdx)
						case chunkenc.ValHistogram:
							ts, h := chkItr.AtHistogram(nil)
							require.Equalf(t, expectedHistograms[sampleIdx].Timestamp, ts, "index: %d", sampleIdx)

							expected := expectedHistograms[sampleIdx].ToIntHistogram()
							test.RequireHistogramEqual(t, expected, h)
						case chunkenc.ValFloatHistogram:
							ts, fh := chkItr.AtFloatHistogram(nil)
							require.Equalf(t, expectedFloatHistograms[sampleIdx].Timestamp, ts, "index: %d", sampleIdx)

							expected := expectedFloatHistograms[sampleIdx].ToFloatHistogram()
							test.RequireFloatHistogramEqual(t, expected, fh)
						default:
							require.Fail(t, "unrecognized value type")
						}
						sampleIdx++
					}
				}
			}

			if expectedSamples != nil {
				require.Len(t, expectedSamples, sampleIdx)
			} else if expectedHistograms != nil {
				require.Len(t, expectedHistograms, sampleIdx)
			}
		})
	}

}
