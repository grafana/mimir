// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/querier_remote_read_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/test"

	"github.com/grafana/mimir/integration/e2emimir"
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

	matcher, err := labels.NewMatcher(labels.MatchEqual, "__name__", seriesName)
	require.NoError(t, err)

	startMs := now.Add(-1*time.Minute).Unix() * 1000
	endMs := now.Add(time.Minute).Unix() * 1000

	q, err := remote.ToQuery(startMs, endMs, []*labels.Matcher{matcher}, &storage.SelectHints{
		Step:  1,
		Start: startMs,
		End:   endMs,
	})
	require.NoError(t, err)

	req := &prompb.ReadRequest{
		Queries:               []*prompb.Query{q},
		AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_SAMPLES},
	}

	data, err := proto.Marshal(req)
	require.NoError(t, err)
	compressed := snappy.Encode(nil, data)

	// Call the remote read API endpoint with a timeout.
	httpReqCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(httpReqCtx, "POST", "http://"+querier.HTTPEndpoint()+"/prometheus/api/v1/read", bytes.NewReader(compressed))
	require.NoError(t, err)
	httpReq.Header.Set("X-Scope-OrgID", "user-1")
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Add("Accept-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", "Prometheus/1.8.2")
	httpReq.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

	httpResp, err := http.DefaultClient.Do(httpReq)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)

	compressed, err = io.ReadAll(httpResp.Body)
	require.NoError(t, err)

	uncompressed, err := snappy.Decode(nil, compressed)
	require.NoError(t, err)

	var resp prompb.ReadResponse
	err = proto.Unmarshal(uncompressed, &resp)
	require.NoError(t, err)

	// Validate the returned remote read data matches what was written
	require.Len(t, resp.Results, 1)
	require.Len(t, resp.Results[0].Timeseries, 1)
	require.Len(t, resp.Results[0].Timeseries[0].Labels, 1)
	require.Equal(t, seriesName, resp.Results[0].Timeseries[0].Labels[0].GetValue())
	isSeriesFloat := len(resp.Results[0].Timeseries[0].Samples) == 1
	isSeriesHistogram := len(resp.Results[0].Timeseries[0].Histograms) == 1
	require.Equal(t, isSeriesFloat, !isSeriesHistogram)
	if isSeriesFloat {
		require.Equal(t, int64(expectedVectors[0].Timestamp), resp.Results[0].Timeseries[0].Samples[0].Timestamp)
		require.Equal(t, float64(expectedVectors[0].Value), resp.Results[0].Timeseries[0].Samples[0].Value)
	} else if isSeriesHistogram {
		isEqualSampleAndHistogram(t, expectedVectors[0], resp.Results[0].Timeseries[0].Histograms[0])
	}
}

func isEqualSampleAndHistogram(t *testing.T, expectedVector *model.Sample, histogram prompb.Histogram) {
	require.Equal(t, int64(expectedVector.Timestamp), histogram.Timestamp)
	require.Equal(t, uint64(expectedVector.Histogram.Count), histogram.GetCountInt())
	require.Equal(t, float64(expectedVector.Histogram.Sum), histogram.Sum)
	idx := 0
	it := remote.HistogramProtoToHistogram(histogram).ToFloat().AllBucketIterator()
	for it.Next() {
		bucket := it.At()
		if bucket.Count == 0 {
			continue
		}
		require.Equal(t, float64(expectedVector.Histogram.Buckets[idx].Lower), bucket.Lower)
		require.Equal(t, float64(expectedVector.Histogram.Buckets[idx].Upper), bucket.Upper)
		require.Equal(t, float64(expectedVector.Histogram.Buckets[idx].Count), bucket.Count)
		idx++
	}
	require.Equal(t, len(expectedVector.Histogram.Buckets), idx)
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
			require.NoError(t, err)

			// Generate the series
			startMs := now.Add(-time.Minute).Unix() * 1000
			endMs := now.Add(time.Minute).Unix() * 1000

			var samples []prompb.Sample
			if tc.floats != nil {
				samples = tc.floats(startMs, endMs)
			}
			var histograms []prompb.Histogram
			if tc.histograms != nil {
				histograms = tc.histograms(startMs, endMs)
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

			matcher, err := labels.NewMatcher(labels.MatchEqual, "__name__", "series_1")
			require.NoError(t, err)

			q, err := remote.ToQuery(startMs, endMs, []*labels.Matcher{matcher}, &storage.SelectHints{
				Step:  1,
				Start: startMs,
				End:   endMs,
			})
			require.NoError(t, err)

			req := &prompb.ReadRequest{
				Queries:               []*prompb.Query{q},
				AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS},
			}

			data, err := proto.Marshal(req)
			require.NoError(t, err)
			compressed := snappy.Encode(nil, data)

			// Call the remote read API endpoint with a timeout.
			httpReqCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			httpReq, err := http.NewRequestWithContext(httpReqCtx, "POST", "http://"+querier.HTTPEndpoint()+"/prometheus/api/v1/read", bytes.NewReader(compressed))
			require.NoError(t, err)
			httpReq.Header.Add("Accept-Encoding", "snappy")
			httpReq.Header.Set("X-Scope-OrgID", "user-1")
			httpReq.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

			httpResp, err := http.DefaultClient.Do(httpReq)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, httpResp.StatusCode)

			// Fetch streaming response
			stream := remote.NewChunkedReader(httpResp.Body, remote.DefaultChunkedReadLimit, nil)

			var results []prompb.ChunkedReadResponse
			for {
				var res prompb.ChunkedReadResponse
				err := stream.NextProto(&res)
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)
				results = append(results, res)
			}

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
					// TODO: Once prompb.Chunk_FLOAT_HISTOGRAM gets added in https://github.com/prometheus/prometheus/pull/12085 we can switch to using that
					case prompb.Chunk_Encoding(3):
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
							ts, h := chkItr.AtHistogram()
							require.Equal(t, histograms[sampleIdx].Timestamp, ts)

							expected := remote.HistogramProtoToHistogram(histograms[sampleIdx])
							test.RequireHistogramEqual(t, expected, h)
						case chunkenc.ValFloatHistogram:
							ts, fh := chkItr.AtFloatHistogram()
							require.Equal(t, histograms[sampleIdx].Timestamp, ts)

							expected := remote.HistogramProtoToFloatHistogram(histograms[sampleIdx])
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
