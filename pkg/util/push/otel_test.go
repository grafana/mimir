// SPDX-License-Identifier: AGPL-3.0-only

package push

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func BenchmarkOTLPHandler(b *testing.B) {
	defaultMaxRecvMsgSize := 100 << 20
	reg := prometheus.NewPedanticRegistry()
	push := func(ctx context.Context, req *Request) (*mimirpb.WriteResponse, error) {
		_, err := req.WriteRequest()

		if err != nil {
			b.Log(err)
		}

		return nil, err
	}
	handler := OTLPHandler(defaultMaxRecvMsgSize, nil, false, reg, push)

	for _, resourceAttributeCount := range []int{0, 5, 50} {
		b.Run(fmt.Sprintf("resource attribute count: %v", resourceAttributeCount), func(b *testing.B) {
			for _, histogramCount := range []int{0, 1000} {
				b.Run(fmt.Sprintf("histogram count: %v", histogramCount), func(b *testing.B) {
					for _, nonHistogramCount := range []int{0, 1000} {
						b.Run(fmt.Sprintf("non-histogram count: %v", nonHistogramCount), func(b *testing.B) {
							if resourceAttributeCount == 0 && histogramCount == 0 && nonHistogramCount == 0 {
								// We won't generate any series.
								return
							}

							for _, labelsPerMetric := range []int{2, 20} {
								b.Run(fmt.Sprintf("labels per metric: %v", labelsPerMetric), func(b *testing.B) {
									for _, exemplarsPerSeries := range []int{0, 5, 10} {
										payload := createOTLPExportRequest(resourceAttributeCount, histogramCount, nonHistogramCount, labelsPerMetric, exemplarsPerSeries)
										payloadBytes, err := payload.MarshalProto()
										require.NoError(b, err)

										buffer := &bytes.Buffer{}
										gzipWriter := gzip.NewWriter(buffer)
										_, err = gzipWriter.Write(payloadBytes)
										require.NoError(b, err)
										require.NoError(b, gzipWriter.Close())
										compressedBytes := buffer.Bytes()

										b.Logf("Payload size: %v B\n", len(compressedBytes))

										reader := bytes.NewReader(compressedBytes)
										req, err := http.NewRequest("POST", "/otlp/v1/metrics", reader)
										require.NoError(b, err)
										req.Header.Set("Content-Encoding", "gzip")
										req.Header.Set("Content-Type", pbContentType)
										req.Header.Set("X-Scope-OrgID", "test")

										_, ctx, err := tenant.ExtractTenantIDFromHTTPRequest(req)
										require.NoError(b, err)
										req = req.WithContext(ctx)

										b.Run(fmt.Sprintf("exemplars per series: %v", exemplarsPerSeries), func(b *testing.B) {
											for i := 0; i < b.N; i++ {
												reader.Reset(compressedBytes)
												w := &httptest.ResponseRecorder{}

												handler.ServeHTTP(w, req)

												result := w.Result()

												if result.StatusCode != 200 {
													require.Equalf(b, 200, result.StatusCode, "test request failed with HTTP %v (%v)", result.StatusCode, result.Status)
												}
											}
										})
									}
								})
							}
						})
					}
				})
			}
		})
	}
}

func createOTLPExportRequest(resourceAttributeCount int, histogramCount int, nonHistogramCount int, labelsPerMetric int, exemplarsPerSeries int) pmetricotlp.ExportRequest {
	request := pmetricotlp.NewExportRequest()

	rm := request.Metrics().ResourceMetrics().AppendEmpty()
	generateAttributes(rm.Resource().Attributes(), "resource", resourceAttributeCount)

	metrics := rm.ScopeMetrics().AppendEmpty().Metrics()
	ts := pcommon.NewTimestampFromTime(time.Now())

	for i := 1; i <= histogramCount; i++ {
		m := metrics.AppendEmpty()
		m.SetEmptyHistogram()
		m.SetName(fmt.Sprintf("histogram-%v", i))
		m.Histogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		h := m.Histogram().DataPoints().AppendEmpty()
		h.SetTimestamp(ts)

		// TODO: test with different numbers of buckets
		// Set 50 samples, 10 each with values 0.5, 1, 2, 4, and 8
		h.SetCount(50)
		h.SetSum(155)
		h.BucketCounts().FromRaw([]uint64{10, 10, 10, 10, 10, 0})
		h.ExplicitBounds().FromRaw([]float64{.5, 1, 2, 4, 8, 16}) // Bucket boundaries include the upper limit (ie. each sample is on the upper limit of its bucket)

		generateAttributes(h.Attributes(), "series", labelsPerMetric)
		generateExemplars(h.Exemplars(), exemplarsPerSeries, ts)
	}

	for i := 1; i <= nonHistogramCount; i++ {
		m := metrics.AppendEmpty()
		m.SetEmptySum()
		m.SetName(fmt.Sprintf("sum-%v", i))
		point := m.Sum().DataPoints().AppendEmpty()
		point.SetTimestamp(ts)
		point.SetDoubleValue(1.23)
		generateAttributes(point.Attributes(), "series", labelsPerMetric)
		generateExemplars(point.Exemplars(), exemplarsPerSeries, ts)
	}

	for i := 1; i <= nonHistogramCount; i++ {
		m := metrics.AppendEmpty()
		m.SetEmptyGauge()
		m.SetName(fmt.Sprintf("gauge-%v", i))
		point := m.Gauge().DataPoints().AppendEmpty()
		point.SetTimestamp(ts)
		point.SetDoubleValue(1.23)
		generateAttributes(point.Attributes(), "series", labelsPerMetric)
		generateExemplars(point.Exemplars(), exemplarsPerSeries, ts)
	}

	return request
}

func generateAttributes(m pcommon.Map, prefix string, count int) {
	for i := 1; i <= count; i++ {
		m.PutStr(fmt.Sprintf("%v-name-%v", prefix, i), fmt.Sprintf("value-%v", i))
	}
}

func generateExemplars(exemplars pmetric.ExemplarSlice, count int, ts pcommon.Timestamp) {
	for i := 1; i <= count; i++ {
		e := exemplars.AppendEmpty()
		e.SetTimestamp(ts)
		e.SetDoubleValue(2.22)
		e.SetSpanID(pcommon.SpanID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08})
		e.SetTraceID(pcommon.TraceID{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f})
	}
}
