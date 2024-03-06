// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func BenchmarkOTLPHandler(b *testing.B) {
	var samples []prompb.Sample
	for i := 0; i < 1000; i++ {
		ts := time.Date(2020, 4, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Second)
		samples = append(samples, prompb.Sample{
			Value:     1,
			Timestamp: ts.UnixNano(),
		})
	}
	sampleSeries := []prompb.TimeSeries{
		{
			Labels: []prompb.Label{
				{Name: "__name__", Value: "foo"},
			},
			Samples: samples,
			Histograms: []prompb.Histogram{
				remote.HistogramToHistogramProto(1337, test.GenerateTestHistogram(1)),
			},
		},
	}
	// Sample metadata needs to correspond to every series in the sampleSeries
	sampleMetadata := []mimirpb.MetricMetadata{
		{
			Help: "metric_help",
			Unit: "metric_unit",
		},
	}
	exportReq := TimeseriesToOTLPRequest(sampleSeries, sampleMetadata)

	pushFunc := func(ctx context.Context, pushReq *Request) error {
		if _, err := pushReq.WriteRequest(); err != nil {
			return err
		}

		pushReq.CleanUp()
		return nil
	}
	limits, err := validation.NewOverrides(
		validation.Limits{},
		validation.NewMockTenantLimits(map[string]*validation.Limits{}),
	)
	require.NoError(b, err)
	handler := OTLPHandler(100000, nil, false, true, limits, RetryConfig{}, prometheus.NewPedanticRegistry(), pushFunc, log.NewNopLogger())

	b.Run("protobuf", func(b *testing.B) {
		req := createOTLPProtoRequest(b, exportReq, false)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)
			require.Equal(b, http.StatusOK, resp.Code)
			req.Body.(*reusableReader).Reset()
		}
	})

	b.Run("JSON", func(b *testing.B) {
		req := createOTLPJSONRequest(b, exportReq, false)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)
			require.Equal(b, http.StatusOK, resp.Code)
			req.Body.(*reusableReader).Reset()
		}
	})
}

func createOTLPProtoRequest(tb testing.TB, metricRequest pmetricotlp.ExportRequest, compress bool) *http.Request {
	tb.Helper()

	body, err := metricRequest.MarshalProto()
	require.NoError(tb, err)

	return createOTLPRequest(tb, body, compress, "application/x-protobuf")
}

func createOTLPJSONRequest(tb testing.TB, metricRequest pmetricotlp.ExportRequest, compress bool) *http.Request {
	tb.Helper()

	body, err := metricRequest.MarshalJSON()
	require.NoError(tb, err)

	return createOTLPRequest(tb, body, compress, "application/json")
}

func createOTLPRequest(tb testing.TB, body []byte, compress bool, contentType string) *http.Request {
	tb.Helper()

	if compress {
		var b bytes.Buffer
		gz := gzip.NewWriter(&b)
		_, err := gz.Write(body)
		require.NoError(tb, err)
		require.NoError(tb, gz.Close())

		body = b.Bytes()
	}

	// reusableReader is suitable for benchmarks
	req, err := http.NewRequest("POST", "http://localhost/", newReusableReader(body))
	require.NoError(tb, err)
	// Since http.NewRequest will deduce content length only from known io.Reader implementations,
	// define it ourselves
	req.ContentLength = int64(len(body))
	req.Header.Set("Content-Type", contentType)
	const tenantID = "test"
	req.Header.Set("X-Scope-OrgID", tenantID)
	ctx := user.InjectOrgID(context.Background(), tenantID)
	req = req.WithContext(ctx)
	if compress {
		req.Header.Set("Content-Encoding", "gzip")
	}

	return req
}

type reusableReader struct {
	*bytes.Reader
	raw []byte
}

func newReusableReader(raw []byte) *reusableReader {
	return &reusableReader{
		Reader: bytes.NewReader(raw),
		raw:    raw,
	}
}

func (r *reusableReader) Close() error {
	return nil
}

func (r *reusableReader) Reset() {
	r.Reader.Reset(r.raw)
}

var _ io.ReadCloser = &reusableReader{}
