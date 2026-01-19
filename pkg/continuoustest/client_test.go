// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"compress/gzip"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/dskit/flagext"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/api"
)

func TestOTLPHttpClient_WriteSeries(t *testing.T) {
	var (
		nextStatusCode   = http.StatusOK
		receivedRequests []pmetricotlp.ExportRequest
	)

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// Handle compression
		reader, err := gzip.NewReader(request.Body)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, reader.Close())
		})

		// Then Unmarshal
		body, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.NoError(t, request.Body.Close())

		req := pmetricotlp.NewExportRequest()
		require.NoError(t, req.UnmarshalProto(body))

		receivedRequests = append(receivedRequests, req)
		writer.WriteHeader(nextStatusCode)
	}))
	t.Cleanup(server.Close)

	cfg := ClientConfig{}
	flagext.DefaultValues(&cfg)
	cfg.WriteBatchSize = 10
	cfg.WriteProtocol = "otlp-http"

	require.NoError(t, cfg.WriteBaseEndpoint.Set(server.URL))
	require.NoError(t, cfg.ReadBaseEndpoint.Set(server.URL))

	c, err := NewClient(cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry())
	require.NoError(t, err)

	ctx := context.Background()
	now := time.Now()

	t.Run("write series in a single batch", func(t *testing.T) {
		receivedRequests = nil
		nextStatusCode = http.StatusOK

		series := generateSineWaveSeries("test", now, 10)
		metadata := []prompb.MetricMetadata{{
			Type:             prompb.MetricMetadata_GAUGE,
			MetricFamilyName: "test",
			Help:             "Test metric",
			Unit:             "seconds",
		}}
		statusCode, err := c.WriteSeries(ctx, series, metadata)
		require.NoError(t, err)
		assert.Equal(t, 200, statusCode)

		require.Len(t, receivedRequests, 1)
		assert.Equal(t, len(series), receivedRequests[0].Metrics().MetricCount())
		receivedMetric := receivedRequests[0].Metrics().ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
		assert.Equal(t, "test", receivedMetric.Name())
		assert.Equal(t, "Test metric", receivedMetric.Description())
		assert.Equal(t, "seconds", receivedMetric.Unit())
	})

	t.Run("write series in multiple batches", func(t *testing.T) {
		receivedRequests = nil
		nextStatusCode = http.StatusOK

		series := generateSineWaveSeries("test", now, 22)
		metadata := []prompb.MetricMetadata{{
			Type:             prompb.MetricMetadata_COUNTER,
			MetricFamilyName: "test",
			Help:             "Test counter",
			Unit:             "bytes",
		}}
		statusCode, err := c.WriteSeries(ctx, series, metadata)
		require.NoError(t, err)
		assert.Equal(t, 200, statusCode)

		require.Len(t, receivedRequests, 3)
		assert.Equal(t, 10, receivedRequests[0].Metrics().MetricCount())
		assert.Equal(t, 10, receivedRequests[1].Metrics().MetricCount())
		assert.Equal(t, 2, receivedRequests[2].Metrics().MetricCount())
	})

	t.Run("request failed with 4xx error", func(t *testing.T) {
		receivedRequests = nil
		nextStatusCode = http.StatusBadRequest

		series := generateSineWaveSeries("test", now, 1)
		statusCode, err := c.WriteSeries(ctx, series, nil)
		require.Error(t, err)
		assert.Equal(t, 400, statusCode)
	})

	t.Run("request failed with 5xx error", func(t *testing.T) {
		receivedRequests = nil
		nextStatusCode = http.StatusInternalServerError

		series := generateSineWaveSeries("test", now, 1)
		statusCode, err := c.WriteSeries(ctx, series, nil)
		require.Error(t, err)
		assert.Equal(t, 500, statusCode)
	})

}

func TestPromWriterClient_WriteSeries(t *testing.T) {
	var (
		nextStatusCode   = http.StatusOK
		receivedRequests []prompb.WriteRequest
	)

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// Read the entire body.
		body, err := io.ReadAll(request.Body)
		require.NoError(t, err)
		require.NoError(t, request.Body.Close())

		// Decode and unmarshal it.
		body, err = snappy.Decode(nil, body)
		require.NoError(t, err)

		var req prompb.WriteRequest
		require.NoError(t, proto.Unmarshal(body, &req))
		receivedRequests = append(receivedRequests, req)

		writer.WriteHeader(nextStatusCode)
	}))
	t.Cleanup(server.Close)

	cfg := ClientConfig{}
	flagext.DefaultValues(&cfg)
	cfg.WriteBatchSize = 10
	require.NoError(t, cfg.WriteBaseEndpoint.Set(server.URL))
	require.NoError(t, cfg.ReadBaseEndpoint.Set(server.URL))

	c, err := NewClient(cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry())
	require.NoError(t, err)

	ctx := context.Background()
	now := time.Now()

	t.Run("write series in a single batch", func(t *testing.T) {
		receivedRequests = nil
		nextStatusCode = http.StatusOK

		series := generateSineWaveSeries("test", now, 10)
		metadata := []prompb.MetricMetadata{{
			Type:             prompb.MetricMetadata_GAUGE,
			MetricFamilyName: "test",
			Help:             "Test metric",
			Unit:             "seconds",
		}}
		statusCode, err := c.WriteSeries(ctx, series, metadata)
		require.NoError(t, err)
		assert.Equal(t, 200, statusCode)

		require.Len(t, receivedRequests, 1)
		assert.Equal(t, metadata, receivedRequests[0].Metadata)
	})

	t.Run("write series in multiple batches", func(t *testing.T) {
		receivedRequests = nil
		nextStatusCode = http.StatusOK

		series := generateSineWaveSeries("test", now, 22)
		metadata := []prompb.MetricMetadata{{
			Type:             prompb.MetricMetadata_COUNTER,
			MetricFamilyName: "test",
			Help:             "Test counter",
			Unit:             "bytes",
		}}
		statusCode, err := c.WriteSeries(ctx, series, metadata)
		require.NoError(t, err)
		assert.Equal(t, 200, statusCode)

		require.Len(t, receivedRequests, 3)
		assert.Equal(t, series[0:10], receivedRequests[0].Timeseries)
		assert.Equal(t, metadata, receivedRequests[0].Metadata)
		assert.Equal(t, series[10:20], receivedRequests[1].Timeseries)
		assert.Equal(t, metadata, receivedRequests[1].Metadata)
		assert.Equal(t, series[20:22], receivedRequests[2].Timeseries)
		assert.Equal(t, metadata, receivedRequests[2].Metadata)
	})

	t.Run("request failed with 4xx error", func(t *testing.T) {
		receivedRequests = nil
		nextStatusCode = http.StatusBadRequest

		series := generateSineWaveSeries("test", now, 1)
		metadata := []prompb.MetricMetadata{{
			Type:             prompb.MetricMetadata_GAUGE,
			MetricFamilyName: "test",
			Help:             "Test metric",
			Unit:             "seconds",
		}}
		statusCode, err := c.WriteSeries(ctx, series, metadata)
		require.Error(t, err)
		assert.Equal(t, 400, statusCode)
	})

	t.Run("request failed with 5xx error", func(t *testing.T) {
		receivedRequests = nil
		nextStatusCode = http.StatusInternalServerError

		series := generateSineWaveSeries("test", now, 1)
		metadata := []prompb.MetricMetadata{{
			Type:             prompb.MetricMetadata_GAUGE,
			MetricFamilyName: "test",
			Help:             "Test metric",
			Unit:             "seconds",
		}}
		statusCode, err := c.WriteSeries(ctx, series, metadata)
		require.Error(t, err)
		assert.Equal(t, 500, statusCode)
	})

}

func TestProm2WriterClient_WriteSeries(t *testing.T) {
	var (
		nextStatusCode   = http.StatusOK
		receivedRequests []prompb.WriteRequest
	)

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// Read the entire body.
		body, err := io.ReadAll(request.Body)
		require.NoError(t, err)
		require.NoError(t, request.Body.Close())

		// Decode and unmarshal it.
		body, err = snappy.Decode(nil, body)
		require.NoError(t, err)

		// Use mimirpb.PreallocWriteRequest, because it knows how to unmarshal
		// from RW2 too.
		var reqRW2 mimirpb.PreallocWriteRequest
		reqRW2.UnmarshalFromRW2 = true
		require.NoError(t, reqRW2.Unmarshal(body))

		// Re-encode as prompb.WriteRequest to match the expected type.
		var req prompb.WriteRequest
		raw, err := reqRW2.Marshal()
		require.NoError(t, err)
		err = proto.Unmarshal(raw, &req)
		require.NoError(t, err)
		for _, ts := range req.Timeseries {
			slices.SortFunc(ts.Labels, func(a, b prompb.Label) int {
				return strings.Compare(a.Name, b.Name)
			})
		}

		receivedRequests = append(receivedRequests, req)

		writer.WriteHeader(nextStatusCode)
	}))
	t.Cleanup(server.Close)

	cfg := ClientConfig{}
	flagext.DefaultValues(&cfg)
	cfg.WriteBatchSize = 10
	cfg.WriteProtocol = "prometheus2"
	require.NoError(t, cfg.WriteBaseEndpoint.Set(server.URL))
	require.NoError(t, cfg.ReadBaseEndpoint.Set(server.URL))

	c, err := NewClient(cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry())
	require.NoError(t, err)

	ctx := context.Background()
	now := time.Now()

	t.Run("write series in a single batch", func(t *testing.T) {
		receivedRequests = nil
		nextStatusCode = http.StatusOK

		series := generateSineWaveSeries("test", now, 10)
		metadata := []prompb.MetricMetadata{{
			Type:             prompb.MetricMetadata_GAUGE,
			MetricFamilyName: "test",
			Help:             "Test metric",
			Unit:             "seconds",
		}}
		statusCode, err := c.WriteSeries(ctx, series, metadata)
		require.NoError(t, err)
		assert.Equal(t, 200, statusCode)

		require.Len(t, receivedRequests, 1)
		for _, ts := range series {
			slices.SortFunc(ts.Labels, func(a, b prompb.Label) int {
				return strings.Compare(a.Name, b.Name)
			})
		}
		assert.Equal(t, series, receivedRequests[0].Timeseries)
		assert.Equal(t, metadata, receivedRequests[0].Metadata)
	})

	t.Run("write series in multiple batches", func(t *testing.T) {
		receivedRequests = nil
		nextStatusCode = http.StatusOK

		series := generateSineWaveSeries("test", now, 22)
		metadata := []prompb.MetricMetadata{{
			Type:             prompb.MetricMetadata_COUNTER,
			MetricFamilyName: "test",
			Help:             "Test counter",
			Unit:             "bytes",
		}}
		statusCode, err := c.WriteSeries(ctx, series, metadata)
		require.NoError(t, err)
		assert.Equal(t, 200, statusCode)

		require.Len(t, receivedRequests, 3)
		for _, ts := range series {
			slices.SortFunc(ts.Labels, func(a, b prompb.Label) int {
				return strings.Compare(a.Name, b.Name)
			})
		}
		assert.Equal(t, series[0:10], receivedRequests[0].Timeseries)
		assert.Equal(t, metadata, receivedRequests[0].Metadata)
		assert.Equal(t, series[10:20], receivedRequests[1].Timeseries)
		assert.Equal(t, metadata, receivedRequests[1].Metadata)
		assert.Equal(t, series[20:22], receivedRequests[2].Timeseries)
		assert.Equal(t, metadata, receivedRequests[2].Metadata)
	})

	t.Run("request failed with 4xx error", func(t *testing.T) {
		receivedRequests = nil
		nextStatusCode = http.StatusBadRequest

		series := generateSineWaveSeries("test", now, 1)
		metadata := []prompb.MetricMetadata{{
			Type:             prompb.MetricMetadata_GAUGE,
			MetricFamilyName: "test",
			Help:             "Test metric",
			Unit:             "seconds",
		}}
		statusCode, err := c.WriteSeries(ctx, series, metadata)
		require.Error(t, err)
		assert.Equal(t, 400, statusCode)
	})

	t.Run("request failed with 5xx error", func(t *testing.T) {
		receivedRequests = nil
		nextStatusCode = http.StatusInternalServerError

		series := generateSineWaveSeries("test", now, 1)
		metadata := []prompb.MetricMetadata{{
			Type:             prompb.MetricMetadata_COUNTER,
			MetricFamilyName: "test",
			Help:             "Test counter",
			Unit:             "bytes",
		}}
		statusCode, err := c.WriteSeries(ctx, series, metadata)
		require.Error(t, err)
		assert.Equal(t, 500, statusCode)
	})
}

func TestClient_QueryRange(t *testing.T) {
	var (
		receivedRequests []*http.Request
	)

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		receivedRequests = append(receivedRequests, request)

		// Read requests must go through strong read consistency
		require.Equal(t, api.ReadConsistencyStrong, request.Header.Get(api.ReadConsistencyHeader))

		writer.WriteHeader(http.StatusOK)
		_, err := writer.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	cfg := ClientConfig{}
	flagext.DefaultValues(&cfg)
	require.NoError(t, cfg.WriteBaseEndpoint.Set(server.URL))
	require.NoError(t, cfg.ReadBaseEndpoint.Set(server.URL))

	c, err := NewClient(cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry())
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("results cache not explicitly disabled", func(t *testing.T) {
		receivedRequests = nil

		_, err := c.QueryRange(ctx, "up", time.Unix(0, 0), time.Unix(1000, 0), 10)
		require.NoError(t, err)

		require.Len(t, receivedRequests, 1)
		assert.Empty(t, receivedRequests[0].Header.Get("Cache-Control"))
	})

	t.Run("results cache disabled", func(t *testing.T) {
		receivedRequests = nil

		_, err := c.QueryRange(ctx, "up", time.Unix(0, 0), time.Unix(1000, 0), 10, WithResultsCacheEnabled(false))
		require.NoError(t, err)

		require.Len(t, receivedRequests, 1)
		assert.Equal(t, "no-store", receivedRequests[0].Header.Get("Cache-Control"))
	})
}

func TestClient_Query(t *testing.T) {
	var (
		receivedRequests []*http.Request
	)

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		receivedRequests = append(receivedRequests, request)

		// Read requests must go through strong read consistency
		require.Equal(t, api.ReadConsistencyStrong, request.Header.Get(api.ReadConsistencyHeader))

		writer.WriteHeader(http.StatusOK)
		_, err := writer.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	cfg := ClientConfig{}
	flagext.DefaultValues(&cfg)
	require.NoError(t, cfg.WriteBaseEndpoint.Set(server.URL))
	require.NoError(t, cfg.ReadBaseEndpoint.Set(server.URL))

	c, err := NewClient(cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry())
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("results cache not explicitly disabled", func(t *testing.T) {
		receivedRequests = nil

		_, err := c.Query(ctx, "up", time.Unix(0, 0))
		require.NoError(t, err)

		require.Len(t, receivedRequests, 1)
		assert.Empty(t, receivedRequests[0].Header.Get("Cache-Control"))
	})

	t.Run("results cache disabled", func(t *testing.T) {
		receivedRequests = nil

		_, err := c.Query(ctx, "up", time.Unix(0, 0), WithResultsCacheEnabled(false))
		require.NoError(t, err)

		require.Len(t, receivedRequests, 1)
		assert.Equal(t, "no-store", receivedRequests[0].Header.Get("Cache-Control"))
	})
}

func TestClient_QueryHeaders(t *testing.T) {
	var (
		receivedRequests []*http.Request
	)

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		receivedRequests = append(receivedRequests, request)

		// Read requests must go through strong read consistency
		require.Equal(t, api.ReadConsistencyStrong, request.Header.Get(api.ReadConsistencyHeader))

		writer.WriteHeader(http.StatusOK)
		_, err := writer.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	basicAuth := func(user, pass string) string {
		return fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(user+":"+pass)))
	}

	testCases := map[string]struct {
		cfgMutator func(*ClientConfig)
		reqOptions []RequestOption

		// There may be other headers on the resulting request, but we'll only check these:
		expectedHeaders      map[string]string
		expectedEmptyHeaders []string
	}{
		"default tenant header is used without auth": {
			expectedHeaders: map[string]string{
				"X-Scope-OrgID": "anonymous",
			},
			expectedEmptyHeaders: []string{"Authorization"},
		},
		"tenant header is not used when basic auth is used": {
			cfgMutator: func(cfg *ClientConfig) {
				cfg.BasicAuthUser = "mimir-user"
				cfg.BasicAuthPassword = "guest"
			},
			expectedHeaders: map[string]string{
				"Authorization": basicAuth("mimir-user", "guest"),
			},
			expectedEmptyHeaders: []string{"X-Scope-OrgID"},
		},
		"tenant header is not used when bearer token used": {
			cfgMutator: func(cfg *ClientConfig) {
				cfg.BearerToken = "mimir-token"
			},
			expectedHeaders: map[string]string{
				"Authorization": "Bearer mimir-token",
			},
			expectedEmptyHeaders: []string{"X-Scope-OrgID"},
		},
		"tenant header can be used as well as basic auth": {
			cfgMutator: func(cfg *ClientConfig) {
				cfg.BasicAuthUser = "mimir-user"
				cfg.BasicAuthPassword = "guest"
				cfg.TenantID = "tenant1"
			},
			expectedHeaders: map[string]string{
				"X-Scope-OrgID": "tenant1",
				"Authorization": basicAuth("mimir-user", "guest"),
			},
		},
		"tenant header can be used as well as bearer token": {
			cfgMutator: func(cfg *ClientConfig) {
				cfg.BearerToken = "mimir-token"
				cfg.TenantID = "tenant1"
			},
			expectedHeaders: map[string]string{
				"X-Scope-OrgID": "tenant1",
				"Authorization": "Bearer mimir-token",
			},
		},
		"default user agent": {
			expectedHeaders: map[string]string{
				"User-Agent": "mimir-continuous-test",
			},
		},
		"non-default user agent": {
			cfgMutator: func(cfg *ClientConfig) {
				cfg.UserAgent = "other-user-agent"
			},
			expectedHeaders: map[string]string{
				"User-Agent": "other-user-agent",
			},
		},
		"additional headers": {
			reqOptions: []RequestOption{
				WithAdditionalHeaders(map[string]string{"added": "header-val"}),
			},
			expectedHeaders: map[string]string{
				"added": "header-val",
			},
		},
	}

	for testName, tc := range testCases {
		t.Run(testName, func(t *testing.T) {
			receivedRequests = nil

			cfg := ClientConfig{}
			flagext.DefaultValues(&cfg)
			require.NoError(t, cfg.WriteBaseEndpoint.Set(server.URL))
			require.NoError(t, cfg.ReadBaseEndpoint.Set(server.URL))
			if tc.cfgMutator != nil {
				tc.cfgMutator(&cfg)
			}

			c, err := NewClient(cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry())
			require.NoError(t, err)

			ctx := context.Background()

			_, err = c.Query(ctx, "up", time.Unix(0, 0), tc.reqOptions...)
			require.NoError(t, err)

			require.Len(t, receivedRequests, 1)
			for k, v := range tc.expectedHeaders {
				require.Equal(t, v, receivedRequests[0].Header.Get(k))
			}
			for _, k := range tc.expectedEmptyHeaders {
				require.Empty(t, receivedRequests[0].Header.Get(k))
			}
		})
	}
}

// ClientMock mocks MimirClient.
type ClientMock struct {
	mock.Mock
}

func (m *ClientMock) WriteSeries(ctx context.Context, series []prompb.TimeSeries, metadata []prompb.MetricMetadata) (int, error) {
	args := m.Called(ctx, series, metadata)
	return args.Int(0), args.Error(1)
}

func (m *ClientMock) QueryRange(ctx context.Context, query string, start, end time.Time, step time.Duration, options ...RequestOption) (model.Matrix, error) {
	args := m.Called(ctx, query, start, end, step, options)
	return args.Get(0).(model.Matrix), args.Error(1)
}

func (m *ClientMock) Query(ctx context.Context, query string, ts time.Time, options ...RequestOption) (model.Vector, error) {
	args := m.Called(ctx, query, ts, options)
	return args.Get(0).(model.Vector), args.Error(1)
}

func (m *ClientMock) Metadata(ctx context.Context, metricName string) (v1.Metadata, error) {
	args := m.Called(ctx, metricName)
	return args.Get(0).(v1.Metadata), args.Error(1)
}

func (m *ClientMock) Protocol() string {
	args := m.Called()
	return args.String(0)
}
