// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestClient_WriteSeries(t *testing.T) {
	var (
		nextStatusCode   = http.StatusOK
		receivedRequests []prompb.WriteRequest
	)

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		// Read the entire body.
		body, err := ioutil.ReadAll(request.Body)
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

	c, err := NewClient(cfg, log.NewNopLogger())
	require.NoError(t, err)

	ctx := context.Background()
	now := time.Now()

	t.Run("write series in a single batch", func(t *testing.T) {
		receivedRequests = nil
		nextStatusCode = http.StatusOK

		series := generateSineWaveSeries("test", now, 10)
		statusCode, err := c.WriteSeries(ctx, series)
		require.NoError(t, err)
		assert.Equal(t, 200, statusCode)

		require.Len(t, receivedRequests, 1)
		assert.Equal(t, series, receivedRequests[0].Timeseries)
	})

	t.Run("write series in multiple batches", func(t *testing.T) {
		receivedRequests = nil
		nextStatusCode = http.StatusOK

		series := generateSineWaveSeries("test", now, 22)
		statusCode, err := c.WriteSeries(ctx, series)
		require.NoError(t, err)
		assert.Equal(t, 200, statusCode)

		require.Len(t, receivedRequests, 3)
		assert.Equal(t, series[0:10], receivedRequests[0].Timeseries)
		assert.Equal(t, series[10:20], receivedRequests[1].Timeseries)
		assert.Equal(t, series[20:22], receivedRequests[2].Timeseries)
	})

	t.Run("request failed with 4xx error", func(t *testing.T) {
		receivedRequests = nil
		nextStatusCode = http.StatusBadRequest

		series := generateSineWaveSeries("test", now, 1)
		statusCode, err := c.WriteSeries(ctx, series)
		require.Error(t, err)
		assert.Equal(t, 400, statusCode)
	})

	t.Run("request failed with 5xx error", func(t *testing.T) {
		receivedRequests = nil
		nextStatusCode = http.StatusInternalServerError

		series := generateSineWaveSeries("test", now, 1)
		statusCode, err := c.WriteSeries(ctx, series)
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

		writer.WriteHeader(http.StatusOK)
		_, err := writer.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	cfg := ClientConfig{}
	flagext.DefaultValues(&cfg)
	require.NoError(t, cfg.WriteBaseEndpoint.Set(server.URL))
	require.NoError(t, cfg.ReadBaseEndpoint.Set(server.URL))

	c, err := NewClient(cfg, log.NewNopLogger())
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

		writer.WriteHeader(http.StatusOK)
		_, err := writer.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	cfg := ClientConfig{}
	flagext.DefaultValues(&cfg)
	require.NoError(t, cfg.WriteBaseEndpoint.Set(server.URL))
	require.NoError(t, cfg.ReadBaseEndpoint.Set(server.URL))

	c, err := NewClient(cfg, log.NewNopLogger())
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

// ClientMock mocks MimirClient.
type ClientMock struct {
	mock.Mock
}

func (m *ClientMock) WriteSeries(ctx context.Context, series []prompb.TimeSeries) (int, error) {
	args := m.Called(ctx, series)
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
