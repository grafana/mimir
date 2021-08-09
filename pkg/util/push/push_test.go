// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/push/push_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package push

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/middleware"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestHandler_remoteWrite(t *testing.T) {
	req := createRequest(t, createPrometheusRemoteWriteProtobuf(t))
	resp := httptest.NewRecorder()
	handler := Handler(100000, nil, verifyWriteRequestHandler(t, mimirpb.API))
	handler.ServeHTTP(resp, req)
	assert.Equal(t, 200, resp.Code)
}

func TestHandler_cortexWriteRequest(t *testing.T) {
	req := createRequest(t, createCortexWriteRequestProtobuf(t, false))
	resp := httptest.NewRecorder()
	sourceIPs, _ := middleware.NewSourceIPs("SomeField", "(.*)")
	handler := Handler(100000, sourceIPs, verifyWriteRequestHandler(t, mimirpb.RULE))
	handler.ServeHTTP(resp, req)
	assert.Equal(t, 200, resp.Code)
}

func TestHandler_ignoresSkipLabelNameValidationIfSet(t *testing.T) {
	for _, req := range []*http.Request{
		createRequest(t, createCortexWriteRequestProtobuf(t, true)),
		createRequest(t, createCortexWriteRequestProtobuf(t, false)),
	} {
		resp := httptest.NewRecorder()
		handler := Handler(100000, nil, verifyWriteRequestHandler(t, mimirpb.RULE))
		handler.ServeHTTP(resp, req)
		assert.Equal(t, 200, resp.Code)
	}
}

func verifyWriteRequestHandler(t *testing.T, expectSource mimirpb.WriteRequest_SourceEnum) func(ctx context.Context, request *mimirpb.WriteRequest) (response *mimirpb.WriteResponse, err error) {
	t.Helper()
	return func(ctx context.Context, request *mimirpb.WriteRequest) (response *mimirpb.WriteResponse, err error) {
		assert.Len(t, request.Timeseries, 1)
		assert.Equal(t, "__name__", request.Timeseries[0].Labels[0].Name)
		assert.Equal(t, "foo", request.Timeseries[0].Labels[0].Value)
		assert.Equal(t, expectSource, request.Source)
		assert.False(t, request.SkipLabelNameValidation)
		return &mimirpb.WriteResponse{}, nil
	}
}

func createRequest(t *testing.T, protobuf []byte) *http.Request {
	t.Helper()
	inoutBytes := snappy.Encode(nil, protobuf)
	req, err := http.NewRequest("POST", "http://localhost/", bytes.NewReader(inoutBytes))
	require.NoError(t, err)
	req.Header.Add("Content-Encoding", "snappy")
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	return req
}

func createPrometheusRemoteWriteProtobuf(t *testing.T) []byte {
	t.Helper()
	input := prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: "__name__", Value: "foo"},
				},
				Samples: []prompb.Sample{
					{Value: 1, Timestamp: time.Date(2020, 4, 1, 0, 0, 0, 0, time.UTC).UnixNano()},
				},
			},
		},
	}
	inoutBytes, err := input.Marshal()
	require.NoError(t, err)
	return inoutBytes
}
func createCortexWriteRequestProtobuf(t *testing.T, skipLabelNameValidation bool) []byte {
	t.Helper()
	ts := mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels: []mimirpb.LabelAdapter{
				{Name: "__name__", Value: "foo"},
			},
			Samples: []mimirpb.Sample{
				{Value: 1, TimestampMs: time.Date(2020, 4, 1, 0, 0, 0, 0, time.UTC).UnixNano()},
			},
		},
	}
	input := mimirpb.WriteRequest{
		Timeseries:              []mimirpb.PreallocTimeseries{ts},
		Source:                  mimirpb.RULE,
		SkipLabelNameValidation: skipLabelNameValidation,
	}
	inoutBytes, err := input.Marshal()
	require.NoError(t, err)
	return inoutBytes
}
