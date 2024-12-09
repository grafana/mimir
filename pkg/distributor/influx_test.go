// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestHandleSeriesPush(t *testing.T) {
	defaultExpectedWriteRequest := &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			{
				TimeSeries: &mimirpb.TimeSeries{
					Labels: []mimirpb.LabelAdapter{
						{Name: "__name__", Value: "measurement_f1"},
						{Name: "__mimir_source__", Value: "influx"},
						{Name: "t1", Value: "v1"},
					},
					Samples: []mimirpb.Sample{
						{Value: 2, TimestampMs: 1465839830100},
					},
				},
			},
		},
	}

	tests := []struct {
		name                string
		url                 string
		data                string
		expectedCode        int
		push                func(t *testing.T) PushFunc
		maxRequestSizeBytes int
	}{
		{
			name:         "POST",
			url:          "/write",
			data:         "measurement,t1=v1 f1=2 1465839830100400200",
			expectedCode: http.StatusNoContent,
			push: func(t *testing.T) PushFunc {
				return func(ctx context.Context, req *mimirpb.WriteRequest, cleanup func()) (*mimirpb.WriteResponse, error) {
					assert.Equal(t, defaultExpectedWriteRequest, req)
					return &mimirpb.WriteResponse{}, nil
				}
			},
			maxRequestSizeBytes: 1 << 20,
		},
		{
			name:         "POST with precision",
			url:          "/write?precision=ns",
			data:         "measurement,t1=v1 f1=2 1465839830100400200",
			expectedCode: http.StatusNoContent,
			push: func(t *testing.T) PushFunc {
				return func(ctx context.Context, req *mimirpb.WriteRequest, cleanup func()) (*mimirpb.WriteResponse, error) {
					assert.Equal(t, defaultExpectedWriteRequest, req)
					return &mimirpb.WriteResponse{}, nil
				}
			},
			maxRequestSizeBytes: 1 << 20,
		},
		{
			name:         "invalid parsing error handling",
			url:          "/write",
			data:         "measurement,t1=v1 f1= 1465839830100400200",
			expectedCode: http.StatusBadRequest,
			push: func(t *testing.T) PushFunc {
				return func(ctx context.Context, req *mimirpb.WriteRequest, cleanup func()) (*mimirpb.WriteResponse, error) {
					t.Error("Push should not be called on bad request")
					return &mimirpb.WriteResponse{}, nil
				}
			},
			maxRequestSizeBytes: 1 << 20,
		},
		{
			name:         "invalid query params",
			url:          "/write?precision=?",
			data:         "measurement,t1=v1 f1=2 1465839830100400200",
			expectedCode: http.StatusBadRequest,
			push: func(t *testing.T) PushFunc {
				return func(ctx context.Context, req *mimirpb.WriteRequest, cleanup func()) (*mimirpb.WriteResponse, error) {
					t.Error("Push should not be called on bad request")
					return &mimirpb.WriteResponse{}, nil
				}
			},
			maxRequestSizeBytes: 1 << 20,
		},
		{
			name:         "internal server error",
			url:          "/write",
			data:         "measurement,t1=v1 f1=2 1465839830100400200",
			expectedCode: http.StatusInternalServerError,
			push: func(t *testing.T) PushFunc {
				return func(ctx context.Context, req *mimirpb.WriteRequest, cleanup func()) (*mimirpb.WriteResponse, error) {
					return nil, context.DeadlineExceeded
				}
			},
			maxRequestSizeBytes: 1 << 20,
		},
		{
			name:         "max batch size violated",
			url:          "/write",
			data:         "measurement,t1=v1 f1=2 0123456789",
			expectedCode: http.StatusBadRequest,
			push: func(t *testing.T) PushFunc {
				return func(ctx context.Context, req *mimirpb.WriteRequest, cleanup func()) (*mimirpb.WriteResponse, error) {
					t.Error("Push should not be called on bad request")
					return &mimirpb.WriteResponse{}, nil
				}
			},
			maxRequestSizeBytes: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := InfluxHandler(tt.maxRequestSizeBytes, nil, tt.push(t))
			req := httptest.NewRequest("POST", tt.url, bytes.NewReader([]byte(tt.data)))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			assert.Equal(t, tt.expectedCode, rec.Code)
		})
	}
}
