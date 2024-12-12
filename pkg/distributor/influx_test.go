// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"

	io2 "github.com/influxdata/influxdb/v2/kit/io"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestInfluxHandleSeriesPush(t *testing.T) {
	defaultExpectedWriteRequest := &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			{
				TimeSeries: &mimirpb.TimeSeries{
					Labels: []mimirpb.LabelAdapter{
						{Name: "__mimir_source__", Value: "influx"},
						{Name: "__name__", Value: "measurement_f1"},
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
				return func(ctx context.Context, pushReq *Request) error {
					req, err := pushReq.WriteRequest()
					assert.Equal(t, defaultExpectedWriteRequest, req)
					assert.Nil(t, err)
					return err
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
				return func(ctx context.Context, pushReq *Request) error {
					req, err := pushReq.WriteRequest()
					assert.Equal(t, defaultExpectedWriteRequest, req)
					assert.Nil(t, err)
					return err
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
				return func(ctx context.Context, pushReq *Request) error {
					req, err := pushReq.WriteRequest()
					assert.Nil(t, req)
					// TODO(alexg): assert on specific err
					// assert.NoError(t) // reminder to fix
					return err
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
				// return func(ctx context.Context, req *mimirpb.WriteRequest) error {
				return func(ctx context.Context, pushReq *Request) error {
					req, err := pushReq.WriteRequest()
					assert.Nil(t, req)
					// TODO(alexg): assert on specific err
					// assert.NoError(t, err) // reminder to fix
					return err
				}
			},
			maxRequestSizeBytes: 1 << 20,
		},
		{
			name:         "internal server error",
			url:          "/write",
			data:         "measurement,t1=v1 f1=2 1465839830100400200",
			expectedCode: http.StatusServiceUnavailable,
			push: func(t *testing.T) PushFunc {
				return func(ctx context.Context, pushReq *Request) error {
					assert.Error(t, context.DeadlineExceeded)
					return context.DeadlineExceeded
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
				return func(ctx context.Context, pushReq *Request) error {
					req, err := pushReq.WriteRequest()
					assert.Nil(t, req)
					assert.Error(t, io2.ErrReadLimitExceeded)
					return err
				}
			},
			maxRequestSizeBytes: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := InfluxHandler(tt.maxRequestSizeBytes, nil, nil, RetryConfig{}, tt.push(t), nil, nil, log.NewNopLogger())
			req := httptest.NewRequest("POST", tt.url, bytes.NewReader([]byte(tt.data)))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			assert.Equal(t, tt.expectedCode, rec.Code)
		})
	}
}
