// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/grafana/dskit/middleware"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestHeaderOptionsMiddleware(t *testing.T) {
	const headerName = "X-Test-Header"

	testCases := map[string]struct {
		headers        map[string]string
		cfg            HeaderOptionsConfig
		expectedStatus int
		expectedValue  string
		expectedOk     bool
	}{
		"no header": {
			cfg: HeaderOptionsConfig{
				Headers: map[string]func(string) error{
					headerName: func(string) error { return nil },
				},
			},
			expectedStatus: http.StatusOK,
			expectedOk:     false,
		},
		"valid header": {
			headers: map[string]string{
				headerName: "value",
			},
			cfg: HeaderOptionsConfig{
				Headers: map[string]func(string) error{
					headerName: func(v string) error {
						if v == "value" {
							return nil
						}
						return fmt.Errorf("invalid value")
					},
				},
			},
			expectedStatus: http.StatusOK,
			expectedOk:     true,
			expectedValue:  "value",
		},
		"invalid header": {
			headers: map[string]string{
				headerName: "invalid",
			},
			cfg: HeaderOptionsConfig{
				Headers: map[string]func(string) error{
					headerName: func(v string) error {
						if v == "value" {
							return nil
						}
						return fmt.Errorf("invalid value")
					},
				},
			},
			expectedStatus: http.StatusBadRequest,
			expectedOk:     false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			hom := HeaderOptionsMiddleware(tc.cfg)
			var (
				recordedValue string
				recordedOk    bool
			)
			recorderHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := r.Context()
				recordedValue, recordedOk = HeaderOptionFromContext(ctx, headerName)
				w.WriteHeader(http.StatusOK)
			})
			handler := middleware.Merge(hom).Wrap(recorderHandler)

			req := httptest.NewRequest("GET", "/", nil)
			for header, value := range tc.headers {
				req.Header.Set(header, value)
			}
			resp := httptest.NewRecorder()

			handler.ServeHTTP(resp, req)
			_, err := io.ReadAll(resp.Body)

			require.NoError(t, err)
			require.Equal(t, tc.expectedStatus, resp.Code)
			require.Equal(t, tc.expectedOk, recordedOk)
			if tc.expectedOk {
				require.Equal(t, tc.expectedValue, recordedValue)
			}
		})
	}
}

func TestHeaderOptionsClientServerUnaryInterceptor(t *testing.T) {
	// Check that the client interceptor sets the header in the metadata.
	ctx := context.Background()
	ctx = ContextWithHeaderOptions(ctx, HeaderOptionsContext{
		headers: map[string]string{
			"X-Test-Header": "value",
		},
	})
	var recordedMetadata metadata.MD
	err := HeaderOptionsClientUnaryInterceptor(
		ctx,
		"test",
		nil,
		nil,
		nil,
		func(ctx context.Context, _ string, _, _ interface{}, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
			recordedMetadata, _ = metadata.FromOutgoingContext(ctx)
			return nil
		},
	)
	require.NoError(t, err)
	require.Len(t, recordedMetadata.Get(headerOptionsGrpcMdKey), 1)
	require.JSONEq(t, "{\"X-Test-Header\":\"value\"}", recordedMetadata.Get(headerOptionsGrpcMdKey)[0])
}

func TestHeaderOptionsServerUnaryInterceptor(t *testing.T) {
	var (
		recordedValue string
		recordedOk    bool
	)
	recorderHandler := func(ctx context.Context, _ any) (any, error) {
		recordedValue, recordedOk = HeaderOptionFromContext(ctx, "X-Test-Header")
		return nil, nil
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(headerOptionsGrpcMdKey, "{\"X-Test-Header\":\"value\"}"))
	_, err := HeaderOptionsServerUnaryInterceptor(
		ctx,
		nil,
		nil,
		recorderHandler,
	)
	require.NoError(t, err)
	require.True(t, recordedOk)
	require.Equal(t, "value", recordedValue)
}

func TestHeaderOptionsClientStreamInterceptor(t *testing.T) {
	ctx := context.Background()
	ctx = ContextWithHeaderOptions(ctx, HeaderOptionsContext{
		headers: map[string]string{
			"X-Test-Header": "value",
		},
	})
	var recordedMetadata metadata.MD
	_, err := HeaderOptionsClientStreamInterceptor(
		ctx,
		nil,
		nil,
		"test",
		func(ctx context.Context, _ *grpc.StreamDesc, _ *grpc.ClientConn, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
			recordedMetadata, _ = metadata.FromOutgoingContext(ctx)
			return nil, nil
		},
	)
	require.NoError(t, err)
	require.Len(t, recordedMetadata.Get(headerOptionsGrpcMdKey), 1)
	require.JSONEq(t, "{\"X-Test-Header\":\"value\"}", recordedMetadata.Get(headerOptionsGrpcMdKey)[0])
}

func TestHeaderOptionsServerStreamInterceptor(t *testing.T) {
	var (
		recordedValue string
		recordedOk    bool
	)
	recorderHandler := func(_ any, ss grpc.ServerStream) error {
		recordedValue, recordedOk = HeaderOptionFromContext(ss.Context(), "X-Test-Header")
		return nil
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(headerOptionsGrpcMdKey, "{\"X-Test-Header\":\"value\"}"))
	err := HeaderOptionsServerStreamInterceptor(
		nil,
		serverStreamMock{ctx: ctx},
		nil,
		recorderHandler,
	)
	require.NoError(t, err)
	require.True(t, recordedOk)
	require.Equal(t, "value", recordedValue)
}

type serverStreamMock struct {
	grpc.ServerStream

	ctx context.Context
}

func (m serverStreamMock) Context() context.Context {
	return m.ctx
}

func TestSetHeaderOptions(t *testing.T) {
	ctx := context.Background()
	ctx = ContextWithHeaderOptions(ctx, HeaderOptionsContext{
		headers: map[string]string{
			"X-Test-Header": "value",
		},
	})
	req := httptest.NewRequest("GET", "/", nil)
	SetHeaderOptions(ctx, req)
	require.Equal(t, "value", req.Header.Get("X-Test-Header"))
}
