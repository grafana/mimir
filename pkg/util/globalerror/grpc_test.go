// SPDX-License-Identifier: AGPL-3.0-only

package globalerror

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/gogo/status"
	"github.com/golang/protobuf/ptypes/empty"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	dskitserver "github.com/grafana/dskit/server"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestWrapContextError(t *testing.T) {
	t.Run("should wrap gRPC context errors", func(t *testing.T) {
		tests := map[string]struct {
			origErr            error
			expectedGrpcCode   codes.Code
			expectedContextErr error
		}{
			"gogo Canceled error": {
				origErr:            status.Error(codes.Canceled, context.Canceled.Error()),
				expectedGrpcCode:   codes.Canceled,
				expectedContextErr: context.Canceled,
			},
			"gRPC Canceled error": {
				origErr:            grpcstatus.Error(codes.Canceled, context.Canceled.Error()),
				expectedGrpcCode:   codes.Canceled,
				expectedContextErr: context.Canceled,
			},
			"wrapped gogo Canceled error": {
				origErr:            errors.Wrap(status.Error(codes.Canceled, context.Canceled.Error()), "custom message"),
				expectedGrpcCode:   codes.Canceled,
				expectedContextErr: context.Canceled,
			},
			"wrapped gRPC Canceled error": {
				origErr:            errors.Wrap(grpcstatus.Error(codes.Canceled, context.Canceled.Error()), "custom message"),
				expectedGrpcCode:   codes.Canceled,
				expectedContextErr: context.Canceled,
			},
			"gogo DeadlineExceeded error": {
				origErr:            status.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error()),
				expectedGrpcCode:   codes.DeadlineExceeded,
				expectedContextErr: context.DeadlineExceeded,
			},
			"gRPC DeadlineExceeded error": {
				origErr:            grpcstatus.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error()),
				expectedGrpcCode:   codes.DeadlineExceeded,
				expectedContextErr: context.DeadlineExceeded,
			},
			"wrapped gogo DeadlineExceeded error": {
				origErr:            errors.Wrap(status.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error()), "custom message"),
				expectedGrpcCode:   codes.DeadlineExceeded,
				expectedContextErr: context.DeadlineExceeded,
			},
			"wrapped gRPC DeadlineExceeded error": {
				origErr:            errors.Wrap(grpcstatus.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error()), "custom message"),
				expectedGrpcCode:   codes.DeadlineExceeded,
				expectedContextErr: context.DeadlineExceeded,
			},
			"ErrorWithStatus with Canceled status": {
				origErr:            WrapErrorWithGRPCStatus(errors.New("cancel error"), codes.Canceled, nil),
				expectedGrpcCode:   codes.Canceled,
				expectedContextErr: context.Canceled,
			},
			"ErrorWithStatus with DeadlineExceeded status": {
				origErr:            WrapErrorWithGRPCStatus(errors.New("timeout error"), codes.DeadlineExceeded, nil),
				expectedGrpcCode:   codes.DeadlineExceeded,
				expectedContextErr: context.DeadlineExceeded,
			},
			"grpc.ErrClientConnClosing": {
				origErr:            status.Error(codes.Canceled, grpcClientConnectionIsClosingErr),
				expectedGrpcCode:   codes.Canceled,
				expectedContextErr: nil,
			},
		}

		for testName, testData := range tests {
			t.Run(testName, func(t *testing.T) {
				wrapped := WrapGRPCErrorWithContextError(testData.origErr)
				assert.ErrorIs(t, wrapped, testData.origErr)

				if testData.expectedContextErr != nil {
					assert.ErrorIs(t, wrapped, testData.expectedContextErr)
					assert.NotEqual(t, testData.origErr, wrapped)
				} else {
					assert.Equal(t, testData.origErr, wrapped)
				}
				assert.Equal(t, testData.expectedGrpcCode, grpcutil.ErrorToStatusCode(wrapped))

				//lint:ignore faillint We want to explicitly assert on status.FromError()
				gogoStatus, ok := status.FromError(wrapped)
				require.True(t, ok)
				assert.Equal(t, testData.expectedGrpcCode, gogoStatus.Code())

				gogoStatus, ok = grpcutil.ErrorToStatus(wrapped)
				require.True(t, ok)
				assert.Equal(t, testData.expectedGrpcCode, gogoStatus.Code())

				//lint:ignore faillint We want to explicitly assert on status.FromError()
				grpcStatus, ok := grpcstatus.FromError(wrapped)
				require.True(t, ok)
				assert.Equal(t, testData.expectedGrpcCode, grpcStatus.Code())
			})
		}
	})

	t.Run("should return the input error on a non-gRPC error", func(t *testing.T) {
		orig := errors.New("mock error")
		assert.Equal(t, orig, WrapGRPCErrorWithContextError(orig))

		assert.Equal(t, context.Canceled, WrapGRPCErrorWithContextError(context.Canceled))
		assert.Equal(t, context.DeadlineExceeded, WrapGRPCErrorWithContextError(context.DeadlineExceeded))
		assert.Equal(t, io.EOF, WrapGRPCErrorWithContextError(io.EOF))
	})
}

func TestWrapErrorWithGRPCStatus(t *testing.T) {
	genericErrMsg := "this is an error"
	genericErr := errors.New(genericErrMsg)

	tests := map[string]struct {
		originErr            error
		details              *mimirpb.ErrorDetails
		doNotLog             bool
		expectedErrorMessage string
		expectedErrorDetails *mimirpb.ErrorDetails
	}{
		"new ErrorWithStatus backed by a genericErr contains ErrorDetails": {
			originErr:            genericErr,
			details:              &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
			expectedErrorMessage: genericErrMsg,
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"new ErrorWithStatus backed by a DoNotLog error of genericErr contains ErrorDetails": {
			originErr:            middleware.DoNotLogError{Err: genericErr},
			details:              &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
			doNotLog:             true,
			expectedErrorMessage: genericErrMsg,
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"new ErrorWithStatus without ErrorDetails backed by a DoNotLog error of genericErr contains ErrorDetails": {
			originErr:            middleware.DoNotLogError{Err: genericErr},
			doNotLog:             true,
			expectedErrorMessage: genericErrMsg,
		},
		"new ErrorWithStatus without ErrorDetails": {
			originErr:            genericErr,
			expectedErrorMessage: genericErrMsg,
		},
	}

	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			const statusCode = codes.Unimplemented
			errWithStatus := WrapErrorWithGRPCStatus(data.originErr, statusCode, data.details)
			require.Error(t, errWithStatus)
			require.Errorf(t, errWithStatus, data.expectedErrorMessage)

			// Ensure that errWithStatus preserves the original error
			require.ErrorIs(t, errWithStatus, data.originErr)

			// Ensure gogo's status.FromError recognizes errWithStatus.
			//lint:ignore faillint We want to explicitly assert on status.FromError()
			stat, ok := status.FromError(errWithStatus)
			require.True(t, ok)
			require.Equal(t, statusCode, stat.Code())
			require.Equal(t, stat.Message(), data.expectedErrorMessage)
			checkErrorWithStatusDetails(t, stat.Details(), data.expectedErrorDetails)

			// Ensure dskit's grpcutil.ErrorToStatus recognizes errWithHTTPStatus.
			stat, ok = grpcutil.ErrorToStatus(errWithStatus)
			require.True(t, ok)
			require.Equal(t, statusCode, stat.Code())
			require.Equal(t, stat.Message(), data.expectedErrorMessage)
			checkErrorWithStatusDetails(t, stat.Details(), data.expectedErrorDetails)

			// Ensure grpc's status.FromError recognizes errWithStatus.
			//lint:ignore faillint We want to explicitly assert on status.FromError()
			st, ok := grpcstatus.FromError(errWithStatus)
			require.True(t, ok)
			require.Equal(t, statusCode, st.Code())
			require.Equal(t, st.Message(), data.expectedErrorMessage)

			// Ensure httpgrpc's HTTPResponseFromError doesn't recognize errWithStatus.
			resp, ok := httpgrpc.HTTPResponseFromError(errWithStatus)
			require.False(t, ok)
			require.Nil(t, resp)

			if data.doNotLog {
				var optional middleware.OptionalLogging
				require.ErrorAs(t, errWithStatus, &optional)
				shouldLog, _ := optional.ShouldLog(context.Background())
				require.False(t, shouldLog)
			}
		})
	}
}

func TestErrorWithStatus_Err(t *testing.T) {
	genericErrMsg := "this is an error"
	genericErr := errors.New(genericErrMsg)

	tests := map[string]struct {
		originErr            error
		details              *mimirpb.ErrorDetails
		expectedErrorMessage string
		expectedErrorDetails *mimirpb.ErrorDetails
	}{
		"Err() of an ErrorWithStatus backed by a genericErr contains ErrorDetails": {
			originErr:            genericErr,
			details:              &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
			expectedErrorMessage: genericErrMsg,
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"Err() of an ErrorWithStatus backed by a DoNotLog error of genericErr contains ErrorDetails": {
			originErr:            middleware.DoNotLogError{Err: genericErr},
			details:              &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
			expectedErrorMessage: genericErrMsg,
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
		"Err() of an ErrorWithStatus without ErrorDetails backed by a DoNotLog error of genericErr contains ErrorDetails": {
			originErr:            middleware.DoNotLogError{Err: genericErr},
			expectedErrorMessage: genericErrMsg,
		},
		"Err() of an ErrorWithStatus without ErrorDetails": {
			originErr:            genericErr,
			expectedErrorMessage: genericErrMsg,
		},
	}

	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			const statusCode = codes.Unimplemented
			errWithStatus := WrapErrorWithGRPCStatus(data.originErr, statusCode, data.details)
			err := errWithStatus.Err()
			require.Error(t, err)
			require.Errorf(t, err, data.expectedErrorMessage)

			// Ensure that err does not preserve the original error
			require.NotErrorIs(t, err, data.originErr)

			// Ensure gogo's status.FromError recognizes errWithStatus.
			//lint:ignore faillint We want to explicitly assert on status.FromError()
			stat, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, statusCode, stat.Code())
			require.Equal(t, stat.Message(), data.expectedErrorMessage)
			checkErrorWithStatusDetails(t, stat.Details(), data.expectedErrorDetails)

			// Ensure dskit's grpcutil.ErrorToStatus recognizes errWithHTTPStatus.
			stat, ok = grpcutil.ErrorToStatus(err)
			require.True(t, ok)
			require.Equal(t, statusCode, stat.Code())
			require.Equal(t, stat.Message(), data.expectedErrorMessage)
			checkErrorWithStatusDetails(t, stat.Details(), data.expectedErrorDetails)

			// Ensure grpc's status.FromError recognizes errWithStatus.
			//lint:ignore faillint We want to explicitly assert on status.FromError()
			st, ok := grpcstatus.FromError(err)
			require.True(t, ok)
			require.Equal(t, statusCode, st.Code())
			require.Equal(t, st.Message(), data.expectedErrorMessage)

			// Ensure httpgrpc's HTTPResponseFromError doesn't recognize errWithStatus.
			resp, ok := httpgrpc.HTTPResponseFromError(err)
			require.False(t, ok)
			require.Nil(t, resp)

			var optional middleware.OptionalLogging
			require.False(t, errors.As(err, &optional))
		})
	}
}

func checkErrorWithStatusDetails(t *testing.T, details []any, expected *mimirpb.ErrorDetails) {
	if expected == nil {
		require.Empty(t, details)
	} else {
		require.Len(t, details, 1)
		errDetails, ok := details[0].(*mimirpb.ErrorDetails)
		require.True(t, ok)
		require.Equal(t, expected, errDetails)
	}
}

func TestGRPCClientClosingConnectionError(t *testing.T) {
	ctx := context.Background()

	_, client, cc := prepareTest(t)

	// Calls to Succeed() should be successful when cc is open.
	_, err := client.Succeed(ctx, nil)
	require.NoError(t, err)

	// We close cc.
	err = cc.Close()
	require.NoError(t, err)

	// Calls to Succeed() should fail with "grpc: the client connection is closing" when cc is closed.
	_, err = client.Succeed(ctx, nil)
	checkGRPCConnectionIsClosingError(t, err)
}

func prepareTest(t *testing.T) (dskitserver.FakeServerServer, dskitserver.FakeServerClient, *grpc.ClientConn) {
	grpcServer := grpc.NewServer()
	t.Cleanup(grpcServer.GracefulStop)

	server := &mockServer{}
	dskitserver.RegisterFakeServerServer(grpcServer, server)

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go func() {
		require.NoError(t, grpcServer.Serve(listener))
	}()

	// Create a real gRPC client connecting to the gRPC server we control in this test.
	clientCfg := grpcclient.Config{}
	flagext.DefaultValues(&clientCfg)

	opts, err := clientCfg.DialOption(nil, nil)
	require.NoError(t, err)

	cc, err := grpc.NewClient(listener.Addr().String(), opts...)
	require.NoError(t, err)

	client := dskitserver.NewFakeServerClient(cc)

	// This is another source of "grpc: the client connection is closing",
	// because at this point the connection is already closed.
	t.Cleanup(func() {
		err := cc.Close()
		checkGRPCConnectionIsClosingError(t, err)
	})

	return server, client, cc
}

func checkGRPCConnectionIsClosingError(t *testing.T, err error) {
	require.Error(t, err)
	require.NotErrorIs(t, err, context.Canceled)

	errWithCtx := WrapGRPCErrorWithContextError(err)
	stat, ok := grpcstatus.FromError(errWithCtx)
	require.True(t, ok)
	require.Equal(t, codes.Canceled, stat.Code())
	require.Equal(t, grpcClientConnectionIsClosingErr, stat.Message())
	require.NotErrorIs(t, errWithCtx, context.Canceled)
}

type mockServer struct {
	dskitserver.UnimplementedFakeServerServer
}

func (s *mockServer) Succeed(_ context.Context, _ *empty.Empty) (*empty.Empty, error) {
	return nil, nil
}
