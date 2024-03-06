// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"context"
	"io"
	"testing"

	gogostatus "github.com/gogo/status"
	"github.com/grafana/dskit/grpcutil"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

func TestWrapContextError(t *testing.T) {
	t.Run("should wrap gRPC context errors", func(t *testing.T) {
		tests := map[string]struct {
			origErr            error
			expectedGrpcCode   codes.Code
			expectedContextErr error
		}{
			"gogo Canceled error": {
				origErr:            gogostatus.Error(codes.Canceled, context.Canceled.Error()),
				expectedGrpcCode:   codes.Canceled,
				expectedContextErr: context.Canceled,
			},
			"gRPC Canceled error": {
				origErr:            grpcstatus.Error(codes.Canceled, context.Canceled.Error()),
				expectedGrpcCode:   codes.Canceled,
				expectedContextErr: context.Canceled,
			},
			"wrapped gogo Canceled error": {
				origErr:            errors.Wrap(gogostatus.Error(codes.Canceled, context.Canceled.Error()), "custom message"),
				expectedGrpcCode:   codes.Canceled,
				expectedContextErr: context.Canceled,
			},
			"wrapped gRPC Canceled error": {
				origErr:            errors.Wrap(grpcstatus.Error(codes.Canceled, context.Canceled.Error()), "custom message"),
				expectedGrpcCode:   codes.Canceled,
				expectedContextErr: context.Canceled,
			},
			"gogo DeadlineExceeded error": {
				origErr:            gogostatus.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error()),
				expectedGrpcCode:   codes.DeadlineExceeded,
				expectedContextErr: context.DeadlineExceeded,
			},
			"gRPC DeadlineExceeded error": {
				origErr:            grpcstatus.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error()),
				expectedGrpcCode:   codes.DeadlineExceeded,
				expectedContextErr: context.DeadlineExceeded,
			},
			"wrapped gogo DeadlineExceeded error": {
				origErr:            errors.Wrap(gogostatus.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error()), "custom message"),
				expectedGrpcCode:   codes.DeadlineExceeded,
				expectedContextErr: context.DeadlineExceeded,
			},
			"wrapped gRPC DeadlineExceeded error": {
				origErr:            errors.Wrap(grpcstatus.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error()), "custom message"),
				expectedGrpcCode:   codes.DeadlineExceeded,
				expectedContextErr: context.DeadlineExceeded,
			},
		}

		for testName, testData := range tests {
			t.Run(testName, func(t *testing.T) {
				wrapped := WrapGrpcContextError(testData.origErr)

				assert.NotEqual(t, testData.origErr, wrapped)
				assert.Equal(t, testData.origErr, errors.Unwrap(wrapped))

				assert.True(t, errors.Is(wrapped, testData.expectedContextErr))
				assert.Equal(t, testData.expectedGrpcCode, grpcutil.ErrorToStatusCode(wrapped))

				//lint:ignore faillint We want to explicitly assert on status.FromError()
				gogoStatus, ok := gogostatus.FromError(wrapped)
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
		assert.Equal(t, orig, WrapGrpcContextError(orig))

		assert.Equal(t, context.Canceled, WrapGrpcContextError(context.Canceled))
		assert.Equal(t, context.DeadlineExceeded, WrapGrpcContextError(context.DeadlineExceeded))
		assert.Equal(t, io.EOF, WrapGrpcContextError(io.EOF))
	})
}
