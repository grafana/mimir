package storegatewaypb

import (
	"context"
	"io"
	"testing"

	"github.com/gogo/status"
	"github.com/grafana/dskit/grpcutil"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
)

func TestWrapContextError(t *testing.T) {
	t.Run("should return the input error on a non-gRPC error", func(t *testing.T) {
		orig := errors.New("mock error")
		assert.Equal(t, orig, wrapContextError(orig))

		assert.Equal(t, context.Canceled, wrapContextError(context.Canceled))
		assert.Equal(t, context.DeadlineExceeded, wrapContextError(context.DeadlineExceeded))
		assert.Equal(t, io.EOF, wrapContextError(io.EOF))
	})

	t.Run("should wrap gRPC Canceled error", func(t *testing.T) {
		orig := status.Error(codes.Canceled, context.Canceled.Error())
		wrapped := wrapContextError(orig)

		assert.NotEqual(t, orig, wrapped)
		assert.Equal(t, orig, errors.Unwrap(wrapped))

		assert.True(t, errors.Is(wrapped, context.Canceled))
		assert.False(t, errors.Is(wrapped, context.DeadlineExceeded))

		assert.Equal(t, codes.Canceled, grpcutil.ErrorToStatusCode(wrapped))
	})

	t.Run("should wrap a wrapped gRPC Canceled error", func(t *testing.T) {
		orig := errors.Wrap(status.Error(codes.Canceled, context.Canceled.Error()), "custom message")
		wrapped := wrapContextError(orig)

		assert.NotEqual(t, orig, wrapped)
		assert.Equal(t, orig, errors.Unwrap(wrapped))

		assert.True(t, errors.Is(wrapped, context.Canceled))
		assert.False(t, errors.Is(wrapped, context.DeadlineExceeded))

		assert.Equal(t, codes.Canceled, grpcutil.ErrorToStatusCode(wrapped))
	})

	t.Run("should wrap gRPC DeadlineExceeded error", func(t *testing.T) {
		orig := status.Error(codes.DeadlineExceeded, context.Canceled.Error())
		wrapped := wrapContextError(orig)

		assert.NotEqual(t, orig, wrapped)
		assert.Equal(t, orig, errors.Unwrap(wrapped))

		assert.True(t, errors.Is(wrapped, context.DeadlineExceeded))
		assert.False(t, errors.Is(wrapped, context.Canceled))

		assert.Equal(t, codes.DeadlineExceeded, grpcutil.ErrorToStatusCode(wrapped))
	})

	t.Run("should wrap a wrapped gRPC DeadlineExceeded error", func(t *testing.T) {
		orig := errors.Wrap(status.Error(codes.DeadlineExceeded, context.Canceled.Error()), "custom message")
		wrapped := wrapContextError(orig)

		assert.NotEqual(t, orig, wrapped)
		assert.Equal(t, orig, errors.Unwrap(wrapped))

		assert.True(t, errors.Is(wrapped, context.DeadlineExceeded))
		assert.False(t, errors.Is(wrapped, context.Canceled))

		assert.Equal(t, codes.DeadlineExceeded, grpcutil.ErrorToStatusCode(wrapped))
	})
}
