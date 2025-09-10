package grpcutil

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestPriorityClientInterceptor(t *testing.T) {
	interceptor := PriorityClientInterceptor(log.NewNopLogger())
	ctx := WithPriorityLevel(context.Background(), 350)
	
	var capturedCtx context.Context
	mockInvoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		capturedCtx = ctx
		return nil
	}

	err := interceptor(ctx, "/test", nil, nil, nil, mockInvoker)
	require.NoError(t, err)

	md, ok := metadata.FromOutgoingContext(capturedCtx)
	require.True(t, ok)
	values := md.Get(PriorityLevelKey)
	assert.Equal(t, "350", values[0])
}

func TestPriorityServerInterceptor(t *testing.T) {
	interceptor := PriorityServerInterceptor(log.NewNopLogger())
	md := metadata.Pairs(PriorityLevelKey, "420")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	
	var capturedCtx context.Context
	mockHandler := func(ctx context.Context, req interface{}) (interface{}, error) {
		capturedCtx = ctx
		return nil, nil
	}

	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, mockHandler)
	require.NoError(t, err)

	level := GetPriorityLevel(capturedCtx)
	assert.Equal(t, 420, level)
}

func TestPriorityHelpers(t *testing.T) {
	ctx := WithPriorityLevel(context.Background(), 375)
	level := GetPriorityLevel(ctx)
	assert.Equal(t, 375, level)
	
	// Test default
	level = GetPriorityLevel(context.Background())
	assert.Equal(t, 200, level)
}