// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"context"
	"testing"

	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestOnlyReplicaClientUnaryInterceptor_And_OnlyReplicaServerUnaryInterceptor(t *testing.T) {
	// Run the gRPC client interceptor
	clientIncomingCtx := ring.ContextWithAvailableReplicas(context.Background(), 1)

	var clientOutgoingCtx context.Context
	clientHandler := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		clientOutgoingCtx = ctx
		return nil
	}

	require.NoError(t, OnlyReplicaClientUnaryInterceptor(clientIncomingCtx, "", nil, nil, nil, clientHandler, nil))

	// The server only gets the gRPC metadata in the incoming context
	clientOutgoingMetadata, ok := metadata.FromOutgoingContext(clientOutgoingCtx)
	require.True(t, ok)

	serverIncomingCtx := metadata.NewIncomingContext(context.Background(), clientOutgoingMetadata)

	// Run the gRPC server interceptor
	var serverOutgoingCtx context.Context
	serverHandler := func(ctx context.Context, _ any) (any, error) {
		serverOutgoingCtx = ctx
		return nil, nil
	}

	_, err := OnlyReplicaServerUnaryInterceptor(serverIncomingCtx, nil, nil, serverHandler)
	require.NoError(t, err)

	// Should inject only replica in the context
	require.True(t, IsOnlyReplicaContext(serverOutgoingCtx))
}

func TestOnlyReplicaClientStreamInterceptor_And_OnlyReplicaServerStreamInterceptor(t *testing.T) {
	// Run the gRPC client interceptor
	clientIncomingCtx := ring.ContextWithAvailableReplicas(context.Background(), 1)

	var clientOutgoingCtx context.Context
	clientHandler := func(ctx context.Context, _ *grpc.StreamDesc, _ *grpc.ClientConn, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
		clientOutgoingCtx = ctx
		return nil, nil
	}

	_, err := OnlyReplicaClientStreamInterceptor(clientIncomingCtx, nil, nil, "", clientHandler, nil)
	require.NoError(t, err)

	// The server only gets the gRPC metadata in the incoming context
	clientOutgoingMetadata, ok := metadata.FromOutgoingContext(clientOutgoingCtx)
	require.True(t, ok)

	serverIncomingCtx := metadata.NewIncomingContext(context.Background(), clientOutgoingMetadata)

	// Run the gRPC server interceptor.
	var serverOutgoingCtx context.Context
	serverHandler := func(_ any, stream grpc.ServerStream) error {
		serverOutgoingCtx = stream.Context()
		return nil
	}

	require.NoError(t, OnlyReplicaServerStreamInterceptor(nil, &ctxStream{ctx: serverIncomingCtx}, nil, serverHandler))

	// Should inject only replica in the context
	require.True(t, IsOnlyReplicaContext(serverOutgoingCtx))
}

func TestContextWithOnlyReplica(t *testing.T) {
	t.Run("when only replica exists in a context", func(t *testing.T) {
		ctx := ContextWithOnlyReplica(context.Background())
		require.True(t, IsOnlyReplicaContext(ctx))
	})

	t.Run("when is only replica does not exist in a context", func(t *testing.T) {
		require.False(t, IsOnlyReplicaContext(context.Background()))
	})
}
