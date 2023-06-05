// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"context"
	"net"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestSerializedWriteRequestMarshal(t *testing.T) {
	metricLabelAdapters := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := mimirpb.FromLabelAdaptersToLabels(metricLabelAdapters)

	writeReq := mimirpb.ToWriteRequest(
		[]labels.Labels{metricLabels},
		[]mimirpb.Sample{{Value: 2, TimestampMs: 1575043969}},
		nil,
		nil,
		mimirpb.API,
	)
	b, err := proto.Marshal(writeReq)
	require.NoError(t, err)

	serializedReq := &SerializedWriteRequest{
		Payload: b,
	}
	b2, err := proto.Marshal(serializedReq)
	require.NoError(t, err)
	require.True(t, len(b2) > 0)

	var out mimirpb.WriteRequest
	err = proto.Unmarshal(b2, &out)
	require.NoError(t, err)
}

func TestPushSerialized(t *testing.T) {
	// Create a new gRPC server with in-memory communication.
	listen := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	bufDialer := func(context.Context, string) (net.Conn, error) {
		return listen.Dial()
	}

	conn, err := grpc.DialContext(context.Background(), "buf", grpc.WithContextDialer(bufDialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	serverMock := &IngesterServerMock{}
	serverMock.On("Push", mock.Anything, mock.AnythingOfType("*mimirpb.WriteRequest")).
		Return(&mimirpb.WriteResponse{}, nil).
		Once()

	RegisterIngesterServer(server, serverMock)

	go func() {
		require.NoError(t, server.Serve(listen))
	}()
	t.Cleanup(server.Stop)

	writeReq := &mimirpb.WriteRequest{}
	b, err := proto.Marshal(writeReq)
	require.NoError(t, err)

	client := NewIngesterClient(conn)
	_, err = client.(IngesterClientExt).PushSerialized(context.Background(), &SerializedWriteRequest{Payload: b})
	require.NoError(t, err)

	// Check that Push method was called on the server.
	serverMock.AssertExpectations(t)
}
