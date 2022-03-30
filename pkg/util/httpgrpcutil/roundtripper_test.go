// SPDX-License-Identifier: AGPL-3.0-only

package httpgrpcutil

import (
	"context"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

type mockHTTPServer struct {
	httpgrpc.UnimplementedHTTPServer
}

func (s *mockHTTPServer) Handle(_ context.Context, req *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error) {
	// reply with echo response
	return &httpgrpc.HTTPResponse{
		Code:    http.StatusOK,
		Headers: req.Headers,
		Body:    req.Body,
	}, nil
}

func TestTransport_RoundTrip(t *testing.T) {
	ctx := context.Background()

	conn, err := grpc.DialContext(ctx, "", grpc.WithInsecure(), grpc.WithContextDialer(dialer(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	hnd := &Transport{
		Client: httpgrpc.NewHTTPClient(conn),
	}
	headers := []*httpgrpc.Header{
		{Key: "Content-Type", Values: []string{"application/json"}},
	}
	body := []byte(`{"status": "ok"}`)

	resp, err := hnd.RoundTrip(context.Background(), &httpgrpc.HTTPRequest{
		Method:  http.MethodPost,
		Headers: headers,
		Body:    body,
	})

	require.Nil(t, err)
	require.Equal(t, int32(http.StatusOK), resp.Code)
	require.Equal(t, headers, resp.Headers)
	require.Equal(t, body, resp.Body)
}

func dialer(t *testing.T) func(context.Context, string) (net.Conn, error) {
	t.Helper()

	srv := grpc.NewServer()
	t.Cleanup(func() {
		srv.Stop()
	})

	httpgrpc.RegisterHTTPServer(srv, &mockHTTPServer{})

	ln := bufconn.Listen(1024 * 1024)
	go func() {
		_ = srv.Serve(ln)
	}()
	return func(context.Context, string) (net.Conn, error) {
		return ln.Dial()
	}
}
