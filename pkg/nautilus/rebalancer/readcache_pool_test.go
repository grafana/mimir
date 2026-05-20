// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
)

// captureOrgIDServer is a minimal IngesterServer that records the
// org ID it sees on the GetHashRanges path. We embed
// UnimplementedIngesterServer so unrelated RPCs return a clean
// Unimplemented status instead of panicking with a nil method, which
// would mask the assertion we actually care about here.
type captureOrgIDServer struct {
	ingester_client.UnimplementedIngesterServer

	gotOrgID    atomic.Value // string
	callsServed atomic.Int32
}

func (s *captureOrgIDServer) GetHashRanges(ctx context.Context, _ *ingester_client.GetHashRangesRequest) (*ingester_client.GetHashRangesResponse, error) {
	orgID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}
	s.gotOrgID.Store(orgID)
	s.callsServed.Add(1)
	return &ingester_client.GetHashRangesResponse{}, nil
}

// TestReadcachePool_PropagatesOrgIDOnOutboundRPC is a regression
// guard for the silent outage we hit when ReadcachePool's dial
// options omitted ClientUserHeaderInterceptor. Without it the
// rebalancer's GetHashRanges / HashRangeStats / SetHashRanges calls
// were all rejected on the server side with "no org id", every
// reconstructRound quorum failed, and the rebalancer ended up in a
// FineEvenSplit cold-start loop that grew the assignment log
// without bound.
//
// The test stands up a real gRPC server gated by
// ServerUserHeaderInterceptor (the same gate readcache pods have on
// their ingester service surface), dials it through the pool exactly
// the way rebalance() does — with an org ID injected on the Go
// context but no explicit metadata — and asserts the server sees
// the synthetic "nautilus-rebalancer" org ID and the call succeeds.
func TestReadcachePool_PropagatesOrgIDOnOutboundRPC(t *testing.T) {
	// Server: same auth posture as a production readcache pod.
	srv := grpc.NewServer(grpc.UnaryInterceptor(middleware.ServerUserHeaderInterceptor))
	capture := &captureOrgIDServer{}
	ingester_client.RegisterIngesterServer(srv, capture)

	lis, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)

	// Pool: production constructor, real interceptors. We do not
	// pass a cluster validation label so the server can run without
	// one; the interceptor under test is the user-header one, not
	// the cluster one.
	ringStub := stubReadcacheRing{set: ring.ReplicationSet{Instances: []ring.InstanceDesc{
		{Id: "readcache-test-0", Addr: lis.Addr().String()},
	}}}
	pool, err := NewReadcachePool(ReadcacheClientConfig{}, ringStub, "", log.NewNopLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close() })

	// Call exactly the way rebalance() does: inject the synthetic
	// org ID on the Go context and let the dial-side interceptor
	// translate that into outbound gRPC metadata.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = user.InjectOrgID(ctx, "nautilus-rebalancer")

	cli, err := pool.clientFor(ctx, ring.InstanceDesc{Id: "readcache-test-0", Addr: lis.Addr().String()})
	require.NoError(t, err)

	_, err = cli.GetHashRanges(ctx, &ingester_client.GetHashRangesRequest{})
	require.NoError(t, err, "GetHashRanges must succeed once X-Scope-OrgID is on the wire")

	// Server saw exactly the org ID we injected. Asserting on the
	// concrete value (not just non-empty) catches the case where
	// some interceptor passes through metadata blindly without the
	// dskit user-header propagation.
	assert.Equal(t, int32(1), capture.callsServed.Load(), "exactly one RPC should have reached the server")
	got, _ := capture.gotOrgID.Load().(string)
	assert.Equal(t, "nautilus-rebalancer", got, "server must extract the org ID propagated by the client interceptor")
}
