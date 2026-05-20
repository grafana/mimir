// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"context"
	"flag"
	"fmt"
	"sync"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
)

// ReadcacheClientConfig configures the gRPC client the rebalancer
// uses to dial readcache pods (for HashRangeStats / SetHashRanges /
// GetHashRanges). Mirrors the distributor's ReadcacheConfig but is
// intentionally separate so the two consumers can be tuned
// independently (e.g. the rebalancer might want a higher per-call
// timeout because it's polling all pods sequentially).
type ReadcacheClientConfig struct {
	GRPCClientConfig grpcclient.Config `yaml:"grpc_client_config" doc:"description=Configures the gRPC client used to communicate with readcache pods from the rebalancer."`
}

// RegisterFlagsWithPrefix registers the readcache client config's
// flags on f under prefix (typically "nautilus-rebalancer.readcache-client.").
func (cfg *ReadcacheClientConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.GRPCClientConfig.RegisterFlagsWithPrefix(prefix+"grpc-client-config", f)
}

// readcacheRingClient is the subset of *ring.Ring we need. Defined
// as an interface so tests can substitute a fake without a real KV
// store.
type readcacheRingClient interface {
	GetAllHealthy(op ring.Operation) (ring.ReplicationSet, error)
}

// ReadcachePool dials readcache pods by instance ID, resolving the
// dial target through the readcache instance ring. Connections are
// cached per instance and reused.
//
// This is the rebalancer-side analogue of pkg/distributor/readcachePool;
// keeping them separate avoids cross-package coupling (the rebalancer
// shouldn't depend on the distributor) and lets the rebalancer pick
// its own gRPC options. X-Scope-OrgID propagation IS required even
// though the rebalancer's RPCs are administrative: the readcache
// gRPC server reuses the ingester service surface, whose handlers
// run behind ServerUserHeaderInterceptor and reject any inbound RPC
// lacking X-Scope-OrgID with "no org id". rebalance() injects a
// synthetic "nautilus-rebalancer" org ID upstream; the dial
// interceptors below are what actually carries it onto the wire.
type ReadcachePool struct {
	ring     readcacheRingClient
	dialOpts []grpc.DialOption
	logger   log.Logger

	mu      sync.Mutex
	clients map[string]readcacheClient
}

type readcacheClient struct {
	conn *grpc.ClientConn
	cli  ingester_client.IngesterClient
	addr string
}

// NewReadcachePool constructs a pool. ringClient must be non-nil;
// the rebalancer never operates without a discovery source.
//
// clusterValidationLabel must match readcache's
// -server.cluster-validation.label when that server-side check is
// enabled; otherwise gRPC returns FailedPrecondition ("empty
// cluster validation label"). Same value as -ingester-client.grpc-
// client-config.cluster-validation.label / common client-cluster-
// validation in a typical Mimir deployment.
func NewReadcachePool(_ ReadcacheClientConfig, ringClient readcacheRingClient, clusterValidationLabel string, logger log.Logger) (*ReadcachePool, error) {
	if ringClient == nil {
		return nil, errors.New("rebalancer readcache pool requires a non-nil ring client")
	}
	// X-Scope-OrgID propagation: the readcache pods run their gRPC
	// surface behind ServerUserHeaderInterceptor (inherited from the
	// shared ingester service registration), which rejects any
	// inbound RPC whose context lacks an org ID. rebalance() injects
	// a synthetic "nautilus-rebalancer" org ID into the Go context,
	// but it only makes it onto the wire if we install
	// ClientUserHeaderInterceptor on the dial. Omitting it (the
	// previous behaviour) made every HashRangeStats / SetHashRanges
	// / GetHashRanges call fail with "no org id" on the server side,
	// which in turn forced reconstructRound's quorum check to fail
	// and the rebalancer to fall into the FineEvenSplit cold-start
	// branch every round, producing a self-sustaining outage.
	unary := []grpc.UnaryClientInterceptor{middleware.ClientUserHeaderInterceptor}
	if clusterValidationLabel != "" {
		unary = append(unary, middleware.ClusterUnaryClientInterceptor(clusterValidationLabel, middleware.NoOpInvalidClusterValidationReporter))
	}
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(unary...),
		grpc.WithStreamInterceptor(middleware.StreamClientUserHeaderInterceptor),
	}
	return &ReadcachePool{
		ring:     ringClient,
		dialOpts: dialOpts,
		logger:   logger,
		clients:  map[string]readcacheClient{},
	}, nil
}

// healthyInstances returns the readcache instance descriptors the
// rebalancer should poll this round. Sorted by ID for deterministic
// log output.
func (p *ReadcachePool) healthyInstances() ([]ring.InstanceDesc, error) {
	set, err := p.ring.GetAllHealthy(readcacheRingOp)
	if err != nil {
		return nil, fmt.Errorf("readcache ring lookup: %w", err)
	}
	return set.Instances, nil
}

// clientFor returns (or lazily dials) a readcache client by instance
// ID. If the ring reports a different address than the one we cached
// (pod restart with a new IP), the stale connection is closed and a
// fresh one is dialed.
func (p *ReadcachePool) clientFor(ctx context.Context, inst ring.InstanceDesc) (ingester_client.IngesterClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if c, ok := p.clients[inst.Id]; ok {
		if c.addr == inst.Addr {
			return c.cli, nil
		}
		_ = c.conn.Close()
		delete(p.clients, inst.Id)
	}

	// nolint:staticcheck // grpc.DialContext is deprecated but consistent with the rest of the codebase.
	conn, err := grpc.DialContext(ctx, inst.Addr, p.dialOpts...)
	if err != nil {
		return nil, errors.Wrapf(err, "dialing readcache %s at %s", inst.Id, inst.Addr)
	}
	cli := ingester_client.NewIngesterClient(conn)
	p.clients[inst.Id] = readcacheClient{conn: conn, cli: cli, addr: inst.Addr}
	return cli, nil
}

// Close shuts down all cached connections. Called from the
// rebalancer's stopping fn.
func (p *ReadcachePool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var firstErr error
	for id, c := range p.clients {
		if err := c.conn.Close(); err != nil && firstErr == nil {
			firstErr = errors.Wrapf(err, "closing readcache %s", id)
		}
	}
	p.clients = nil
	return firstErr
}
