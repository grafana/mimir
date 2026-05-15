// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/readcache"
)

// ReadcacheConfig holds the distributor-side configuration for
// dialling readcache instances. In production the distributor
// discovers readcache pods via the readcache instance ring; the
// static Addresses map is retained as an escape hatch for tests and
// degraded-mode bring-ups where no ring KV is available.
type ReadcacheConfig struct {
	// Addresses is an optional comma-separated list of
	// `instance_id=host:port` pairs. When set, it takes precedence
	// over the ring (per-instance), letting an operator pin specific
	// dial targets. When empty (the production default), the
	// distributor reads addresses from the readcache ring entries
	// populated by each pod's BasicLifecycler.
	Addresses string `yaml:"addresses" category:"experimental"`

	// GRPCClientConfig configures the gRPC client used to dial
	// readcache instances. Inherits sensible defaults from the
	// ingester client config.
	GRPCClientConfig grpcclient.Config `yaml:"grpc_client_config" doc:"description=Configures the gRPC client used to communicate with readcache pods."`
}

// RegisterFlagsWithPrefix registers the readcache pool's flags.
func (cfg *ReadcacheConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Addresses, prefix+"addresses", "", "Optional comma-separated list of instance_id=host:port pairs identifying readcache pods. When set, each listed instance overrides ring-based discovery; when empty (the default), the distributor resolves addresses from the readcache instance ring.")
	cfg.GRPCClientConfig.RegisterFlagsWithPrefix(prefix+"grpc-client-config", f)
}

// parseReadcacheAddresses parses the comma-separated
// instance_id=host:port list.
func parseReadcacheAddresses(s string) (map[string]string, error) {
	out := map[string]string{}
	for _, kv := range strings.Split(s, ",") {
		kv = strings.TrimSpace(kv)
		if kv == "" {
			continue
		}
		i := strings.IndexByte(kv, '=')
		if i <= 0 || i == len(kv)-1 {
			return nil, fmt.Errorf("readcache address %q must be of the form instance_id=host:port", kv)
		}
		id := strings.TrimSpace(kv[:i])
		addr := strings.TrimSpace(kv[i+1:])
		if _, exists := out[id]; exists {
			return nil, fmt.Errorf("readcache instance %q listed multiple times", id)
		}
		out[id] = addr
	}
	return out, nil
}

// readcacheRingReader is the subset of *ring.Ring the pool needs.
// Modelled as an interface so tests can substitute a fake without a
// real KV.
type readcacheRingReader interface {
	GetAllHealthy(op ring.Operation) (ring.ReplicationSet, error)
}

// readcachePool resolves readcache instance IDs to ingester gRPC
// clients (readcache implements the same gRPC service as the
// ingester minus Push, so an ingester client is what we want).
// Connections are cached per instance and reused; the cache is
// invalidated when the ring reports a different address for a
// previously-known instance (pod restarts that change IP).
type readcachePool struct {
	staticAddresses map[string]string
	ring            readcacheRingReader
	dialOpts        []grpc.DialOption
	logger          log.Logger

	mu      sync.Mutex
	clients map[string]readcacheClient
}

// readcacheClient bundles a gRPC connection, a typed IngesterClient,
// and the address it was dialed against. The address is retained so
// the pool can detect ring updates that move an instance to a new
// host:port and drop the stale connection.
type readcacheClient struct {
	conn *grpc.ClientConn
	cli  client.IngesterClient
	addr string
}

// newReadcachePool returns a pool that resolves clients through the
// readcache ring (preferred) with a static-address override map for
// tests and operator escape hatches. ringClient may be nil iff the
// caller has supplied a complete static address map; otherwise the
// pool would have nowhere to look up unknown instance IDs.
//
// clusterValidationLabel must match readcache's server cluster
// validation label when gRPC cluster validation is enabled on
// readcache (same as other internal clients, e.g. ingester pool).
func newReadcachePool(cfg ReadcacheConfig, ringClient readcacheRingReader, clusterValidationLabel string, logger log.Logger) (*readcachePool, error) {
	addresses, err := parseReadcacheAddresses(cfg.Addresses)
	if err != nil {
		return nil, errors.Wrap(err, "parsing readcache addresses")
	}
	if ringClient == nil && len(addresses) == 0 {
		return nil, errors.New("readcache pool requires either a ring client or a non-empty -distributor.readcache.addresses map")
	}
	// Install the X-Scope-OrgID propagation interceptors so reads
	// arriving at readcache pass the standard StreamServerUserHeaderInterceptor
	// gate. Without this every QueryStream against a readcache pod
	// returns "no org id" on the server side.
	unary := []grpc.UnaryClientInterceptor{middleware.ClientUserHeaderInterceptor}
	if clusterValidationLabel != "" {
		unary = append(unary, middleware.ClusterUnaryClientInterceptor(clusterValidationLabel, middleware.NoOpInvalidClusterValidationReporter))
	}
	return &readcachePool{
		staticAddresses: addresses,
		ring:            ringClient,
		dialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithChainUnaryInterceptor(unary...),
			grpc.WithStreamInterceptor(middleware.StreamClientUserHeaderInterceptor),
		},
		logger:  logger,
		clients: map[string]readcacheClient{},
	}, nil
}

// resolveAddr returns the dial target for instanceID. Static map
// wins when present so the operator escape hatch isn't shadowed by
// stale ring entries.
func (p *readcachePool) resolveAddr(instanceID string) (string, error) {
	if addr, ok := p.staticAddresses[instanceID]; ok {
		return addr, nil
	}
	if p.ring == nil {
		return "", fmt.Errorf("readcache instance %q has no configured address and no ring is wired", instanceID)
	}
	set, err := p.ring.GetAllHealthy(readcache.ReadcacheRingOp)
	if err != nil {
		return "", fmt.Errorf("readcache ring lookup: %w", err)
	}
	for _, inst := range set.Instances {
		if inst.Id == instanceID {
			return inst.Addr, nil
		}
	}
	return "", fmt.Errorf("readcache instance %q not found in ring", instanceID)
}

// GetClientForInstance returns (or lazily dials) a client for the
// readcache instance identified by instanceID. The returned client
// implements the same gRPC surface as an ingester (minus Push).
//
// If the ring reports a different address than the one we previously
// dialed for this instance (pod restart with a new IP), the cached
// connection is closed and a fresh dial happens transparently.
func (p *readcachePool) GetClientForInstance(ctx context.Context, instanceID string) (client.IngesterClient, error) {
	addr, err := p.resolveAddr(instanceID)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if c, ok := p.clients[instanceID]; ok {
		if c.addr == addr {
			return c.cli, nil
		}
		// Address changed: drop the stale connection. Logging
		// here makes pod-restart events visible without spamming
		// the steady-state path.
		_ = c.conn.Close()
		delete(p.clients, instanceID)
	}

	// nolint:staticcheck // grpc.DialContext is deprecated but is still the form used elsewhere in this package.
	conn, err := grpc.DialContext(ctx, addr, p.dialOpts...)
	if err != nil {
		return nil, errors.Wrapf(err, "dialing readcache %s at %s", instanceID, addr)
	}
	cli := client.NewIngesterClient(conn)
	p.clients[instanceID] = readcacheClient{conn: conn, cli: cli, addr: addr}
	return cli, nil
}

// Close shuts down all open connections.
func (p *readcachePool) Close() error {
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
