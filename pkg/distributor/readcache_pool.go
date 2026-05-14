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
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/mimir/pkg/ingester/client"
)

// ReadcacheConfig holds the distributor-side configuration for
// dialling readcache instances. Until the readcache ring is wired up
// (deferred from phase 2A), the distributor resolves readcache
// instance IDs to network addresses through a static
// flag-configurable map.
type ReadcacheConfig struct {
	// Addresses is a comma-separated list of `instance_id=host:port`
	// pairs. When the readcache rebalancer publishes a lease
	// referencing instance_id, the distributor looks up host:port
	// in this map to dial the pod.
	//
	// Once a readcache ring exists, this field becomes redundant
	// (instance addresses are read from the ring's Desc); for now it
	// is the bridge that lets the Phase 2C distributor route reads
	// without a ring.
	Addresses string `yaml:"addresses" category:"experimental"`

	// GRPCClientConfig configures the gRPC client used to dial
	// readcache instances. Inherits sensible defaults from the
	// ingester client config.
	GRPCClientConfig grpcclient.Config `yaml:"grpc_client_config" doc:"description=Configures the gRPC client used to communicate with readcache pods."`
}

// RegisterFlagsWithPrefix registers the readcache pool's flags.
func (cfg *ReadcacheConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Addresses, prefix+"addresses", "", "Comma-separated list of instance_id=host:port pairs identifying readcache pods this distributor may dial for reads. Replaced by ring-based discovery in a follow-up patch.")
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

// readcachePool resolves readcache instance IDs to ingester gRPC
// clients (readcache implements the same gRPC service as the
// ingester minus Push, so an ingester client is what we want).
// Connections are cached per instance and reused.
type readcachePool struct {
	addresses map[string]string
	dialOpts  []grpc.DialOption
	logger    log.Logger

	mu      sync.Mutex
	clients map[string]readcacheClient
}

// readcacheClient bundles a gRPC connection and a typed IngesterClient.
// The connection is kept so Stop can close it cleanly.
type readcacheClient struct {
	conn *grpc.ClientConn
	cli  client.IngesterClient
}

// newReadcachePool returns a pool that resolves the configured
// addresses. Returns an error if the addresses string is malformed.
func newReadcachePool(cfg ReadcacheConfig, logger log.Logger) (*readcachePool, error) {
	addresses, err := parseReadcacheAddresses(cfg.Addresses)
	if err != nil {
		return nil, errors.Wrap(err, "parsing readcache addresses")
	}
	// Install the X-Scope-OrgID propagation interceptors so reads
	// arriving at readcache pass the standard StreamServerUserHeaderInterceptor
	// gate. Without this every QueryStream against a readcache pod
	// returns "no org id" on the server side.
	return &readcachePool{
		addresses: addresses,
		dialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithUnaryInterceptor(middleware.ClientUserHeaderInterceptor),
			grpc.WithStreamInterceptor(middleware.StreamClientUserHeaderInterceptor),
		},
		logger:  logger,
		clients: map[string]readcacheClient{},
	}, nil
}

// GetClientForInstance returns (or lazily dials) a client for the
// readcache instance identified by instanceID. The returned client
// implements the same gRPC surface as an ingester (minus Push).
func (p *readcachePool) GetClientForInstance(ctx context.Context, instanceID string) (client.IngesterClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if c, ok := p.clients[instanceID]; ok {
		return c.cli, nil
	}
	addr, ok := p.addresses[instanceID]
	if !ok {
		return nil, fmt.Errorf("readcache instance %q has no configured address", instanceID)
	}
	// nolint:staticcheck // grpc.DialContext is deprecated but is still the form used elsewhere in this package.
	conn, err := grpc.DialContext(ctx, addr, p.dialOpts...)
	if err != nil {
		return nil, errors.Wrapf(err, "dialing readcache %s at %s", instanceID, addr)
	}
	cli := client.NewIngesterClient(conn)
	p.clients[instanceID] = readcacheClient{conn: conn, cli: cli}
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
