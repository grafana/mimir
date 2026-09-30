// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
	"github.com/grafana/mimir/pkg/readcache"
	"github.com/grafana/mimir/pkg/util"
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

	// IgnoreReplicaMapForQueries keeps query routing on the logical
	// owner ID recorded in the assignment log even when the ring-derived
	// slot view lists zone pods. Used while a legacy non-zonal fleet is
	// still the dial target. Clear the flag once that fleet is gone.
	IgnoreReplicaMapForQueries bool `yaml:"ignore_replica_map_for_queries" category:"experimental"`

	// GRPCClientConfig configures the gRPC client used to dial
	// readcache instances. Inherits sensible defaults from the
	// ingester client config.
	GRPCClientConfig grpcclient.Config `yaml:"grpc_client_config" doc:"description=Configures the gRPC client used to communicate with readcache pods."`
}

// RegisterFlagsWithPrefix registers the readcache pool's flags.
func (cfg *ReadcacheConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Addresses, prefix+"addresses", "", "Optional comma-separated list of instance_id=host:port pairs identifying readcache pods. When set, each listed instance overrides ring-based discovery; when empty (the default), the distributor resolves addresses from the readcache instance ring.")
	f.BoolVar(&cfg.IgnoreReplicaMapForQueries, prefix+"ignore-replica-map-for-queries", false, "When true, query routing dials the logical owner ID from the assignment log and does not consult the ring-derived slot view. Used to keep dialing legacy non-zonal pods until that fleet is gone.")
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
	slots           *readcacheassignment.SlotViewCache

	mu      sync.Mutex
	clients map[string]readcacheClient
}

// readcachePoolMetrics instruments readcache-directed RPCs. It is a
// separate metric family from
// cortex_ingester_client_request_duration_seconds so dashboards can
// tell reads served by readcache apart from reads served by
// ingesters; sum both families to get total read-path client
// traffic.
type readcachePoolMetrics struct {
	requestDuration                *prometheus.HistogramVec
	invalidClusterValidationLabels *prometheus.CounterVec
}

func newReadcachePoolMetrics(reg prometheus.Registerer) *readcachePoolMetrics {
	return &readcachePoolMetrics{
		requestDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cortex_readcache_client_request_duration_seconds",
			Help:    "Time spent doing readcache requests.",
			Buckets: prometheus.ExponentialBuckets(0.001, 4, 8),
		}, []string{"operation", "status_code"}),
		invalidClusterValidationLabels: util.NewRequestInvalidClusterValidationLabelsTotalCounter(reg, "readcache", util.GRPCProtocol),
	}
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
func newReadcachePool(cfg ReadcacheConfig, ringClient readcacheRingReader, clusterValidationLabel string, reg prometheus.Registerer, logger log.Logger) (*readcachePool, error) {
	addresses, err := parseReadcacheAddresses(cfg.Addresses)
	if err != nil {
		return nil, errors.Wrap(err, "parsing readcache addresses")
	}
	if ringClient == nil && len(addresses) == 0 {
		return nil, errors.New("readcache pool requires either a ring client or a non-empty -distributor.readcache.addresses map")
	}

	// A zero-valued GRPCClientConfig (tests constructing
	// ReadcacheConfig{} directly, without RegisterFlags) carries
	// zero message-size limits that would reject every RPC; detect
	// that and apply the flag defaults.
	if cfg.GRPCClientConfig.MaxRecvMsgSize <= 0 {
		flagext.DefaultValues(&cfg.GRPCClientConfig)
	}
	// Inherit the fleet-wide cluster validation label (the one the
	// ingester client uses) unless the readcache client config sets
	// its own.
	if cfg.GRPCClientConfig.ClusterValidation.Label == "" {
		cfg.GRPCClientConfig.ClusterValidation.Label = clusterValidationLabel
	}

	metrics := newReadcachePoolMetrics(reg)

	// grpcclient.Instrument installs both the request-duration
	// interceptors and the X-Scope-OrgID propagation
	// (Client/StreamClientUserHeaderInterceptor). The latter is
	// load-bearing: without it every QueryStream against a readcache
	// pod fails the server-side StreamServerUserHeaderInterceptor
	// gate with "no org id".
	unary, stream := grpcclient.Instrument(metrics.requestDuration, middleware.ReportGRPCStatusOption)
	dialOpts, err := cfg.GRPCClientConfig.DialOption(unary, stream,
		util.NewInvalidClusterValidationReporter(cfg.GRPCClientConfig.ClusterValidation.Label, metrics.invalidClusterValidationLabels, logger))
	if err != nil {
		return nil, errors.Wrap(err, "building readcache gRPC dial options")
	}
	dialOpts = append(dialOpts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))

	p := &readcachePool{
		staticAddresses: addresses,
		ring:            ringClient,
		dialOpts:        dialOpts,
		logger:          logger,
		slots:           readcacheassignment.NewSlotViewCache(),
		clients:         map[string]readcacheClient{},
	}
	// Static-only pools have no ring to refresh later. The address
	// keys are the healthy set, so a nil replica map still dials the
	// logged owner and resolveAddr uses the static address.
	p.refreshSlotView()
	return p, nil
}

// refreshSlotView re-reads the readcache ring into the cached grouping.
// A timestamp-only ring update that leaves the same pods healthy is a
// no-op inside Observe. The query path reads the cache and does not
// call this.
func (p *readcachePool) refreshSlotView() {
	if p == nil || p.slots == nil {
		return
	}
	if p.ring == nil {
		p.observeStaticAddresses()
		return
	}
	set, err := p.ring.GetAllHealthy(readcache.ReadcacheRingOp)
	if err != nil {
		if errors.Is(err, ring.ErrEmptyRing) {
			p.slots.Observe(nil)
			return
		}
		level.Warn(p.logger).Log("msg", "readcache ring lookup failed while refreshing slot view", "err", err)
		p.slots.MarkUnavailable()
		return
	}
	pods := make([]readcacheassignment.HealthyPod, 0, len(set.Instances))
	for _, inst := range set.Instances {
		pods = append(pods, readcacheassignment.HealthyPod{
			InstanceID: inst.Id,
			Zone:       inst.Zone,
			Addr:       inst.Addr,
		})
	}
	p.slots.Observe(pods)
}

// observeStaticAddresses records the configured address map as the
// slot view. Used when no ring is wired. Grouping still parses the
// hostname, so a zonal name lands on its logical slot.
func (p *readcachePool) observeStaticAddresses() {
	if len(p.staticAddresses) == 0 {
		return
	}
	pods := make([]readcacheassignment.HealthyPod, 0, len(p.staticAddresses))
	for id := range p.staticAddresses {
		pods = append(pods, readcacheassignment.HealthyPod{InstanceID: id, Addr: id})
	}
	p.slots.Observe(pods)
}

// resolveAddr returns the dial target for instanceID. Static map
// wins when present so the operator escape hatch isn't shadowed by
// stale ring entries. Otherwise the address comes from the slot view
// refreshed off the query path.
func (p *readcachePool) resolveAddr(instanceID string) (string, error) {
	if addr, ok := p.staticAddresses[instanceID]; ok {
		return addr, nil
	}
	if p.slots == nil {
		return "", fmt.Errorf("readcache instance %q has no configured address and no slot view is wired", instanceID)
	}
	view, ok := p.slots.Current()
	if !ok {
		return "", fmt.Errorf("readcache slot view is unavailable")
	}
	addr, ok := view.Addr(instanceID)
	if !ok {
		return "", fmt.Errorf("readcache instance %q not found in slot view", instanceID)
	}
	return addr, nil
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
