// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/siderolabs/grpc-proxy/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// RingBackendConfig is the configuration of a backend discoverable via memberlist.
type RingBackendConfig struct {
	Name    string           `yaml:"name"`
	Type    string           `yaml:"type"`
	Address string           `yaml:"address"`
	Ring    RingClientConfig `yaml:"ring"`
}

func (cfg *RingBackendConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Name, prefix+"name", "backend-primary", "Name of the backend. grpc-tee uses it in logs, metrics, and the ring status page path.")
	f.StringVar(&cfg.Type, prefix+"type", backendTypeOpaque, fmt.Sprintf("Type of the backend. The type selects the codec that decodes the proxied messages. Supported values: %s, %s.", backendTypeOpaque, backendTypeStoreGateway))
	f.StringVar(&cfg.Address, prefix+"address", "localhost:9096", "Address of the gRPC backend to forward requests to.")
	cfg.Ring.RegisterFlagsWithPrefix(prefix+"ring.", f)
}

// RingClientConfig is the configuration of a client that reads a ring.
// It holds only the options that a ring client uses. The options of the instances that join the ring are not necessary.
type RingClientConfig struct {
	Key                  string                 `yaml:"key"`
	KVStore              kv.Config              `yaml:"kvstore"`
	HeartbeatTimeout     time.Duration          `yaml:"heartbeat_timeout"`
	ReplicationFactor    int                    `yaml:"replication_factor"`
	ZoneAwarenessEnabled bool                   `yaml:"zone_awareness_enabled"`
	ExcludedZones        flagext.StringSliceCSV `yaml:"excluded_zones"`
}

func (cfg *RingClientConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Key, prefix+"key", "", "Key of the ring in the KV store. It must be the same as the key that the ring members use.")
	cfg.KVStore.Store = "memberlist"
	cfg.KVStore.RegisterFlagsWithPrefix(prefix, "collectors/", f)
	f.DurationVar(&cfg.HeartbeatTimeout, prefix+"heartbeat-timeout", time.Minute, "Heartbeat timeout after which a ring member is unhealthy. It must be the same as the value that the ring members use.")
	f.IntVar(&cfg.ReplicationFactor, prefix+"replication-factor", 3, "Replication factor of the ring. It must be the same as the value that the ring members use.")
	f.BoolVar(&cfg.ZoneAwarenessEnabled, prefix+"zone-awareness-enabled", false, "True if the ring replicates across availability zones. It must be the same as the value that the ring members use.")
	f.Var(&cfg.ExcludedZones, prefix+"excluded-zones", "Comma-separated list of zones to exclude from the ring.")
}

func (cfg *RingClientConfig) Validate() error {
	if cfg.Key == "" {
		return errors.New("ring key must not be empty")
	}
	return nil
}

func (cfg *RingClientConfig) ToRingConfig() ring.Config {
	rc := ring.Config{}
	flagext.DefaultValues(&rc)

	rc.KVStore = cfg.KVStore
	rc.HeartbeatTimeout = cfg.HeartbeatTimeout
	rc.ReplicationFactor = cfg.ReplicationFactor
	rc.ZoneAwarenessEnabled = cfg.ZoneAwarenessEnabled
	rc.ExcludedZones = cfg.ExcludedZones
	return rc
}

// RingBackend is a cluster that grpc-tee forwards requests to.
// It reads the ring of the cluster members the same way as the querier reads the store-gateway ring.
type RingBackend struct {
	services.Service

	name    string
	codec   MessageCodec
	cfg     RingBackendConfig
	ring    *ring.Ring
	conn    *grpc.ClientConn
	proxy   proxy.Backend
	watcher *services.FailureWatcher
}

func NewRingBackend(cfg RingBackendConfig, logger log.Logger, reg prometheus.Registerer) (*RingBackend, error) {
	name := cfg.Name
	if name == "" {
		return nil, errors.New("backend name must not be empty")
	}
	if err := cfg.Ring.Validate(); err != nil {
		return nil, fmt.Errorf("invalid ring config for backend %s: %w", name, err)
	}

	codec, err := newMessageCodec(cfg.Type)
	if err != nil {
		return nil, fmt.Errorf("invalid config for backend %s: %w", name, err)
	}

	ringCfg := cfg.Ring.ToRingConfig()
	ringKV, err := kv.NewClient(ringCfg.KVStore, ring.GetCodec(), kv.RegistererWithKVName(reg, "grpc-tee-backend-"+name), logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create ring KV client for backend %s: %w", name, err)
	}

	// The ring name is the backend name, so the ring metrics of each backend have a different "name" label.
	backendRing, err := ring.NewWithStoreClientAndStrategy(ringCfg, name, cfg.Ring.Key, ringKV, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), reg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create ring client for backend %s: %w", name, err)
	}

	// The client uses the raw codec, so response frames return as opaque bytes.
	conn, err := grpc.NewClient(
		cfg.Address,
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(proxy.Codec())),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client for backend %s: %w", name, err)
	}

	b := &RingBackend{
		name:    name,
		codec:   codec,
		cfg:     cfg,
		ring:    backendRing,
		conn:    conn,
		watcher: services.NewFailureWatcher(),
	}
	b.proxy = &proxy.SingleBackend{
		GetConn: func(ctx context.Context) (context.Context, *grpc.ClientConn, error) {
			// Forward the inbound metadata (for example X-Scope-OrgID) to the backend.
			md, _ := metadata.FromIncomingContext(ctx) //lint:ignore faillint The proxy forwards all keys.
			return metadata.NewOutgoingContext(ctx, md), b.conn, nil
		},
	}
	b.Service = services.NewBasicService(b.starting, b.running, b.stopping).WithName("backend " + name)

	return b, nil
}

func (b *RingBackend) starting(ctx context.Context) error {
	b.watcher.WatchService(b.ring)
	return services.StartAndAwaitRunning(ctx, b.ring)
}

func (b *RingBackend) running(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-b.watcher.Chan():
		return fmt.Errorf("ring client for backend %s failed: %w", b.name, err)
	}
}

func (b *RingBackend) stopping(_ error) error {
	ringErr := services.StopAndAwaitTerminated(context.Background(), b.ring)
	if err := b.conn.Close(); err != nil {
		return fmt.Errorf("failed to close gRPC client for backend %s: %w", b.name, err)
	}
	return ringErr
}

func (b *RingBackend) Name() string {
	return b.name
}

// Codec returns the codec that decodes the messages of this backend, or nil if the backend is opaque.
func (b *RingBackend) Codec() MessageCodec {
	return b.codec
}

// Ring returns the ring of this backend.
func (b *RingBackend) Ring() ring.ReadRing {
	return b.ring
}

// ProxyBackend returns the backend that the gRPC proxy forwards requests to.
// All requests go to the configured address. They do not use the ring yet.
func (b *RingBackend) ProxyBackend() proxy.Backend {
	return b.proxy
}

// ServeHTTP serves the status page of the ring of this backend.
func (b *RingBackend) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	b.ring.ServeHTTP(w, req)
}
