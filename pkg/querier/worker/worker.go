// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/worker/worker.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package worker

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/servicediscovery"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/scheduler/schedulerdiscovery"
	"github.com/grafana/mimir/pkg/util/grpcencoding/s2"
	"github.com/grafana/mimir/pkg/util/math"
)

type Config struct {
	FrontendAddress                string            `yaml:"frontend_address"`
	SchedulerAddress               string            `yaml:"scheduler_address"`
	DNSLookupPeriod                time.Duration     `yaml:"dns_lookup_duration" category:"advanced"`
	QuerierID                      string            `yaml:"id" category:"advanced"`
	QueryFrontendGRPCClientConfig  grpcclient.Config `yaml:"grpc_client_config" doc:"description=Configures the gRPC client used to communicate between the querier and the query-frontend."`
	QuerySchedulerGRPCClientConfig grpcclient.Config `yaml:"query_scheduler_grpc_client_config" doc:"description=Configures the gRPC client used to communicate between the querier and the query-scheduler."`
	ResponseStreamingEnabled       bool              `yaml:"response_streaming_enabled" category:"experimental"`

	// This configuration is injected internally.
	MaxConcurrentRequests   int                       `yaml:"-"` // Must be same as passed to PromQL Engine.
	QuerySchedulerDiscovery schedulerdiscovery.Config `yaml:"-"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.SchedulerAddress, "querier.scheduler-address", "", fmt.Sprintf("Address of the query-scheduler component, in host:port format. The host should resolve to all query-scheduler instances. This option should be set only when query-scheduler component is in use and -%s is set to '%s'.", schedulerdiscovery.ModeFlagName, schedulerdiscovery.ModeDNS))
	f.StringVar(&cfg.FrontendAddress, "querier.frontend-address", "", "Address of the query-frontend component, in host:port format. If multiple query-frontends are running, the host should be a DNS resolving to all query-frontend instances. This option should be set only when query-scheduler component is not in use.")
	f.DurationVar(&cfg.DNSLookupPeriod, "querier.dns-lookup-period", 10*time.Second, "How often to query DNS for query-frontend or query-scheduler address.")
	f.StringVar(&cfg.QuerierID, "querier.id", "", "Querier ID, sent to the query-frontend to identify requests from the same querier. Defaults to hostname.")
	f.BoolVar(&cfg.ResponseStreamingEnabled, "querier.response-streaming-enabled", false, "Enables streaming of responses from querier to query-frontend for response types that support it (currently only `active_series` responses do).")

	cfg.QueryFrontendGRPCClientConfig.CustomCompressors = []string{s2.Name}
	cfg.QueryFrontendGRPCClientConfig.RegisterFlagsWithPrefix("querier.frontend-client", f)
	cfg.QuerySchedulerGRPCClientConfig.CustomCompressors = []string{s2.Name}
	cfg.QuerySchedulerGRPCClientConfig.RegisterFlagsWithPrefix("querier.scheduler-client", f)
}

func (cfg *Config) Validate() error {
	if cfg.FrontendAddress != "" && cfg.SchedulerAddress != "" {
		return errors.New("frontend address and scheduler address are mutually exclusive, please use only one")
	}
	if cfg.QuerySchedulerDiscovery.Mode == schedulerdiscovery.ModeRing && (cfg.FrontendAddress != "" || cfg.SchedulerAddress != "") {
		return fmt.Errorf("frontend address and scheduler address cannot be specified when query-scheduler service discovery mode is set to '%s'", cfg.QuerySchedulerDiscovery.Mode)
	}

	if err := cfg.QueryFrontendGRPCClientConfig.Validate(); err != nil {
		return err
	}

	if err := cfg.QuerySchedulerGRPCClientConfig.Validate(); err != nil {
		return err
	}

	return nil
}

func (cfg *Config) IsFrontendOrSchedulerConfigured() bool {
	return cfg.FrontendAddress != "" || cfg.SchedulerAddress != "" || cfg.QuerySchedulerDiscovery.Mode == schedulerdiscovery.ModeRing
}

// RequestHandler for HTTP requests wrapped in protobuf messages.
type RequestHandler interface {
	Handle(context.Context, *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error)
}

// Single processor handles all streaming operations to query-frontend or query-scheduler to fetch queries
// and process them.
type processor interface {
	// Each invocation of processQueriesOnSingleStream starts new streaming operation to query-frontend
	// or query-scheduler to fetch queries and execute them.
	//
	// This method must react on context being finished, and stop when that happens.
	//
	// processorManager (not processor) is responsible for starting as many goroutines as needed for each connection.
	processQueriesOnSingleStream(ctx context.Context, conn *grpc.ClientConn, address string)

	// notifyShutdown notifies the remote query-frontend or query-scheduler that the querier is
	// shutting down.
	notifyShutdown(ctx context.Context, conn *grpc.ClientConn, address string)
}

// serviceDiscoveryFactory makes a new service discovery instance.
type serviceDiscoveryFactory func(receiver servicediscovery.Notifications) (services.Service, error)

type querierWorker struct {
	*services.BasicService

	maxConcurrentRequests int
	grpcClientConfig      grpcclient.Config
	log                   log.Logger

	processor processor

	// Subservices manager.
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher

	mu        sync.Mutex
	managers  map[string]*processorManager
	instances map[string]servicediscovery.Instance
}

func NewQuerierWorker(cfg Config, handler RequestHandler, log log.Logger, reg prometheus.Registerer) (services.Service, error) {
	if cfg.QuerierID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get hostname for configuring querier ID")
		}
		cfg.QuerierID = hostname
	}

	var processor processor
	var grpcCfg grpcclient.Config
	var servs []services.Service
	var factory serviceDiscoveryFactory

	switch {
	case cfg.SchedulerAddress != "" || cfg.QuerySchedulerDiscovery.Mode == schedulerdiscovery.ModeRing:
		level.Info(log).Log("msg", "Starting querier worker connected to query-scheduler", "scheduler", cfg.SchedulerAddress)

		factory = func(receiver servicediscovery.Notifications) (services.Service, error) {
			return schedulerdiscovery.New(cfg.QuerySchedulerDiscovery, cfg.SchedulerAddress, cfg.DNSLookupPeriod, "querier", receiver, log, reg)
		}

		grpcCfg = cfg.QuerySchedulerGRPCClientConfig
		processor, servs = newSchedulerProcessor(cfg, handler, log, reg)

	case cfg.FrontendAddress != "":
		level.Info(log).Log("msg", "Starting querier worker connected to query-frontend", "frontend", cfg.FrontendAddress)

		factory = func(receiver servicediscovery.Notifications) (services.Service, error) {
			return servicediscovery.NewDNS(log, cfg.FrontendAddress, cfg.DNSLookupPeriod, receiver)
		}

		grpcCfg = cfg.QueryFrontendGRPCClientConfig
		processor = newFrontendProcessor(cfg, handler, log)

	default:
		return nil, errors.New("no query-scheduler or query-frontend address")
	}

	return newQuerierWorkerWithProcessor(grpcCfg, cfg.MaxConcurrentRequests, log, processor, factory, servs)
}

func newQuerierWorkerWithProcessor(grpcCfg grpcclient.Config, maxConcReq int, log log.Logger, processor processor, newServiceDiscovery serviceDiscoveryFactory, servs []services.Service) (*querierWorker, error) {
	f := &querierWorker{
		grpcClientConfig:      grpcCfg,
		maxConcurrentRequests: maxConcReq,
		log:                   log,
		managers:              map[string]*processorManager{},
		instances:             map[string]servicediscovery.Instance{},
		processor:             processor,
	}

	// There's no service discovery in some tests.
	if newServiceDiscovery != nil {
		w, err := newServiceDiscovery(f)
		if err != nil {
			return nil, err
		}

		servs = append(servs, w)
	}

	if len(servs) > 0 {
		subservices, err := services.NewManager(servs...)
		if err != nil {
			return nil, errors.Wrap(err, "querier worker subservices")
		}

		f.subservices = subservices
		f.subservicesWatcher = services.NewFailureWatcher()
	}

	f.BasicService = services.NewBasicService(f.starting, f.running, f.stopping)
	return f, nil
}

func (w *querierWorker) starting(ctx context.Context) error {
	if w.subservices == nil {
		return nil
	}

	w.subservicesWatcher.WatchManager(w.subservices)
	return services.StartManagerAndAwaitHealthy(ctx, w.subservices)
}

func (w *querierWorker) running(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-w.subservicesWatcher.Chan(): // The channel will be nil if w.subservicesWatcher is not set.
		return errors.Wrap(err, "querier worker subservice failed")
	}
}

func (w *querierWorker) stopping(_ error) error {
	// Stop all goroutines fetching queries. Note that in Stopping state,
	// worker no longer creates new managers in InstanceAdded method.
	w.mu.Lock()
	for address, m := range w.managers {
		m.stop("querier shutting down")

		delete(w.managers, address)
		delete(w.instances, address)
	}
	w.mu.Unlock()

	if w.subservices == nil {
		return nil
	}

	// Stop service discovery and services used by processor.
	return services.StopManagerAndAwaitStopped(context.Background(), w.subservices)
}

func (w *querierWorker) InstanceAdded(instance servicediscovery.Instance) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Ensure the querier worker hasn't been stopped (or is stopping).
	// This check is done inside the lock, to avoid any race condition with the stopping() function.
	ctx := w.ServiceContext()
	if ctx == nil || ctx.Err() != nil {
		return
	}

	address := instance.Address
	if m := w.managers[address]; m != nil {
		return
	}

	level.Info(w.log).Log("msg", "adding connection", "addr", address, "in-use", instance.InUse)
	conn, err := w.connect(context.Background(), address)
	if err != nil {
		level.Error(w.log).Log("msg", "error connecting", "addr", address, "err", err)
		return
	}

	w.managers[address] = newProcessorManager(ctx, w.processor, conn, address)
	w.instances[address] = instance

	// Called with lock.
	w.resetConcurrency()
}

func (w *querierWorker) InstanceRemoved(instance servicediscovery.Instance) {
	address := instance.Address

	level.Info(w.log).Log("msg", "removing connection", "addr", address, "in-use", instance.InUse)

	w.mu.Lock()
	p := w.managers[address]
	delete(w.managers, address)
	delete(w.instances, address)
	w.mu.Unlock()

	if p != nil {
		p.stop("instance removed")
	}

	// Re-balance the connections between the available query-frontends / query-schedulers.
	w.mu.Lock()
	w.resetConcurrency()
	w.mu.Unlock()
}

func (w *querierWorker) InstanceChanged(instance servicediscovery.Instance) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Ensure the querier worker hasn't been stopped (or is stopping).
	// This check is done inside the lock, to avoid any race condition with the stopping() function.
	ctx := w.ServiceContext()
	if ctx == nil || ctx.Err() != nil {
		return
	}

	// Ensure there's a manager for the instance. If there's no, then it's a bug.
	if m := w.managers[instance.Address]; m == nil {
		level.Error(w.log).Log("msg", "received a notification about an unknown backend instance", "addr", instance.Address, "in-use", instance.InUse)
		return
	}

	level.Info(w.log).Log("msg", "updating connection", "addr", instance.Address, "in-use", instance.InUse)

	// Update instance and adjust concurrency.
	w.instances[instance.Address] = instance

	// Called with lock.
	w.resetConcurrency()
}

// MinConcurrencyPerRequestQueue prevents RequestQueue starvation in query-frontend or query-scheduler instances.
// When the RequestQueue utilizes the querier-worker queue prioritization algorithm, querier-worker connections
// are partitioned across up to 4 queue dimensions representing the 4 possible assignments for expected query component:
// ingester, store-gateway, ingester-and-store-gateway, and unknown.
// Failure to assign any querier-worker connections to a queue dimension can result in starvation of that queue dimension.
const MinConcurrencyPerRequestQueue = 4

// Must be called with lock.
func (w *querierWorker) resetConcurrency() {
	desiredConcurrency := w.getDesiredConcurrency()

	for _, m := range w.managers {
		concurrency, ok := desiredConcurrency[m.address]
		if !ok {
			// This error should never happen. If it does, it means there's a bug in the code.
			level.Error(w.log).Log("msg", "a querier worker is connected to an unknown remote endpoint", "addr", m.address)

			// Consider it as not in-use.
			concurrency = MinConcurrencyPerRequestQueue
		}

		m.concurrency(concurrency, "resetting worker concurrency")
	}
}

// getDesiredConcurrency returns the number of desired connections for each discovered query-frontend / query-scheduler instance.
// Must be called with lock.
func (w *querierWorker) getDesiredConcurrency() map[string]int {
	// Count the number of in-use instances.
	numInUse := 0
	for _, instance := range w.instances {
		if instance.InUse {
			numInUse++
		}
	}

	var (
		desired    = make(map[string]int, len(w.instances))
		inUseIndex = 0
	)

	// new adjusted minimum to ensure that each in-use instance has at least MinConcurrencyPerRequestQueue connections.
	maxConcurrentWithMinPerInstance := math.Max(
		w.maxConcurrentRequests, MinConcurrencyPerRequestQueue*numInUse,
	)
	if maxConcurrentWithMinPerInstance > w.maxConcurrentRequests {
		level.Warn(w.log).Log("msg", "max concurrency does not meet the minimum required per request queue instance, increasing to minimum")
	}

	// Compute the number of desired connections for each discovered instance.
	for address, instance := range w.instances {
		if !instance.InUse {
			// We expect that a not-in-use instance is either empty or being drained and will be removed soon
			// and therefore these connections will not contribute to the overall steady-state query concurrency.
			// As the state is expected to be temporary, we allocate the minimum number of connections
			// and do not count them against the connection pool size for the in-use instances.
			desired[address] = MinConcurrencyPerRequestQueue
			continue
		}

		concurrency := maxConcurrentWithMinPerInstance / numInUse

		// If max concurrency does not evenly divide into in-use instances, then a subset will be chosen
		// to receive an extra connection. Since we're iterating a map (whose iteration order is not guaranteed),
		// then this should practically select a random address for the extra connection.
		if inUseIndex < maxConcurrentWithMinPerInstance%numInUse {
			level.Warn(w.log).Log("msg", "max concurrency is not evenly divisible across request queue instances, adding an extra connection", "addr", address)
			concurrency++
		}

		desired[address] = concurrency
		inUseIndex++
	}

	return desired
}

func (w *querierWorker) connect(ctx context.Context, address string) (*grpc.ClientConn, error) {
	// Because we only use single long-running method, it doesn't make sense to inject user ID, send over tracing or add metrics.
	opts, err := w.grpcClientConfig.DialOption(nil, nil)

	if err != nil {
		return nil, err
	}

	// nolint:staticcheck // grpc.DialContext() has been deprecated; we'll address it before upgrading to gRPC 2.
	conn, err := grpc.DialContext(ctx, address, opts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
