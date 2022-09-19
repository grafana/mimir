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
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/scheduler/schedulerdiscovery"
	"github.com/grafana/mimir/pkg/util"
)

type Config struct {
	FrontendAddress  string            `yaml:"frontend_address"`
	SchedulerAddress string            `yaml:"scheduler_address"`
	DNSLookupPeriod  time.Duration     `yaml:"dns_lookup_duration" category:"advanced"`
	QuerierID        string            `yaml:"id" category:"advanced"`
	GRPCClientConfig grpcclient.Config `yaml:"grpc_client_config"`

	// This configuration is injected internally.
	MaxConcurrentRequests   int                       `yaml:"-"` // Must be same as passed to PromQL Engine.
	QuerySchedulerDiscovery schedulerdiscovery.Config `yaml:"-"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.SchedulerAddress, "querier.scheduler-address", "", fmt.Sprintf("Address of the query-scheduler component, in host:port format. The host should resolve to all query-scheduler instances. This option should be set only when query-scheduler component is in use and -%s is set to '%s'.", schedulerdiscovery.ModeFlagName, schedulerdiscovery.ModeDNS))
	f.StringVar(&cfg.FrontendAddress, "querier.frontend-address", "", "Address of the query-frontend component, in host:port format. If multiple query-frontends are running, the host should be a DNS resolving to all query-frontend instances. This option should be set only when query-scheduler component is not in use.")
	f.DurationVar(&cfg.DNSLookupPeriod, "querier.dns-lookup-period", 10*time.Second, "How often to query DNS for query-frontend or query-scheduler address.")
	f.StringVar(&cfg.QuerierID, "querier.id", "", "Querier ID, sent to the query-frontend to identify requests from the same querier. Defaults to hostname.")

	cfg.GRPCClientConfig.RegisterFlagsWithPrefix("querier.frontend-client", f)
}

func (cfg *Config) Validate(log log.Logger) error {
	if cfg.FrontendAddress != "" && cfg.SchedulerAddress != "" {
		return errors.New("frontend address and scheduler address are mutually exclusive, please use only one")
	}
	if cfg.QuerySchedulerDiscovery.Mode == schedulerdiscovery.ModeRing && (cfg.FrontendAddress != "" || cfg.SchedulerAddress != "") {
		return fmt.Errorf("frontend address and scheduler address cannot be specified when query-scheduler service discovery mode is set to '%s'", cfg.QuerySchedulerDiscovery.Mode)
	}

	return cfg.GRPCClientConfig.Validate(log)
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

// serviceDiscoveryNotifications is the interface implemented by the component receiving service discovery notifications.
type serviceDiscoveryNotifications interface {
	AddressAdded(address string)
	AddressRemoved(address string)
}

// serviceDiscoveryFactory makes a new service discovery instance.
type serviceDiscoveryFactory func(receiver serviceDiscoveryNotifications) (services.Service, error)

type querierWorker struct {
	*services.BasicService

	cfg Config
	log log.Logger

	processor processor

	// Subservices manager.
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher

	mu sync.Mutex
	// Set to nil when stop is called... no more managers are created afterwards.
	managers map[string]*processorManager
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
	var servs []services.Service
	var factory serviceDiscoveryFactory

	switch {
	case cfg.SchedulerAddress != "" || cfg.QuerySchedulerDiscovery.Mode == schedulerdiscovery.ModeRing:
		level.Info(log).Log("msg", "Starting querier worker connected to query-scheduler", "scheduler", cfg.SchedulerAddress)

		factory = func(receiver serviceDiscoveryNotifications) (services.Service, error) {
			return schedulerdiscovery.NewServiceDiscovery(cfg.QuerySchedulerDiscovery, cfg.SchedulerAddress, cfg.DNSLookupPeriod, "querier", receiver, log, reg)
		}

		processor, servs = newSchedulerProcessor(cfg, handler, log, reg)

	case cfg.FrontendAddress != "":
		level.Info(log).Log("msg", "Starting querier worker connected to query-frontend", "frontend", cfg.FrontendAddress)

		factory = func(receiver serviceDiscoveryNotifications) (services.Service, error) {
			return util.NewDNSWatcher(cfg.FrontendAddress, cfg.DNSLookupPeriod, receiver)
		}

		processor = newFrontendProcessor(cfg, handler, log)

	default:
		return nil, errors.New("no query-scheduler or query-frontend address")
	}

	return newQuerierWorkerWithProcessor(cfg, log, processor, factory, servs)
}

func newQuerierWorkerWithProcessor(cfg Config, log log.Logger, processor processor, newServiceDiscovery serviceDiscoveryFactory, servs []services.Service) (*querierWorker, error) {
	f := &querierWorker{
		cfg:       cfg,
		log:       log,
		managers:  map[string]*processorManager{},
		processor: processor,
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
	// worker no longer creates new managers in AddressAdded method.
	w.mu.Lock()
	for _, m := range w.managers {
		m.stop()
	}
	w.mu.Unlock()

	if w.subservices == nil {
		return nil
	}

	// Stop service discovery and services used by processor.
	return services.StopManagerAndAwaitStopped(context.Background(), w.subservices)
}

func (w *querierWorker) AddressAdded(address string) {
	ctx := w.ServiceContext()
	if ctx == nil || ctx.Err() != nil {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if m := w.managers[address]; m != nil {
		return
	}

	level.Info(w.log).Log("msg", "adding connection", "addr", address)
	conn, err := w.connect(context.Background(), address)
	if err != nil {
		level.Error(w.log).Log("msg", "error connecting", "addr", address, "err", err)
		return
	}

	w.managers[address] = newProcessorManager(ctx, w.processor, conn, address)
	// Called with lock.
	w.resetConcurrency()
}

func (w *querierWorker) AddressRemoved(address string) {
	level.Info(w.log).Log("msg", "removing connection", "addr", address)

	w.mu.Lock()
	p := w.managers[address]
	delete(w.managers, address)
	w.mu.Unlock()

	if p != nil {
		p.stop()
	}
}

// Must be called with lock.
func (w *querierWorker) resetConcurrency() {
	totalConcurrency := 0
	index := 0

	for _, m := range w.managers {
		concurrency := w.cfg.MaxConcurrentRequests / len(w.managers)

		// If max concurrency does not evenly divide into our frontends a subset will be chosen
		// to receive an extra connection.  Frontend addresses were shuffled above so this will be a
		// random selection of frontends.
		if index < w.cfg.MaxConcurrentRequests%len(w.managers) {
			level.Warn(w.log).Log("msg", "max concurrency is not evenly divisible across targets, adding an extra connection", "addr", m.address)
			concurrency++
		}

		// If concurrency is 0 then MaxConcurrentRequests is less than the total number of
		// frontends/schedulers. In order to prevent accidentally starving a frontend or scheduler we are just going to
		// always connect once to every target.  This is dangerous b/c we may start exceeding PromQL
		// max concurrency.
		if concurrency == 0 {
			concurrency = 1
		}

		totalConcurrency += concurrency
		m.concurrency(concurrency)
		index++
	}

	if totalConcurrency > w.cfg.MaxConcurrentRequests {
		level.Warn(w.log).Log("msg", "total worker concurrency is greater than promql max concurrency. Queries may be queued in the querier which reduces QOS")
	}
}

func (w *querierWorker) connect(ctx context.Context, address string) (*grpc.ClientConn, error) {
	// Because we only use single long-running method, it doesn't make sense to inject user ID, send over tracing or add metrics.
	opts, err := w.cfg.GRPCClientConfig.DialOption(nil, nil)
	if err != nil {
		return nil, err
	}

	conn, err := grpc.DialContext(ctx, address, opts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
