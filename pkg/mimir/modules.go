// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortex/modules.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimir

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/dns"
	httpgrpc_server "github.com/grafana/dskit/httpgrpc/server"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/modules"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/server"
	"github.com/grafana/dskit/services"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/alertmanager/featurecontrol"
	"github.com/prometheus/alertmanager/matchers/compat"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/rules"
	prom_storage "github.com/prometheus/prometheus/storage"
	prom_remote "github.com/prometheus/prometheus/storage/remote"

	"github.com/grafana/mimir/pkg/alertmanager"
	"github.com/grafana/mimir/pkg/alertmanager/alertstore"
	"github.com/grafana/mimir/pkg/alertmanager/alertstore/bucketclient"
	"github.com/grafana/mimir/pkg/api"
	"github.com/grafana/mimir/pkg/blockbuilder"
	blockbuilderscheduler "github.com/grafana/mimir/pkg/blockbuilder/scheduler"
	"github.com/grafana/mimir/pkg/compactor"
	"github.com/grafana/mimir/pkg/continuoustest"
	"github.com/grafana/mimir/pkg/costattribution"
	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/flusher"
	"github.com/grafana/mimir/pkg/frontend"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/frontend/transport"
	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/querier"
	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/querier/engine"
	"github.com/grafana/mimir/pkg/querier/tenantfederation"
	querier_worker "github.com/grafana/mimir/pkg/querier/worker"
	"github.com/grafana/mimir/pkg/ruler"
	"github.com/grafana/mimir/pkg/scheduler"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/usagestats"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/activitytracker"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
	"github.com/grafana/mimir/pkg/util/validation/exporter"
	"github.com/grafana/mimir/pkg/util/version"
	"github.com/grafana/mimir/pkg/vault"
)

// The various modules that make up Mimir.
const (
	ActivityTracker                  string = "activity-tracker"
	API                              string = "api"
	SanityCheck                      string = "sanity-check"
	IngesterRing                     string = "ingester-ring"
	IngesterPartitionRing            string = "ingester-partitions-ring"
	RuntimeConfig                    string = "runtime-config"
	Overrides                        string = "overrides"
	OverridesExporter                string = "overrides-exporter"
	Server                           string = "server"
	ActiveGroupsCleanupService       string = "active-groups-cleanup-service"
	Distributor                      string = "distributor"
	DistributorService               string = "distributor-service"
	Ingester                         string = "ingester"
	IngesterService                  string = "ingester-service"
	Flusher                          string = "flusher"
	Querier                          string = "querier"
	Queryable                        string = "queryable"
	StoreQueryable                   string = "store-queryable"
	QueryFrontend                    string = "query-frontend"
	QueryFrontendCodec               string = "query-frontend-codec"
	QueryFrontendTripperware         string = "query-frontend-tripperware"
	QueryFrontendTopicOffsetsReaders string = "query-frontend-topic-offsets-reader"
	RulerStorage                     string = "ruler-storage"
	Ruler                            string = "ruler"
	AlertManager                     string = "alertmanager"
	Compactor                        string = "compactor"
	StoreGateway                     string = "store-gateway"
	MemberlistKV                     string = "memberlist-kv"
	QueryScheduler                   string = "query-scheduler"
	Vault                            string = "vault"
	TenantFederation                 string = "tenant-federation"
	UsageStats                       string = "usage-stats"
	BlockBuilder                     string = "block-builder"
	BlockBuilderScheduler            string = "block-builder-scheduler"
	ContinuousTest                   string = "continuous-test"
	All                              string = "all"
	CostAttributionService           string = "cost-attribution-service"

	// Write Read and Backend are the targets used when using the read-write deployment mode.
	Write   string = "write"
	Read    string = "read"
	Backend string = "backend"
)

var (
	// Both queriers and rulers create their own instances of Queryables and federated Queryables,
	// so we need to make sure the series registered by the individual queryables are unique.
	querierEngine = prometheus.Labels{"engine": "querier"}
	rulerEngine   = prometheus.Labels{"engine": "ruler"}
)

func newDefaultConfig() *Config {
	defaultConfig := &Config{}
	defaultFS := flag.NewFlagSet("", flag.PanicOnError)
	defaultConfig.RegisterFlags(defaultFS, util_log.Logger)
	return defaultConfig
}

func (t *Mimir) initAPI() (services.Service, error) {
	t.Cfg.API.ServerPrefix = t.Cfg.Server.PathPrefix

	a, err := api.New(t.Cfg.API, t.Cfg.TenantFederation, t.Cfg.Server, t.Server, util_log.Logger)
	if err != nil {
		return nil, err
	}

	t.BuildInfoHandler = version.BuildInfoHandler(
		t.Cfg.ApplicationName,
		version.BuildInfoFeatures{
			AlertmanagerConfigAPI: strconv.FormatBool(t.Cfg.Alertmanager.EnableAPI),
			QuerySharding:         strconv.FormatBool(t.Cfg.Frontend.QueryMiddleware.ShardedQueries),
			RulerConfigAPI:        strconv.FormatBool(t.Cfg.Ruler.EnableAPI),
			FederatedRules:        strconv.FormatBool(t.Cfg.Ruler.TenantFederation.Enabled),
		})

	t.API = a
	t.API.RegisterAPI(t.Cfg.Server.PathPrefix, t.Cfg, newDefaultConfig(), t.BuildInfoHandler)

	return nil, nil
}

func (t *Mimir) initActivityTracker() (services.Service, error) {
	if t.Cfg.ActivityTracker.Filepath == "" {
		return nil, nil
	}

	entries, err := activitytracker.LoadUnfinishedEntries(t.Cfg.ActivityTracker.Filepath)

	l := log.With(util_log.Logger, "component", "activity-tracker")
	if err != nil {
		level.Warn(l).Log("msg", "failed to fully read file with unfinished activities", "err", err)
	}
	if len(entries) > 0 {
		level.Warn(l).Log("msg", "found unfinished activities from previous run", "count", len(entries))
	}
	for _, e := range entries {
		level.Warn(l).Log("start", e.Timestamp.UTC().Format(time.RFC3339Nano), "activity", e.Activity)
	}

	at, err := activitytracker.NewActivityTracker(t.Cfg.ActivityTracker, t.Registerer)
	if err != nil {
		return nil, err
	}

	t.ActivityTracker = at

	return services.NewIdleService(nil, func(_ error) error {
		entries, err := activitytracker.LoadUnfinishedEntries(t.Cfg.ActivityTracker.Filepath)
		if err != nil {
			level.Warn(l).Log("msg", "failed to fully read file with unfinished activities during shutdown", "err", err)
		}
		if len(entries) > 0 {
			level.Warn(l).Log("msg", "found unfinished activities during shutdown", "count", len(entries))
		}
		for _, e := range entries {
			level.Warn(l).Log("start", e.Timestamp.UTC().Format(time.RFC3339Nano), "activity", e.Activity)
		}
		return nil
	}), nil
}

func (t *Mimir) initVault() (services.Service, error) {
	if !t.Cfg.Vault.Enabled {
		return nil, nil
	}

	v, err := vault.NewVault(t.Cfg.Vault, util_log.Logger, t.Registerer)
	if err != nil {
		return nil, err
	}
	t.Vault = v

	// Update Configs - KVStore
	t.Cfg.MemberlistKV.TCPTransport.TLS.Reader = t.Vault
	t.Cfg.Distributor.HATrackerConfig.KVStore.StoreConfig.Etcd.TLS.Reader = t.Vault
	t.Cfg.Alertmanager.ShardingRing.Common.KVStore.StoreConfig.Etcd.TLS.Reader = t.Vault
	t.Cfg.Compactor.ShardingRing.Common.KVStore.StoreConfig.Etcd.TLS.Reader = t.Vault
	t.Cfg.Distributor.DistributorRing.Common.KVStore.StoreConfig.Etcd.TLS.Reader = t.Vault
	t.Cfg.Ingester.IngesterRing.KVStore.StoreConfig.Etcd.TLS.Reader = t.Vault
	t.Cfg.Ingester.IngesterPartitionRing.KVStore.StoreConfig.Etcd.TLS.Reader = t.Vault
	t.Cfg.Ruler.Ring.Common.KVStore.StoreConfig.Etcd.TLS.Reader = t.Vault
	t.Cfg.StoreGateway.ShardingRing.KVStore.StoreConfig.Etcd.TLS.Reader = t.Vault
	t.Cfg.QueryScheduler.ServiceDiscovery.SchedulerRing.KVStore.StoreConfig.Etcd.TLS.Reader = t.Vault
	t.Cfg.OverridesExporter.Ring.Common.KVStore.StoreConfig.Etcd.TLS.Reader = t.Vault

	// Update Configs - Redis Clients
	t.Cfg.BlocksStorage.BucketStore.IndexCache.BackendConfig.Redis.TLS.Reader = t.Vault
	t.Cfg.BlocksStorage.BucketStore.ChunksCache.BackendConfig.Redis.TLS.Reader = t.Vault
	t.Cfg.BlocksStorage.BucketStore.MetadataCache.BackendConfig.Redis.TLS.Reader = t.Vault
	t.Cfg.Frontend.QueryMiddleware.ResultsCacheConfig.BackendConfig.Redis.TLS.Reader = t.Vault

	// Update Configs - GRPC Clients
	t.Cfg.IngesterClient.GRPCClientConfig.TLS.Reader = t.Vault
	t.Cfg.Worker.QueryFrontendGRPCClientConfig.TLS.Reader = t.Vault
	t.Cfg.Worker.QuerySchedulerGRPCClientConfig.TLS.Reader = t.Vault
	t.Cfg.Querier.StoreGatewayClient.TLS.Reader = t.Vault
	t.Cfg.Frontend.FrontendV2.GRPCClientConfig.TLS.Reader = t.Vault
	t.Cfg.Ruler.ClientTLSConfig.TLS.Reader = t.Vault
	t.Cfg.Ruler.Notifier.TLS.Reader = t.Vault
	t.Cfg.Ruler.QueryFrontend.GRPCClientConfig.TLS.Reader = t.Vault
	t.Cfg.Alertmanager.AlertmanagerClient.GRPCClientConfig.TLS.Reader = t.Vault
	t.Cfg.QueryScheduler.GRPCClientConfig.TLS.Reader = t.Vault

	// Update the Server
	updateServerTLSCfgFunc := func(vault *vault.Vault, tlsConfig *server.TLSConfig) error {
		cert, err := vault.ReadSecret(tlsConfig.TLSCertPath)
		if err != nil {
			return err
		}
		tlsConfig.TLSCert = string(cert)
		tlsConfig.TLSCertPath = ""

		key, err := vault.ReadSecret(tlsConfig.TLSKeyPath)
		if err != nil {
			return err
		}
		tlsConfig.TLSKey = config.Secret(key)
		tlsConfig.TLSKeyPath = ""

		var ca []byte
		if tlsConfig.ClientCAs != "" {
			ca, err = vault.ReadSecret(tlsConfig.ClientCAs)
			if err != nil {
				return err
			}
			tlsConfig.ClientCAsText = string(ca)
			tlsConfig.ClientCAs = ""
		}

		return nil
	}

	if len(t.Cfg.Server.HTTPTLSConfig.TLSCertPath) > 0 && len(t.Cfg.Server.HTTPTLSConfig.TLSKeyPath) > 0 {
		err := updateServerTLSCfgFunc(t.Vault, &t.Cfg.Server.HTTPTLSConfig)
		if err != nil {
			return nil, err
		}
	}

	if len(t.Cfg.Server.GRPCTLSConfig.TLSCertPath) > 0 && len(t.Cfg.Server.GRPCTLSConfig.TLSKeyPath) > 0 {
		err := updateServerTLSCfgFunc(t.Vault, &t.Cfg.Server.GRPCTLSConfig)
		if err != nil {
			return nil, err
		}
	}

	runFunc := func(ctx context.Context) error {
		err := t.Vault.KeepRenewingTokenLease(ctx)
		// We don't want to turn Mimir into an unready state if Vault fails here
		<-ctx.Done()
		return err
	}

	return services.NewBasicService(nil, runFunc, nil), nil
}

func (t *Mimir) initSanityCheck() (services.Service, error) {
	return services.NewIdleService(func(ctx context.Context) error {
		return runSanityCheck(ctx, t.Cfg, util_log.Logger)
	}, nil), nil
}

func (t *Mimir) initServer() (services.Service, error) {
	// We can't inject t.Ingester and t.Distributor directly, because they may not be set yet. However by the time when grpcInflightMethodLimiter runs
	// t.Ingester or t.Distributor will be available. There's no race condition here, because gRPC server (service returned by this method, ie. initServer)
	// is started only after t.Ingester and t.Distributor are set in initIngester or initDistributorService.

	ingFn := func() ingesterReceiver {
		// Return explicit nil if there's no ingester. We don't want to return typed-nil as interface value.
		if t.Ingester == nil {
			return nil
		}
		return t.Ingester
	}

	distFn := func() pushReceiver {
		// Return explicit nil if there's no distributor. We don't want to return typed-nil as interface value.
		if t.Distributor == nil {
			return nil
		}
		return t.Distributor
	}

	// Installing this allows us to reject push requests received via gRPC early -- before they are fully read into memory.
	t.Cfg.Server.GrpcMethodLimiter = newGrpcInflightMethodLimiter(ingFn, distFn)

	// Allow reporting HTTP 4xx codes in status_code label of request duration metrics
	t.Cfg.Server.ReportHTTP4XXCodesInInstrumentationLabel = true

	// Mimir handles signals on its own.
	DisableSignalHandling(&t.Cfg.Server)
	serv, err := server.New(t.Cfg.Server)
	if err != nil {
		return nil, err
	}

	t.Server = serv

	servicesToWaitFor := func() []services.Service {
		svs := []services.Service(nil)

		serverDeps := t.ModuleManager.DependenciesForModule(Server)

		for m, s := range t.ServiceMap {
			// Server should not wait for itself or for any of its dependencies.
			if m == Server {
				continue
			}

			if util.StringsContain(serverDeps, m) {
				continue
			}

			svs = append(svs, s)
		}
		return svs
	}

	s := NewServerService(t.Server, servicesToWaitFor)

	return s, nil
}

func (t *Mimir) initIngesterRing() (serv services.Service, err error) {
	t.Cfg.Ingester.IngesterRing.HideTokensInStatusPage = t.Cfg.IngestStorage.Enabled

	t.IngesterRing, err = ring.New(t.Cfg.Ingester.IngesterRing.ToRingConfig(), "ingester", ingester.IngesterRingKey, util_log.Logger, prometheus.WrapRegistererWithPrefix("cortex_", t.Registerer))
	if err != nil {
		return nil, err
	}
	return t.IngesterRing, nil
}

func (t *Mimir) initIngesterPartitionRing() (services.Service, error) {
	if !t.Cfg.IngestStorage.Enabled {
		return nil, nil
	}

	kvClient, err := kv.NewClient(t.Cfg.Ingester.IngesterPartitionRing.KVStore, ring.GetPartitionRingCodec(), kv.RegistererWithKVName(t.Registerer, ingester.PartitionRingName+"-watcher"), util_log.Logger)
	if err != nil {
		return nil, errors.Wrap(err, "creating KV store for ingester partitions ring watcher")
	}

	t.IngesterPartitionRingWatcher = ring.NewPartitionRingWatcher(ingester.PartitionRingName, ingester.PartitionRingKey, kvClient, util_log.Logger, prometheus.WrapRegistererWithPrefix("cortex_", t.Registerer))
	t.IngesterPartitionInstanceRing = ring.NewPartitionInstanceRing(t.IngesterPartitionRingWatcher, t.IngesterRing, t.Cfg.Ingester.IngesterRing.HeartbeatTimeout)

	// Expose a web page to view the partitions ring state.
	t.API.RegisterIngesterPartitionRing(ring.NewPartitionRingPageHandler(t.IngesterPartitionRingWatcher, ring.NewPartitionRingEditor(ingester.PartitionRingKey, kvClient)))

	return t.IngesterPartitionRingWatcher, nil
}

func (t *Mimir) initRuntimeConfig() (services.Service, error) {
	if len(t.Cfg.RuntimeConfig.LoadPath) == 0 {
		// no need to initialize module if load path is empty
		return nil, nil
	}

	serv, err := NewRuntimeManager(&t.Cfg, "mimir-runtime-config", prometheus.WrapRegistererWithPrefix("cortex_", t.Registerer), util_log.Logger)
	if err == nil {
		// TenantLimits just delegates to RuntimeConfig and doesn't have any state or need to do
		// anything in the start/stopping phase. Thus we can create it as part of runtime config
		// setup without any service instance of its own.
		t.TenantLimits = newTenantLimits(serv)
	}

	t.RuntimeConfig = serv
	t.API.RegisterRuntimeConfig(runtimeConfigHandler(t.RuntimeConfig, t.Cfg.LimitsConfig), validation.UserLimitsHandler(t.Cfg.LimitsConfig, t.TenantLimits))

	// Update config fields using runtime config. Only if multiKV is used for given ring these returned functions will be
	// called and register the listener.
	//
	// By doing the initialization here instead of per-module init function, we avoid the problem
	// of projects based on Mimir forgetting the wiring if they override module's init method (they also don't have access to private symbols).
	t.Cfg.Alertmanager.ShardingRing.Common.KVStore.Multi.ConfigProvider = multiClientRuntimeConfigChannel(t.RuntimeConfig)
	t.Cfg.Compactor.ShardingRing.Common.KVStore.Multi.ConfigProvider = multiClientRuntimeConfigChannel(t.RuntimeConfig)
	t.Cfg.Distributor.DistributorRing.Common.KVStore.Multi.ConfigProvider = multiClientRuntimeConfigChannel(t.RuntimeConfig)
	t.Cfg.Ingester.IngesterRing.KVStore.Multi.ConfigProvider = multiClientRuntimeConfigChannel(t.RuntimeConfig)
	t.Cfg.Ingester.IngesterPartitionRing.KVStore.Multi.ConfigProvider = multiClientRuntimeConfigChannel(t.RuntimeConfig)
	t.Cfg.Ruler.Ring.Common.KVStore.Multi.ConfigProvider = multiClientRuntimeConfigChannel(t.RuntimeConfig)
	t.Cfg.StoreGateway.ShardingRing.KVStore.Multi.ConfigProvider = multiClientRuntimeConfigChannel(t.RuntimeConfig)
	t.Cfg.QueryScheduler.ServiceDiscovery.SchedulerRing.KVStore.Multi.ConfigProvider = multiClientRuntimeConfigChannel(t.RuntimeConfig)
	t.Cfg.OverridesExporter.Ring.Common.KVStore.Multi.ConfigProvider = multiClientRuntimeConfigChannel(t.RuntimeConfig)

	return serv, err
}

func (t *Mimir) initOverrides() (serv services.Service, err error) {
	t.Overrides, err = validation.NewOverrides(t.Cfg.LimitsConfig, t.TenantLimits)
	// overrides don't have operational state, nor do they need to do anything more in starting/stopping phase,
	// so there is no need to return any service.
	return nil, err
}

func (t *Mimir) initOverridesExporter() (services.Service, error) {
	t.Cfg.OverridesExporter.Ring.Common.ListenPort = t.Cfg.Server.GRPCListenPort

	overridesExporter, err := exporter.NewOverridesExporter(
		t.Cfg.OverridesExporter,
		&t.Cfg.LimitsConfig,
		t.TenantLimits,
		util_log.Logger,
		t.Registerer,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to instantiate overrides-exporter")
	}
	if t.Registerer != nil {
		t.Registerer.MustRegister(overridesExporter)
	}

	t.API.RegisterOverridesExporter(overridesExporter)

	return overridesExporter, nil
}

func (t *Mimir) initDistributorService() (serv services.Service, err error) {
	t.Cfg.Distributor.DistributorRing.Common.ListenPort = t.Cfg.Server.GRPCListenPort
	t.Cfg.Distributor.InstanceLimitsFn = distributorInstanceLimits(t.RuntimeConfig)

	if t.Cfg.Querier.ShuffleShardingIngestersEnabled {
		t.Cfg.Distributor.ShuffleShardingLookbackPeriod = t.Cfg.BlocksStorage.TSDB.Retention
	}

	// Check whether the distributor can join the distributors ring, which is
	// whenever it's not running as an internal dependency (ie. querier or
	// ruler's dependency)
	canJoinDistributorsRing := t.Cfg.isAnyModuleEnabled(Distributor, Write, All)

	t.Cfg.Distributor.StreamingChunksPerIngesterSeriesBufferSize = t.Cfg.Querier.StreamingChunksPerIngesterSeriesBufferSize
	t.Cfg.Distributor.MinimizeIngesterRequests = t.Cfg.Querier.MinimizeIngesterRequests
	t.Cfg.Distributor.MinimiseIngesterRequestsHedgingDelay = t.Cfg.Querier.MinimiseIngesterRequestsHedgingDelay
	t.Cfg.Distributor.PreferAvailabilityZone = t.Cfg.Querier.PreferAvailabilityZone
	t.Cfg.Distributor.IngestStorageConfig = t.Cfg.IngestStorage

	t.Distributor, err = distributor.New(t.Cfg.Distributor, t.Cfg.IngesterClient, t.Overrides,
		t.ActiveGroupsCleanup, t.CostAttributionManager, t.IngesterRing, t.IngesterPartitionInstanceRing,
		canJoinDistributorsRing, t.Registerer, util_log.Logger)
	if err != nil {
		return
	}

	if t.ActiveGroupsCleanup != nil {
		t.ActiveGroupsCleanup.Register(t.Distributor)
	}

	return t.Distributor, nil
}

func (t *Mimir) initDistributor() (serv services.Service, err error) {
	t.API.RegisterDistributor(t.Distributor, t.Cfg.Distributor, t.Registerer, t.Overrides)

	return nil, nil
}

// initQueryable instantiates the queryable and promQL engine used to service queries to
// Mimir. It also registers the API endpoints associated with those two services.
func (t *Mimir) initQueryable() (serv services.Service, err error) {
	registerer := prometheus.WrapRegistererWith(querierEngine, t.Registerer)

	if t.Cfg.Querier.FilterQueryablesEnabled {
		t.Server.HTTP.Use(querier.FilterQueryablesMiddleware().Wrap)
	}

	// Create a querier queryable and PromQL engine
	t.QuerierQueryable, t.ExemplarQueryable, t.QuerierEngine, err = querier.New(
		t.Cfg.Querier, t.Overrides, t.Distributor, t.AdditionalStorageQueryables, registerer, util_log.Logger, t.ActivityTracker,
	)
	if err != nil {
		return nil, fmt.Errorf("could not create queryable: %w", err)
	}

	// Use the distributor to return metric metadata by default
	t.MetadataSupplier = t.Distributor

	// Register the default endpoints that are always enabled for the querier module
	t.API.RegisterQueryable(t.Distributor)

	return nil, nil
}

// Enable merge querier if multi tenant query federation is enabled
func (t *Mimir) initTenantFederation() (serv services.Service, err error) {
	if t.Cfg.TenantFederation.Enabled {
		// Make sure the mergeQuerier is only used for request with more than a
		// single tenant. This allows for a less impactful enabling of tenant
		// federation.
		const bypassForSingleQuerier = true

		// Make sure we use the "engine" label for queryables we create here just
		// like the non-federated queryables above. This differentiates between querier
		// and ruler metrics and prevents duplicate registration.
		registerer := prometheus.WrapRegistererWith(querierEngine, t.Registerer)

		t.QuerierQueryable = querier.NewSampleAndChunkQueryable(tenantfederation.NewQueryable(t.QuerierQueryable, bypassForSingleQuerier, t.Cfg.TenantFederation.MaxConcurrent, registerer, util_log.Logger))
		t.ExemplarQueryable = tenantfederation.NewExemplarQueryable(t.ExemplarQueryable, bypassForSingleQuerier, t.Cfg.TenantFederation.MaxConcurrent, registerer, util_log.Logger)
		t.MetadataSupplier = tenantfederation.NewMetadataSupplier(t.MetadataSupplier, t.Cfg.TenantFederation.MaxConcurrent, util_log.Logger)
	}
	return nil, nil
}

// initQuerier registers an internal HTTP router with a Prometheus API backed by the
// Mimir Queryable. Then it does one of the following:
//
//  1. Query-Frontend Enabled: If Mimir has an All or QueryFrontend target, the internal
//     HTTP router is wrapped with Tenant ID parsing middleware and passed to the frontend
//     worker.
//
//  2. Querier Standalone: The querier will register the internal HTTP router with the external
//     HTTP router for the Prometheus API routes. Then the external HTTP server will be passed
//     as a http.Handler to the frontend worker.
//
// Route Diagram:
//
//	                      │  query
//	                      │ request
//	                      │
//	                      ▼
//	            ┌──────────────────┐    QF to      ┌──────────────────┐
//	            │  external HTTP   │    Worker     │                  │
//	            │      router      │──────────────▶│ frontend worker  │
//	            │                  │               │                  │
//	            └──────────────────┘               └──────────────────┘
//	                      │                                  │
//	                                                         │
//	             only in  │                                  │
//	          microservice         ┌──────────────────┐      │
//	            querier   │        │ internal Querier │      │
//	                       ─ ─ ─ ─▶│      router      │◀─────┘
//	                               │                  │
//	                               └──────────────────┘
//	                                         │
//	                                         │
//	/metadata & /chunk ┌─────────────────────┼─────────────────────┐
//	      requests     │                     │                     │
//	                   │                     │                     │
//	                   ▼                     ▼                     ▼
//	         ┌──────────────────┐  ┌──────────────────┐  ┌────────────────────┐
//	         │                  │  │                  │  │                    │
//	         │Querier Queryable │  │  /api/v1 router  │  │ /prometheus router │
//	         │                  │  │                  │  │                    │
//	         └──────────────────┘  └──────────────────┘  └────────────────────┘
//	                   ▲                     │                     │
//	                   │                     └──────────┬──────────┘
//	                   │                                ▼
//	                   │                      ┌──────────────────┐
//	                   │                      │                  │
//	                   └──────────────────────│  Prometheus API  │
//	                                          │                  │
//	                                          └──────────────────┘
func (t *Mimir) initQuerier() (serv services.Service, err error) {
	t.Cfg.Worker.MaxConcurrentRequests = t.Cfg.Querier.EngineConfig.MaxConcurrent
	t.Cfg.Worker.QuerySchedulerDiscovery = t.Cfg.QueryScheduler.ServiceDiscovery

	// Create an internal HTTP handler that is configured with the Prometheus API routes and points
	// to a Prometheus API struct instantiated with the Mimir Queryable.
	internalQuerierRouter := api.NewQuerierHandler(
		t.Cfg.API,
		t.QuerierQueryable,
		t.ExemplarQueryable,
		t.MetadataSupplier,
		t.QuerierEngine,
		t.Distributor,
		t.Registerer,
		util_log.Logger,
		t.Overrides,
	)

	// If the querier is running standalone without the query-frontend or query-scheduler, we must register it's internal
	// HTTP handler externally and provide the external Mimir Server HTTP handler to the frontend worker
	// to ensure requests it processes use the default middleware instrumentation.
	if !t.Cfg.isAnyModuleEnabled(QueryFrontend, QueryScheduler, Read, All) {
		// First, register the internal querier handler with the external HTTP server
		t.API.RegisterQueryAPI(internalQuerierRouter, t.BuildInfoHandler)

		// Second, set the http.Handler that the frontend worker will use to process requests to point to
		// the external HTTP server. This will allow the querier to consolidate query metrics both external
		// and internal using the default instrumentation when running as a standalone service.
		internalQuerierRouter = t.Server.HTTPServer.Handler
	} else {
		// Monolithic mode requires a query-frontend endpoint for the worker. If no frontend and scheduler endpoint
		// is configured, Mimir will default to using frontend on localhost on it's own gRPC listening port.
		if !t.Cfg.Worker.IsFrontendOrSchedulerConfigured() {
			address := fmt.Sprintf("127.0.0.1:%d", t.Cfg.Server.GRPCListenPort)
			level.Info(util_log.Logger).Log("msg", "The querier worker has not been configured with either the query-frontend or query-scheduler address. Because Mimir is running in monolithic mode, it's attempting an automatic worker configuration. If queries are unresponsive, consider explicitly configuring the query-frontend or query-scheduler address for querier worker.", "address", address)
			t.Cfg.Worker.FrontendAddress = address
		}

		// Add a middleware to extract the trace context and add a header.
		internalQuerierRouter = nethttp.MiddlewareFunc(opentracing.GlobalTracer(), internalQuerierRouter.ServeHTTP, nethttp.OperationNameFunc(func(*http.Request) string {
			return "internalQuerier"
		}))

		// If queries are processed using the external HTTP Server, we need wrap the internal querier with
		// HTTP router with middleware to parse the tenant ID from the HTTP header and inject it into the
		// request context.
		internalQuerierRouter = t.API.AuthMiddleware.Wrap(internalQuerierRouter)
	}

	// If neither query-frontend nor query-scheduler is in use, then no worker is needed.
	if !t.Cfg.Worker.IsFrontendOrSchedulerConfigured() {
		return nil, nil
	}

	return querier_worker.NewQuerierWorker(t.Cfg.Worker, httpgrpc_server.NewServer(internalQuerierRouter, httpgrpc_server.WithReturn4XXErrors), util_log.Logger, t.Registerer)
}

func (t *Mimir) initStoreQueryable() (services.Service, error) {
	q, err := querier.NewBlocksStoreQueryableFromConfig(
		t.Cfg.Querier, t.Cfg.StoreGateway, t.Cfg.BlocksStorage, t.Overrides, util_log.Logger, t.Registerer,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize block store queryable: %v", err)
	}
	t.AdditionalStorageQueryables = append(t.AdditionalStorageQueryables, querier.NewStoreGatewayTimeRangeQueryable(q, t.Cfg.Querier))
	return q, nil
}

func (t *Mimir) initActiveGroupsCleanupService() (services.Service, error) {
	t.ActiveGroupsCleanup = util.NewActiveGroupsCleanupService(3*time.Minute, t.Cfg.Ingester.ActiveSeriesMetrics.IdleTimeout, t.Cfg.MaxSeparateMetricsGroupsPerUser)
	return t.ActiveGroupsCleanup, nil
}

func (t *Mimir) initCostAttributionService() (services.Service, error) {
	// The cost attribution service is only initilized if the custom registry path is provided.
	if t.Cfg.CostAttributionRegistryPath != "" {
		reg := prometheus.NewRegistry()
		var err error
		t.CostAttributionManager, err = costattribution.NewManager(t.Cfg.CostAttributionCleanupInterval, t.Cfg.CostAttributionEvictionInterval, util_log.Logger, t.Overrides, reg)
		t.API.RegisterCostAttribution(t.Cfg.CostAttributionRegistryPath, reg)
		return t.CostAttributionManager, err
	}
	return nil, nil
}

func (t *Mimir) tsdbIngesterConfig() {
	t.Cfg.Ingester.BlocksStorageConfig = t.Cfg.BlocksStorage
}

func (t *Mimir) initIngesterService() (serv services.Service, err error) {
	t.Cfg.Ingester.IngesterRing.ListenPort = t.Cfg.Server.GRPCListenPort
	t.Cfg.Ingester.IngesterRing.HideTokensInStatusPage = t.Cfg.IngestStorage.Enabled
	t.Cfg.Ingester.StreamTypeFn = ingesterChunkStreaming(t.RuntimeConfig)
	t.Cfg.Ingester.InstanceLimitsFn = ingesterInstanceLimits(t.RuntimeConfig)
	t.Cfg.Ingester.IngestStorageConfig = t.Cfg.IngestStorage
	t.tsdbIngesterConfig()

	t.Ingester, err = ingester.New(t.Cfg.Ingester, t.Overrides, t.IngesterRing, t.IngesterPartitionRingWatcher, t.ActiveGroupsCleanup, t.CostAttributionManager, t.Registerer, util_log.Logger)
	if err != nil {
		return
	}

	if t.ActiveGroupsCleanup != nil {
		t.ActiveGroupsCleanup.Register(t.Ingester)
	}

	return t.Ingester, nil
}

func (t *Mimir) initIngester() (serv services.Service, err error) {
	var ing api.Ingester

	ing = t.Ingester
	if t.ActivityTracker != nil {
		ing = ingester.NewIngesterActivityTracker(t.Ingester, t.ActivityTracker)
	}
	t.API.RegisterIngester(ing)
	return nil, nil
}

func (t *Mimir) initFlusher() (serv services.Service, err error) {
	t.tsdbIngesterConfig()

	t.Flusher, err = flusher.New(
		t.Cfg.Flusher,
		t.Cfg.Ingester,
		t.Overrides,
		t.Registerer,
		util_log.Logger,
	)
	if err != nil {
		return
	}

	return t.Flusher, nil
}

// initQueryFrontendCodec initializes query frontend codec.
// NOTE: Grafana Enterprise Metrics depends on this.
func (t *Mimir) initQueryFrontendCodec() (services.Service, error) {
	t.QueryFrontendCodec = querymiddleware.NewPrometheusCodec(t.Registerer, t.Cfg.Querier.EngineConfig.LookbackDelta, t.Cfg.Frontend.QueryMiddleware.QueryResultResponseFormat, t.Cfg.Frontend.QueryMiddleware.ExtraPropagateHeaders)
	return nil, nil
}

// initQueryFrontendTopicOffsetsReaders instantiates the topic offsets reader used by the query-frontend
// when the ingest storage is enabled.
func (t *Mimir) initQueryFrontendTopicOffsetsReaders() (services.Service, error) {
	if !t.Cfg.IngestStorage.Enabled {
		return nil, nil
	}

	var err error

	kafkaMetrics := ingest.NewKafkaReaderClientMetrics(ingest.ReaderMetricsPrefix, "query-frontend", t.Registerer)
	kafkaClient, err := ingest.NewKafkaReaderClient(t.Cfg.IngestStorage.KafkaConfig, kafkaMetrics, util_log.Logger)
	if err != nil {
		return nil, err
	}

	// The Kafka partitions may have been pre-provisioned. There are may be much more existing partitions in Kafka
	// than the actual number we use. To improve performance, we only look up the actual partitions
	// we're currently using in Mimir. We include all partition states because ACTIVE and INACTIVE partitions
	// must be queried, and PENDING partitions may switch to ACTIVE between when the query-frontend fetch the offsets
	// and the querier builds the replicaset of partitions to query.
	getPartitionIDs := func(_ context.Context) ([]int32, error) {
		return t.IngesterPartitionRingWatcher.PartitionRing().PartitionIDs(), nil
	}

	ingestTopicOffsetsReader := ingest.NewTopicOffsetsReader(kafkaClient, t.Cfg.IngestStorage.KafkaConfig.Topic, getPartitionIDs, t.Cfg.IngestStorage.KafkaConfig.LastProducedOffsetPollInterval, t.Registerer, util_log.Logger)

	if t.QueryFrontendTopicOffsetsReaders == nil {
		t.QueryFrontendTopicOffsetsReaders = make(map[string]*ingest.TopicOffsetsReader)
	}
	t.QueryFrontendTopicOffsetsReaders[querierapi.ReadConsistencyOffsetsHeader] = ingestTopicOffsetsReader

	return ingestTopicOffsetsReader, nil
}

// initQueryFrontendTripperware instantiates the tripperware used by the query frontend
// to optimize Prometheus query requests.
func (t *Mimir) initQueryFrontendTripperware() (serv services.Service, err error) {
	promqlEngineRegisterer := prometheus.WrapRegistererWith(prometheus.Labels{"engine": "query-frontend"}, t.Registerer)

	engineOpts, _, engineExperimentalFunctionsEnabled := engine.NewPromQLEngineOptions(t.Cfg.Querier.EngineConfig, t.ActivityTracker, util_log.Logger, promqlEngineRegisterer)

	tripperware, err := querymiddleware.NewTripperware(
		t.Cfg.Frontend.QueryMiddleware,
		util_log.Logger,
		t.Overrides,
		t.QueryFrontendCodec,
		querymiddleware.PrometheusResponseExtractor{},
		engineOpts,
		engineExperimentalFunctionsEnabled,
		t.QueryFrontendTopicOffsetsReaders,
		t.Registerer,
	)
	if err != nil {
		return nil, err
	}

	t.QueryFrontendTripperware = tripperware
	return nil, nil
}

func (t *Mimir) initQueryFrontend() (serv services.Service, err error) {
	t.Cfg.Frontend.FrontendV2.QuerySchedulerDiscovery = t.Cfg.QueryScheduler.ServiceDiscovery
	t.Cfg.Frontend.FrontendV2.LookBackDelta = t.Cfg.Querier.EngineConfig.LookbackDelta
	t.Cfg.Frontend.FrontendV2.QueryStoreAfter = t.Cfg.Querier.QueryStoreAfter

	roundTripper, frontendV1, frontendV2, err := frontend.InitFrontend(
		t.Cfg.Frontend,
		t.Overrides,
		t.Overrides,
		t.Cfg.Server.GRPCListenPort,
		util_log.Logger,
		t.Registerer,
		t.QueryFrontendCodec,
	)
	if err != nil {
		return nil, err
	}

	var frontendSvc services.Service
	if frontendV1 != nil {
		t.API.RegisterQueryFrontend1(frontendV1)
		t.FrontendV1 = frontendV1
		frontendSvc = frontendV1
	} else if frontendV2 != nil {
		t.API.RegisterQueryFrontend2(frontendV2)
		frontendSvc = frontendV2
	}

	// Wrap roundtripper into Tripperware and then wrap this with the roundtripper that checks
	// that the frontend is ready to receive requests when running v1 or v2 of the query-frontend,
	// i.e. not using the "downstream-url" feature.
	roundTripper = t.QueryFrontendTripperware(roundTripper)
	if frontendSvc != nil {
		roundTripper = querymiddleware.NewFrontendRunningRoundTripper(roundTripper, frontendSvc, t.Cfg.Frontend.QueryMiddleware.NotRunningTimeout, util_log.Logger)
	}

	handler := transport.NewHandler(t.Cfg.Frontend.Handler, roundTripper, util_log.Logger, t.Registerer, t.ActivityTracker)
	t.API.RegisterQueryFrontendHandler(handler, t.BuildInfoHandler)

	w := services.NewFailureWatcher()
	return services.NewBasicService(func(_ context.Context) error {
		if frontendSvc != nil {
			w.WatchService(frontendSvc)
			// Note that we pass an independent context to the service, since we want to
			// delay stopping it until in-flight requests are waited on.
			return services.StartAndAwaitRunning(context.Background(), frontendSvc)
		}
		return nil
	}, func(serviceContext context.Context) error {
		select {
		case <-serviceContext.Done():
			return nil
		case err := <-w.Chan():
			return err
		}
	}, func(_ error) error {
		handler.Stop()

		if frontendSvc != nil {
			return services.StopAndAwaitTerminated(context.Background(), frontendSvc)
		}
		return nil
	}), nil
}

func (t *Mimir) initRulerStorage() (serv services.Service, err error) {
	// If the ruler is not configured and Mimir is running in monolithic or read-write mode, then we just skip starting the ruler.
	if t.Cfg.isAnyModuleEnabled(Backend, All) && t.Cfg.RulerStorage.IsDefaults() {
		level.Info(util_log.Logger).Log("msg", "The ruler is not being started because you need to configure the ruler storage.")
		return
	}

	// For any ruler operation that supports reading stale data for a short period,
	// we do accept stale data for about a polling interval (2 intervals in the worst
	// case scenario due to the jitter applied).
	cacheTTL := t.Cfg.Ruler.PollInterval
	t.RulerStorage, err = ruler.NewRuleStore(context.Background(), t.Cfg.RulerStorage, t.Overrides, rules.FileLoader{}, cacheTTL, util_log.Logger, t.Registerer)
	return
}

func (t *Mimir) initRuler() (serv services.Service, err error) {
	if t.RulerStorage == nil {
		level.Info(util_log.Logger).Log("msg", "The ruler storage has not been configured. Not starting the ruler.")
		return nil, nil
	}

	t.Cfg.Ruler.Ring.Common.ListenPort = t.Cfg.Server.GRPCListenPort

	var embeddedQueryable prom_storage.Queryable
	var queryFunc rules.QueryFunc

	if t.Cfg.Ruler.QueryFrontend.Address != "" {
		queryFrontendClient, err := ruler.DialQueryFrontend(t.Cfg.Ruler.QueryFrontend)
		if err != nil {
			return nil, err
		}
		remoteQuerier := ruler.NewRemoteQuerier(queryFrontendClient, t.Cfg.Querier.EngineConfig.Timeout, t.Cfg.Ruler.QueryFrontend.MaxRetriesRate, t.Cfg.Ruler.QueryFrontend.QueryResultResponseFormat, t.Cfg.API.PrometheusHTTPPrefix, util_log.Logger, ruler.WithOrgIDMiddleware)

		embeddedQueryable = prom_remote.NewSampleAndChunkQueryableClient(
			remoteQuerier,
			labels.Labels{},
			nil,
			true,
			func() (int64, error) { return 0, nil },
		)
		queryFunc = remoteQuerier.Query
	} else {
		var queryable, federatedQueryable prom_storage.Queryable

		// TODO: Consider wrapping logger to differentiate from querier module logger
		rulerRegisterer := prometheus.WrapRegistererWith(rulerEngine, t.Registerer)

		queryable, _, eng, err := querier.New(t.Cfg.Querier, t.Overrides, t.Distributor, t.AdditionalStorageQueryables, rulerRegisterer, util_log.Logger, t.ActivityTracker)
		if err != nil {
			return nil, fmt.Errorf("could not create queryable for ruler: %w", err)
		}

		queryable = querier.NewErrorTranslateQueryableWithFn(queryable, ruler.WrapQueryableErrors)

		if t.Cfg.Ruler.TenantFederation.Enabled {
			if !t.Cfg.TenantFederation.Enabled {
				return nil, errors.New("-" + ruler.TenantFederationFlag + "=true requires -tenant-federation.enabled=true")
			}
			// Setting bypassForSingleQuerier=false forces `tenantfederation.NewQueryable` to add
			// the `__tenant_id__` label on all metrics regardless if they're for a single tenant or multiple tenants.
			// This makes this label more consistent and hopefully less confusing to users.
			const bypassForSingleQuerier = false

			federatedQueryable = tenantfederation.NewQueryable(queryable, bypassForSingleQuerier, t.Cfg.TenantFederation.MaxConcurrent, rulerRegisterer, util_log.Logger)

			regularQueryFunc := rules.EngineQueryFunc(eng, queryable)
			federatedQueryFunc := rules.EngineQueryFunc(eng, federatedQueryable)

			embeddedQueryable = federatedQueryable
			queryFunc = ruler.TenantFederationQueryFunc(regularQueryFunc, federatedQueryFunc)

		} else {
			embeddedQueryable = queryable
			queryFunc = rules.EngineQueryFunc(eng, queryable)
		}
	}

	var concurrencyController ruler.MultiTenantRuleConcurrencyController
	concurrencyController = &ruler.NoopMultiTenantConcurrencyController{}
	if t.Cfg.Ruler.MaxIndependentRuleEvaluationConcurrency > 0 {
		concurrencyController = ruler.NewMultiTenantConcurrencyController(
			util_log.Logger,
			t.Cfg.Ruler.MaxIndependentRuleEvaluationConcurrency,
			t.Cfg.Ruler.IndependentRuleEvaluationConcurrencyMinDurationPercentage,
			t.Registerer,
			t.Overrides,
		)
	}
	managerFactory := ruler.DefaultTenantManagerFactory(
		t.Cfg.Ruler,
		t.Distributor,
		embeddedQueryable,
		queryFunc,
		concurrencyController,
		t.Overrides,
		t.Registerer,
	)

	// We need to prefix and add a label to the metrics for the DNS resolver because, unlike other mimir components,
	// it doesn't already have the `cortex_` prefix and the `component` label to the metrics it emits
	dnsProviderReg := prometheus.WrapRegistererWithPrefix(
		"cortex_",
		prometheus.WrapRegistererWith(
			prometheus.Labels{"component": "ruler"},
			t.Registerer,
		),
	)

	dnsResolver := dns.NewProvider(util_log.Logger, dnsProviderReg, dns.GolangResolverType)
	manager, err := ruler.NewDefaultMultiTenantManager(t.Cfg.Ruler, managerFactory, t.Registerer, util_log.Logger, dnsResolver)
	if err != nil {
		return nil, err
	}

	t.Ruler, err = ruler.NewRuler(
		t.Cfg.Ruler,
		manager,
		t.Registerer,
		util_log.Logger,
		t.RulerStorage,
		t.Overrides,
	)
	if err != nil {
		return
	}

	// Expose HTTP/GRPC admin endpoints for the Ruler service
	t.API.RegisterRuler(t.Ruler)

	// Expose HTTP configuration and prometheus-compatible Ruler APIs
	t.API.RegisterRulerAPI(ruler.NewAPI(t.Ruler, t.RulerStorage, util_log.Logger), t.Cfg.Ruler.EnableAPI, t.BuildInfoHandler)

	return t.Ruler, nil
}

func (t *Mimir) initAlertManager() (serv services.Service, err error) {
	mode := featurecontrol.FeatureClassicMode
	if t.Cfg.Alertmanager.UTF8StrictMode {
		level.Info(util_log.Logger).Log("msg", "Starting Alertmanager in UTF-8 strict mode")
		mode = featurecontrol.FeatureUTF8StrictMode
	} else {
		level.Info(util_log.Logger).Log("msg", "Starting Alertmanager in classic mode")
	}
	features, err := featurecontrol.NewFlags(util_log.Logger, mode)
	util_log.CheckFatal("initializing Alertmanager feature flags", err)

	compatLogger := log.NewNopLogger()
	if t.Cfg.Alertmanager.LogParsingLabelMatchers {
		compatLogger = util_log.Logger
	}
	compat.InitFromFlags(compatLogger, features)

	t.Cfg.Alertmanager.ShardingRing.Common.ListenPort = t.Cfg.Server.GRPCListenPort
	t.Cfg.Alertmanager.CheckExternalURL(t.Cfg.API.AlertmanagerHTTPPrefix, util_log.Logger)

	bCfg := bucketclient.BucketAlertStoreConfig{
		FetchGrafanaConfig: t.Cfg.Alertmanager.GrafanaAlertmanagerCompatibilityEnabled,
	}
	store, err := alertstore.NewAlertStore(context.Background(), t.Cfg.AlertmanagerStorage, t.Overrides, bCfg, util_log.Logger, t.Registerer)
	if err != nil {
		return
	}

	t.Alertmanager, err = alertmanager.NewMultitenantAlertmanager(&t.Cfg.Alertmanager, store, t.Overrides, features, util_log.Logger, t.Registerer)
	if err != nil {
		return
	}

	t.API.RegisterAlertmanager(t.Alertmanager, t.Cfg.Alertmanager.EnableAPI, t.Cfg.Alertmanager.GrafanaAlertmanagerCompatibilityEnabled, t.BuildInfoHandler)
	return t.Alertmanager, nil
}

func (t *Mimir) initCompactor() (serv services.Service, err error) {
	t.Cfg.Compactor.ShardingRing.Common.ListenPort = t.Cfg.Server.GRPCListenPort

	t.Compactor, err = compactor.NewMultitenantCompactor(t.Cfg.Compactor, t.Cfg.BlocksStorage, t.Overrides, util_log.Logger, t.Registerer)
	if err != nil {
		return
	}

	// Expose HTTP endpoints.
	t.API.RegisterCompactor(t.Compactor)
	return t.Compactor, nil
}

func (t *Mimir) initStoreGateway() (serv services.Service, err error) {
	t.Cfg.StoreGateway.ShardingRing.ListenPort = t.Cfg.Server.GRPCListenPort
	t.StoreGateway, err = storegateway.NewStoreGateway(t.Cfg.StoreGateway, t.Cfg.BlocksStorage, t.Overrides, util_log.Logger, t.Registerer, t.ActivityTracker)
	if err != nil {
		return nil, err
	}

	// Expose HTTP endpoints.
	t.API.RegisterStoreGateway(t.StoreGateway)

	return t.StoreGateway, nil
}

func (t *Mimir) initMemberlistKV() (services.Service, error) {
	// Append to the list of codecs instead of overwriting the value to allow third parties to inject their own codecs.
	t.Cfg.MemberlistKV.Codecs = append(t.Cfg.MemberlistKV.Codecs, ring.GetCodec())
	t.Cfg.MemberlistKV.Codecs = append(t.Cfg.MemberlistKV.Codecs, ring.GetPartitionRingCodec())
	t.Cfg.MemberlistKV.Codecs = append(t.Cfg.MemberlistKV.Codecs, distributor.GetReplicaDescCodec())

	dnsProviderReg := prometheus.WrapRegistererWithPrefix(
		"cortex_",
		prometheus.WrapRegistererWith(
			prometheus.Labels{"component": "memberlist"},
			t.Registerer,
		),
	)
	dnsProvider := dns.NewProvider(util_log.Logger, dnsProviderReg, dns.GolangResolverType)
	t.MemberlistKV = memberlist.NewKVInitService(&t.Cfg.MemberlistKV, util_log.Logger, dnsProvider, t.Registerer)
	t.API.RegisterMemberlistKV(t.Cfg.Server.PathPrefix, t.MemberlistKV)

	// Update the config.
	t.Cfg.Distributor.DistributorRing.Common.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV
	t.Cfg.Distributor.HATrackerConfig.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV
	t.Cfg.Ingester.IngesterRing.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV
	t.Cfg.Ingester.IngesterPartitionRing.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV
	t.Cfg.StoreGateway.ShardingRing.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV
	t.Cfg.Compactor.ShardingRing.Common.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV
	t.Cfg.Ruler.Ring.Common.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV
	t.Cfg.Alertmanager.ShardingRing.Common.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV
	t.Cfg.QueryScheduler.ServiceDiscovery.SchedulerRing.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV
	t.Cfg.OverridesExporter.Ring.Common.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV

	return t.MemberlistKV, nil
}

func (t *Mimir) initQueryScheduler() (services.Service, error) {
	t.Cfg.QueryScheduler.ServiceDiscovery.SchedulerRing.ListenPort = t.Cfg.Server.GRPCListenPort

	s, err := scheduler.NewScheduler(t.Cfg.QueryScheduler, t.Overrides, util_log.Logger, t.Registerer)
	if err != nil {
		return nil, errors.Wrap(err, "query-scheduler init")
	}

	t.API.RegisterQueryScheduler(s)
	return s, nil
}

func (t *Mimir) initUsageStats() (services.Service, error) {
	if !t.Cfg.UsageStats.Enabled {
		return nil, nil
	}

	// Since it requires the access to the blocks storage, we enable it only for components
	// accessing the blocks storage.
	if !t.Cfg.isAnyModuleEnabled(All, Write, Read, Backend, Ingester, Querier, StoreGateway, Compactor) {
		return nil, nil
	}

	bucketClient, err := bucket.NewClient(context.Background(), t.Cfg.BlocksStorage.Bucket, UsageStats, util_log.Logger, t.Registerer)
	if err != nil {
		return nil, errors.Wrapf(err, "create %s bucket client", UsageStats)
	}

	// Track anonymous usage statistics.
	usagestats.GetString("blocks_storage_backend").Set(t.Cfg.BlocksStorage.Bucket.Backend)
	usagestats.GetString("installation_mode").Set(t.Cfg.UsageStats.InstallationMode)

	t.UsageStatsReporter = usagestats.NewReporter(bucketClient, util_log.Logger, t.Registerer)
	return t.UsageStatsReporter, nil
}

func (t *Mimir) initBlockBuilder() (_ services.Service, err error) {
	t.Cfg.BlockBuilder.Kafka = t.Cfg.IngestStorage.KafkaConfig
	t.Cfg.BlockBuilder.BlocksStorage = t.Cfg.BlocksStorage
	t.BlockBuilder, err = blockbuilder.New(t.Cfg.BlockBuilder, util_log.Logger, t.Registerer, t.Overrides)
	if err != nil {
		return nil, errors.Wrap(err, "block-builder init")
	}
	return t.BlockBuilder, nil
}

func (t *Mimir) initBlockBuilderScheduler() (services.Service, error) {
	t.Cfg.BlockBuilderScheduler.Kafka = t.Cfg.IngestStorage.KafkaConfig

	s, err := blockbuilderscheduler.New(t.Cfg.BlockBuilderScheduler, util_log.Logger, t.Registerer)
	if err != nil {
		return nil, errors.Wrap(err, "block-builder-scheduler init")
	}
	t.BlockBuilderScheduler = s
	t.API.RegisterBlockBuilderScheduler(s)
	return s, nil
}

func (t *Mimir) initContinuousTest() (services.Service, error) {
	client, err := continuoustest.NewClient(t.Cfg.ContinuousTest.Client, util_log.Logger)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to initialize continuous-test client")
	}

	t.ContinuousTestManager = continuoustest.NewManager(t.Cfg.ContinuousTest.Manager, util_log.Logger)
	t.ContinuousTestManager.AddTest(continuoustest.NewWriteReadSeriesTest(t.Cfg.ContinuousTest.WriteReadSeriesTest, client, util_log.Logger, t.Registerer))

	return services.NewBasicService(nil, func(ctx context.Context) error {
		return t.ContinuousTestManager.Run(ctx)
	}, nil), nil
}

func (t *Mimir) setupModuleManager() error {
	mm := modules.NewManager(util_log.Logger)

	// Register all modules here.
	// RegisterModule(name string, initFn func()(services.Service, error))
	mm.RegisterModule(Server, t.initServer, modules.UserInvisibleModule)
	mm.RegisterModule(ActivityTracker, t.initActivityTracker, modules.UserInvisibleModule)
	mm.RegisterModule(SanityCheck, t.initSanityCheck, modules.UserInvisibleModule)
	mm.RegisterModule(API, t.initAPI, modules.UserInvisibleModule)
	mm.RegisterModule(RuntimeConfig, t.initRuntimeConfig, modules.UserInvisibleModule)
	mm.RegisterModule(MemberlistKV, t.initMemberlistKV)
	mm.RegisterModule(IngesterRing, t.initIngesterRing, modules.UserInvisibleModule)
	mm.RegisterModule(IngesterPartitionRing, t.initIngesterPartitionRing, modules.UserInvisibleModule)
	mm.RegisterModule(Overrides, t.initOverrides, modules.UserInvisibleModule)
	mm.RegisterModule(OverridesExporter, t.initOverridesExporter)
	mm.RegisterModule(ActiveGroupsCleanupService, t.initActiveGroupsCleanupService, modules.UserInvisibleModule)
	mm.RegisterModule(CostAttributionService, t.initCostAttributionService, modules.UserInvisibleModule)
	mm.RegisterModule(Distributor, t.initDistributor)
	mm.RegisterModule(DistributorService, t.initDistributorService, modules.UserInvisibleModule)
	mm.RegisterModule(Ingester, t.initIngester)
	mm.RegisterModule(IngesterService, t.initIngesterService, modules.UserInvisibleModule)
	mm.RegisterModule(Flusher, t.initFlusher)
	mm.RegisterModule(Queryable, t.initQueryable, modules.UserInvisibleModule)
	mm.RegisterModule(Querier, t.initQuerier)
	mm.RegisterModule(StoreQueryable, t.initStoreQueryable, modules.UserInvisibleModule)
	mm.RegisterModule(QueryFrontendCodec, t.initQueryFrontendCodec, modules.UserInvisibleModule)
	mm.RegisterModule(QueryFrontendTripperware, t.initQueryFrontendTripperware, modules.UserInvisibleModule)
	mm.RegisterModule(QueryFrontendTopicOffsetsReaders, t.initQueryFrontendTopicOffsetsReaders, modules.UserInvisibleModule)
	mm.RegisterModule(QueryFrontend, t.initQueryFrontend)
	mm.RegisterModule(RulerStorage, t.initRulerStorage, modules.UserInvisibleModule)
	mm.RegisterModule(Ruler, t.initRuler)
	mm.RegisterModule(AlertManager, t.initAlertManager)
	mm.RegisterModule(Compactor, t.initCompactor)
	mm.RegisterModule(StoreGateway, t.initStoreGateway)
	mm.RegisterModule(QueryScheduler, t.initQueryScheduler)
	mm.RegisterModule(TenantFederation, t.initTenantFederation, modules.UserInvisibleModule)
	mm.RegisterModule(UsageStats, t.initUsageStats, modules.UserInvisibleModule)
	mm.RegisterModule(BlockBuilder, t.initBlockBuilder)
	mm.RegisterModule(BlockBuilderScheduler, t.initBlockBuilderScheduler)
	mm.RegisterModule(ContinuousTest, t.initContinuousTest)
	mm.RegisterModule(Vault, t.initVault, modules.UserInvisibleModule)
	mm.RegisterModule(Write, nil)
	mm.RegisterModule(Read, nil)
	mm.RegisterModule(Backend, nil)
	mm.RegisterModule(All, nil)

	// Add dependencies
	deps := map[string][]string{
		Server:                           {ActivityTracker, SanityCheck, UsageStats},
		API:                              {Server},
		MemberlistKV:                     {API, Vault},
		RuntimeConfig:                    {API},
		IngesterRing:                     {API, RuntimeConfig, MemberlistKV, Vault},
		IngesterPartitionRing:            {MemberlistKV, IngesterRing, API},
		Overrides:                        {RuntimeConfig},
		OverridesExporter:                {Overrides, MemberlistKV, Vault},
		Distributor:                      {DistributorService, API, ActiveGroupsCleanupService, Vault},
		DistributorService:               {IngesterRing, IngesterPartitionRing, Overrides, Vault, CostAttributionService},
		CostAttributionService:           {API, Overrides},
		Ingester:                         {IngesterService, API, ActiveGroupsCleanupService, Vault},
		IngesterService:                  {IngesterRing, IngesterPartitionRing, Overrides, RuntimeConfig, MemberlistKV, CostAttributionService},
		Flusher:                          {Overrides, API},
		Queryable:                        {Overrides, DistributorService, IngesterRing, IngesterPartitionRing, API, StoreQueryable, MemberlistKV},
		Querier:                          {TenantFederation, Vault},
		StoreQueryable:                   {Overrides, MemberlistKV},
		QueryFrontendTripperware:         {API, Overrides, QueryFrontendCodec, QueryFrontendTopicOffsetsReaders},
		QueryFrontend:                    {QueryFrontendTripperware, MemberlistKV, Vault},
		QueryFrontendTopicOffsetsReaders: {IngesterPartitionRing},
		QueryScheduler:                   {API, Overrides, MemberlistKV, Vault},
		Ruler:                            {DistributorService, StoreQueryable, RulerStorage, Vault},
		RulerStorage:                     {Overrides},
		AlertManager:                     {API, MemberlistKV, Overrides, Vault},
		Compactor:                        {API, MemberlistKV, Overrides, Vault},
		StoreGateway:                     {API, Overrides, MemberlistKV, Vault},
		TenantFederation:                 {Queryable},
		BlockBuilder:                     {API, Overrides},
		BlockBuilderScheduler:            {API},
		ContinuousTest:                   {API},
		Write:                            {Distributor, Ingester},
		Read:                             {QueryFrontend, Querier},
		Backend:                          {QueryScheduler, Ruler, StoreGateway, Compactor, AlertManager, OverridesExporter},
		All:                              {QueryFrontend, Querier, Ingester, Distributor, StoreGateway, Ruler, Compactor},
	}
	for mod, targets := range deps {
		if err := mm.AddDependency(mod, targets...); err != nil {
			return err
		}
	}

	t.ModuleManager = mm

	return nil
}
