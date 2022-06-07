// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortex/cortex.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimir

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strconv"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/modules"
	"github.com/grafana/dskit/multierror"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/runtimeconfig"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/signals"
	"google.golang.org/grpc/health/grpc_health_v1"
	"gopkg.in/yaml.v2"

	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/alertmanager"
	"github.com/grafana/mimir/pkg/alertmanager/alertstore"
	alertstorelocal "github.com/grafana/mimir/pkg/alertmanager/alertstore/local"
	"github.com/grafana/mimir/pkg/api"
	"github.com/grafana/mimir/pkg/compactor"
	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/flusher"
	"github.com/grafana/mimir/pkg/frontend"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	frontendv1 "github.com/grafana/mimir/pkg/frontend/v1"
	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/querier/tenantfederation"
	querier_worker "github.com/grafana/mimir/pkg/querier/worker"
	"github.com/grafana/mimir/pkg/ruler"
	"github.com/grafana/mimir/pkg/ruler/rulestore"
	rulestorelocal "github.com/grafana/mimir/pkg/ruler/rulestore/local"
	"github.com/grafana/mimir/pkg/scheduler"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/activitytracker"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/noauth"
	"github.com/grafana/mimir/pkg/util/process"
	"github.com/grafana/mimir/pkg/util/validation"
)

var errInvalidBucketConfig = errors.New("invalid bucket config")

// The design pattern for Mimir is a series of config objects, which are
// registered for command line flags, and then a series of components that
// are instantiated and composed.  Some rules of thumb:
// - Config types should only contain 'simple' types (ints, strings, urls etc).
// - Flag validation should be done by the flag; use a flag.Value where
//   appropriate.
// - Config types should map 1:1 with a component type.
// - Config types should define flags with a common prefix.
// - It's fine to nest configs within configs, but this should match the
//   nesting of components within components.
// - Limit as much is possible sharing of configuration between config types.
//   Where necessary, use a pointer for this - avoid repetition.
// - Where a nesting of components its not obvious, it's fine to pass
//   references to other components constructors to compose them.
// - First argument for a components constructor should be its matching config
//   object.

// Config is the root config for Mimir.
type Config struct {
	Target              flagext.StringSliceCSV `yaml:"target"`
	MultitenancyEnabled bool                   `yaml:"multitenancy_enabled"`
	NoAuthTenant        string                 `yaml:"no_auth_tenant" category:"advanced"`
	PrintConfig         bool                   `yaml:"-"`
	ApplicationName     string                 `yaml:"-"`

	API              api.Config                      `yaml:"api"`
	Server           server.Config                   `yaml:"server"`
	Distributor      distributor.Config              `yaml:"distributor"`
	Querier          querier.Config                  `yaml:"querier"`
	IngesterClient   client.Config                   `yaml:"ingester_client"`
	Ingester         ingester.Config                 `yaml:"ingester"`
	Flusher          flusher.Config                  `yaml:"flusher"`
	LimitsConfig     validation.Limits               `yaml:"limits"`
	Worker           querier_worker.Config           `yaml:"frontend_worker"`
	Frontend         frontend.CombinedFrontendConfig `yaml:"frontend"`
	BlocksStorage    tsdb.BlocksStorageConfig        `yaml:"blocks_storage"`
	Compactor        compactor.Config                `yaml:"compactor"`
	StoreGateway     storegateway.Config             `yaml:"store_gateway"`
	TenantFederation tenantfederation.Config         `yaml:"tenant_federation"`
	ActivityTracker  activitytracker.Config          `yaml:"activity_tracker"`

	Ruler               ruler.Config                               `yaml:"ruler"`
	RulerStorage        rulestore.Config                           `yaml:"ruler_storage"`
	Alertmanager        alertmanager.MultitenantAlertmanagerConfig `yaml:"alertmanager"`
	AlertmanagerStorage alertstore.Config                          `yaml:"alertmanager_storage"`
	RuntimeConfig       runtimeconfig.Config                       `yaml:"runtime_config"`
	MemberlistKV        memberlist.KVConfig                        `yaml:"memberlist"`
	QueryScheduler      scheduler.Config                           `yaml:"query_scheduler"`
}

// RegisterFlags registers flag.
func (c *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	c.ApplicationName = "Grafana Mimir"
	c.Server.MetricsNamespace = "cortex"
	c.Server.ExcludeRequestInLog = true

	// Set the default module list to 'all'
	c.Target = []string{All}

	f.Var(&c.Target, "target", "Comma-separated list of components to include in the instantiated process. "+
		"The default value 'all' includes all components that are required to form a functional Grafana Mimir instance in single-binary mode. "+
		"Use the '-modules' command line flag to get a list of available components, and to see which components are included with 'all'.")

	f.BoolVar(&c.MultitenancyEnabled, "auth.multitenancy-enabled", true, "When set to true, incoming HTTP requests must specify tenant ID in HTTP X-Scope-OrgId header. When set to false, tenant ID from -auth.no-auth-tenant is used instead.")
	f.StringVar(&c.NoAuthTenant, "auth.no-auth-tenant", "anonymous", "Tenant ID to use when multitenancy is disabled.")
	f.BoolVar(&c.PrintConfig, "print.config", false, "Print the config and exit.")

	c.API.RegisterFlags(f)
	c.registerServerFlagsWithChangedDefaultValues(f)
	c.Distributor.RegisterFlags(f, logger)
	c.Querier.RegisterFlags(f)
	c.IngesterClient.RegisterFlags(f)
	c.Ingester.RegisterFlags(f, logger)
	c.Flusher.RegisterFlags(f)
	c.LimitsConfig.RegisterFlags(f)
	c.Worker.RegisterFlags(f)
	c.Frontend.RegisterFlags(f, logger)
	c.BlocksStorage.RegisterFlags(f)
	c.Compactor.RegisterFlags(f, logger)
	c.StoreGateway.RegisterFlags(f, logger)
	c.TenantFederation.RegisterFlags(f)

	c.Ruler.RegisterFlags(f, logger)
	c.RulerStorage.RegisterFlags(f)
	c.Alertmanager.RegisterFlags(f, logger)
	c.AlertmanagerStorage.RegisterFlags(f)
	c.RuntimeConfig.RegisterFlags(f)
	c.MemberlistKV.RegisterFlags(f)
	c.ActivityTracker.RegisterFlags(f)
	c.QueryScheduler.RegisterFlags(f)
}

// Validate the mimir config and return an error if the validation
// doesn't pass
func (c *Config) Validate(log log.Logger) error {
	if err := c.validateYAMLEmptyNodes(); err != nil {
		return err
	}

	if err := c.validateBucketConfigs(); err != nil {
		return fmt.Errorf("%w: %s", errInvalidBucketConfig, err)
	}
	if err := c.RulerStorage.Validate(); err != nil {
		return errors.Wrap(err, "invalid rulestore config")
	}
	if err := c.Ruler.Validate(c.LimitsConfig, log); err != nil {
		return errors.Wrap(err, "invalid ruler config")
	}
	if err := c.BlocksStorage.Validate(); err != nil {
		return errors.Wrap(err, "invalid TSDB config")
	}
	if err := c.Distributor.Validate(c.LimitsConfig); err != nil {
		return errors.Wrap(err, "invalid distributor config")
	}
	if err := c.Querier.Validate(); err != nil {
		return errors.Wrap(err, "invalid querier config")
	}
	if err := c.IngesterClient.Validate(log); err != nil {
		return errors.Wrap(err, "invalid ingester_client config")
	}
	if err := c.Worker.Validate(log); err != nil {
		return errors.Wrap(err, "invalid frontend_worker config")
	}
	if err := c.Frontend.QueryMiddleware.Validate(); err != nil {
		return errors.Wrap(err, "invalid query-frontend middleware config")
	}
	if err := c.StoreGateway.Validate(c.LimitsConfig); err != nil {
		return errors.Wrap(err, "invalid store-gateway config")
	}
	if err := c.Compactor.Validate(); err != nil {
		return errors.Wrap(err, "invalid compactor config")
	}
	if err := c.AlertmanagerStorage.Validate(); err != nil {
		return errors.Wrap(err, "invalid alertmanager storage config")
	}
	if c.isModuleEnabled(AlertManager) {
		if err := c.Alertmanager.Validate(c.AlertmanagerStorage); err != nil {
			return errors.Wrap(err, "invalid alertmanager config")
		}
	}
	return nil
}

func (c *Config) isModuleEnabled(m string) bool {
	return util.StringsContain(c.Target, m)
}

func (c *Config) isAnyModuleEnabled(modules ...string) bool {
	for _, m := range modules {
		if c.isModuleEnabled(m) {
			return true
		}
	}

	return false
}

// validateYAMLEmptyNodes ensure that no empty node has been specified in the YAML config file.
// When an empty node is defined in YAML, the YAML parser sets the whole struct to its zero value
// and so we loose all default values. It's very difficult to detect this case for the user, so we
// try to prevent it (on the root level) with this custom validation.
func (c *Config) validateYAMLEmptyNodes() error {
	defaults := Config{}
	flagext.DefaultValues(&defaults)

	defStruct := reflect.ValueOf(defaults)
	cfgStruct := reflect.ValueOf(*c)

	// We expect all structs are the exact same. This check should never fail.
	if cfgStruct.NumField() != defStruct.NumField() {
		return errors.New("unable to validate configuration because of mismatching internal config data structure")
	}

	for i := 0; i < cfgStruct.NumField(); i++ {
		// If the struct has been reset due to empty YAML value and the zero struct value
		// doesn't match the default one, then we should warn the user about the issue.
		if cfgStruct.Field(i).Kind() == reflect.Struct && cfgStruct.Field(i).IsZero() && !defStruct.Field(i).IsZero() {
			return fmt.Errorf("the %s configuration in YAML has been specified as an empty YAML node", cfgStruct.Type().Field(i).Name)
		}
	}

	return nil
}

func (c *Config) validateBucketConfigs() error {
	errs := multierror.New()

	// Validate alertmanager bucket config.
	if c.isAnyModuleEnabled(AlertManager) && c.AlertmanagerStorage.Backend != alertstorelocal.Name {
		errs.Add(errors.Wrap(validateBucketConfig(c.AlertmanagerStorage.Config, c.BlocksStorage.Bucket), "alertmanager storage"))
	}

	// Validate ruler bucket config.
	if c.isAnyModuleEnabled(All, Ruler) && c.RulerStorage.Backend != rulestorelocal.Name {
		errs.Add(errors.Wrap(validateBucketConfig(c.RulerStorage.Config, c.BlocksStorage.Bucket), "ruler storage"))
	}

	return errs.Err()
}

func validateBucketConfig(cfg bucket.Config, blockStorageBucketCfg bucket.Config) error {
	if cfg.Backend != blockStorageBucketCfg.Backend {
		return nil
	}

	if cfg.StoragePrefix != blockStorageBucketCfg.StoragePrefix {
		return nil
	}

	switch cfg.Backend {
	case bucket.S3:
		if cfg.S3.BucketName == blockStorageBucketCfg.S3.BucketName {
			return errors.New("S3 bucket name and storage prefix cannot be the same as the one used in blocks storage config")
		}

	case bucket.GCS:
		if cfg.GCS.BucketName == blockStorageBucketCfg.GCS.BucketName {
			return errors.New("GCS bucket name and storage prefix cannot be the same as the one used in blocks storage config")
		}

	case bucket.Azure:
		if cfg.Azure.ContainerName == blockStorageBucketCfg.Azure.ContainerName && cfg.Azure.StorageAccountName == blockStorageBucketCfg.Azure.StorageAccountName {
			return errors.New("Azure container and account names and storage prefix cannot be the same as the ones used in blocks storage config")
		}

	// To keep it simple here we only check that container and project names are not the same.
	// We could also verify both configuration endpoints to determine uniqueness,
	// however different auth URLs do not imply different clusters, since a single cluster
	// may have several configured endpoints.
	case bucket.Swift:
		if cfg.Swift.ContainerName == blockStorageBucketCfg.Swift.ContainerName && cfg.Swift.ProjectName == blockStorageBucketCfg.Swift.ProjectName {
			return errors.New("Swift container and project names and storage prefix cannot be the same as the ones used in blocks storage config")
		}
	}
	return nil
}

func (c *Config) registerServerFlagsWithChangedDefaultValues(fs *flag.FlagSet) {
	throwaway := flag.NewFlagSet("throwaway", flag.PanicOnError)

	// Register to throwaway flags first. Default values are remembered during registration and cannot be changed,
	// but we can take values from throwaway flag set and reregister into supplied flags with new default values.
	c.Server.RegisterFlags(throwaway)

	throwaway.VisitAll(func(f *flag.Flag) {
		// Ignore errors when setting new values. We have a test to verify that it works.
		switch f.Name {
		case "server.grpc.keepalive.min-time-between-pings":
			_ = f.Value.Set("10s")

		case "server.grpc.keepalive.ping-without-stream-allowed":
			_ = f.Value.Set("true")

		case "server.http-listen-port":
			_ = f.Value.Set("8080")

		case "server.grpc-max-recv-msg-size-bytes":
			_ = f.Value.Set(strconv.Itoa(100 * 1024 * 1024))

		case "server.grpc-max-send-msg-size-bytes":
			_ = f.Value.Set(strconv.Itoa(100 * 1024 * 1024))
		}

		fs.Var(f.Value, f.Name, f.Usage)
	})
}

// Mimir is the root datastructure for Mimir.
type Mimir struct {
	Cfg Config

	// set during initialization
	ServiceMap    map[string]services.Service
	ModuleManager *modules.Manager

	API                      *api.API
	Server                   *server.Server
	Ring                     *ring.Ring
	TenantLimits             validation.TenantLimits
	Overrides                *validation.Overrides
	Distributor              *distributor.Distributor
	Ingester                 *ingester.Ingester
	Flusher                  *flusher.Flusher
	Frontend                 *frontendv1.Frontend
	RuntimeConfig            *runtimeconfig.Manager
	QuerierQueryable         prom_storage.SampleAndChunkQueryable
	ExemplarQueryable        prom_storage.ExemplarQueryable
	QuerierEngine            *promql.Engine
	QueryFrontendTripperware querymiddleware.Tripperware
	Ruler                    *ruler.Ruler
	RulerStorage             rulestore.RuleStore
	Alertmanager             *alertmanager.MultitenantAlertmanager
	Compactor                *compactor.MultitenantCompactor
	StoreGateway             *storegateway.StoreGateway
	MemberlistKV             *memberlist.KVInitService
	ActivityTracker          *activitytracker.ActivityTracker
	BuildInfoHandler         http.Handler

	// Queryables that the querier should use to query the long term storage.
	StoreQueryables []querier.QueryableWithFilter
}

// New makes a new Mimir.
func New(cfg Config) (*Mimir, error) {
	if cfg.PrintConfig {
		if err := yaml.NewEncoder(os.Stdout).Encode(&cfg); err != nil {
			fmt.Println("Error encoding config:", err)
		}
		os.Exit(0)
	}

	// Swap out the default resolver to support multiple tenant IDs separated by a '|'
	if cfg.TenantFederation.Enabled {
		tenant.WithDefaultResolver(tenant.NewMultiResolver())

		if cfg.Ruler.TenantFederation.Enabled {
			util_log.WarnExperimentalUse("ruler.tenant-federation")
		}
	}

	cfg.API.HTTPAuthMiddleware = noauth.SetupAuthMiddleware(&cfg.Server, cfg.MultitenancyEnabled,
		// Also don't check auth for these gRPC methods, since single call is used for multiple users (or no user like health check).
		[]string{
			"/grpc.health.v1.Health/Check",
			"/frontend.Frontend/Process",
			"/frontend.Frontend/NotifyClientShutdown",
			"/schedulerpb.SchedulerForFrontend/FrontendLoop",
			"/schedulerpb.SchedulerForQuerier/QuerierLoop",
			"/schedulerpb.SchedulerForQuerier/NotifyQuerierShutdown",
		}, cfg.NoAuthTenant)

	mimir := &Mimir{
		Cfg: cfg,
	}

	mimir.setupThanosTracing()

	if err := mimir.setupModuleManager(); err != nil {
		return nil, err
	}

	return mimir, nil
}

// setupThanosTracing appends a gRPC middleware used to inject our tracer into the custom
// context used by Thanos, in order to get Thanos spans correctly attached to our traces.
func (t *Mimir) setupThanosTracing() {
	t.Cfg.Server.GRPCMiddleware = append(t.Cfg.Server.GRPCMiddleware, ThanosTracerUnaryInterceptor)
	t.Cfg.Server.GRPCStreamMiddleware = append(t.Cfg.Server.GRPCStreamMiddleware, ThanosTracerStreamInterceptor)
}

// Run starts Mimir running, and blocks until a Mimir stops.
func (t *Mimir) Run() error {
	// Register custom process metrics.
	if c, err := process.NewProcessCollector(); err == nil {
		prometheus.MustRegister(c)
	} else {
		level.Warn(util_log.Logger).Log("msg", "skipped registration of custom process metrics collector", "err", err)
	}

	for _, module := range t.Cfg.Target {
		if !t.ModuleManager.IsUserVisibleModule(module) {
			level.Warn(util_log.Logger).Log("msg", "selected target is an internal module, is this intended?", "target", module)
		}
	}

	var err error
	t.ServiceMap, err = t.ModuleManager.InitModuleServices(t.Cfg.Target...)
	if err != nil {
		return err
	}

	t.API.RegisterServiceMapHandler(http.HandlerFunc(t.servicesHandler))

	// register ingester ring handlers, if they exists prefer the full ring
	// implementation provided by module.Ring over the BasicLifecycler
	// available in ingesters
	if t.Ring != nil {
		t.API.RegisterRing(t.Ring)
	} else if t.Ingester != nil {
		t.API.RegisterRing(t.Ingester.RingHandler())
	}

	// get all services, create service manager and tell it to start
	servs := []services.Service(nil)
	for _, s := range t.ServiceMap {
		servs = append(servs, s)
	}

	sm, err := services.NewManager(servs...)
	if err != nil {
		return err
	}

	// before starting servers, register /ready handler and gRPC health check service.
	// It should reflect entire Mimir.
	t.Server.HTTP.Path("/ready").Handler(t.readyHandler(sm))
	grpc_health_v1.RegisterHealthServer(t.Server.GRPC, grpcutil.NewHealthCheck(sm))

	// Let's listen for events from this manager, and log them.
	healthy := func() { level.Info(util_log.Logger).Log("msg", "Application started") }
	stopped := func() { level.Info(util_log.Logger).Log("msg", "Application stopped") }
	serviceFailed := func(service services.Service) {
		// if any service fails, stop entire Mimir
		sm.StopAsync()

		// let's find out which module failed
		for m, s := range t.ServiceMap {
			if s == service {
				if service.FailureCase() == modules.ErrStopProcess {
					level.Info(util_log.Logger).Log("msg", "received stop signal via return error", "module", m, "err", service.FailureCase())
				} else {
					level.Error(util_log.Logger).Log("msg", "module failed", "module", m, "err", service.FailureCase())
				}
				return
			}
		}

		level.Error(util_log.Logger).Log("msg", "module failed", "module", "unknown", "err", service.FailureCase())
	}

	sm.AddListener(services.NewManagerListener(healthy, stopped, serviceFailed))

	// Setup signal handler. If signal arrives, we stop the manager, which stops all the services.
	handler := signals.NewHandler(t.Server.Log)
	go func() {
		handler.Loop()
		sm.StopAsync()
	}()

	// Start all services. This can really only fail if some service is already
	// in other state than New, which should not be the case.
	err = sm.StartAsync(context.Background())
	if err == nil {
		// Wait until service manager stops. It can stop in two ways:
		// 1) Signal is received and manager is stopped.
		// 2) Any service fails.
		err = sm.AwaitStopped(context.Background())
	}

	// If there is no error yet (= service manager started and then stopped without problems),
	// but any service failed, report that failure as an error to caller.
	if err == nil {
		if failed := sm.ServicesByState()[services.Failed]; len(failed) > 0 {
			for _, f := range failed {
				if f.FailureCase() != modules.ErrStopProcess {
					// Details were reported via failure listener before
					err = errors.New("failed services")
					break
				}
			}
		}
	}
	return err
}

func (t *Mimir) readyHandler(sm *services.Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !sm.IsHealthy() {
			msg := bytes.Buffer{}
			msg.WriteString("Some services are not Running:\n")

			byState := sm.ServicesByState()
			for st, ls := range byState {
				msg.WriteString(fmt.Sprintf("%v: %d\n", st, len(ls)))
			}

			http.Error(w, msg.String(), http.StatusServiceUnavailable)
			return
		}

		// Ingester has a special check that makes sure that it was able to register into the ring,
		// and that all other ring entries are OK too.
		if t.Ingester != nil {
			if err := t.Ingester.CheckReady(r.Context()); err != nil {
				http.Error(w, "Ingester not ready: "+err.Error(), http.StatusServiceUnavailable)
				return
			}
		}

		// Query Frontend has a special check that makes sure that a querier is attached before it signals
		// itself as ready
		if t.Frontend != nil {
			if err := t.Frontend.CheckReady(r.Context()); err != nil {
				http.Error(w, "Query Frontend not ready: "+err.Error(), http.StatusServiceUnavailable)
				return
			}
		}

		util.WriteTextResponse(w, "ready")
	}
}
