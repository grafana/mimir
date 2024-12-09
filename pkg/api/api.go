// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/api/api.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package api

import (
	"flag"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/felixge/fgprof"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gorilla/mux"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/server"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/grafana/mimir/pkg/alertmanager"
	"github.com/grafana/mimir/pkg/alertmanager/alertmanagerpb"
	"github.com/grafana/mimir/pkg/compactor"
	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/distributor/distributorpb"
	frontendv1 "github.com/grafana/mimir/pkg/frontend/v1"
	"github.com/grafana/mimir/pkg/frontend/v1/frontendv1pb"
	frontendv2 "github.com/grafana/mimir/pkg/frontend/v2"
	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/querier"
	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/querier/tenantfederation"
	"github.com/grafana/mimir/pkg/ruler"
	"github.com/grafana/mimir/pkg/scheduler"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/util/gziphandler"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
	"github.com/grafana/mimir/pkg/util/validation/exporter"
)

type ConfigHandler func(actualCfg interface{}, defaultCfg interface{}) http.HandlerFunc

type Config struct {
	SkipLabelNameValidationHeader  bool `yaml:"skip_label_name_validation_header_enabled" category:"advanced"`
	SkipLabelCountValidationHeader bool `yaml:"skip_label_count_validation_header_enabled" category:"advanced"`

	// TODO: Remove option in Mimir 2.15.
	GETRequestForIngesterShutdownEnabled bool `yaml:"get_request_for_ingester_shutdown_enabled" category:"deprecated"`

	AlertmanagerHTTPPrefix string `yaml:"alertmanager_http_prefix" category:"advanced"`
	PrometheusHTTPPrefix   string `yaml:"prometheus_http_prefix" category:"advanced"`

	// The following configs are injected by the upstream caller.
	ServerPrefix       string               `yaml:"-"`
	HTTPAuthMiddleware middleware.Interface `yaml:"-"`

	// The CustomConfigHandler allows for providing a different handler for the
	// `/config` endpoint. If this field is set _before_ the API module is
	// initialized, the custom config handler will be used instead of
	// DefaultConfigHandler.
	CustomConfigHandler ConfigHandler `yaml:"-"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.SkipLabelNameValidationHeader, "api.skip-label-name-validation-header-enabled", false, "Allows to skip label name validation via X-Mimir-SkipLabelNameValidation header on the http write path. Use with caution as it breaks PromQL. Allowing this for external clients allows any client to send invalid label names. After enabling it, requests with a specific HTTP header set to true will not have label names validated.")
	f.BoolVar(&cfg.SkipLabelCountValidationHeader, "api.skip-label-count-validation-header-enabled", false, "Allows to disable enforcement of the label count limit \"max_label_names_per_series\" via X-Mimir-SkipLabelCountValidation header on the http write path. Allowing this for external clients allows any client to send invalid label counts. After enabling it, requests with a specific HTTP header set to true will not have label counts validated.")
	f.BoolVar(&cfg.GETRequestForIngesterShutdownEnabled, "api.get-request-for-ingester-shutdown-enabled", false, "Enable GET requests to the /ingester/shutdown endpoint to trigger an ingester shutdown. This is a potentially dangerous operation and should only be enabled consciously.")
	cfg.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet with the set prefix.
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.AlertmanagerHTTPPrefix, prefix+"http.alertmanager-http-prefix", "/alertmanager", "HTTP URL path under which the Alertmanager ui and api will be served.")
	f.StringVar(&cfg.PrometheusHTTPPrefix, prefix+"http.prometheus-http-prefix", "/prometheus", "HTTP URL path under which the Prometheus api will be served.")
}

type API struct {
	AuthMiddleware middleware.Interface

	cfg       Config
	server    *server.Server
	logger    log.Logger
	sourceIPs *middleware.SourceIPExtractor
	indexPage *IndexPageContent
}

func New(cfg Config, federationCfg tenantfederation.Config, serverCfg server.Config, s *server.Server, logger log.Logger) (*API, error) {
	// Ensure the encoded path is used. Required for the rules API
	s.HTTP.UseEncodedPath()

	var sourceIPs *middleware.SourceIPExtractor
	if serverCfg.LogSourceIPs {
		var err error
		sourceIPs, err = middleware.NewSourceIPs(serverCfg.LogSourceIPsHeader, serverCfg.LogSourceIPsRegex, serverCfg.LogSourceIPsFull)
		if err != nil {
			// This should have already been caught in the Server creation
			return nil, err
		}
	}

	api := &API{
		cfg:            cfg,
		AuthMiddleware: cfg.HTTPAuthMiddleware,
		server:         s,
		logger:         logger,
		sourceIPs:      sourceIPs,
		indexPage:      newIndexPageContent(),
	}

	// If no authentication middleware is present in the config, use the default authentication middleware.
	if api.AuthMiddleware == nil {
		api.AuthMiddleware = middleware.AuthenticateUser
	}

	// Unconditionally add middleware that ensures we only accept requests with an expected number of tenants
	// that is applied after any existing auth middleware has run. Only a single tenant is allowed when federation
	// is disabled. If federation is enabled, there is optionally a max number of tenants that is supported.
	api.AuthMiddleware = middleware.Merge(api.AuthMiddleware, newTenantValidationMiddleware(federationCfg.Enabled, federationCfg.MaxTenants))

	return api, nil
}

// RegisterDeprecatedRoute behaves in a similar way to RegisterRoute. RegisterDeprecatedRoute also logs warnings on
// invocations of the deprecated endpoints.
func (a *API) RegisterDeprecatedRoute(path string, handler http.Handler, auth, gzipEnabled bool, method string, methods ...string) {
	methods = append([]string{method}, methods...)
	handler = a.deprecatedHandler(handler)
	level.Debug(a.logger).Log("msg", "api: registering deprecated route", "methods", strings.Join(methods, ","), "path", path, "auth", auth, "gzip", gzipEnabled)
	a.newRoute(path, handler, false, auth, gzipEnabled, methods...)
}

func (a *API) deprecatedHandler(next http.Handler) http.Handler {
	l := util_log.NewRateLimitedLogger(time.Minute, a.logger, time.Now)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		level.Warn(l).Log("msg", "api: received a request on a deprecated endpoint", "path", r.URL.Path, "method", r.Method)
		next.ServeHTTP(w, r)
	})
}

// RegisterRoute registers a single route enforcing HTTP methods. A single
// route is expected to be specific about which HTTP methods are supported.
func (a *API) RegisterRoute(path string, handler http.Handler, auth, gzipEnabled bool, method string, methods ...string) {
	methods = append([]string{method}, methods...)
	level.Debug(a.logger).Log("msg", "api: registering route", "methods", strings.Join(methods, ","), "path", path, "auth", auth, "gzip", gzipEnabled)
	a.newRoute(path, handler, false, auth, gzipEnabled, methods...)
}

func (a *API) RegisterRoutesWithPrefix(prefix string, handler http.Handler, auth, gzipEnabled bool, methods ...string) {
	level.Debug(a.logger).Log("msg", "api: registering route", "methods", strings.Join(methods, ","), "prefix", prefix, "auth", auth, "gzip", gzipEnabled)
	a.newRoute(prefix, handler, true, auth, gzipEnabled, methods...)
}

func (a *API) newRoute(path string, handler http.Handler, isPrefix, auth, gzip bool, methods ...string) (route *mux.Route) {
	// Propagate the consistency level on all HTTP routes.
	// They are not used everywhere, but for consistency and less surprise it's added everywhere.
	handler = querierapi.ConsistencyMiddleware().Wrap(handler)

	if auth {
		handler = a.AuthMiddleware.Wrap(handler)
	}
	if gzip {
		handler = gziphandler.GzipHandler(handler)
	}
	if isPrefix {
		route = a.server.HTTP.PathPrefix(path)
	} else {
		route = a.server.HTTP.Path(path)
	}
	if len(methods) > 0 {
		route = route.Methods(methods...)
	}
	route = route.Handler(handler)

	return route
}

// RegisterAlertmanager registers endpoints that are associated with the alertmanager.
func (a *API) RegisterAlertmanager(am *alertmanager.MultitenantAlertmanager, apiEnabled bool, grafanaCompatEnabled bool, buildInfoHandler http.Handler) {
	alertmanagerpb.RegisterAlertmanagerServer(a.server.GRPC, am)

	a.indexPage.AddLinks(defaultWeight, "Alertmanager", []IndexPageLink{
		{Desc: "Status", Path: "/multitenant_alertmanager/status"},
		{Desc: "Status", Path: "/multitenant_alertmanager/configs"},
		{Desc: "Ring status", Path: "/multitenant_alertmanager/ring"},
		{Desc: "Alertmanager", Path: "/alertmanager"},
	})

	// Ensure this route is registered before the prefixed AM route
	a.RegisterRoute("/multitenant_alertmanager/status", http.HandlerFunc(am.StatusHandler), false, true, "GET")
	a.RegisterRoute("/multitenant_alertmanager/configs", http.HandlerFunc(am.ListAllConfigs), false, true, "GET")
	a.RegisterRoute("/multitenant_alertmanager/ring", http.HandlerFunc(am.RingHandler), false, true, "GET", "POST")
	a.RegisterRoute("/multitenant_alertmanager/delete_tenant_config", http.HandlerFunc(am.DeleteUserConfig), true, true, "POST")
	a.RegisterRoute(path.Join(a.cfg.AlertmanagerHTTPPrefix, "/api/v1/status/buildinfo"), buildInfoHandler, false, true, "GET")

	// UI components lead to a large number of routes to support, utilize a path prefix instead
	a.RegisterRoutesWithPrefix(a.cfg.AlertmanagerHTTPPrefix, am, true, true)
	level.Debug(a.logger).Log("msg", "api: registering alertmanager", "path_prefix", a.cfg.AlertmanagerHTTPPrefix)

	// MultiTenant Alertmanager API routes
	if apiEnabled {
		a.RegisterRoute("/api/v1/alerts", http.HandlerFunc(am.GetUserConfig), true, true, "GET")
		a.RegisterRoute("/api/v1/alerts", http.HandlerFunc(am.SetUserConfig), true, true, "POST")
		a.RegisterRoute("/api/v1/alerts", http.HandlerFunc(am.DeleteUserConfig), true, true, "DELETE")

		if grafanaCompatEnabled {
			level.Info(a.logger).Log("msg", "enabled experimental grafana routes")

			a.RegisterRoute("/api/v1/grafana/config", http.HandlerFunc(am.GetUserGrafanaConfig), true, true, http.MethodGet)
			a.RegisterRoute("/api/v1/grafana/config", http.HandlerFunc(am.SetUserGrafanaConfig), true, true, http.MethodPost)
			a.RegisterRoute("/api/v1/grafana/config", http.HandlerFunc(am.DeleteUserGrafanaConfig), true, true, http.MethodDelete)

			a.RegisterRoute("/api/v1/grafana/state", http.HandlerFunc(am.GetUserGrafanaState), true, true, http.MethodGet)
			a.RegisterRoute("/api/v1/grafana/state", http.HandlerFunc(am.SetUserGrafanaState), true, true, http.MethodPost)
			a.RegisterRoute("/api/v1/grafana/state", http.HandlerFunc(am.DeleteUserGrafanaState), true, true, http.MethodDelete)

			// These APIs are handled by the per-tenant Alertmanager, so they are handled by the distributor.
			a.RegisterRoute("/api/v1/grafana/receivers", am, true, true, http.MethodGet)
			a.RegisterRoute("/api/v1/grafana/receivers/test", am, true, true, http.MethodPost)
			a.RegisterRoute("/api/v1/grafana/templates/test", am, true, true, http.MethodPost)
		}
	}
}

// RegisterAPI registers the standard endpoints associated with a running Mimir.
func (a *API) RegisterAPI(httpPathPrefix string, actualCfg interface{}, defaultCfg interface{}, buildInfoHandler http.Handler) {
	a.indexPage.AddLinks(configWeight, "Current config", []IndexPageLink{
		{Desc: "Including the default values", Path: "/config"},
		{Desc: "Only values that differ from the defaults", Path: "/config?mode=diff"},
	})

	a.RegisterRoute("/config", a.cfg.configHandler(actualCfg, defaultCfg), false, true, "GET")
	a.RegisterRoute("/", indexHandler(httpPathPrefix, a.indexPage), false, true, "GET")
	a.RegisterRoutesWithPrefix("/static/", http.StripPrefix(httpPathPrefix, http.FileServer(http.FS(staticFiles))), false, true, "GET")
	a.RegisterRoute("/debug/fgprof", fgprof.Handler(), false, true, "GET")
	a.RegisterRoute("/api/v1/status/buildinfo", buildInfoHandler, false, true, "GET")
	a.RegisterRoute("/api/v1/status/config", a.cfg.statusConfigHandler(), false, true, "GET")
	a.RegisterRoute("/api/v1/status/flags", a.cfg.statusFlagsHandler(), false, true, "GET")
}

// RegisterRuntimeConfig registers the endpoints associates with the runtime configuration
func (a *API) RegisterRuntimeConfig(runtimeConfigHandler http.HandlerFunc, userLimitsHandler http.HandlerFunc) {
	a.indexPage.AddLinks(runtimeConfigWeight, "Current runtime config", []IndexPageLink{
		{Desc: "Entire runtime config (including overrides)", Path: "/runtime_config"},
		{Desc: "Only values that differ from the defaults", Path: "/runtime_config?mode=diff"},
	})

	a.RegisterRoute("/runtime_config", runtimeConfigHandler, false, true, "GET")
	a.RegisterRoute("/api/v1/user_limits", userLimitsHandler, true, true, "GET")
}

const PrometheusPushEndpoint = "/api/v1/push"
const OTLPPushEndpoint = "/otlp/v1/metrics"

// RegisterDistributor registers the endpoints associated with the distributor.
func (a *API) RegisterDistributor(d *distributor.Distributor, pushConfig distributor.Config, reg prometheus.Registerer, limits *validation.Overrides) {
	distributorpb.RegisterDistributorServer(a.server.GRPC, d)

	a.RegisterRoute(PrometheusPushEndpoint, distributor.Handler(
		pushConfig.MaxRecvMsgSize, d.RequestBufferPool, a.sourceIPs, a.cfg.SkipLabelNameValidationHeader,
		a.cfg.SkipLabelCountValidationHeader, limits, pushConfig.RetryConfig, d.PushWithMiddlewares, d.PushMetrics, a.logger,
	), true, false, "POST")
	a.RegisterRoute(OTLPPushEndpoint, distributor.OTLPHandler(
		pushConfig.MaxOTLPRequestSize, d.RequestBufferPool, a.sourceIPs, limits, pushConfig.OTelResourceAttributePromotionConfig,
		pushConfig.RetryConfig, d.PushWithMiddlewares, d.PushMetrics, reg, a.logger,
	), true, false, "POST")

	a.indexPage.AddLinks(defaultWeight, "Distributor", []IndexPageLink{
		{Desc: "Ring status", Path: "/distributor/ring"},
		{Desc: "Usage statistics", Path: "/distributor/all_user_stats"},
		{Desc: "HA tracker status", Path: "/distributor/ha_tracker"},
	})

	a.RegisterRoute("/distributor/ring", d, false, true, "GET", "POST")
	a.RegisterRoute("/distributor/all_user_stats", http.HandlerFunc(d.AllUserStatsHandler), false, true, "GET")
	a.RegisterRoute("/distributor/ha_tracker", d.HATracker, false, true, "GET")
}

// RegisterUsageMetricsRoute registers a Prometheus HTTP handler for the custom registry.
func (a *API) RegisterUsageMetricsRoute(customRegistryPath string, reg *prometheus.Registry) {
	a.RegisterRoute(customRegistryPath, promhttp.HandlerFor(reg, promhttp.HandlerOpts{}), false, false, "GET")
}

// Ingester is defined as an interface to allow for alternative implementations
// of ingesters to be passed into the API.RegisterIngester() method.
type Ingester interface {
	client.IngesterServer
	FlushHandler(http.ResponseWriter, *http.Request)
	ShutdownHandler(http.ResponseWriter, *http.Request)
	PrepareShutdownHandler(http.ResponseWriter, *http.Request)
	PreparePartitionDownscaleHandler(http.ResponseWriter, *http.Request)
	PrepareUnregisterHandler(w http.ResponseWriter, r *http.Request)
	UserRegistryHandler(http.ResponseWriter, *http.Request)
	TenantsHandler(http.ResponseWriter, *http.Request)
	TenantTSDBHandler(http.ResponseWriter, *http.Request)
	PrepareInstanceRingDownscaleHandler(http.ResponseWriter, *http.Request)
}

// RegisterIngester registers the ingester HTTP and gRPC services.
func (a *API) RegisterIngester(i Ingester) {
	client.RegisterIngesterServer(a.server.GRPC, i)

	a.indexPage.AddLinks(dangerousWeight, "Dangerous", []IndexPageLink{
		{Dangerous: true, Desc: "Trigger a flush of data from ingester to storage", Path: "/ingester/flush"},
		{Dangerous: true, Desc: "Trigger ingester shutdown", Path: "/ingester/shutdown"},
	})

	a.RegisterRoute("/ingester/flush", http.HandlerFunc(i.FlushHandler), false, true, "GET", "POST")
	a.RegisterRoute("/ingester/prepare-shutdown", http.HandlerFunc(i.PrepareShutdownHandler), false, true, "GET", "POST", "DELETE")
	a.RegisterRoute("/ingester/prepare-partition-downscale", http.HandlerFunc(i.PreparePartitionDownscaleHandler), false, true, "GET", "POST", "DELETE")
	a.RegisterRoute("/ingester/prepare-instance-ring-downscale", http.HandlerFunc(i.PrepareInstanceRingDownscaleHandler), false, true, "GET", "POST", "DELETE")
	a.RegisterRoute("/ingester/unregister-on-shutdown", http.HandlerFunc(i.PrepareUnregisterHandler), false, false, "GET", "PUT", "DELETE")
	a.RegisterRoute("/ingester/shutdown", http.HandlerFunc(i.ShutdownHandler), false, true, "POST")
	if a.cfg.GETRequestForIngesterShutdownEnabled {
		a.RegisterDeprecatedRoute("/ingester/shutdown", http.HandlerFunc(i.ShutdownHandler), false, true, "GET")
	}
	a.RegisterRoute("/ingester/tsdb_metrics", http.HandlerFunc(i.UserRegistryHandler), true, true, "GET")

	a.indexPage.AddLinks(defaultWeight, "Ingester", []IndexPageLink{
		{Dangerous: true, Desc: "Ingester Tenants", Path: "/ingester/tenants"},
	})

	a.RegisterRoute("/ingester/tenants", http.HandlerFunc(i.TenantsHandler), false, true, "GET")
	a.RegisterRoute("/ingester/tsdb/{tenant}", http.HandlerFunc(i.TenantTSDBHandler), false, true, "GET")
}

// RegisterRuler registers routes associated with the Ruler service.
func (a *API) RegisterRuler(r *ruler.Ruler) {
	a.indexPage.AddLinks(defaultWeight, "Ruler", []IndexPageLink{
		{Desc: "Ring status", Path: "/ruler/ring"},
	})
	a.RegisterRoute("/ruler/ring", r, false, true, "GET", "POST")

	// Administrative API, uses authentication to inform which user's configuration to delete.
	a.RegisterRoute("/ruler/delete_tenant_config", http.HandlerFunc(r.DeleteTenantConfiguration), true, true, "POST")

	// List all user rule groups
	a.RegisterRoute("/ruler/rule_groups", http.HandlerFunc(r.ListAllRules), false, true, "GET")

	ruler.RegisterRulerServer(a.server.GRPC, r)
}

// RegisterRulerAPI registers routes associated with the Ruler API
func (a *API) RegisterRulerAPI(r *ruler.API, configAPIEnabled bool, buildInfoHandler http.Handler) {
	// Prometheus Rule API Routes
	// We want to always enable these. They are read-only. Also if using local storage as rule storage,
	// you would like the API to be disabled and still be able to understand in what state rule evaluations are.
	a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/api/v1/rules"), http.HandlerFunc(r.PrometheusRules), true, true, "GET")
	a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/api/v1/alerts"), http.HandlerFunc(r.PrometheusAlerts), true, true, "GET")
	a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/api/v1/status/buildinfo"), buildInfoHandler, false, true, "GET")

	if configAPIEnabled {
		// Long-term maintained configuration API routes
		a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/config/v1/rules"), http.HandlerFunc(r.ListRules), true, true, "GET")
		a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/config/v1/rules/{namespace}"), http.HandlerFunc(r.ListRules), true, true, "GET")
		a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/config/v1/rules/{namespace}/{groupName}"), http.HandlerFunc(r.GetRuleGroup), true, true, "GET")
		a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/config/v1/rules/{namespace}"), http.HandlerFunc(r.CreateRuleGroup), true, true, "POST")
		a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/config/v1/rules/{namespace}/{groupName}"), http.HandlerFunc(r.DeleteRuleGroup), true, true, "DELETE")
		a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/config/v1/rules/{namespace}"), http.HandlerFunc(r.DeleteNamespace), true, true, "DELETE")
	}
}

// RegisterIngesterRing registers the ring UI page associated with the ingesters ring.
func (a *API) RegisterIngesterRing(r http.Handler) {
	a.indexPage.AddLinks(defaultWeight, "Ingester", []IndexPageLink{
		{Desc: "Ring status", Path: "/ingester/ring"},
	})
	a.RegisterRoute("/ingester/ring", r, false, true, "GET", "POST")
}

// RegisterIngesterPartitionRing registers the ring UI page associated with the ingester partitions ring.
func (a *API) RegisterIngesterPartitionRing(r http.Handler) {
	a.indexPage.AddLinks(defaultWeight, "Ingester", []IndexPageLink{
		{Desc: "Partition ring status", Path: "/ingester/partition-ring"},
	})
	a.RegisterRoute("/ingester/partition-ring", r, false, true, "GET", "POST")
}

// RegisterStoreGateway registers the ring UI page associated with the store-gateway.
func (a *API) RegisterStoreGateway(s *storegateway.StoreGateway) {
	storegatewaypb.RegisterStoreGatewayServer(a.server.GRPC, s)

	a.indexPage.AddLinks(defaultWeight, "Store-gateway", []IndexPageLink{
		{Desc: "Ring status", Path: "/store-gateway/ring"},
		{Desc: "Tenants & Blocks", Path: "/store-gateway/tenants"},
	})
	a.RegisterRoute("/store-gateway/ring", http.HandlerFunc(s.RingHandler), false, true, "GET", "POST")
	a.RegisterRoute("/store-gateway/tenants", http.HandlerFunc(s.TenantsHandler), false, true, "GET")
	a.RegisterRoute("/store-gateway/tenant/{tenant}/blocks", http.HandlerFunc(s.BlocksHandler), false, true, "GET")
	a.RegisterRoute("/store-gateway/prepare-shutdown", http.HandlerFunc(s.PrepareShutdownHandler), false, true, "GET", "POST", "DELETE")
}

// RegisterCompactor registers routes associated with the compactor.
func (a *API) RegisterCompactor(c *compactor.MultitenantCompactor) {
	a.indexPage.AddLinks(defaultWeight, "Compactor", []IndexPageLink{
		{Desc: "Ring status", Path: "/compactor/ring"},
		{Desc: "Tenants & compaction jobs", Path: "/compactor/tenants"},
	})
	a.RegisterRoute("/compactor/ring", http.HandlerFunc(c.RingHandler), false, true, "GET", "POST")
	a.RegisterRoute("/api/v1/upload/block/{block}/start", http.HandlerFunc(c.StartBlockUpload), true, false, http.MethodPost)
	a.RegisterRoute("/api/v1/upload/block/{block}/files", a.DisableServerHTTPTimeouts(http.HandlerFunc(c.UploadBlockFile)), true, false, http.MethodPost)
	a.RegisterRoute("/api/v1/upload/block/{block}/finish", http.HandlerFunc(c.FinishBlockUpload), true, false, http.MethodPost)
	a.RegisterRoute("/api/v1/upload/block/{block}/check", http.HandlerFunc(c.GetBlockUploadStateHandler), true, false, http.MethodGet)
	a.RegisterRoute("/compactor/delete_tenant", http.HandlerFunc(c.DeleteTenant), true, true, "POST")
	a.RegisterRoute("/compactor/delete_tenant_status", http.HandlerFunc(c.DeleteTenantStatus), true, true, "GET")
	a.RegisterRoute("/compactor/tenants", http.HandlerFunc(c.TenantsHandler), false, true, "GET")
	a.RegisterRoute("/compactor/tenant/{tenant}/planned_jobs", http.HandlerFunc(c.PlannedJobsHandler), false, true, "GET")
}

func (a *API) DisableServerHTTPTimeouts(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := http.NewResponseController(w)
		zero := time.Time{}

		level.Debug(a.logger).Log("msg", "disabling HTTP server timeouts for URL", "url", r.URL)

		if err := c.SetReadDeadline(zero); err != nil {
			level.Warn(a.logger).Log("msg", "failed to clear read deadline on HTTP connection", "url", r.URL, "err", err)
		}
		if err := c.SetWriteDeadline(zero); err != nil {
			level.Warn(a.logger).Log("msg", "failed to clear write deadline on HTTP connection", "url", r.URL, "err", err)
		}

		next.ServeHTTP(w, r)
	})
}

type Distributor interface {
	querier.Distributor
	UserStatsHandler(w http.ResponseWriter, r *http.Request)
}

// RegisterQueryable registers the default routes associated with the querier
// module.
func (a *API) RegisterQueryable(distributor Distributor) {
	// these routes are always registered to the default server
	a.RegisterRoute("/api/v1/user_stats", http.HandlerFunc(distributor.UserStatsHandler), true, true, "GET")
}

// RegisterQueryAPI registers the Prometheus API routes with the provided handler.
func (a *API) RegisterQueryAPI(handler http.Handler, buildInfoHandler http.Handler) {
	a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/api/v1/read"), handler, true, true, "POST")
	a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/api/v1/query"), handler, true, true, "GET", "POST")
	a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/api/v1/query_range"), handler, true, true, "GET", "POST")
	a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/api/v1/query_exemplars"), handler, true, true, "GET", "POST")
	a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/api/v1/labels"), handler, true, true, "GET", "POST")
	a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/api/v1/label/{name}/values"), handler, true, true, "GET")
	a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/api/v1/series"), handler, true, true, "GET", "POST", "DELETE")
	a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/api/v1/status/buildinfo"), buildInfoHandler, false, true, "GET")
	a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/api/v1/metadata"), handler, true, true, "GET")
	a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/api/v1/cardinality/label_names"), handler, true, true, "GET", "POST")
	a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/api/v1/cardinality/label_values"), handler, true, true, "GET", "POST")
	a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/api/v1/cardinality/active_series"), handler, true, true, "GET", "POST")
	a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/api/v1/cardinality/active_native_histogram_metrics"), handler, true, true, "GET", "POST")
	a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/api/v1/format_query"), handler, true, true, "GET", "POST")
}

// RegisterQueryFrontendHandler registers the Prometheus routes supported by the
// Mimir querier service. Currently, this can not be registered simultaneously
// with the Querier.
func (a *API) RegisterQueryFrontendHandler(h http.Handler, buildInfoHandler http.Handler) {
	a.RegisterQueryAPI(h, buildInfoHandler)
}

func (a *API) RegisterQueryFrontend1(f *frontendv1.Frontend) {
	frontendv1pb.RegisterFrontendServer(a.server.GRPC, f)
}

func (a *API) RegisterQueryFrontend2(f *frontendv2.Frontend) {
	frontendv2pb.RegisterFrontendForQuerierServer(a.server.GRPC, f)
}

func (a *API) RegisterQueryScheduler(f *scheduler.Scheduler) {
	a.indexPage.AddLinks(defaultWeight, "Query-scheduler", []IndexPageLink{
		{Desc: "Ring status", Path: "/query-scheduler/ring"},
	})
	a.RegisterRoute("/query-scheduler/ring", http.HandlerFunc(f.RingHandler), false, true, "GET", "POST")

	schedulerpb.RegisterSchedulerForFrontendServer(a.server.GRPC, f)
	schedulerpb.RegisterSchedulerForQuerierServer(a.server.GRPC, f)
}

func (a *API) RegisterOverridesExporter(oe *exporter.OverridesExporter) {
	a.indexPage.AddLinks(defaultWeight, "Overrides-exporter", []IndexPageLink{
		{Desc: "Ring status", Path: "/overrides-exporter/ring"},
	})
	a.RegisterRoute("/overrides-exporter/ring", http.HandlerFunc(oe.RingHandler), false, true, "GET", "POST")
}

// RegisterServiceMapHandler registers the Mimir structs service handler
// TODO: Refactor this code to be accomplished using the services.ServiceManager
// or a future module manager #2291
func (a *API) RegisterServiceMapHandler(handler http.Handler) {
	a.indexPage.AddLinks(serviceStatusWeight, "Overview", []IndexPageLink{
		{Desc: "Services' status", Path: "/services"},
	})
	a.RegisterRoute("/services", handler, false, true, "GET")
}

func (a *API) RegisterMemberlistKV(pathPrefix string, kvs *memberlist.KVInitService) {
	a.indexPage.AddLinks(memberlistWeight, "Memberlist", []IndexPageLink{
		{Desc: "Status", Path: "/memberlist"},
	})
	a.RegisterRoute("/memberlist", memberlistStatusHandler(pathPrefix, kvs), false, true, "GET")
}
