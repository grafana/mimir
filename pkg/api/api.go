// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/api/api.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package api

import (
	"context"
	"flag"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/felixge/fgprof"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gorilla/mux"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/prometheus/prometheus/storage"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/server"

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
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/purger"
	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/ruler"
	"github.com/grafana/mimir/pkg/scheduler"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/push"
)

// DistributorPushWrapper wraps around a push. It is similar to middleware.Interface.
type DistributorPushWrapper func(next push.Func) push.Func
type ConfigHandler func(actualCfg interface{}, defaultCfg interface{}) http.HandlerFunc

type Config struct {
	SkipLabelNameValidationHeader bool `yaml:"skip_label_name_validation_header_enabled" category:"advanced"`

	AlertmanagerHTTPPrefix string `yaml:"alertmanager_http_prefix" category:"advanced"`
	PrometheusHTTPPrefix   string `yaml:"prometheus_http_prefix" category:"advanced"`

	// The following configs are injected by the upstream caller.
	ServerPrefix       string               `yaml:"-"`
	HTTPAuthMiddleware middleware.Interface `yaml:"-"`

	// This allows downstream projects to wrap the distributor push function
	// and access the deserialized write requests before/after they are pushed.
	DistributorPushWrapper DistributorPushWrapper `yaml:"-"`

	// The CustomConfigHandler allows for providing a different handler for the
	// `/config` endpoint. If this field is set _before_ the API module is
	// initialized, the custom config handler will be used instead of
	// DefaultConfigHandler.
	CustomConfigHandler ConfigHandler `yaml:"-"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.SkipLabelNameValidationHeader, "api.skip-label-name-validation-header-enabled", false, "Allows to skip label name validation via X-Mimir-SkipLabelNameValidation header on the http write path. Use with caution as it breaks PromQL. Allowing this for external clients allows any client to send invalid label names. After enabling it, requests with a specific HTTP header set to true will not have label names validated.")
	cfg.RegisterFlagsWithPrefix("", f)
}

// RegisterFlagsWithPrefix adds the flags required to config this to the given FlagSet with the set prefix.
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.AlertmanagerHTTPPrefix, prefix+"http.alertmanager-http-prefix", "/alertmanager", "HTTP URL path under which the Alertmanager ui and api will be served.")
	f.StringVar(&cfg.PrometheusHTTPPrefix, prefix+"http.prometheus-http-prefix", "/prometheus", "HTTP URL path under which the Prometheus api will be served.")
}

// Push either wraps the distributor push function as configured or returns the distributor push directly.
func (cfg *Config) wrapDistributorPush(d *distributor.Distributor) push.Func {
	if cfg.DistributorPushWrapper != nil {
		return cfg.DistributorPushWrapper(d.PushWithCleanup)
	}

	return d.PushWithCleanup
}

type API struct {
	AuthMiddleware middleware.Interface

	cfg       Config
	server    *server.Server
	logger    log.Logger
	sourceIPs *middleware.SourceIPExtractor
	indexPage *IndexPageContent
}

func New(cfg Config, serverCfg server.Config, s *server.Server, logger log.Logger) (*API, error) {
	// Ensure the encoded path is used. Required for the rules API
	s.HTTP.UseEncodedPath()

	var sourceIPs *middleware.SourceIPExtractor
	if serverCfg.LogSourceIPs {
		var err error
		sourceIPs, err = middleware.NewSourceIPs(serverCfg.LogSourceIPsHeader, serverCfg.LogSourceIPsRegex)
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
	if cfg.HTTPAuthMiddleware == nil {
		api.AuthMiddleware = middleware.AuthenticateUser
	}

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

func (a *API) RegisterRouteWithQueryParameters(path string, handler http.Handler, auth, gzipEnabled bool,
	params []string, method string, methods ...string) {
	methods = append([]string{method}, methods...)
	level.Debug(a.logger).Log("msg", "api: registering route with query parameters", "methods", strings.Join(methods, ","),
		"path", path, "auth", auth, "gzip", gzipEnabled, "query_parameters", params)
	route := a.newRoute(path, handler, false, auth, gzipEnabled, methods...)
	route.Queries(params...)
}

func (a *API) RegisterRoutesWithPrefix(prefix string, handler http.Handler, auth, gzipEnabled bool, methods ...string) {
	level.Debug(a.logger).Log("msg", "api: registering route", "methods", strings.Join(methods, ","), "prefix", prefix, "auth", auth, "gzip", gzipEnabled)
	a.newRoute(prefix, handler, true, auth, gzipEnabled, methods...)
}

func (a *API) newRoute(path string, handler http.Handler, isPrefix, auth, gzip bool, methods ...string) (route *mux.Route) {
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
func (a *API) RegisterAlertmanager(am *alertmanager.MultitenantAlertmanager, apiEnabled bool, buildInfoHandler http.Handler) {
	alertmanagerpb.RegisterAlertmanagerServer(a.server.GRPC, am)

	a.indexPage.AddLinks(defaultWeight, "Alertmanager", []IndexPageLink{
		{Desc: "Status", Path: "/multitenant_alertmanager/status"},
		{Desc: "Ring status", Path: "/multitenant_alertmanager/ring"},
	})

	// Ensure this route is registered before the prefixed AM route
	a.RegisterRoute("/multitenant_alertmanager/status", am.GetStatusHandler(), false, true, "GET")
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
}

// RegisterRuntimeConfig registers the endpoints associates with the runtime configuration
func (a *API) RegisterRuntimeConfig(runtimeConfigHandler http.HandlerFunc) {
	a.indexPage.AddLinks(runtimeConfigWeight, "Current runtime config", []IndexPageLink{
		{Desc: "Entire runtime config (including overrides)", Path: "/runtime_config"},
		{Desc: "Only values that differ from the defaults", Path: "/runtime_config?mode=diff"},
	})

	a.RegisterRoute("/runtime_config", runtimeConfigHandler, false, true, "GET")
}

// RegisterDistributor registers the endpoints associated with the distributor.
func (a *API) RegisterDistributor(d *distributor.Distributor, pushConfig distributor.Config) {
	distributorpb.RegisterDistributorServer(a.server.GRPC, d)

	a.RegisterRoute("/api/v1/push", push.Handler(pushConfig.MaxRecvMsgSize, a.sourceIPs, a.cfg.SkipLabelNameValidationHeader, a.cfg.wrapDistributorPush(d)), true, false, "POST")

	a.indexPage.AddLinks(defaultWeight, "Distributor", []IndexPageLink{
		{Desc: "Ring status", Path: "/distributor/ring"},
		{Desc: "Usage statistics", Path: "/distributor/all_user_stats"},
		{Desc: "HA tracker status", Path: "/distributor/ha_tracker"},
	})

	a.RegisterRoute("/distributor/ring", d, false, true, "GET", "POST")
	a.RegisterRoute("/distributor/all_user_stats", http.HandlerFunc(d.AllUserStatsHandler), false, true, "GET")
	a.RegisterRoute("/distributor/ha_tracker", d.HATracker, false, true, "GET")
}

// Ingester is defined as an interface to allow for alternative implementations
// of ingesters to be passed into the API.RegisterIngester() method.
type Ingester interface {
	client.IngesterServer
	FlushHandler(http.ResponseWriter, *http.Request)
	ShutdownHandler(http.ResponseWriter, *http.Request)
	PushWithCleanup(context.Context, *mimirpb.WriteRequest, func()) (*mimirpb.WriteResponse, error)
}

// RegisterIngester registers the ingesters HTTP and GRPC service
func (a *API) RegisterIngester(i Ingester, pushConfig distributor.Config) {
	client.RegisterIngesterServer(a.server.GRPC, i)

	a.indexPage.AddLinks(dangerousWeight, "Dangerous", []IndexPageLink{
		{Dangerous: true, Desc: "Trigger a flush of data from ingester to storage", Path: "/ingester/flush"},
		{Dangerous: true, Desc: "Trigger ingester shutdown", Path: "/ingester/shutdown"},
	})

	a.RegisterRoute("/ingester/flush", http.HandlerFunc(i.FlushHandler), false, true, "GET", "POST")
	a.RegisterRoute("/ingester/shutdown", http.HandlerFunc(i.ShutdownHandler), false, true, "GET", "POST")
	a.RegisterRoute("/ingester/push", push.Handler(pushConfig.MaxRecvMsgSize, a.sourceIPs, a.cfg.SkipLabelNameValidationHeader, i.PushWithCleanup), true, false, "POST") // For testing and debugging.
}

func (a *API) RegisterTenantDeletion(api *purger.TenantDeletionAPI) {
	a.RegisterRoute("/purger/delete_tenant", http.HandlerFunc(api.DeleteTenant), true, true, "POST")
	a.RegisterRoute("/purger/delete_tenant_status", http.HandlerFunc(api.DeleteTenantStatus), true, true, "GET")
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
func (a *API) RegisterRulerAPI(r *ruler.API, configAPIEnabled bool) {
	// Prometheus Rule API Routes
	// We want to always enable these. They are read-only. Also if using local storage as rule storage,
	// you would like the API to be disabled and still be able to understand in what state rule evaluations are.
	a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/api/v1/rules"), http.HandlerFunc(r.PrometheusRules), true, true, "GET")
	a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/api/v1/alerts"), http.HandlerFunc(r.PrometheusAlerts), true, true, "GET")

	if configAPIEnabled {
		// Ruler API Routes
		// TODO remove the /api/v1/rules/** endpoints in Mimir 2.2.0 as agreed in https://github.com/grafana/mimir/pull/763#discussion_r808270581
		a.RegisterDeprecatedRoute("/api/v1/rules", http.HandlerFunc(r.ListRules), true, true, "GET")
		a.RegisterDeprecatedRoute("/api/v1/rules/{namespace}", http.HandlerFunc(r.ListRules), true, true, "GET")
		a.RegisterDeprecatedRoute("/api/v1/rules/{namespace}/{groupName}", http.HandlerFunc(r.GetRuleGroup), true, true, "GET")
		a.RegisterDeprecatedRoute("/api/v1/rules/{namespace}", http.HandlerFunc(r.CreateRuleGroup), true, true, "POST")
		a.RegisterDeprecatedRoute("/api/v1/rules/{namespace}/{groupName}", http.HandlerFunc(r.DeleteRuleGroup), true, true, "DELETE")
		a.RegisterDeprecatedRoute("/api/v1/rules/{namespace}", http.HandlerFunc(r.DeleteNamespace), true, true, "DELETE")

		// Configuration endpoints with Prometheus prefix, so we keep Prometheus-compatible EPs and config EPs under the same prefix.
		// TODO remove the <prometheus-http-prefix>/v1/rules/** endpoints in Mimir 2.2.0 as agreed in https://github.com/grafana/mimir/pull/1222#issuecomment-1046759965
		a.RegisterDeprecatedRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/rules"), http.HandlerFunc(r.ListRules), true, true, "GET")
		a.RegisterDeprecatedRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/rules/{namespace}"), http.HandlerFunc(r.ListRules), true, true, "GET")
		a.RegisterDeprecatedRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/rules/{namespace}/{groupName}"), http.HandlerFunc(r.GetRuleGroup), true, true, "GET")
		a.RegisterDeprecatedRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/rules/{namespace}"), http.HandlerFunc(r.CreateRuleGroup), true, true, "POST")
		a.RegisterDeprecatedRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/rules/{namespace}/{groupName}"), http.HandlerFunc(r.DeleteRuleGroup), true, true, "DELETE")
		a.RegisterDeprecatedRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/rules/{namespace}"), http.HandlerFunc(r.DeleteNamespace), true, true, "DELETE")

		// Long-term maintained configuration API routes
		a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/config/v1/rules"), http.HandlerFunc(r.ListRules), true, true, "GET")
		a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/config/v1/rules/{namespace}"), http.HandlerFunc(r.ListRules), true, true, "GET")
		a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/config/v1/rules/{namespace}/{groupName}"), http.HandlerFunc(r.GetRuleGroup), true, true, "GET")
		a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/config/v1/rules/{namespace}"), http.HandlerFunc(r.CreateRuleGroup), true, true, "POST")
		a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/config/v1/rules/{namespace}/{groupName}"), http.HandlerFunc(r.DeleteRuleGroup), true, true, "DELETE")
		a.RegisterRoute(path.Join(a.cfg.PrometheusHTTPPrefix, "/config/v1/rules/{namespace}"), http.HandlerFunc(r.DeleteNamespace), true, true, "DELETE")
	}
}

// RegisterRing registers the ring UI page associated with the distributor for writes.
func (a *API) RegisterRing(r http.Handler) {
	a.indexPage.AddLinks(defaultWeight, "Ingester", []IndexPageLink{
		{Desc: "Ring status", Path: "/ingester/ring"},
	})
	a.RegisterRoute("/ingester/ring", r, false, true, "GET", "POST")
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
}

// RegisterCompactor registers routes associated with the compactor.
func (a *API) RegisterCompactor(c *compactor.MultitenantCompactor) {
	a.indexPage.AddLinks(defaultWeight, "Compactor", []IndexPageLink{
		{Desc: "Ring status", Path: "/compactor/ring"},
	})
	a.RegisterRoute("/compactor/ring", http.HandlerFunc(c.RingHandler), false, true, "GET", "POST")
	// Endpoint to handle requests for completion of block backfilling.
	// Placed before less specific handler for same route, that doesn't take query parameter, for precedence.
	a.RegisterRouteWithQueryParameters("/api/v1/upload/block/{block}", http.HandlerFunc(c.CompleteBlockUpload),
		true, false, []string{"uploadComplete", "true"}, http.MethodPost)
	// Endpoint to handle requests for starting of block backfilling.
	// Starting the backfilling of a block means to verify that the backfill can go ahead.
	// In practice this means to check that the block isn't already in block storage.
	a.RegisterRoute("/api/v1/upload/block/{block}", http.HandlerFunc(c.CreateBlockUpload), true,
		false, http.MethodPost)
	// Endpoint to handle requests for uploading block files to a backfill.
	a.RegisterRouteWithQueryParameters("/api/v1/upload/block/{block}/files", http.HandlerFunc(c.UploadBlockFile),
		true, false, []string{"path", "{path}"}, http.MethodPost)
}

type Distributor interface {
	querier.Distributor
	UserStatsHandler(w http.ResponseWriter, r *http.Request)
}

// RegisterQueryable registers the the default routes associated with the querier
// module.
func (a *API) RegisterQueryable(
	queryable storage.SampleAndChunkQueryable,
	distributor Distributor,
) {
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
}

// RegisterQueryFrontend registers the Prometheus routes supported by the
// Mimir querier service. Currently this can not be registered simultaneously
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
	schedulerpb.RegisterSchedulerForFrontendServer(a.server.GRPC, f)
	schedulerpb.RegisterSchedulerForQuerierServer(a.server.GRPC, f)
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
