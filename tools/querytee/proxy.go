// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/tools/querytee/proxy.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querytee

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/clusterutil"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/server"
	"github.com/grafana/dskit/spanlogger"
	"github.com/grafana/regexp"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/util/propagation"
)

var tracer = otel.Tracer("pkg/tools/querytee")

const (
	defaultReportGRPCCodesInInstrumentationLabel = true

	defaultHTTPServerReadTimeOut  = 1 * time.Minute
	defaultHTTPServerWriteTimeout = 2 * time.Minute

	defaultGRPCMaxRecvMsgSize             = 100 * 1024 * 1024
	defaultGRPCMaxSendMsgSize             = 100 * 1024 * 1024
	defaultGRPCServerMaxConcurrentStreams = 10000
	defaultGRPCServerMinTimeBetweenPings  = 10 * time.Second
	defaultGRPCPingWithoutStreamAllowed   = true
)

type ProxyConfig struct {
	DeprecatedServerHTTPServiceAddress      string        // Deprecated: Deprecated in favor of dskit server.Config built-in server.http-listen-address. Deprecated in Mimir 3.1, remove in Mimir 3.3.
	DeprecatedServerHTTPServicePort         int           // Deprecated: Deprecated in favor of dskit server.Config built-in server.http-listen-port. Deprecated in Mimir 3.1, remove in Mimir 3.3.
	DeprecatedServerGracefulShutdownTimeout time.Duration // Deprecated: Deprecated in favor of dskit server.Config built-in server.graceful-shutdown-timeout. Deprecated in Mimir 3.1, remove in Mimir 3.3.
	DeprecatedServerGRPCServiceAddress      string        // Deprecated: Deprecated in favor of dskit server.Config built-in server.grpc-listen-address. Deprecated in Mimir 3.1, remove in Mimir 3.3.
	DeprecatedServerGRPCServicePort         int           // Deprecated: Deprecated in favor of dskit server.Config built-in server.grpc-listen-port. Deprecated in Mimir 3.1, remove in Mimir 3.3.

	Server server.Config

	BackendEndpoints                    string
	PreferredBackend                    string
	BackendReadTimeout                  time.Duration
	BackendConfigFile                   string
	parsedBackendConfig                 map[string]*BackendConfig
	CompareResponses                    bool
	LogSlowQueryResponseThreshold       time.Duration
	ValueComparisonTolerance            float64
	UseRelativeError                    bool
	PassThroughNonRegisteredRoutes      bool
	SkipRecentSamples                   time.Duration
	SkipSamplesBefore                   flagext.Time
	RequireExactErrorMatch              bool
	BackendSkipTLSVerify                bool
	AddMissingTimeParamToInstantQueries bool
	SecondaryBackendsRequestProportion  float64
	SkipPreferredBackendFailures        bool
	BackendsLookbackDelta               time.Duration
	ClusterValidation                   clusterutil.ClusterValidationConfig
}

// registerServerFlagsWithChangedDefaultValues emulates the same method in pkg/mimir/mimir.go,
// as query tee does not currently import and utilize mimir.Config.
func (cfg *ProxyConfig) registerServerFlagsWithChangedDefaultValues(fs *flag.FlagSet) {
	throwaway := flag.NewFlagSet("throwaway", flag.PanicOnError)

	// Register to throwaway flags first. Default values are remembered during registration and cannot be changed,
	// but we can take values from throwaway flag set and re-register into supplied flag set with new default values.
	cfg.Server.RegisterFlags(throwaway)

	defaultsOverrides := map[string]string{
		"server.report-grpc-codes-in-instrumentation-label-enabled": strconv.FormatBool(defaultReportGRPCCodesInInstrumentationLabel),

		"server.http-read-timeout":  defaultHTTPServerReadTimeOut.String(),
		"server.http-write-timeout": defaultHTTPServerWriteTimeout.String(),

		"server.grpc-max-recv-msg-size-bytes":               strconv.Itoa(defaultGRPCMaxRecvMsgSize),
		"server.grpc-max-send-msg-size-bytes":               strconv.Itoa(defaultGRPCMaxSendMsgSize),
		"server.grpc-max-concurrent-streams":                strconv.Itoa(defaultGRPCServerMaxConcurrentStreams),
		"server.grpc.keepalive.min-time-between-pings":      defaultGRPCServerMinTimeBetweenPings.String(),
		"server.grpc.keepalive.ping-without-stream-allowed": strconv.FormatBool(defaultGRPCPingWithoutStreamAllowed),
	}

	throwaway.VisitAll(func(f *flag.Flag) {
		if defaultValue, overridden := defaultsOverrides[f.Name]; overridden {
			// Ignore errors when setting new values. We have a test to verify that it works.
			_ = f.Value.Set(defaultValue)
		}

		fs.Var(f.Value, f.Name, f.Usage)
	})
}

type BackendConfig struct {
	RequestHeaders    http.Header `json:"request_headers" yaml:"request_headers"`
	RequestProportion *float64    `json:"request_proportion,omitempty" yaml:"request_proportion,omitempty"`
	MinDataQueriedAge string      `json:"min_data_queried_age,omitempty" yaml:"min_data_queried_age,omitempty"`
}

func exampleJSONBackendConfig() string {
	proportion := 0.5
	cfg := BackendConfig{
		RequestHeaders: http.Header{
			"Cache-Control": {"no-store"},
		},
		RequestProportion: &proportion,
		MinDataQueriedAge: "24h",
	}
	jsonBytes, err := json.Marshal(cfg)
	if err != nil {
		panic("invalid example backend config" + err.Error())
	}
	return string(jsonBytes)
}

func (cfg *ProxyConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.Server.MetricsNamespace = queryTeeMetricsNamespace

	// Deprecate in favor of built-in server.http-listen-address
	f.StringVar(&cfg.DeprecatedServerHTTPServiceAddress, "server.http-service-address", "", "Bind address for server where query-tee service listens for HTTP requests.")
	// Deprecate in favor of built-in server.http-listen-port
	f.IntVar(&cfg.DeprecatedServerHTTPServicePort, "server.http-service-port", 80, "The HTTP port where the query-tee service listens for HTTP requests.")

	// Deprecate in favor of built-in server.grpc-listen-address
	f.StringVar(&cfg.DeprecatedServerGRPCServiceAddress, "server.grpc-service-address", "", "Bind address for server where query-tee service listens for HTTP over gRPC requests.")
	// Deprecate in favor of built-in server.grpc-listen-port
	f.IntVar(&cfg.DeprecatedServerGRPCServicePort, "server.grpc-service-port", 9095, "The GRPC port where the query-tee service listens for HTTP over gRPC messages.")

	f.StringVar(&cfg.BackendEndpoints, "backend.endpoints", "",
		"Comma-separated list of backend endpoints to query. If the client request contains basic auth, it will be forwarded to the backend. "+
			"Basic auth is also accepted as part of the endpoint URL and takes precedence over the basic auth in the client request. "+
			"If the endpoint URL doesn't contain basic auth password, then the basic auth password from the client request is used. "+
			"If the endpoint basic auth username is __REQUEST_HEADER_X_SCOPE_ORGID__, then the value of the X-Scope-OrgID header will be used as the username.",
	)
	f.BoolVar(&cfg.BackendSkipTLSVerify, "backend.skip-tls-verify", false, "Skip TLS verification on backend targets.")
	f.StringVar(&cfg.PreferredBackend, "backend.preferred", "", "The hostname of the preferred backend when selecting the response to send back to the client. If no preferred backend is configured then the query-tee will send back to the client the first successful response received without waiting for other backends.")
	f.DurationVar(&cfg.BackendReadTimeout, "backend.read-timeout", 150*time.Second, "The timeout when reading the response from a backend.")
	f.StringVar(&cfg.BackendConfigFile, "backend.config-file", "", "Path to a file with YAML or JSON configuration for each backend. Each key in the YAML/JSON document is a backend hostname. This is an example configuration value for a backend in JSON: "+exampleJSONBackendConfig())
	f.BoolVar(&cfg.CompareResponses, "proxy.compare-responses", false, "Compare responses between preferred and secondary endpoints for supported routes.")
	f.DurationVar(&cfg.LogSlowQueryResponseThreshold, "proxy.log-slow-query-response-threshold", 10*time.Second, "The minimum difference in response time between slowest and fastest back-end over which to log the query. 0 to disable.")
	f.Float64Var(&cfg.ValueComparisonTolerance, "proxy.value-comparison-tolerance", 0.000001, "The tolerance to apply when comparing floating point values in the responses. 0 to disable tolerance and require exact match (not recommended).")
	f.BoolVar(&cfg.UseRelativeError, "proxy.compare-use-relative-error", false, "Use relative error tolerance when comparing floating point values.")
	f.DurationVar(&cfg.SkipRecentSamples, "proxy.compare-skip-recent-samples", 2*time.Minute, "The window from now to skip comparing samples. 0 to disable.")
	f.Var(&cfg.SkipSamplesBefore, "proxy.compare-skip-samples-before", "Skip the samples before the given time for comparison. The time can be in RFC3339 format (or) RFC3339 without the timezone and seconds (or) date only.")
	f.BoolVar(&cfg.RequireExactErrorMatch, "proxy.compare-exact-error-matching", false, "If true, errors will be considered the same only if they are exactly the same. If false, errors will be considered the same if they are considered equivalent.")
	f.BoolVar(&cfg.SkipPreferredBackendFailures, "proxy.compare-skip-preferred-backend-failures", false, "If true, the proxy will 'skip' result comparisons where the preferred backend returned a 5xx status code. This is useful in the case where the secondary backend is more reliable.")
	f.BoolVar(&cfg.PassThroughNonRegisteredRoutes, "proxy.passthrough-non-registered-routes", false, "Passthrough requests for non-registered routes to preferred backend.")
	f.BoolVar(&cfg.AddMissingTimeParamToInstantQueries, "proxy.add-missing-time-parameter-to-instant-queries", true, "Add a 'time' parameter to proxied instant query requests if they do not have one.")
	f.Float64Var(&cfg.SecondaryBackendsRequestProportion, "proxy.secondary-backends-request-proportion", DefaultRequestProportion, "Proportion of requests to send to secondary backends. Must be between 0 and 1 (inclusive), and if not 1, then -backend.preferred must be set. Optionally this global setting can be overrided on a per-backend basis via the backend config file.")
	f.DurationVar(&cfg.BackendsLookbackDelta, "proxy.backends-lookback-delta", 5*time.Minute, "The lookback configured in the backends query engines. Used to accurately extract min time from queries for backends that require it.")
	cfg.ClusterValidation.RegisterFlagsWithPrefix("query-tee.client-cluster-validation.", f)
	cfg.registerServerFlagsWithChangedDefaultValues(f)
}

type Route struct {
	Path                string
	RouteName           string
	Methods             []string
	ResponseComparator  ResponsesComparator
	RequestTransformers []RequestTransformer
}

// RequestTransformer manipulates a proxied request before it is sent to downstream endpoints.
//
// r.Body is ignored, use body instead.
type RequestTransformer func(r *http.Request, body []byte, logger *spanlogger.SpanLogger) (*http.Request, []byte, error)

type Proxy struct {
	cfg        ProxyConfig
	backends   []ProxyBackendInterface
	logger     log.Logger
	registerer prometheus.Registerer
	metrics    *ProxyMetrics
	routes     []Route

	// The HTTP and gRPC servers used to run the proxy service.
	server *server.Server

	// Wait group used to wait until the server has done.
	done sync.WaitGroup
}

func NewProxy(cfg ProxyConfig, logger log.Logger, routes []Route, registerer prometheus.Registerer) (*Proxy, error) {
	if cfg.CompareResponses && cfg.PreferredBackend == "" {
		return nil, fmt.Errorf("when enabling comparison of results -backend.preferred flag must be set to hostname of preferred backend")
	}

	if cfg.PassThroughNonRegisteredRoutes && cfg.PreferredBackend == "" {
		return nil, fmt.Errorf("when enabling passthrough for non-registered routes -backend.preferred flag must be set to hostname of backend where those requests needs to be passed")
	}

	if cfg.SecondaryBackendsRequestProportion < 0 || cfg.SecondaryBackendsRequestProportion > 1 {
		return nil, errors.New("secondary request proportion must be between 0 and 1 (inclusive)")
	}

	if cfg.SecondaryBackendsRequestProportion < 1 && cfg.PreferredBackend == "" {
		return nil, errors.New("preferred backend must be set when secondary backends request proportion is not 1")
	}

	if len(cfg.BackendConfigFile) > 0 {
		configBytes, err := os.ReadFile(cfg.BackendConfigFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read backend config file (%s): %w", cfg.BackendConfigFile, err)
		}
		err = yaml.Unmarshal(configBytes, &cfg.parsedBackendConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to parse backend YAML config: %w", err)
		}
	}

	// Query-tee-specific dskit server.Config flag names are deprecated in favor of the built-ins from dskit.
	// Preserve usage of the deprecated flags for now;
	// for all deprecated flags, if they are not set to their defaults, override the new flag's setting.
	if cfg.DeprecatedServerHTTPServiceAddress != "" {
		cfg.Server.HTTPListenAddress = cfg.DeprecatedServerHTTPServiceAddress
	}
	if cfg.DeprecatedServerHTTPServicePort != 80 {
		cfg.Server.HTTPListenPort = cfg.DeprecatedServerHTTPServicePort
	}
	if cfg.DeprecatedServerGracefulShutdownTimeout != 30*time.Second {
		cfg.Server.ServerGracefulShutdownTimeout = cfg.DeprecatedServerGracefulShutdownTimeout
	}
	if cfg.DeprecatedServerGRPCServiceAddress != "" {
		cfg.Server.GRPCListenAddress = cfg.DeprecatedServerGRPCServiceAddress
	}
	if cfg.DeprecatedServerGRPCServicePort != 9095 {
		cfg.Server.GRPCListenPort = cfg.DeprecatedServerGRPCServicePort
	}

	cfg.Server.Registerer = registerer
	p := &Proxy{
		cfg:        cfg,
		logger:     logger,
		registerer: registerer,
		metrics:    NewProxyMetrics(registerer),
		routes:     routes,
	}

	// Parse the backend endpoints (comma separated).
	parts := strings.Split(cfg.BackendEndpoints, ",")

	for idx, part := range parts {
		// Skip empty ones.
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		u, err := url.Parse(part)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid backend endpoint %s", part)
		}

		// The backend name is hardcoded as the backend hostname.
		name := u.Hostname()
		preferred := name == cfg.PreferredBackend

		// In tests, we have the same hostname for all backends, so we also
		// support a numeric preferred backend which is the index in the list
		// of backends.
		if preferredIdx, err := strconv.Atoi(cfg.PreferredBackend); err == nil {
			preferred = preferredIdx == idx
		}

		backendCfg := cfg.parsedBackendConfig[name]
		if backendCfg == nil {
			// In tests, we have the same hostname for all backends, so we also
			// support a numeric preferred backend which is the index in the list
			// of backends.
			backendCfg = cfg.parsedBackendConfig[strconv.Itoa(idx)]
			if backendCfg == nil {
				backendCfg = &BackendConfig{}
			}
		}

		p.backends = append(p.backends, NewProxyBackend(name, u, cfg.BackendReadTimeout, preferred, cfg.BackendSkipTLSVerify, cfg.ClusterValidation.Label, *backendCfg))
	}

	// At least 1 backend is required
	if len(p.backends) < 1 {
		return nil, errors.New("at least 1 backend is required")
	}

	err := validateBackendConfig(p.backends, cfg.parsedBackendConfig)
	if err != nil {
		return nil, fmt.Errorf("validating external backend configs: %w", err)
	}

	// If the preferred backend is configured, then it must exist among the actual backends.
	if cfg.PreferredBackend != "" {
		exists := false
		for _, b := range p.backends {
			if b.Preferred() {
				exists = true
				break
			}
		}

		if !exists {
			return nil, fmt.Errorf("the preferred backend (hostname) has not been found among the list of configured backends")
		}
	}

	if cfg.CompareResponses && len(p.backends) < 2 {
		return nil, fmt.Errorf("when enabling comparison of results at least 2 backends are required")
	}

	// At least 2 backends are suggested
	if len(p.backends) < 2 {
		level.Warn(p.logger).Log("msg", "The proxy is running with only 1 backend. At least 2 backends are required to fulfil the purpose of the proxy and compare results.")
	}

	return p, nil
}

func validateBackendConfig(backends []ProxyBackendInterface, config map[string]*BackendConfig) error {
	// Tests need to pass the same hostname for all backends, so we also
	// support a numeric preferred backend which is the index in the list of backend.
	numericBackendNameRegex := regexp.MustCompile("^[0-9]+$")
	for configuredBackend, backendCfg := range config {
		backendExists := false
		for _, actualBacked := range backends {
			if actualBacked.Name() == configuredBackend {
				backendExists = true
				break
			}
		}
		if !backendExists && !numericBackendNameRegex.MatchString(configuredBackend) {
			return fmt.Errorf("configured backend %s does not exist in the list of actual backends", configuredBackend)
		}

		// Validate request proportion if specified
		if backendCfg.RequestProportion != nil {
			proportion := *backendCfg.RequestProportion
			if proportion < 0 || proportion > 1 {
				return fmt.Errorf("backend %s: request_proportion must be between 0.0 and 1.0, got %f", configuredBackend, proportion)
			}
		}

		// Validate time threshold if specified
		if backendCfg.MinDataQueriedAge != "" {
			if _, err := time.ParseDuration(backendCfg.MinDataQueriedAge); err != nil {
				return fmt.Errorf("backend %s: invalid min_data_queried_age format %q: %w", configuredBackend, backendCfg.MinDataQueriedAge, err)
			}
		}
	}
	return nil
}

func (p *Proxy) Start() error {
	p.cfg.Server.MetricsNamespace = queryTeeMetricsNamespace
	p.cfg.Server.RegisterInstrumentation = false
	// Allow reporting HTTP 4xx codes in status_code label of request duration metrics
	p.cfg.Server.ReportHTTP4XXCodesInInstrumentationLabel = true

	p.cfg.Server.Log = p.logger

	// Setup server first, so we can fail early if the ports are in use.
	serv, err := server.New(p.cfg.Server)
	if err != nil {
		return err
	}

	router := serv.HTTP

	// Health check endpoint.
	router.Path("/").Methods("GET").Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Since we are only doing query requests decoding, we only care about the lookback delta for the Codec instance.
	// The other config parameters are not relevant.
	codec := querymiddleware.NewCodec(p.registerer, p.cfg.BackendsLookbackDelta, "json", nil, &propagation.NoopInjector{})

	// register routes
	for _, route := range p.routes {
		var comparator ResponsesComparator
		if p.cfg.CompareResponses {
			comparator = route.ResponseComparator
		}
		router.Path(route.Path).Methods(route.Methods...).Handler(NewProxyEndpoint(p.backends, route, p.metrics, p.logger, comparator, p.cfg.LogSlowQueryResponseThreshold, p.cfg.SecondaryBackendsRequestProportion, p.cfg.SkipPreferredBackendFailures, codec))
	}

	if p.cfg.PassThroughNonRegisteredRoutes {
		for _, backend := range p.backends {
			if backend.Preferred() {
				router.PathPrefix("/").Handler(httputil.NewSingleHostReverseProxy(backend.Endpoint()))
				break
			}
		}
	}

	p.server = serv

	// Run in a dedicated goroutine.
	p.done.Add(1)
	go func() {
		defer p.done.Done()

		if err := p.server.Run(); err != nil {
			level.Error(p.logger).Log("msg", "Proxy server failed", "err", err)
		}
	}()

	level.Info(p.logger).Log("msg", "The proxy is up and running.", "httpPort", p.cfg.Server.HTTPListenPort, "grpcPort", p.cfg.Server.GRPCListenPort)
	return nil
}

func (p *Proxy) Stop() error {
	if p.server == nil {
		return nil
	}

	p.server.Shutdown()
	return nil
}

func (p *Proxy) Await() {
	// Wait until terminated.
	p.done.Wait()
}
