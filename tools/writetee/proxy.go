// SPDX-License-Identifier: AGPL-3.0-only

package writetee

import (
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/server"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("pkg/tools/writetee")

const (
	defaultReportGRPCCodesInInstrumentationLabel = true

	defaultHTTPServerReadTimeOut  = 1 * time.Minute
	defaultHTTPServerWriteTimeout = 2 * time.Minute
)

type ProxyConfig struct {
	Server server.Config

	BackendMirroredEndpoints      string
	BackendAmplifiedEndpoints     string
	AmplificationFactor           float64
	AmplifiedMaxSeriesPerRequest  int
	PreferredBackend              string
	BackendReadTimeout            time.Duration
	BackendSkipTLSVerify          bool
	AsyncMaxInFlightPerBackend    int
}

// registerServerFlagsWithChangedDefaultValues emulates the same method in pkg/mimir/mimir.go,
// as write-tee does not currently import and utilize mimir.Config.
func (cfg *ProxyConfig) registerServerFlagsWithChangedDefaultValues(fs *flag.FlagSet) {
	throwaway := flag.NewFlagSet("throwaway", flag.PanicOnError)

	// Register to throwaway flags first. Default values are remembered during registration and cannot be changed,
	// but we can take values from throwaway flag set and re-register into supplied flag set with new default values.
	cfg.Server.RegisterFlags(throwaway)

	defaultsOverrides := map[string]string{
		"server.report-grpc-codes-in-instrumentation-label-enabled": strconv.FormatBool(defaultReportGRPCCodesInInstrumentationLabel),

		"server.http-read-timeout":  defaultHTTPServerReadTimeOut.String(),
		"server.http-write-timeout": defaultHTTPServerWriteTimeout.String(),
	}

	throwaway.VisitAll(func(f *flag.Flag) {
		if defaultValue, overridden := defaultsOverrides[f.Name]; overridden {
			// Ignore errors when setting new values. We have a test to verify that it works.
			_ = f.Value.Set(defaultValue)
		}

		fs.Var(f.Value, f.Name, f.Usage)
	})
}

func (cfg *ProxyConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.Server.MetricsNamespace = writeTeeMetricsNamespace

	f.StringVar(&cfg.BackendMirroredEndpoints, "backend.mirrored-endpoints", "",
		"Comma-separated list of backend endpoints to mirror writes to (without amplification). "+
			"If the client request contains basic auth, it will be forwarded to the backend. "+
			"Basic auth is also accepted as part of the endpoint URL and takes precedence over the basic auth in the client request. "+
			"If the endpoint URL doesn't contain basic auth password, then the basic auth password from the client request is used. "+
			"If the endpoint basic auth username is __REQUEST_HEADER_X_SCOPE_ORGID__, then the value of the X-Scope-OrgID header will be used as the username.",
	)
	f.StringVar(&cfg.BackendAmplifiedEndpoints, "backend.amplified-endpoints", "",
		"Comma-separated list of backend endpoints to send amplified writes to. "+
			"Writes to these backends will have metrics duplicated based on the amplification-factor. "+
			"Same auth behavior as backend.mirrored-endpoints.",
	)
	f.Float64Var(&cfg.AmplificationFactor, "backend.amplification-factor", 1.0,
		"The factor by which to amplify or sample writes to amplified backends. "+
			"Values > 1.0 amplify (duplicate) metrics: 3.5 means each metric is duplicated 3.5 times on average. "+
			"Values < 1.0 sample (reduce) metrics: 0.1 means only 10% of metrics are sent. "+
			"Amplified metrics have all label values (except __name__) suffixed with _amp{N} where N is the replica number. "+
			"Only applies to backends specified in backend.amplified-endpoints.",
	)
	f.IntVar(&cfg.AmplifiedMaxSeriesPerRequest, "backend.amplified-max-series-per-request", 2000,
		"Maximum series per request when sending amplified writes to non-preferred backends. "+
			"If amplification results in more series, the request is split into multiple smaller requests. "+
			"Set to 0 to disable splitting.",
	)
	f.BoolVar(&cfg.BackendSkipTLSVerify, "backend.skip-tls-verify", false, "Skip TLS verification on backend targets.")
	f.StringVar(&cfg.PreferredBackend, "backend.preferred", "", "The hostname of the preferred backend. Required. Non-preferred backends receive fire-and-forget requests.")
	f.DurationVar(&cfg.BackendReadTimeout, "backend.read-timeout", 90*time.Second, "The timeout when reading the response from a backend.")
	f.IntVar(&cfg.AsyncMaxInFlightPerBackend, "backend.async-max-in-flight", 1000, "Maximum concurrent in-flight requests per non-preferred backend (async fire-and-forget). Requests are dropped when at capacity.")
	cfg.registerServerFlagsWithChangedDefaultValues(f)
}

type Route struct {
	Path      string
	RouteName string
	Methods   []string
}

type Proxy struct {
	cfg                  ProxyConfig
	backends             []ProxyBackend
	logger               log.Logger
	registerer           prometheus.Registerer
	metrics              *ProxyMetrics
	routes               []Route
	amplificationTracker *AmplificationTracker
	asyncDispatcher      *AsyncBackendDispatcher

	// The HTTP server used to run the proxy service.
	server *server.Server

	// Wait group used to wait until the server has done.
	done sync.WaitGroup
}

func NewProxy(cfg ProxyConfig, logger log.Logger, routes []Route, registerer prometheus.Registerer) (*Proxy, error) {
	cfg.Server.Registerer = registerer
	p := &Proxy{
		cfg:                  cfg,
		logger:               logger,
		registerer:           registerer,
		metrics:              NewProxyMetrics(registerer),
		routes:               routes,
		amplificationTracker: NewAmplificationTracker(),
	}

	// Parse mirrored backend endpoints (comma separated).
	if cfg.BackendMirroredEndpoints != "" {
		parts := strings.Split(cfg.BackendMirroredEndpoints, ",")
		for idx, part := range parts {
			backend, err := p.parseBackendEndpoint(part, idx, BackendTypeMirrored)
			if err != nil {
				return nil, err
			}
			if backend != nil {
				p.backends = append(p.backends, backend)
			}
		}
	}

	// Parse amplified backend endpoints (comma separated).
	if cfg.BackendAmplifiedEndpoints != "" {
		parts := strings.Split(cfg.BackendAmplifiedEndpoints, ",")
		baseIdx := len(p.backends) // offset for numeric preferred backend matching
		for idx, part := range parts {
			backend, err := p.parseBackendEndpoint(part, baseIdx+idx, BackendTypeAmplified)
			if err != nil {
				return nil, err
			}
			if backend != nil {
				p.backends = append(p.backends, backend)
			}
		}
	}

	// At least 1 backend is required
	if len(p.backends) < 1 {
		return nil, errors.New("at least 1 backend is required (specify backend.mirrored-endpoints or backend.amplified-endpoints)")
	}

	// Preferred backend is required
	if cfg.PreferredBackend == "" {
		return nil, errors.New("preferred backend is required (set -backend.preferred)")
	}

	// Validate amplification configuration
	hasAmplifiedBackend := false
	for _, b := range p.backends {
		if b.BackendType() == BackendTypeAmplified {
			hasAmplifiedBackend = true
			break
		}
	}
	if hasAmplifiedBackend && cfg.AmplificationFactor <= 0.0 {
		return nil, errors.New("amplification-factor must be > 0.0 when amplified backends are configured")
	}

	// The preferred backend must exist among the actual backends.
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

	// The preferred backend must not be an amplified backend.
	// This ensures the client always gets a predictable response from the preferred backend.
	for _, b := range p.backends {
		if b.Preferred() && b.BackendType() == BackendTypeAmplified {
			return nil, errors.New("the preferred backend cannot be an amplified backend; move it to backend.mirrored-endpoints")
		}
	}

	// At least 2 backends are suggested
	if len(p.backends) < 2 {
		level.Warn(p.logger).Log("msg", "The proxy is running with only 1 backend. At least 2 backends are required to fulfil the purpose of the proxy and fan out writes.")
	}

	// Validate async max in-flight
	if cfg.AsyncMaxInFlightPerBackend <= 0 {
		return nil, errors.New("backend.async-max-in-flight must be greater than 0")
	}

	// Create the async dispatcher for non-preferred backends
	p.asyncDispatcher = NewAsyncBackendDispatcher(cfg.AsyncMaxInFlightPerBackend, p.metrics, logger)

	return p, nil
}

func (p *Proxy) parseBackendEndpoint(endpoint string, idx int, backendType BackendType) (ProxyBackend, error) {
	// Skip empty ones.
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, nil
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid backend endpoint %s", endpoint)
	}

	// The backend name is hardcoded as the backend hostname.
	name := u.Hostname()
	preferred := name == p.cfg.PreferredBackend

	// In tests, we have the same hostname for all backends, so we also
	// support a numeric preferred backend which is the index in the list
	// of backends.
	if preferredIdx, err := strconv.Atoi(p.cfg.PreferredBackend); err == nil {
		preferred = preferredIdx == idx
	}

	return NewProxyBackend(name, u, p.cfg.BackendReadTimeout, preferred, p.cfg.BackendSkipTLSVerify, backendType), nil
}

func (p *Proxy) Start() error {
	p.cfg.Server.MetricsNamespace = writeTeeMetricsNamespace
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

	// register fan-out routes (explicit endpoints we want to mirror)
	for _, route := range p.routes {
		endpoint, err := NewProxyEndpoint(p.backends, route, p.metrics, p.logger, p.cfg.AmplificationFactor, p.cfg.AmplifiedMaxSeriesPerRequest, p.amplificationTracker, p.asyncDispatcher)
		if err != nil {
			return err
		}
		router.Path(route.Path).Methods(route.Methods...).Handler(endpoint)
	}

	// register catch-all passthrough route for unsupported endpoints
	// this must come AFTER specific routes so they take precedence
	passthroughRoute := Route{
		Path:      "/{path:.*}",
		RouteName: "passthrough",
		Methods:   []string{"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"},
	}
	passthroughEndpoint, err := NewProxyEndpoint(p.backends, passthroughRoute, p.metrics, p.logger, p.cfg.AmplificationFactor, p.cfg.AmplifiedMaxSeriesPerRequest, p.amplificationTracker, p.asyncDispatcher)
	if err != nil {
		return err
	}
	router.PathPrefix("/").Handler(http.HandlerFunc(passthroughEndpoint.ServeHTTPPassthrough))

	p.server = serv

	// Run in a dedicated goroutine.
	p.done.Add(1)
	go func() {
		defer p.done.Done()

		if err := p.server.Run(); err != nil {
			level.Error(p.logger).Log("msg", "Proxy server failed", "err", err)
		}
	}()

	level.Info(p.logger).Log("msg", "The write-tee proxy is up and running.", "httpPort", p.cfg.Server.HTTPListenPort)
	return nil
}

func (p *Proxy) Stop() error {
	if p.server == nil {
		return nil
	}

	p.server.Shutdown()

	// Stop the async dispatcher
	if p.asyncDispatcher != nil {
		p.asyncDispatcher.Stop()
	}

	return nil
}

func (p *Proxy) Await() {
	// Wait until terminated.
	p.done.Wait()

	// Wait for async dispatcher to drain in-flight requests.
	if p.asyncDispatcher != nil {
		p.asyncDispatcher.Await()
	}
}
