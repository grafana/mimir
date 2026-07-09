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
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/server"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("tools/write-tee")

const (
	defaultReportGRPCCodesInInstrumentationLabel = true

	defaultHTTPServerReadTimeOut  = 1 * time.Minute
	defaultHTTPServerWriteTimeout = 2 * time.Minute
)

type ProxyConfig struct {
	Server server.Config

	BackendEndpoint            string
	AmplificationFactor        float64
	BackendReadTimeout         time.Duration
	BackendSkipTLSVerify       bool
	AsyncMaxInFlightPerBackend int

	HTTPConnectionTTLMin                time.Duration
	HTTPConnectionTTLMax                time.Duration
	HTTPConnectionTTLIdleCheckFrequency time.Duration

	GRPCMaxRecvMsgSize int
	GRPCMaxSendMsgSize int
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

	f.StringVar(&cfg.BackendEndpoint, "backend.endpoint", "",
		"The backend endpoint to send writes to. Required. "+
			"The original (unmodified) request is sent synchronously and its response is returned to the client. "+
			"When amplification-factor > 1, additional suffixed copies are sent asynchronously (fire-and-forget). "+
			"If the client request contains basic auth, it will be forwarded to the backend. "+
			"Basic auth is also accepted as part of the endpoint URL and takes precedence over the basic auth in the client request. "+
			"If the endpoint URL doesn't contain basic auth password, then the basic auth password from the client request is used. "+
			"If the endpoint basic auth username is __REQUEST_HEADER_X_SCOPE_ORGID__, then the value of the X-Scope-OrgID header will be used as the username.",
	)
	f.Float64Var(&cfg.AmplificationFactor, "backend.amplification-factor", 1.0,
		"The factor by which to amplify writes to the backend. Must be >= 1.0. "+
			"1.0 means no amplification (only the original request is sent). "+
			"Values > 1.0 amplify (duplicate) metrics: 3.5 means each metric is sent 3.5 times on average. "+
			"Amplified copies have all label values (except __name__) suffixed with _amp{N} where N is the replica number (starting at 2).",
	)
	f.BoolVar(&cfg.BackendSkipTLSVerify, "backend.skip-tls-verify", false, "Skip TLS verification on backend targets.")
	f.DurationVar(&cfg.BackendReadTimeout, "backend.read-timeout", 90*time.Second, "The timeout when reading the response from a backend.")
	f.IntVar(&cfg.AsyncMaxInFlightPerBackend, "backend.async-max-in-flight", 1000, "Maximum concurrent in-flight amplified requests (async fire-and-forget). Requests are dropped when at capacity.")
	f.DurationVar(&cfg.HTTPConnectionTTLMin, "server.http-connection-ttl-min", 0, "Minimum TTL for HTTP connections. Connections will be closed after a random duration between min and max TTL.")
	f.DurationVar(&cfg.HTTPConnectionTTLMax, "server.http-connection-ttl-max", 0, "Maximum TTL for HTTP connections. Set to 0 to disable connection TTL.")
	f.DurationVar(&cfg.HTTPConnectionTTLIdleCheckFrequency, "server.http-connection-ttl-idle-check-frequency", 30*time.Second, "Frequency at which idle connections are checked for TTL expiration.")
	f.IntVar(&cfg.GRPCMaxRecvMsgSize, "backend.grpc-max-recv-msg-size", 100*1024*1024, "Maximum size of received gRPC messages for HTTPgRPC backends (dns:// scheme).")
	f.IntVar(&cfg.GRPCMaxSendMsgSize, "backend.grpc-max-send-msg-size", 100*1024*1024, "Maximum size of sent gRPC messages for HTTPgRPC backends (dns:// scheme).")
	cfg.registerServerFlagsWithChangedDefaultValues(f)
}

type Route struct {
	Path      string
	RouteName string
	Methods   []string
}

type Proxy struct {
	cfg                  ProxyConfig
	backend              ProxyBackend
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

	// Endpoint is required.
	if strings.TrimSpace(cfg.BackendEndpoint) == "" {
		return nil, errors.New("backend endpoint is required (set -backend.endpoint)")
	}

	// The amplification factor must be >= 1.0 so the endpoint always receives the full original request.
	if cfg.AmplificationFactor < 1.0 {
		return nil, errors.New("amplification-factor must be >= 1.0")
	}

	// Validate async max in-flight.
	if cfg.AsyncMaxInFlightPerBackend <= 0 {
		return nil, errors.New("backend.async-max-in-flight must be greater than 0")
	}

	// Parse the single backend endpoint.
	backend, err := p.parseBackendEndpoint(cfg.BackendEndpoint)
	if err != nil {
		return nil, err
	}
	p.backend = backend

	// Create the async dispatcher for amplified (fire-and-forget) copies.
	p.asyncDispatcher = NewAsyncBackendDispatcher(cfg.AsyncMaxInFlightPerBackend, p.metrics, logger)

	return p, nil
}

func (p *Proxy) parseBackendEndpoint(endpoint string) (ProxyBackend, error) {
	endpoint = strings.TrimSpace(endpoint)

	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid backend endpoint %s", endpoint)
	}

	// The backend name is hardcoded as the backend hostname.
	name := u.Hostname()

	switch u.Scheme {
	case "http", "https":
		return NewHTTPProxyBackend(name, u, p.cfg.BackendReadTimeout, p.cfg.BackendSkipTLSVerify), nil
	case "dns":
		grpcCfg := GRPCBackendConfig{
			MaxRecvMsgSize: p.cfg.GRPCMaxRecvMsgSize,
			MaxSendMsgSize: p.cfg.GRPCMaxSendMsgSize,
		}
		return NewGRPCProxyBackend(name, u, p.cfg.BackendReadTimeout, grpcCfg)
	default:
		return nil, fmt.Errorf("unsupported backend scheme %q for endpoint %s (supported: http, https, dns)", u.Scheme, endpoint)
	}
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

	// Readiness endpoint for K8s probes.
	router.Path("/ready").Methods("GET").Handler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// register fan-out routes (explicit endpoints we want to amplify)
	for _, route := range p.routes {
		endpoint := NewProxyEndpoint(p.backend, route, p.metrics, p.logger, p.cfg.AmplificationFactor, p.amplificationTracker, p.asyncDispatcher)
		router.Path(route.Path).Methods(route.Methods...).Handler(endpoint)
	}

	// register catch-all passthrough route for unsupported endpoints
	// this must come AFTER specific routes so they take precedence
	passthroughRoute := Route{
		Path:      "/{path:.*}",
		RouteName: "passthrough",
		Methods:   []string{"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"},
	}
	passthroughEndpoint := NewProxyEndpoint(p.backend, passthroughRoute, p.metrics, p.logger, p.cfg.AmplificationFactor, p.amplificationTracker, p.asyncDispatcher)
	router.PathPrefix("/").Handler(http.HandlerFunc(passthroughEndpoint.ServeHTTPPassthrough))

	// Create HTTP connection TTL middleware if enabled.
	if p.cfg.HTTPConnectionTTLMax > 0 {
		ttlMiddleware, err := middleware.NewHTTPConnectionTTLMiddleware(
			p.cfg.HTTPConnectionTTLMin,
			p.cfg.HTTPConnectionTTLMax,
			p.cfg.HTTPConnectionTTLIdleCheckFrequency,
			prometheus.WrapRegistererWithPrefix("cortex_writetee_server_", p.registerer),
		)
		if err != nil {
			return errors.Wrap(err, "failed to create HTTP connection TTL middleware")
		}
		serv.HTTPServer.Handler = ttlMiddleware.Wrap(serv.HTTPServer.Handler)
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

	level.Info(p.logger).Log("msg", "The write-tee proxy is up and running.", "httpPort", p.cfg.Server.HTTPListenPort, "grpcPort", p.cfg.Server.GRPCListenPort)
	return nil
}

func (p *Proxy) Stop() error {
	if p.server == nil {
		return nil
	}

	p.server.Shutdown()

	// Stop the async dispatcher (prevents new dispatches, but doesn't wait for in-flight).
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

	// Close the backend after async requests have drained (important for gRPC backends to close connections).
	if p.backend != nil {
		if err := p.backend.Close(); err != nil {
			level.Warn(p.logger).Log("msg", "failed to close backend", "backend", p.backend.Name(), "err", err)
		}
	}
}
