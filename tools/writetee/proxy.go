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

	BackendEndpoints         string
	PreferredBackend         string
	BackendReadTimeout       time.Duration
	BackendSkipTLSVerify     bool
	LogSlowResponseThreshold time.Duration
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

	f.StringVar(&cfg.BackendEndpoints, "backend.endpoints", "",
		"Comma-separated list of backend endpoints to write to. If the client request contains basic auth, it will be forwarded to the backend. "+
			"Basic auth is also accepted as part of the endpoint URL and takes precedence over the basic auth in the client request. "+
			"If the endpoint URL doesn't contain basic auth password, then the basic auth password from the client request is used. "+
			"If the endpoint basic auth username is __REQUEST_HEADER_X_SCOPE_ORGID__, then the value of the X-Scope-OrgID header will be used as the username.",
	)
	f.BoolVar(&cfg.BackendSkipTLSVerify, "backend.skip-tls-verify", false, "Skip TLS verification on backend targets.")
	f.StringVar(&cfg.PreferredBackend, "backend.preferred", "", "The hostname of the preferred backend when selecting the response to send back to the client. If no preferred backend is configured then the write-tee will send back to the client the first successful response received without waiting for other backends.")
	f.DurationVar(&cfg.BackendReadTimeout, "backend.read-timeout", 90*time.Second, "The timeout when reading the response from a backend.")
	f.DurationVar(&cfg.LogSlowResponseThreshold, "proxy.log-slow-response-threshold", 10*time.Second, "The minimum difference in response time between slowest and fastest back-end over which to log the request. 0 to disable.")
	cfg.registerServerFlagsWithChangedDefaultValues(f)
}

type Route struct {
	Path      string
	RouteName string
	Methods   []string
}

type Proxy struct {
	cfg        ProxyConfig
	backends   []ProxyBackend
	logger     log.Logger
	registerer prometheus.Registerer
	metrics    *ProxyMetrics
	routes     []Route

	// The HTTP server used to run the proxy service.
	server *server.Server

	// Wait group used to wait until the server has done.
	done sync.WaitGroup
}

func NewProxy(cfg ProxyConfig, logger log.Logger, routes []Route, registerer prometheus.Registerer) (*Proxy, error) {
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

		p.backends = append(p.backends, NewProxyBackend(name, u, cfg.BackendReadTimeout, preferred, cfg.BackendSkipTLSVerify))
	}

	// At least 1 backend is required
	if len(p.backends) < 1 {
		return nil, errors.New("at least 1 backend is required")
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

	// At least 2 backends are suggested
	if len(p.backends) < 2 {
		level.Warn(p.logger).Log("msg", "The proxy is running with only 1 backend. At least 2 backends are required to fulfil the purpose of the proxy and fan out writes.")
	}

	return p, nil
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

	// register routes
	for _, route := range p.routes {
		router.Path(route.Path).Methods(route.Methods...).Handler(NewProxyEndpoint(p.backends, route, p.metrics, p.logger, p.cfg.LogSlowResponseThreshold))
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

	level.Info(p.logger).Log("msg", "The write-tee proxy is up and running.", "httpPort", p.cfg.Server.HTTPListenPort)
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
