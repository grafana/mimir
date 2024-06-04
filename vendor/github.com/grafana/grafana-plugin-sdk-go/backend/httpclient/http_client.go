package httpclient

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"

	"github.com/grafana/grafana-plugin-sdk-go/backend/proxy"
)

// New creates a new http.Client.
// If opts is nil the http.DefaultClient will be returned.
// If no middlewares are provided the DefaultMiddlewares will be used. If you
// provide middlewares you have to manually add the DefaultMiddlewares for it to be
// enabled.
// Note: Middlewares will be executed in the same order as provided.
// Note: If more than one Options is provided a panic is raised.
func New(opts ...Options) (*http.Client, error) {
	if opts == nil {
		return http.DefaultClient, nil
	}

	clientOpts := createOptions(opts...)
	transport, err := GetTransport(clientOpts)
	if err != nil {
		return nil, err
	}

	c := &http.Client{
		Transport: transport,
		Timeout:   clientOpts.Timeouts.Timeout,
	}

	if clientOpts.ConfigureClient != nil {
		clientOpts.ConfigureClient(clientOpts, c)
	}

	return c, nil
}

// GetTransport creates a new http.RoundTripper given provided options.
// If opts is nil the http.DefaultTransport will be returned.
// If no middlewares are provided the DefaultMiddlewares() will be used. If you
// provide middlewares you have to manually add the DefaultMiddlewares() for it to be
// enabled.
// Note: Middlewares will be executed in the same order as provided.
// Note: If more than one Options is provided a panic is raised.
func GetTransport(opts ...Options) (http.RoundTripper, error) {
	if opts == nil {
		return http.DefaultTransport, nil
	}

	clientOpts := createOptions(opts...)
	tlsConfig, err := GetTLSConfig(clientOpts)
	if err != nil {
		return nil, err
	}

	if clientOpts.ConfigureTLSConfig != nil {
		clientOpts.ConfigureTLSConfig(clientOpts, tlsConfig)
	}

	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
		Proxy:           http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   clientOpts.Timeouts.DialTimeout,
			KeepAlive: clientOpts.Timeouts.KeepAlive,
		}).DialContext,
		ResponseHeaderTimeout: clientOpts.Timeouts.Timeout,
		TLSHandshakeTimeout:   clientOpts.Timeouts.TLSHandshakeTimeout,
		ExpectContinueTimeout: clientOpts.Timeouts.ExpectContinueTimeout,
		MaxConnsPerHost:       clientOpts.Timeouts.MaxConnsPerHost,
		MaxIdleConns:          clientOpts.Timeouts.MaxIdleConns,
		MaxIdleConnsPerHost:   clientOpts.Timeouts.MaxIdleConnsPerHost,
		IdleConnTimeout:       clientOpts.Timeouts.IdleConnTimeout,
	}

	if clientOpts.ConfigureTransport != nil {
		clientOpts.ConfigureTransport(clientOpts, transport)
	}

	if clientOpts.ConfigureMiddleware != nil {
		clientOpts.Middlewares = clientOpts.ConfigureMiddleware(clientOpts, clientOpts.Middlewares)
	}

	pc := proxy.New(clientOpts.ProxyOptions)
	err = pc.ConfigureSecureSocksHTTPProxy(transport)
	if err != nil {
		return nil, err
	}

	return roundTripperFromMiddlewares(clientOpts, clientOpts.Middlewares, transport), nil
}

// GetTLSConfig creates a new tls.Config given provided options.
// Note: If more than one Options is provided a panic is raised.
func GetTLSConfig(opts ...Options) (*tls.Config, error) {
	clientOpts := createOptions(opts...)
	if clientOpts.TLS == nil {
		// #nosec
		return &tls.Config{}, nil
	}

	tlsOpts := clientOpts.TLS

	// #nosec
	config := &tls.Config{
		InsecureSkipVerify: tlsOpts.InsecureSkipVerify,
		ServerName:         tlsOpts.ServerName,
	}

	if len(tlsOpts.CACertificate) > 0 {
		caPool := x509.NewCertPool()
		ok := caPool.AppendCertsFromPEM([]byte(tlsOpts.CACertificate))
		if !ok {
			return nil, errors.New("failed to parse TLS CA PEM certificate")
		}
		config.RootCAs = caPool
	}

	if len(tlsOpts.ClientCertificate) > 0 && len(tlsOpts.ClientKey) > 0 {
		cert, err := tls.X509KeyPair([]byte(tlsOpts.ClientCertificate), []byte(tlsOpts.ClientKey))
		if err != nil {
			return nil, err
		}
		config.Certificates = []tls.Certificate{cert}
	}

	if tlsOpts.MinVersion > 0 {
		config.MinVersion = tlsOpts.MinVersion
	}

	if tlsOpts.MaxVersion > 0 {
		config.MaxVersion = tlsOpts.MaxVersion
	}

	return config, nil
}

func createOptions(providedOpts ...Options) Options {
	var opts Options

	switch len(providedOpts) {
	case 0:
		opts = Options{}
	case 1:
		opts = providedOpts[0]
	default:
		panic("only an empty or one Options is valid as argument")
	}

	if opts.Timeouts == nil {
		opts.Timeouts = &DefaultTimeoutOptions
	}

	if opts.Middlewares == nil {
		opts.Middlewares = DefaultMiddlewares()
	}

	return opts
}

// The RoundTripperFunc type is an adapter to allow the use of ordinary
// functions as RoundTrippers. If f is a function with the appropriate
// signature, RountTripperFunc(f) is a RoundTripper that calls f.
type RoundTripperFunc func(req *http.Request) (*http.Response, error)

// RoundTrip implements the RoundTripper interface.
func (rt RoundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return rt(r)
}

// Middleware is an interface representing the ability to create a middleware
// that implements the http.RoundTripper interface.
type Middleware interface {
	// CreateMiddleware creates a new middleware.
	CreateMiddleware(opts Options, next http.RoundTripper) http.RoundTripper
}

// The MiddlewareFunc type is an adapter to allow the use of ordinary
// functions as Middlewares. If f is a function with the appropriate
// signature, MiddlewareFunc(f) is a Middleware that calls f.
type MiddlewareFunc func(opts Options, next http.RoundTripper) http.RoundTripper

// CreateMiddleware implements the Middleware interface.
func (fn MiddlewareFunc) CreateMiddleware(opts Options, next http.RoundTripper) http.RoundTripper {
	return fn(opts, next)
}

// MiddlewareName is an interface representing the ability for a middleware to have a name.
type MiddlewareName interface {
	// MiddlewareName returns the middleware name.
	MiddlewareName() string
}

// ConfigureMiddlewareFunc function signature for configuring middleware chain.
type ConfigureMiddlewareFunc func(opts Options, existingMiddleware []Middleware) []Middleware

// DefaultMiddlewares is the default middleware applied when creating
// new HTTP clients and no middleware is provided.
// TracingMiddleware, BasicAuthenticationMiddleware and CustomHeadersMiddleware are
// the default middlewares.
func DefaultMiddlewares() []Middleware {
	return []Middleware{
		TracingMiddleware(nil),
		BasicAuthenticationMiddleware(),
		CustomHeadersMiddleware(),
		ContextualMiddleware(),
	}
}

func roundTripperFromMiddlewares(opts Options, middlewares []Middleware, finalRoundTripper http.RoundTripper) http.RoundTripper {
	reversed := reverseMiddlewares(middlewares)
	next := finalRoundTripper

	for _, m := range reversed {
		next = m.CreateMiddleware(opts, next)
	}

	return next
}

func reverseMiddlewares(middlewares []Middleware) []Middleware {
	reversed := make([]Middleware, len(middlewares))
	copy(reversed, middlewares)

	for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
		reversed[i], reversed[j] = reversed[j], reversed[i]
	}

	return reversed
}

type namedMiddleware struct {
	Name       string
	Middleware Middleware
}

// NamedMiddlewareFunc type is an adapter to allow the use of ordinary
// functions as Middleware. If f is a function with the appropriate
// signature, NamedMiddlewareFunc(f) is a Middleware that calls f.
func NamedMiddlewareFunc(name string, fn MiddlewareFunc) Middleware {
	return &namedMiddleware{
		Name:       name,
		Middleware: fn,
	}
}

func (nm *namedMiddleware) CreateMiddleware(opts Options, next http.RoundTripper) http.RoundTripper {
	return nm.Middleware.CreateMiddleware(opts, next)
}

func (nm *namedMiddleware) MiddlewareName() string {
	return nm.Name
}
