// Package httphttp provides an implementtion of httpgrpc.HTTPClient on top of net/http
package httphttp

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/grafana/dskit/httpgrpc"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
)

// HTTPClient is a httpgrpc.HTTPClient implementation using net/http.
type HTTPClient struct {
	address string
	client  *http.Client
	tracer  opentracing.Tracer
}

// Config for a gRPC client.
type Config struct {
	ConnectTimeout time.Duration `yaml:"connect_timeout" category:"advanced"`
}

// RegisterFlagsWithPrefix registers flags with the provided prefix.
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.DurationVar(&cfg.ConnectTimeout, prefix+".connect-timeout", 30*time.Second, "Timeout for establishing a connection to the query frontend.")
}

// NewHTTPClient creates a new HTTPClient that connects to the given address with the provided configuration.
//
// If a tracer is provided, requests will be traced using opentracing.
func NewHTTPClient(address string, cfg *Config, tracer opentracing.Tracer) *HTTPClient {
	// use a copy of http.DefaultTransport with configuration applied
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{
		Timeout:   cfg.ConnectTimeout,
		KeepAlive: 30 * time.Second,
	}).DialContext

	// setup tracing if tracer is set
	var roundTripper http.RoundTripper = transport
	if tracer != nil {
		roundTripper = &nethttp.Transport{RoundTripper: transport}
	}

	client := &http.Client{
		Transport: roundTripper,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return errors.New("httphttp.HTTPClient does not follow redirects")
		},
	}

	return &HTTPClient{
		address: address,
		client:  client,
		tracer:  tracer,
	}
}

// Handle implements httpgrpc.HTTPClient by making a HTTP request to the configured address.
func (c *HTTPClient) Handle(ctx context.Context, in *httpgrpc.HTTPRequest, opts ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
	if len(opts) > 0 {
		return nil, errors.New("GRPC options aren't supported when using plain HTTP")
	}

	// build http.Request from httpgrpc.HTTPRequest
	req, err := http.NewRequestWithContext(ctx, in.Method, c.address+in.Url, bytes.NewReader(in.Body))
	if err != nil {
		return nil, err
	}

	httpgrpc.ToHeader(in.Headers, req.Header)

	// setup tracing if tracer is set
	if c.tracer != nil {
		var ht *nethttp.Tracer
		req, ht = nethttp.TraceRequest(c.tracer, req)
		defer ht.Finish()
	}

	// perform the request
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// build httpgrpc.HTTPResponse from http.Response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return &httpgrpc.HTTPResponse{
		Code:    int32(resp.StatusCode),
		Headers: httpgrpc.FromHeader(resp.Header),
		Body:    body,
	}, nil
}
