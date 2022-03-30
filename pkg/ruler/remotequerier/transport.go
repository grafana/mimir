// SPDX-License-Identifier: AGPL-3.0-only

package remotequerier

import (
	"flag"
	"time"

	"github.com/grafana/dskit/crypto/tls"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/grafana/mimir/pkg/util/httpgrpcutil"
)

const (
	keepAlive        = time.Second * 10
	keepAliveTimeout = time.Second * 5

	serviceConfig = `{"loadBalancingPolicy": "round_robin"}`
)

// Config defines remote querier transport configuration.
type Config struct {
	// The address of the remote querier to connect to.
	Address string `yaml:"address"`

	// TLSEnabled tells whether TLS should be used to establish remote connection.
	TLSEnabled bool `yaml:"tls_enabled" category:"advanced"`

	// TLS is the config for client TLS.
	TLS tls.ClientConfig `yaml:",inline"`
}

func (c *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&c.Address,
		"ruler.querier.address",
		"",
		"GRPC listen address of the remote querier(s). Must be a DNS address (prefixed with dns:///) "+
			"to enable client side load balancing.")

	f.BoolVar(&c.TLSEnabled, "ruler.querier.tls-enabled", false, "Set to true if remote querier connection requires TLS.")

	c.TLS.RegisterFlagsWithPrefix("ruler.querier", f)
}

// NewTransport creates and initializes a new ruler Transport instance.
func NewTransport(cfg Config) (httpgrpcutil.RoundTripper, error) {
	tlsDialOptions, err := cfg.TLS.GetGRPCDialOptions(cfg.TLSEnabled)
	if err != nil {
		return nil, err
	}
	dialOptions := append(
		[]grpc.DialOption{
			grpc.WithKeepaliveParams(
				keepalive.ClientParameters{
					Time:                keepAlive,
					Timeout:             keepAliveTimeout,
					PermitWithoutStream: true,
				},
			),
			grpc.WithUnaryInterceptor(
				grpc_middleware.ChainUnaryClient(
					otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
					middleware.ClientUserHeaderInterceptor,
				),
			),
			grpc.WithDefaultServiceConfig(serviceConfig),
		},
		tlsDialOptions...,
	)

	conn, err := grpc.Dial(cfg.Address, dialOptions...)
	if err != nil {
		return nil, err
	}
	return &httpgrpcutil.Transport{
		Client: httpgrpc.NewHTTPClient(conn),
	}, nil
}
