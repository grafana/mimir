// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/client/client.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	"flag"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/grpcclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// HealthAndIngesterClient is the union of IngesterClient and grpc_health_v1.HealthClient.
type HealthAndIngesterClient interface {
	IngesterClient
	grpc_health_v1.HealthClient
	Close() error
}

type closableHealthAndIngesterClient struct {
	IngesterClient
	grpc_health_v1.HealthClient
	conn *grpc.ClientConn
}

// MakeIngesterClient makes a new IngesterClient
func MakeIngesterClient(addr string, cfg Config, metrics *Metrics, logger log.Logger) (HealthAndIngesterClient, error) {
	logger = log.With(logger, "component", "ingester-client")
	unary, stream := grpcclient.Instrument(metrics.requestDuration)
	if cfg.CircuitBreaker.Enabled {
		unary = append([]grpc.UnaryClientInterceptor{NewCircuitBreaker(addr, cfg.CircuitBreaker, metrics, logger)}, unary...)
	}

	dialOpts, err := cfg.GRPCClientConfig.DialOption(unary, stream)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.Dial(addr, dialOpts...)
	if err != nil {
		return nil, err
	}

	ingClient := NewIngesterClient(conn)
	ingClient = newBufferPoolingIngesterClient(ingClient, conn)

	return &closableHealthAndIngesterClient{
		IngesterClient: ingClient,
		HealthClient:   grpc_health_v1.NewHealthClient(conn),
		conn:           conn,
	}, nil
}

func (c *closableHealthAndIngesterClient) Close() error {
	return c.conn.Close()
}

// Config is the configuration struct for the ingester client
type Config struct {
	GRPCClientConfig grpcclient.Config    `yaml:"grpc_client_config" doc:"description=Configures the gRPC client used to communicate with ingesters from distributors, queriers and rulers."`
	CircuitBreaker   CircuitBreakerConfig `yaml:"circuit_breaker"`
}

// RegisterFlags registers configuration settings used by the ingester client config.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.GRPCClientConfig.RegisterFlagsWithPrefix("ingester.client", f)
	cfg.CircuitBreaker.RegisterFlagsWithPrefix("ingester.client", f)
}

func (cfg *Config) Validate() error {
	if err := cfg.GRPCClientConfig.Validate(); err != nil {
		return err
	}

	return cfg.CircuitBreaker.Validate()
}

type CircuitBreakerConfig struct {
	Enabled                   bool          `yaml:"enabled" category:"experimental"`
	FailureThreshold          uint          `yaml:"failure_threshold" category:"experimental"`
	FailureExecutionThreshold uint          `yaml:"failure_execution_threshold" category:"experimental"`
	Period                    time.Duration `yaml:"period" category:"experimental"`
	CooldownPeriod            time.Duration `yaml:"cooldown_period" category:"experimental"`
}

func (cfg *CircuitBreakerConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, prefix+".circuit-breaker.enabled", false, "Enable circuit breaking when making requests to ingesters")
	f.UintVar(&cfg.FailureThreshold, prefix+".circuit-breaker.failure-threshold", 10, "Max percentage of requests that can fail over period before the circuit breaker opens")
	f.UintVar(&cfg.FailureExecutionThreshold, prefix+".circuit-breaker.failure-execution-threshold", 100, "How many requests must have been executed in period for the circuit breaker to be eligible to open for the rate of failures")
	f.DurationVar(&cfg.Period, prefix+".circuit-breaker.period", 10*time.Second, "Moving window of time that the percentage of failed requests is computed over")
	f.DurationVar(&cfg.CooldownPeriod, prefix+".circuit-breaker.cooldown-period", 1*time.Minute, "How long the circuit breaker will stay in the open state before allowing some requests")
}

func (cfg *CircuitBreakerConfig) Validate() error {
	return nil
}

type CombinedQueryStreamResponse struct {
	Chunkseries     []TimeSeriesChunk
	Timeseries      []mimirpb.TimeSeries
	StreamingSeries []StreamingSeries
}
