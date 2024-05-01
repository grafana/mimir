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
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/mimir/pkg/mimirpb"
	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/util"
)

const deprecatedReportGRPCStatusCodesFlag = "ingester.client.report-grpc-codes-in-instrumentation-label-enabled" // Deprecated. TODO: Remove in Mimir 2.14.

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
func MakeIngesterClient(inst ring.InstanceDesc, cfg Config, metrics *Metrics, logger log.Logger) (HealthAndIngesterClient, error) {
	logger = log.With(logger, "component", "ingester-client")
	var reportGRPCStatusesOptions []middleware.InstrumentationOption
	if cfg.DeprecatedReportGRPCStatusCodes {
		reportGRPCStatusesOptions = []middleware.InstrumentationOption{middleware.ReportGRPCStatusOption}
	}
	unary, stream := grpcclient.Instrument(metrics.requestDuration, reportGRPCStatusesOptions...)
	if cfg.CircuitBreaker.Enabled {
		unary = append([]grpc.UnaryClientInterceptor{NewCircuitBreaker(inst, cfg.CircuitBreaker, metrics, logger)}, unary...)
	}
	unary = append(unary, querierapi.ReadConsistencyClientUnaryInterceptor)
	stream = append(stream, querierapi.ReadConsistencyClientStreamInterceptor)

	dialOpts, err := cfg.GRPCClientConfig.DialOption(unary, stream)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.Dial(inst.Addr, dialOpts...)
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
	GRPCClientConfig                grpcclient.Config    `yaml:"grpc_client_config" doc:"description=Configures the gRPC client used to communicate with ingesters from distributors, queriers and rulers."`
	CircuitBreaker                  CircuitBreakerConfig `yaml:"circuit_breaker"`
	DeprecatedReportGRPCStatusCodes bool                 `yaml:"report_grpc_codes_in_instrumentation_label_enabled" category:"deprecated"` // Deprecated: Deprecated in Mimir 2.12, remove in Mimir 2.14 (https://github.com/grafana/mimir/issues/6008#issuecomment-1854320098)
}

// RegisterFlags registers configuration settings used by the ingester client config.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.GRPCClientConfig.RegisterFlagsWithPrefix("ingester.client", f)
	cfg.CircuitBreaker.RegisterFlagsWithPrefix("ingester.client", f)
	// The ingester.client.report-grpc-codes-in-instrumentation-label-enabled flag has been deprecated.
	// According to the migration plan (https://github.com/grafana/mimir/issues/6008#issuecomment-1854320098)
	// the default behaviour of Mimir should be as this flag were set to true.
	// TODO: Remove in Mimir 2.14.0
	f.BoolVar(&cfg.DeprecatedReportGRPCStatusCodes, deprecatedReportGRPCStatusCodesFlag, true, "If set to true, gRPC status codes will be reported in \"status_code\" label of \"cortex_ingester_client_request_duration_seconds\" metric. Otherwise, they will be reported as \"error\"")
}

func (cfg *Config) Validate(logger log.Logger) error {
	if err := cfg.GRPCClientConfig.Validate(); err != nil {
		return err
	}

	if !cfg.DeprecatedReportGRPCStatusCodes {
		util.WarnDeprecatedConfig(deprecatedReportGRPCStatusCodesFlag, logger)
	}

	return cfg.CircuitBreaker.Validate()
}

type CircuitBreakerConfig struct {
	Enabled                   bool          `yaml:"enabled" category:"experimental"`
	FailureThreshold          uint          `yaml:"failure_threshold" category:"experimental"`
	FailureExecutionThreshold uint          `yaml:"failure_execution_threshold" category:"experimental"`
	ThresholdingPeriod        time.Duration `yaml:"thresholding_period" category:"experimental"`
	CooldownPeriod            time.Duration `yaml:"cooldown_period" category:"experimental"`
}

func (cfg *CircuitBreakerConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, prefix+".circuit-breaker.enabled", false, "Enable circuit breaking when making requests to ingesters")
	f.UintVar(&cfg.FailureThreshold, prefix+".circuit-breaker.failure-threshold", 10, "Max percentage of requests that can fail over period before the circuit breaker opens")
	f.UintVar(&cfg.FailureExecutionThreshold, prefix+".circuit-breaker.failure-execution-threshold", 100, "How many requests must have been executed in period for the circuit breaker to be eligible to open for the rate of failures")
	f.DurationVar(&cfg.ThresholdingPeriod, prefix+".circuit-breaker.thresholding-period", time.Minute, "Moving window of time that the percentage of failed requests is computed over")
	f.DurationVar(&cfg.CooldownPeriod, prefix+".circuit-breaker.cooldown-period", 10*time.Second, "How long the circuit breaker will stay in the open state before allowing some requests")
}

func (cfg *CircuitBreakerConfig) Validate() error {
	return nil
}

type CombinedQueryStreamResponse struct {
	Chunkseries     []TimeSeriesChunk
	Timeseries      []mimirpb.TimeSeries
	StreamingSeries []StreamingSeries
}
