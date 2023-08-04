// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/client/client.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	"flag"

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
func MakeIngesterClient(addr string, cfg Config, metrics *Metrics) (HealthAndIngesterClient, error) {
	dialOpts, err := cfg.GRPCClientConfig.DialOption(grpcclient.Instrument(metrics.RequestDuration))
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
	GRPCClientConfig grpcclient.Config `yaml:"grpc_client_config" doc:"description=Configures the gRPC client used to communicate with ingesters from distributors, queriers and rulers."`
}

// RegisterFlags registers configuration settings used by the ingester client config.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.GRPCClientConfig.RegisterFlagsWithPrefix("ingester.client", f)
}

func (cfg *Config) Validate() error {
	return cfg.GRPCClientConfig.Validate()
}

type CombinedQueryStreamResponse struct {
	Chunkseries     []TimeSeriesChunk
	Timeseries      []mimirpb.TimeSeries
	StreamingSeries []StreamingSeries
}
