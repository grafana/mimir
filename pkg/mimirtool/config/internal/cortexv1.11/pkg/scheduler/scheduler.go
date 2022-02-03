// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"flag"
	"time"

	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/util/grpcclient"
)

type Config struct {
	MaxOutstandingPerTenant int               `yaml:"max_outstanding_requests_per_tenant"`
	QuerierForgetDelay      time.Duration     `yaml:"querier_forget_delay"`
	GRPCClientConfig        grpcclient.Config `yaml:"grpc_client_config" doc:"description=This configures the gRPC client used to report errors back to the query-frontend."`
}

var (
	errSchedulerIsNotRunning = errors.New("scheduler is not running")
)

type Limits interface {
	// MaxQueriersPerUser returns max queriers to use per tenant, or 0 if shuffle sharding is disabled.
	MaxQueriersPerUser(user string) int
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.MaxOutstandingPerTenant, "query-scheduler.max-outstanding-requests-per-tenant", 100, "Maximum number of outstanding requests per tenant per query-scheduler. In-flight requests above this limit will fail with HTTP response status code 429.")
	f.DurationVar(&cfg.QuerierForgetDelay, "query-scheduler.querier-forget-delay", 0, "If a querier disconnects without sending notification about graceful shutdown, the query-scheduler will keep the querier in the tenant's shard until the forget delay has passed. This feature is useful to reduce the blast radius when shuffle-sharding is enabled.")
	cfg.GRPCClientConfig.RegisterFlagsWithPrefix("query-scheduler.grpc-client-config", f)
}
