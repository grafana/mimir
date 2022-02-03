// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"flag"
	"time"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/util/grpcclient"
)

type Config struct {
	FrontendAddress  string        `yaml:"frontend_address"`
	SchedulerAddress string        `yaml:"scheduler_address"`
	DNSLookupPeriod  time.Duration `yaml:"dns_lookup_duration"`

	Parallelism         int  `yaml:"parallelism"`
	MatchMaxConcurrency bool `yaml:"match_max_concurrent"`

	QuerierID string `yaml:"id"`

	GRPCClientConfig grpcclient.Config `yaml:"grpc_client_config"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.SchedulerAddress, "querier.scheduler-address", "", "Hostname (and port) of scheduler that querier will periodically resolve, connect to and receive queries from. Only one of -querier.frontend-address or -querier.scheduler-address can be set. If neither is set, queries are only received via HTTP endpoint.")
	f.StringVar(&cfg.FrontendAddress, "querier.frontend-address", "", "Address of query frontend service, in host:port format. If -querier.scheduler-address is set as well, querier will use scheduler instead. Only one of -querier.frontend-address or -querier.scheduler-address can be set. If neither is set, queries are only received via HTTP endpoint.")

	f.DurationVar(&cfg.DNSLookupPeriod, "querier.dns-lookup-period", 10*time.Second, "How often to query DNS for query-frontend or query-scheduler address.")

	f.IntVar(&cfg.Parallelism, "querier.worker-parallelism", 10, "Number of simultaneous queries to process per query-frontend or query-scheduler.")
	f.BoolVar(&cfg.MatchMaxConcurrency, "querier.worker-match-max-concurrent", false, "Force worker concurrency to match the -querier.max-concurrent option. Overrides querier.worker-parallelism.")
	f.StringVar(&cfg.QuerierID, "querier.id", "", "Querier ID, sent to frontend service to identify requests from the same querier. Defaults to hostname.")

	cfg.GRPCClientConfig.RegisterFlagsWithPrefix("querier.frontend-client", f)
}
