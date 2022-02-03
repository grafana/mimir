// SPDX-License-Identifier: AGPL-3.0-only

package v2

import (
	"flag"
	"time"

	"github.com/grafana/dskit/flagext"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/util/grpcclient"
)

type Config struct {
	SchedulerAddress  string            `yaml:"scheduler_address"`
	DNSLookupPeriod   time.Duration     `yaml:"scheduler_dns_lookup_period"`
	WorkerConcurrency int               `yaml:"scheduler_worker_concurrency"`
	GRPCClientConfig  grpcclient.Config `yaml:"grpc_client_config"`

	// Used to find local IP address, that is sent to scheduler and querier-worker.
	InfNames []string `yaml:"instance_interface_names"`

	// If set, address is not computed from interfaces.
	Addr string `yaml:"address" doc:"hidden"`
	Port int    `doc:"hidden"`
}

const (
	// Sent to scheduler successfully, and frontend should wait for response now.
	waitForResponse enqueueStatus = iota

	// Failed to forward request to scheduler, frontend will try again.
	failed
)

type enqueueStatus int

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.SchedulerAddress, "frontend.scheduler-address", "", "DNS hostname used for finding query-schedulers.")
	f.DurationVar(&cfg.DNSLookupPeriod, "frontend.scheduler-dns-lookup-period", 10*time.Second, "How often to resolve the scheduler-address, in order to look for new query-scheduler instances.")
	f.IntVar(&cfg.WorkerConcurrency, "frontend.scheduler-worker-concurrency", 5, "Number of concurrent workers forwarding queries to single query-scheduler.")

	cfg.InfNames = []string{"eth0", "en0"}
	f.Var((*flagext.StringSlice)(&cfg.InfNames), "frontend.instance-interface-names", "Name of network interface to read address from. This address is sent to query-scheduler and querier, which uses it to send the query response back to query-frontend.")
	f.StringVar(&cfg.Addr, "frontend.instance-addr", "", "IP address to advertise to querier (via scheduler) (resolved via interfaces by default).")
	f.IntVar(&cfg.Port, "frontend.instance-port", 0, "Port to advertise to querier (via scheduler) (defaults to server.grpc-listen-port).")

	cfg.GRPCClientConfig.RegisterFlagsWithPrefix("frontend.grpc-client-config", f)
}
