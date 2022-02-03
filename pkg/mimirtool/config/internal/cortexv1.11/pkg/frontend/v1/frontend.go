// SPDX-License-Identifier: AGPL-3.0-only

package v1

import (
	"flag"
	"net/http"
	"time"

	"github.com/weaveworks/common/httpgrpc"
)

type Config struct {
	MaxOutstandingPerTenant int           `yaml:"max_outstanding_per_tenant"`
	QuerierForgetDelay      time.Duration `yaml:"querier_forget_delay"`
}

var (
	errTooManyRequest = httpgrpc.Errorf(http.StatusTooManyRequests, "too many outstanding requests")
)

type Limits interface {
	// Returns max queriers to use per tenant, or 0 if shuffle sharding is disabled.
	MaxQueriersPerUser(user string) int
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.MaxOutstandingPerTenant, "querier.max-outstanding-requests-per-tenant", 100, "Maximum number of outstanding requests per tenant per frontend; requests beyond this error with HTTP 429.")
	f.DurationVar(&cfg.QuerierForgetDelay, "query-frontend.querier-forget-delay", 0, "If a querier disconnects without sending notification about graceful shutdown, the query-frontend will keep the querier in the tenant's shard until the forget delay has passed. This feature is useful to reduce the blast radius when shuffle-sharding is enabled.")
}
