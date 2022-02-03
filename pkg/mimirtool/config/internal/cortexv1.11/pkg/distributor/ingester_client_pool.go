// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"flag"
	"time"
)

type PoolConfig struct {
	ClientCleanupPeriod  time.Duration `yaml:"client_cleanup_period"`
	HealthCheckIngesters bool          `yaml:"health_check_ingesters"`
}

func (cfg *PoolConfig) RegisterFlags(f *flag.FlagSet) {
	f.DurationVar(&cfg.ClientCleanupPeriod, "distributor.client-cleanup-period", 15*time.Second, "How frequently to clean up clients for ingesters that have gone away.")
	f.BoolVar(&cfg.HealthCheckIngesters, "distributor.health-check-ingesters", true, "Run a health check on each ingester client during periodic cleanup.")
}
