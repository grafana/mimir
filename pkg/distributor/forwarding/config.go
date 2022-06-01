// SPDX-License-Identifier: AGPL-3.0-only

package forwarding

import (
	"flag"
	"time"
)

type Config struct {
	Enabled         bool          `yaml:"enabled" category:"experimental"`
	RequestTimeout  time.Duration `yaml:"request_timeout" category:"experimental"`
	PropagateErrors bool          `yaml:"propagate_errors" category:"experimental"`
}

func (c *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&c.Enabled, "distributor.forwarding.enabled", false, "Enables the feature to forward certain metrics in remote_write requests, depending on defined rules.")
	f.DurationVar(&c.RequestTimeout, "distributor.forwarding.request-timeout", 10*time.Second, "Timeout for requests to ingestion endpoints to which we forward metrics.")
	f.BoolVar(&c.PropagateErrors, "distributor.forwarding.propagate-errors", true, "If disabled then forwarding requests are always considered to be successful, errors are ignored.")
}
