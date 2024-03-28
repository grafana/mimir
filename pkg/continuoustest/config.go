// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"flag"

	"github.com/grafana/dskit/log"
)

type Config struct {
	ServerMetricsPort   int
	LogLevel            log.Level
	Client              ClientConfig
	Manager             ManagerConfig
	WriteReadSeriesTest WriteReadSeriesTestConfig
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.Client.RegisterFlags(f)
	cfg.Manager.RegisterFlags(f)
	cfg.WriteReadSeriesTest.RegisterFlags(f)
}
