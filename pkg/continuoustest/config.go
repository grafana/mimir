// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"flag"

	"github.com/grafana/dskit/log"
)

type Config struct {
	ServerMetricsPort   int                      
	LogLevel            log.Level                 
	Client              ClientConfig              `yaml:"mimir_client"`
	Manager             ManagerConfig             `yaml:"manager"`
	WriteReadSeriesTest WriteReadSeriesTestConfig `yaml:"write_read_series_test"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.Client.RegisterFlags(f)
	cfg.Manager.RegisterFlags(f)
	cfg.WriteReadSeriesTest.RegisterFlags(f)
}
