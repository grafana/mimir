// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"flag"

	"github.com/grafana/dskit/log"
)

type Config struct {
	ServerMetricsPort   int                       `yaml:"server_metrics_port"`
	LogLevel            log.Level                 `yaml:"log_level"`
	Client              ClientConfig              `yaml:"mimir_client"`
	Manager             ManagerConfig             `yaml:"manager"`
	WriteReadSeriesTest WriteReadSeriesTestConfig `yaml:"write_read_series_test"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.ServerMetricsPort, "server.metrics-port", 9900, "The port where metrics are exposed.")
	cfg.Client.RegisterFlags(f)
	cfg.Manager.RegisterFlags(f)
	cfg.WriteReadSeriesTest.RegisterFlags(f)
}
