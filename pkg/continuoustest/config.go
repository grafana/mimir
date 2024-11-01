// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"flag"
)

type Config struct {
	Client                      ClientConfig                      `yaml:"-"`
	Manager                     ManagerConfig                     `yaml:"-"`
	WriteReadSeriesTest         WriteReadSeriesTestConfig         `yaml:"-"`
	RecordingRuleReadSeriesTest RecordingRuleReadSeriesTestConfig `yaml:"-"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.Client.RegisterFlags(f)
	cfg.Manager.RegisterFlags(f)
	cfg.WriteReadSeriesTest.RegisterFlags(f)
	cfg.RecordingRuleReadSeriesTest.RegisterFlags(f)
}
