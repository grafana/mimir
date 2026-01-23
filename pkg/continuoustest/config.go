// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"flag"
)

type Config struct {
	Client                  ClientConfig                  `yaml:"-"`
	Manager                 ManagerConfig                 `yaml:"-"`
	WriteReadSeriesTest     WriteReadSeriesTestConfig     `yaml:"-"`
	IngestStorageRecordTest IngestStorageRecordTestConfig `yaml:"-"`
	WriteReadOOOTest        WriteReadOOOTestConfig        `yaml:"-"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.Client.RegisterFlags(f)
	cfg.Manager.RegisterFlags(f)
	cfg.WriteReadSeriesTest.RegisterFlags(f)
	cfg.IngestStorageRecordTest.RegisterFlags(f)
	cfg.WriteReadOOOTest.RegisterFlags(f)
}
