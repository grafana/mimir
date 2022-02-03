// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"flag"
	"time"

	"github.com/pkg/errors"
)

type PersisterConfig struct {
	Interval time.Duration `yaml:"persist_interval"`
}

const (
	defaultPersistTimeout = 30 * time.Second
)

var (
	errInvalidPersistInterval = errors.New("invalid alertmanager persist interval, must be greater than zero")
)

func (cfg *PersisterConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.DurationVar(&cfg.Interval, prefix+".persist-interval", 15*time.Minute, "The interval between persisting the current alertmanager state (notification log and silences) to object storage. This is only used when sharding is enabled. This state is read when all replicas for a shard can not be contacted. In this scenario, having persisted the state more frequently will result in potentially fewer lost silences, and fewer duplicate notifications.")
}
