// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"errors"
	"flag"
	"time"
)

var (
	errInvalidExpandedReplicationMaxTimeThreshold = errors.New("invalid expanded replication max time threshold, the value must be greater than 0")
)

type ExpandedReplicationConfig struct {
	Enabled          bool          `yaml:"enabled" category:"experimental"`
	MaxTimeThreshold time.Duration `yaml:"max_time_threshold" category:"experimental"`
}

func (cfg *ExpandedReplicationConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.BoolVar(&cfg.Enabled, prefix+"expanded-replication.enabled", false, "Use a higher number of replicas for recent blocks. Useful to spread query load more evenly at the cost of slightly higher disk usage.")
	f.DurationVar(&cfg.MaxTimeThreshold, prefix+"expanded-replication.max-time-threshold", 25*time.Hour, "Threshold of the most recent sample in a block used to determine it is eligible for higher than default replication. If a block has samples within this amount of time, it is considered recent and will be owned by more replicas.")
}

func (cfg *ExpandedReplicationConfig) Validate() error {
	if cfg.Enabled && cfg.MaxTimeThreshold <= 0 {
		return errInvalidExpandedReplicationMaxTimeThreshold
	}

	return nil
}

// ReplicatedBlock is metadata about a TSDB block that may be eligible for expanded
// replication (replication by more store-gateways than the configured replication factor).
type ReplicatedBlock interface {
	GetMinTime() time.Time
	GetMaxTime() time.Time
}

// ExpandedReplication implementations determine if a block should be replicated to more
// store-gateways than the configured replication factor based on its metadata.
type ExpandedReplication interface {
	Eligible(b ReplicatedBlock) bool
}

func NewNopExpandedReplication() *NopExpandedReplication {
	return &NopExpandedReplication{}
}

// NopExpandedReplication is an ExpandedReplication implementation that always returns false.
type NopExpandedReplication struct{}

func (n NopExpandedReplication) Eligible(ReplicatedBlock) bool {
	return false
}

func NewMaxTimeExpandedReplication(maxTime time.Duration) *MaxTimeExpandedReplication {
	return &MaxTimeExpandedReplication{maxTime: maxTime, now: time.Now}
}

// MaxTimeExpandedReplication is an ExpandedReplication implementation that determines
// if a block is eligible for expanded replication based on how recent its MaxTime (most
// recent sample) is.
type MaxTimeExpandedReplication struct {
	maxTime time.Duration
	now     func() time.Time
}

func (e *MaxTimeExpandedReplication) Eligible(b ReplicatedBlock) bool {
	now := e.now()
	delta := now.Sub(b.GetMaxTime())
	return delta <= e.maxTime
}
