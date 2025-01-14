// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"errors"
	"flag"
	"time"
)

var (
	errInvalidExpandedReplicationMaxTimeThreshold = errors.New("invalid expanded replication max time threshold, the value must be at least one hour")
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
	if cfg.Enabled && cfg.MaxTimeThreshold < time.Hour {
		return errInvalidExpandedReplicationMaxTimeThreshold
	}

	return nil
}

// ReplicatedBlock is a TSDB block that may be eligible to be synced to and queried from
// more store-gateways than the configured replication factor based on metadata about the
// block.
type ReplicatedBlock interface {
	GetMinTime() time.Time
	GetMaxTime() time.Time
}

// ExpandedReplication determines if a TSDB block is eligible to be sync to and queried from more
// store-gateways than the configured replication factor based on metadata about the block.
type ExpandedReplication interface {
	// EligibleForSync returns true if the block can be synced to more than the configured (via
	// replication factor) number of store-gateways, false otherwise.
	EligibleForSync(b ReplicatedBlock) bool

	// EligibleForQuerying returns true if the block can be safely queried from more than the
	// configured (via replication factor) number of store-gateways, false otherwise.
	EligibleForQuerying(b ReplicatedBlock) bool
}

func NewNopExpandedReplication() *NopExpandedReplication {
	return &NopExpandedReplication{}
}

// NopExpandedReplication is an ExpandedReplication implementation that always returns false.
type NopExpandedReplication struct{}

func (n NopExpandedReplication) EligibleForSync(ReplicatedBlock) bool {
	return false
}

func (n NopExpandedReplication) EligibleForQuerying(ReplicatedBlock) bool {
	return false
}

func NewMaxTimeExpandedReplication(maxTime time.Duration, gracePeriod time.Duration) *MaxTimeExpandedReplication {
	return &MaxTimeExpandedReplication{
		maxTime:     maxTime,
		gracePeriod: gracePeriod,
		now:         time.Now,
	}
}

// MaxTimeExpandedReplication is an ExpandedReplication implementation that determines
// if a block is eligible for expanded replication based on how recent its MaxTime (most
// recent sample) is. An upload grace period can optionally be used to ensure that blocks
// are synced to store-gateways before they are expected to be available by queriers.
type MaxTimeExpandedReplication struct {
	maxTime     time.Duration
	gracePeriod time.Duration
	now         func() time.Time
}

func (e *MaxTimeExpandedReplication) EligibleForSync(b ReplicatedBlock) bool {
	now := e.now()
	maxTimeDelta := now.Sub(b.GetMaxTime())
	// We start syncing blocks `gracePeriod` before they become eligible for querying to
	// ensure that they've been loaded before queriers expect them to be available.
	return maxTimeDelta <= (e.maxTime + e.gracePeriod)
}

func (e *MaxTimeExpandedReplication) EligibleForQuerying(b ReplicatedBlock) bool {
	now := e.now()
	maxTimeDelta := now.Sub(b.GetMaxTime())
	return maxTimeDelta <= e.maxTime
}
