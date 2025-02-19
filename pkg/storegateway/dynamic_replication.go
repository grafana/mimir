// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"errors"
	"flag"
	"time"
)

var (
	errInvalidDynamicReplicationMaxTimeThreshold = errors.New("invalid dynamic replication max time threshold, the value must be at least one hour")
	errInvalidDynamicReplicationFactor           = errors.New("invalid dynamic replication factor, the value must be at least 2")
)

type DynamicReplicationConfig struct {
	Enabled          bool          `yaml:"enabled" category:"experimental"`
	MaxTimeThreshold time.Duration `yaml:"max_time_threshold" category:"experimental"`
	Multiple         int           `yaml:"multiple" category:"experimental"`
}

func (cfg *DynamicReplicationConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.BoolVar(&cfg.Enabled, prefix+"dynamic-replication.enabled", false, "Use a higher number of replicas for recent blocks. Useful to spread query load more evenly at the cost of slightly higher disk usage.")
	f.DurationVar(&cfg.MaxTimeThreshold, prefix+"dynamic-replication.max-time-threshold", 25*time.Hour, "Threshold of the most recent sample in a block used to determine it is eligible for higher than default replication. If a block has samples within this amount of time, it is considered recent and will be owned by more replicas.")
	f.IntVar(&cfg.Multiple, prefix+"dynamic-replication.multiple", 2, "Multiple of the default replication factor that should be used for recent blocks. Minimum value is 2")
}

func (cfg *DynamicReplicationConfig) Validate() error {
	if cfg.Enabled {
		if cfg.MaxTimeThreshold < time.Hour {
			return errInvalidDynamicReplicationMaxTimeThreshold
		}

		if cfg.Multiple < 2 {
			return errInvalidDynamicReplicationFactor
		}
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

// DynamicReplication determines if a TSDB block is eligible to be sync to and queried from more
// store-gateways than the configured replication factor based on metadata about the block.
type DynamicReplication interface {
	// EligibleForSync returns true if the block can be synced to more than the default number of
	// store-gateways and the appropriate replication factor to use, false and an undefined replication
	// factor otherwise.
	EligibleForSync(b ReplicatedBlock) (bool, int)

	// EligibleForQuerying returns true if the block can be safely queried from more than the
	// default number of store-gateways and the appropriate replication factor to use, false and
	// an undefined replication factor otherwise.
	EligibleForQuerying(b ReplicatedBlock) (bool, int)
}

func NewNopDynamicReplication() *NopDynamicReplication {
	return &NopDynamicReplication{}
}

// NopDynamicReplication is an DynamicReplication implementation that always returns false.
type NopDynamicReplication struct{}

func (n NopDynamicReplication) EligibleForSync(ReplicatedBlock) (bool, int) {
	return false, 0
}

func (n NopDynamicReplication) EligibleForQuerying(ReplicatedBlock) (bool, int) {
	return false, 0
}

func NewMaxTimeDynamicReplication(cfg Config, gracePeriod time.Duration) *MaxTimeDynamicReplication {
	return &MaxTimeDynamicReplication{
		maxTimeThreshold:  cfg.DynamicReplication.MaxTimeThreshold,
		replicationFactor: cfg.DynamicReplication.Multiple * cfg.ShardingRing.ReplicationFactor,
		gracePeriod:       gracePeriod,
		now:               time.Now,
	}
}

// MaxTimeDynamicReplication is an DynamicReplication implementation that determines
// if a block is eligible for expanded replication based on how recent its MaxTime (most
// recent sample) is. A grace period can optionally be used to ensure that blocks are
// synced to store-gateways until they are no longer being queried.
type MaxTimeDynamicReplication struct {
	maxTimeThreshold  time.Duration
	replicationFactor int
	gracePeriod       time.Duration
	now               func() time.Time
}

func (e *MaxTimeDynamicReplication) EligibleForSync(b ReplicatedBlock) (bool, int) {
	now := e.now()
	maxTimeDelta := now.Sub(b.GetMaxTime())
	// We keep syncing blocks for `gracePeriod` after they are no longer eligible for
	// querying to ensure that they are not unloaded by store-gateways while still being
	// queried.
	return maxTimeDelta <= (e.maxTimeThreshold + e.gracePeriod), e.replicationFactor
}

func (e *MaxTimeDynamicReplication) EligibleForQuerying(b ReplicatedBlock) (bool, int) {
	now := e.now()
	maxTimeDelta := now.Sub(b.GetMaxTime())
	return maxTimeDelta <= e.maxTimeThreshold, e.replicationFactor
}
