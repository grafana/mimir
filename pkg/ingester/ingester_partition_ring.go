// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"flag"
	"fmt"
	"math"
	"time"

	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"

	"github.com/grafana/mimir/pkg/util/validation"
)

type PartitionRingConfig struct {
	KVStore kv.Config `yaml:"kvstore" doc:"description=The key-value store used to share the hash ring across multiple instances. This option needs be set on ingesters, distributors, queriers, and rulers when running in microservices mode."`

	// MinOwnersCount maps to ring.PartitionInstanceLifecyclerConfig's WaitOwnersCountOnPending.
	MinOwnersCount int `yaml:"min_partition_owners_count"`

	// MinOwnersDuration maps to ring.PartitionInstanceLifecyclerConfig's WaitOwnersDurationOnPending.
	MinOwnersDuration time.Duration `yaml:"min_partition_owners_duration"`

	// DeleteInactivePartitionAfter is added to the longest query-ingesters-within (across the default and all tenant
	// overrides) and -ingest-storage.query.include-inactive-partitions-offset-duration to compute
	// ring.PartitionInstanceLifecyclerConfig's DeleteInactivePartitionAfterDurationFunc.
	DeleteInactivePartitionAfter time.Duration `yaml:"delete_inactive_partition_after"`

	// CreatePartitionsWithDerivedTokens makes the ingester create its partition without stored tokens.
	// Components that read the ring derive the tokens from the partition ID.
	CreatePartitionsWithDerivedTokens bool `yaml:"create_partitions_with_derived_tokens" category:"experimental"`

	// MaxDerivedTokenPartitions is the exclusive upper limit of partition IDs that can use derived tokens.
	// Components that read the ring pre-generate the tokens of these partitions.
	MaxDerivedTokenPartitions int `yaml:"max_derived_token_partitions" category:"experimental"`

	// lifecyclerPollingInterval is the lifecycler polling interval. This setting is used to lower it in tests.
	lifecyclerPollingInterval time.Duration
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *PartitionRingConfig) RegisterFlags(f *flag.FlagSet) {
	// Ring flags
	cfg.KVStore.Store = "memberlist" // Override default value.
	cfg.KVStore.RegisterFlagsWithPrefix("ingester.partition-ring.", "collectors/", f)

	f.IntVar(&cfg.MinOwnersCount, "ingester.partition-ring.min-partition-owners-count", 1, "Minimum number of owners to wait before a PENDING partition gets switched to ACTIVE.")
	f.DurationVar(&cfg.MinOwnersDuration, "ingester.partition-ring.min-partition-owners-duration", 10*time.Second, "How long the minimum number of owners are enforced before a PENDING partition gets switched to ACTIVE.")
	f.DurationVar(&cfg.DeleteInactivePartitionAfter, "ingester.partition-ring.delete-inactive-partition-after", 10*time.Minute, "Additional time to wait, on top of the longest -querier.query-ingesters-within (across the default and all tenant overrides) plus -ingest-storage.query.include-inactive-partitions-offset-duration, before an INACTIVE partition is eligible for deletion. The partition is deleted only if it has been in INACTIVE state for at least this total duration and it has no owners registered. A value of 0 disables partitions deletion. Partitions are also never deleted if -querier.query-ingesters-within is 0 for the default or any tenant, or if -ingest-storage.query.include-inactive-partitions-offset-duration is 0 or negative.")
	f.BoolVar(&cfg.CreatePartitionsWithDerivedTokens, "ingester.partition-ring.create-partitions-with-derived-tokens", false, "When the ingester creates its partition, create it with tokens that are derived from the partition ID instead of storing the tokens in the partition ring. Only ingesters, which create partitions, read this flag. Before you enable it, ensure that -ingester.partition-ring.max-derived-token-partitions is applied to all partition ring readers and ingesters, with a value higher than the highest partition ID.")
	f.IntVar(&cfg.MaxDerivedTokenPartitions, "ingester.partition-ring.max-derived-token-partitions", 0, "Partitions with an ID lower than this value can use derived tokens. "+
		"Set it on all partition ring readers and ingesters. "+
		"Components that read the partition ring pre-generate the tokens for these partitions and keep them in a table. "+
		"Partition IDs at or above this value are rejected: ingesters fail to create such a partition, and other components that read the ring fail until the partition is removed. "+
		"Set it above the highest partition ID you expect to use. These components generate the tokens at startup, so a higher value means a longer startup time.")
}

func (cfg *PartitionRingConfig) Validate() error {
	if cfg.MaxDerivedTokenPartitions < 0 || cfg.MaxDerivedTokenPartitions > math.MaxInt32 {
		return fmt.Errorf("-ingester.partition-ring.max-derived-token-partitions must be between 0 and %d, got %d", math.MaxInt32, cfg.MaxDerivedTokenPartitions)
	}
	return nil
}

// ToLifecyclerConfig returns the lifecycler config. The duration after which inactive partitions are deleted is
// computed dynamically from the current limits, so that it respects tenant-specific query-ingesters-within overrides.
func (cfg *PartitionRingConfig) ToLifecyclerConfig(partitionID int32, instanceID string, limits *validation.Overrides, includeInactivePartitionsOffsetDuration time.Duration) ring.PartitionInstanceLifecyclerConfig {
	return ring.PartitionInstanceLifecyclerConfig{
		PartitionID:                 partitionID,
		InstanceID:                  instanceID,
		WaitOwnersCountOnPending:    cfg.MinOwnersCount,
		WaitOwnersDurationOnPending: cfg.MinOwnersDuration,
		DeleteInactivePartitionAfterDurationFunc: func() (time.Duration, error) {
			return cfg.deleteInactivePartitionAfter(limits, includeInactivePartitionsOffsetDuration), nil
		},
		PollingInterval:                   cfg.lifecyclerPollingInterval,
		CreatePartitionsWithDerivedTokens: cfg.CreatePartitionsWithDerivedTokens,
		MaxDerivedTokenPartitions:         int32(cfg.MaxDerivedTokenPartitions),
	}
}

// deleteInactivePartitionAfter returns how long a partition must have been inactive before it can be deleted:
// the longest query-ingesters-within across the default and all tenants, plus the include-inactive-partitions
// offset duration, plus the configured DeleteInactivePartitionAfter buffer.
//
// It returns 0 (never delete) if the buffer is not positive, if query-ingesters-within is disabled for the default
// or any tenant (so inactive partitions may always be queried), or if the offset duration is not positive.
func (cfg *PartitionRingConfig) deleteInactivePartitionAfter(limits *validation.Overrides, includeInactivePartitionsOffsetDuration time.Duration) time.Duration {
	if cfg.DeleteInactivePartitionAfter <= 0 || includeInactivePartitionsOffsetDuration <= 0 {
		return 0
	}

	queryIngestersWithin := limits.MaxDurationAcrossAllTenants(func(l *validation.Limits) time.Duration {
		if l.QueryIngestersWithin <= 0 {
			return math.MaxInt64 // Disabled: ingesters are always queried.
		}
		return time.Duration(l.QueryIngestersWithin)
	})

	if queryIngestersWithin == math.MaxInt64 {
		return 0
	}

	return queryIngestersWithin + includeInactivePartitionsOffsetDuration + cfg.DeleteInactivePartitionAfter
}
