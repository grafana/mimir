// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"flag"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/regexp"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	PartitionRingKey  = "usage-tracker-partitions"
	PartitionRingName = "usage-tracker-partitions"
)

var (
	// Regular expression used to parse the numeric ID from instance ID.
	instanceIDRegexp = regexp.MustCompile("-([0-9]+)$")
)

type PartitionRingConfig struct {
	KVStore kv.Config `yaml:"kvstore" doc:"description=The key-value store used to share the hash ring across multiple instances."`

	// lifecyclerPollingInterval is the lifecycler polling interval. This setting is used to lower it in tests.
	lifecyclerPollingInterval time.Duration `yaml:"-"`

	// waitOwnersDurationOnPending is how long each owner should have been added to the
	// partition before it's considered eligible for the WaitOwnersCountOnPending count.
	// This setting is used to lower it in tests.
	waitOwnersDurationOnPending time.Duration `yaml:"-"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *PartitionRingConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.KVStore.Store = "memberlist" // Override default value.
	cfg.KVStore.RegisterFlagsWithPrefix("usage-tracker.partition-ring.", "collectors/", f)
}

func (cfg *PartitionRingConfig) ToLifecyclerConfig(partitionID int32, instanceID string) ring.PartitionInstanceLifecyclerConfig {
	waitOwnersDurationOnPending := cfg.waitOwnersDurationOnPending
	if waitOwnersDurationOnPending == 0 {
		waitOwnersDurationOnPending = 10 * time.Second // Default value without test override.
	}
	return ring.PartitionInstanceLifecyclerConfig{
		PartitionID:                          partitionID,
		InstanceID:                           instanceID,
		WaitOwnersCountOnPending:             1,
		WaitOwnersDurationOnPending:          waitOwnersDurationOnPending,
		DeleteInactivePartitionAfterDuration: 1 * time.Hour,
		PollingInterval:                      cfg.lifecyclerPollingInterval,
	}
}

func NewPartitionRingKVClient(cfg PartitionRingConfig, component string, logger log.Logger, registerer prometheus.Registerer) (kv.Client, error) {
	if cfg.KVStore.Mock != nil {
		return cfg.KVStore.Mock, nil
	}

	client, err := kv.NewClient(cfg.KVStore, ring.GetPartitionRingCodec(), kv.RegistererWithKVName(registerer, PartitionRingName+"-"+component), logger)
	if err != nil {
		return nil, errors.Wrap(err, "creating KV store for usage-tracker partition ring")
	}

	return client, nil
}

func NewPartitionRingLifecycler(cfg PartitionRingConfig, partitionID int32, instanceID string, partitionRingKV kv.Client, logger log.Logger, registerer prometheus.Registerer) (*ring.PartitionInstanceLifecycler, error) {
	return ring.NewPartitionInstanceLifecycler(
		cfg.ToLifecyclerConfig(partitionID, instanceID),
		PartitionRingName,
		PartitionRingKey,
		partitionRingKV,
		logger,
		prometheus.WrapRegistererWithPrefix("cortex_", registerer)), nil
}

func NewPartitionRingWatcher(partitionRingKV kv.Client, logger log.Logger, registerer prometheus.Registerer) *ring.PartitionRingWatcher {
	return ring.NewPartitionRingWatcher(PartitionRingName, PartitionRingKey, partitionRingKV, logger, prometheus.WrapRegistererWithPrefix("cortex_", registerer))
}
