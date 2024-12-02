// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"flag"
	"fmt"
	"strconv"
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
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *PartitionRingConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.KVStore.Store = "memberlist" // Override default value.
	cfg.KVStore.RegisterFlagsWithPrefix("usage-tracker.partition-ring.", "collectors/", f)
}

func (cfg *PartitionRingConfig) ToLifecyclerConfig(partitionID int32, instanceID string) ring.PartitionInstanceLifecyclerConfig {
	return ring.PartitionInstanceLifecyclerConfig{
		PartitionID:                          partitionID,
		InstanceID:                           instanceID,
		WaitOwnersCountOnPending:             1,
		WaitOwnersDurationOnPending:          10 * time.Second,
		DeleteInactivePartitionAfterDuration: 1 * time.Hour,
		PollingInterval:                      cfg.lifecyclerPollingInterval,
	}
}

func NewPartitionRingKVClient(cfg PartitionRingConfig, logger log.Logger, registerer prometheus.Registerer) (kv.Client, error) {
	if cfg.KVStore.Mock != nil {
		return cfg.KVStore.Mock, nil
	}

	client, err := kv.NewClient(cfg.KVStore, ring.GetPartitionRingCodec(), kv.RegistererWithKVName(registerer, PartitionRingName+"-lifecycler"), logger)
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

// partitionIDFromInstanceID returns the partition ID from the instance ID.
func partitionIDFromInstanceID(instanceID string) (int32, error) {
	match := instanceIDRegexp.FindStringSubmatch(instanceID)
	if len(match) == 0 {
		return 0, fmt.Errorf("instance ID %s doesn't match regular expression %q", instanceID, instanceIDRegexp.String())
	}

	// Parse the instance sequence number.
	seq, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, fmt.Errorf("no sequence number in instance ID %s", instanceID)
	}

	return int32(seq), nil
}
