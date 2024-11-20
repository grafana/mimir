// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/tsdb"
)

type Config struct {
	InstanceID          string             `yaml:"instance_id" doc:"default=<hostname>" category:"advanced"`
	PartitionAssignment map[string][]int32 `yaml:"partition_assignment" category:"experimental"`
	DataDir             string             `yaml:"data_dir"`

	ConsumerGroup         string        `yaml:"consumer_group"`
	ConsumeInterval       time.Duration `yaml:"consume_interval"`
	ConsumeIntervalBuffer time.Duration `yaml:"consume_interval_buffer"`
	LookbackOnNoCommit    time.Duration `yaml:"lookback_on_no_commit" category:"advanced"`

	ApplyGlobalSeriesLimitUnder int `yaml:"apply_global_series_limit_under" category:"advanced"`

	// Config parameters defined outside the block-builder config and are injected dynamically.
	Kafka         ingest.KafkaConfig       `yaml:"-"`
	BlocksStorage tsdb.BlocksStorageConfig `yaml:"-"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	hostname, err := os.Hostname()
	if err != nil {
		level.Error(logger).Log("msg", "failed to get hostname", "err", err)
		os.Exit(1)
	}

	f.StringVar(&cfg.InstanceID, "block-builder.instance-id", hostname, "Instance id.")
	f.Var(newPartitionAssignmentVar(&cfg.PartitionAssignment), "block-builder.partition-assignment", "Static partition assignment. Format is a JSON encoded map[instance-id][]partitions).")
	f.StringVar(&cfg.DataDir, "block-builder.data-dir", "./data-block-builder/", "Directory to temporarily store blocks during building. This directory is wiped out between the restarts.")
	f.StringVar(&cfg.ConsumerGroup, "block-builder.consumer-group", "block-builder", "The Kafka consumer group used to keep track of the consumed offsets for assigned partitions.")
	f.DurationVar(&cfg.ConsumeInterval, "block-builder.consume-interval", time.Hour, "Interval between consumption cycles.")
	f.DurationVar(&cfg.ConsumeIntervalBuffer, "block-builder.consume-interval-buffer", 15*time.Minute, "Extra buffer between subsequent consumption cycles. To avoid small blocks the block-builder consumes until the last hour boundary of the consumption interval, plus the buffer.")
	f.DurationVar(&cfg.LookbackOnNoCommit, "block-builder.lookback-on-no-commit", 12*time.Hour, "How much of the historical records to look back when there is no kafka commit for a partition.")
	f.IntVar(&cfg.ApplyGlobalSeriesLimitUnder, "block-builder.apply-global-series-under", 0, "Apply the global series limit per partition if the global series limit for the user is under this given value. 0 means limits are disabled. If a user's limit is more than the given value, then the limits are not applied as well.")
}

func (cfg *Config) Validate() error {
	if err := cfg.Kafka.Validate(); err != nil {
		return err
	}

	if len(cfg.PartitionAssignment) == 0 {
		return fmt.Errorf("partition assignment is required")
	}
	if _, ok := cfg.PartitionAssignment[cfg.InstanceID]; !ok {
		return fmt.Errorf("instance id %q must be present in partition assignment", cfg.InstanceID)
	}
	if cfg.DataDir == "" {
		return fmt.Errorf("data-dir is required")
	}
	// TODO(codesome): validate the consumption interval. Must be <=2h and can divide 2h into an integer.
	if cfg.ConsumeInterval < 0 {
		return fmt.Errorf("consume-interval cannot be negative")
	}
	if cfg.LookbackOnNoCommit < 0 {
		return fmt.Errorf("lookback-on-no-commit cannot be negative")
	}

	return nil
}

type partitionAssignmentVar map[string][]int32

func newPartitionAssignmentVar(p *map[string][]int32) *partitionAssignmentVar {
	return (*partitionAssignmentVar)(p)
}

func (v *partitionAssignmentVar) Set(s string) error {
	if s == "" {
		return nil
	}
	val := make(map[string][]int32)
	err := json.Unmarshal([]byte(s), &val)
	if err != nil {
		return fmt.Errorf("unmarshal partition assignment: %w", err)
	}
	*v = val
	return nil
}

func (v partitionAssignmentVar) String() string {
	return fmt.Sprintf("%v", map[string][]int32(v))
}
