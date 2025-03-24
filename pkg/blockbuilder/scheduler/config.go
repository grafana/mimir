// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"flag"
	"fmt"
	"time"

	"github.com/grafana/mimir/pkg/storage/ingest"
)

type Config struct {
	ConsumerGroup       string        `yaml:"consumer_group"`
	SchedulingInterval  time.Duration `yaml:"scheduling_interval"`
	ConsumeInterval     time.Duration `yaml:"consume_interval"`
	StartupObserveTime  time.Duration `yaml:"startup_observe_time"`
	JobLeaseExpiry      time.Duration `yaml:"job_lease_expiry"`
	LookbackOnNoCommit  time.Duration `yaml:"lookback_on_no_commit" category:"advanced"`
	MaxJobsPerPartition int           `yaml:"max_jobs_per_partition" category:"advanced"`

	// Config parameters defined outside the block-builder-scheduler config and are injected dynamically.
	Kafka ingest.KafkaConfig `yaml:"-"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.ConsumerGroup, "block-builder-scheduler.consumer-group", "block-builder", "The Kafka consumer group used for getting/setting commmitted offsets.")
	f.DurationVar(&cfg.SchedulingInterval, "block-builder-scheduler.scheduling-interval", 20*time.Second, "How frequently to recompute the schedule.")
	f.DurationVar(&cfg.ConsumeInterval, "block-builder-scheduler.consume-interval", 1*time.Hour, "Interval between consumption cycles.")
	f.DurationVar(&cfg.StartupObserveTime, "block-builder-scheduler.startup-observe-time", 25*time.Second, "How long to observe worker state before scheduling jobs.")
	f.DurationVar(&cfg.JobLeaseExpiry, "block-builder-scheduler.job-lease-expiry", 2*time.Minute, "How long a job lease will live for before expiring.")
	f.DurationVar(&cfg.LookbackOnNoCommit, "block-builder-scheduler.lookback-on-no-commit", 1*time.Hour, "How much to look back if a commit is not found for a partition.")
	f.IntVar(&cfg.MaxJobsPerPartition, "block-builder-scheduler.max-jobs-per-partition", 1, "The maximum number of jobs that can be scheduled for a partition.")
}

func (cfg *Config) Validate() error {
	if err := cfg.Kafka.Validate(); err != nil {
		return err
	}
	if cfg.ConsumerGroup == "" {
		return fmt.Errorf("consumer group cannot be empty")
	}
	if cfg.SchedulingInterval <= 0 {
		return fmt.Errorf("scheduling interval (%d) must be positive", cfg.SchedulingInterval)
	}
	if cfg.ConsumeInterval <= 0 {
		return fmt.Errorf("consume interval (%d) must be positive", cfg.ConsumeInterval)
	}
	if cfg.StartupObserveTime <= 0 {
		return fmt.Errorf("startup observe time (%d) must be positive", cfg.StartupObserveTime)
	}
	if cfg.JobLeaseExpiry <= 0 {
		return fmt.Errorf("job lease expiry (%d) must be positive", cfg.JobLeaseExpiry)
	}
	if cfg.LookbackOnNoCommit <= 0 {
		return fmt.Errorf("lookback on no commit (%d) must be positive", cfg.LookbackOnNoCommit)
	}
	if cfg.MaxJobsPerPartition != 1 {
		// TODO: revise once we've implemented safe bookkeeping under parallel region consumption.
		return fmt.Errorf("max jobs per partition (%d) must be 1", cfg.MaxJobsPerPartition)
	}
	return nil
}
