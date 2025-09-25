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
	JobSize             time.Duration `yaml:"job_size"`
	StartupObserveTime  time.Duration `yaml:"startup_observe_time"`
	JobLeaseExpiry      time.Duration `yaml:"job_lease_expiry"`
	LookbackOnNoCommit  time.Duration `yaml:"lookback_on_no_commit" category:"advanced"`
	MaxScanAge          time.Duration `yaml:"max_scan_age" category:"advanced"`
	MaxJobsPerPartition int           `yaml:"max_jobs_per_partition" category:"experimental"`
	EnqueueInterval     time.Duration `yaml:"enqueue_interval" category:"experimental"`
	JobFailuresAllowed  int           `yaml:"job_failures_allowed" category:"advanced"`

	// Config parameters defined outside the block-builder-scheduler config and are injected dynamically.
	Kafka ingest.KafkaConfig `yaml:"-"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.ConsumerGroup, "block-builder-scheduler.consumer-group", "block-builder", "The Kafka consumer group used for getting/setting commmitted offsets.")
	f.DurationVar(&cfg.SchedulingInterval, "block-builder-scheduler.scheduling-interval", 1*time.Minute, "How frequently to recompute the schedule.")
	f.DurationVar(&cfg.JobSize, "block-builder-scheduler.job-size", 1*time.Hour, "How long jobs (and therefore blocks) should be.")
	f.DurationVar(&cfg.StartupObserveTime, "block-builder-scheduler.startup-observe-time", 1*time.Minute, "How long to observe worker state before scheduling jobs.")
	f.DurationVar(&cfg.JobLeaseExpiry, "block-builder-scheduler.job-lease-expiry", 2*time.Minute, "How long a job lease will live before expiring.")
	f.DurationVar(&cfg.LookbackOnNoCommit, "block-builder-scheduler.lookback-on-no-commit", 6*time.Hour, "How much to look back if a commit is not found for a partition.")
	f.DurationVar(&cfg.MaxScanAge, "block-builder-scheduler.max-scan-age", 12*time.Hour, "The oldest record age to consider when scanning for jobs.")
	f.IntVar(&cfg.MaxJobsPerPartition, "block-builder-scheduler.max-jobs-per-partition", 1, "The maximum number of jobs that can be scheduled for a partition.")
	f.DurationVar(&cfg.EnqueueInterval, "block-builder-scheduler.enqueue-interval", 2*time.Second, "How frequently to enqueue pending jobs.")
	f.IntVar(&cfg.JobFailuresAllowed, "block-builder-scheduler.job-failures-allowed", 2, "The maximum number of times a job can fail before errors are emitted")
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
	if cfg.JobSize <= 0 {
		return fmt.Errorf("job size (%d) must be positive", cfg.JobSize)
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
	if cfg.MaxScanAge <= 0 {
		return fmt.Errorf("min scan age (%d) must be positive", cfg.MaxScanAge)
	}
	if cfg.MaxJobsPerPartition < 0 {
		return fmt.Errorf("max jobs per partition (%d) must be non-negative", cfg.MaxJobsPerPartition)
	}
	if cfg.EnqueueInterval <= 0 {
		return fmt.Errorf("enqueue interval (%d) must be positive", cfg.EnqueueInterval)
	}
	if cfg.JobFailuresAllowed < 0 {
		return fmt.Errorf("job failures allowed (%d) must be non-negative", cfg.JobFailuresAllowed)
	}
	return nil
}
