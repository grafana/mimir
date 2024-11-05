// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"flag"
	"fmt"
	"time"

	"github.com/grafana/mimir/pkg/storage/ingest"
)

type Config struct {
	BuilderConsumerGroup string        `yaml:"builder_consumer_group"`
	SchedulingInterval   time.Duration `yaml:"kafka_monitor_interval"`
	ConsumeInterval      time.Duration `yaml:"consume_interval"`
	StartupObserveTime   time.Duration `yaml:"startup_observe_time"`
	JobLeaseTime         time.Duration `yaml:"job_lease_time"`
	UpdateJobInterval    time.Duration `yaml:"update_job_interval"`

	// Config parameters defined outside the block-builder-scheduler config and are injected dynamically.
	Kafka ingest.KafkaConfig `yaml:"-"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.BuilderConsumerGroup, "block-builder-scheduler.builder-consumer-group", "block-builder", "The Kafka consumer group used for getting/setting commmitted offsets.")
	f.DurationVar(&cfg.SchedulingInterval, "block-builder-scheduler.scheduling-interval", 20*time.Second, "How frequently to recompute the schedule.")
	f.DurationVar(&cfg.ConsumeInterval, "block-builder-scheduler.consume-interval", 1*time.Hour, "Interval between consumption cycles.")
	f.DurationVar(&cfg.StartupObserveTime, "block-builder-scheduler.startup-observe-time", 25*time.Second, "How long to observe worker state before scheduling jobs.")
	f.DurationVar(&cfg.JobLeaseTime, "block-builder-scheduler.job-lease-time", 2*time.Minute, "How long a job lease will live for before expiring.")
	f.DurationVar(&cfg.UpdateJobInterval, "block-builder-scheduler.update-job-interval", 8*time.Second, "How often clients should send job updates.")
}

func (cfg *Config) Validate() error {
	if err := cfg.Kafka.Validate(); err != nil {
		return err
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
	if cfg.JobLeaseTime <= 0 {
		return fmt.Errorf("job lease time (%d) must be positive", cfg.JobLeaseTime)
	}
	if cfg.UpdateJobInterval <= 0 {
		return fmt.Errorf("update job interval (%d) must be positive", cfg.UpdateJobInterval)
	}
	return nil
}
