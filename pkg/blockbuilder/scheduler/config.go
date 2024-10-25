// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"flag"
	"fmt"
	"time"

	"github.com/grafana/mimir/pkg/storage/ingest"
)

type Config struct {
	BuilderConsumerGroup   string        `yaml:"builder_consumer_group"`
	SchedulerConsumerGroup string        `yaml:"scheduler_consumer_group"`
	SchedulingInterval     time.Duration `yaml:"kafka_monitor_interval"`
	ConsumeInterval        time.Duration `yaml:"consume_interval"`
	ConsumeIntervalBuffer  time.Duration `yaml:"consume_interval_buffer"`

	// Config parameters defined outside the block-builder-scheduler config and are injected dynamically.
	Kafka ingest.KafkaConfig `yaml:"-"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.BuilderConsumerGroup, "block-builder-scheduler.builder-consumer-group", "block-builder", "The Kafka consumer group used by block-builders.")
	f.StringVar(&cfg.SchedulerConsumerGroup, "block-builder-scheduler.scheduler-consumer-group", "block-builder-scheduler", "The Kafka consumer group used by block-builder-scheduler.")
	f.DurationVar(&cfg.SchedulingInterval, "block-builder-scheduler.scheduling-interval", 20*time.Second, "How frequently to recompute the schedule.")
	f.DurationVar(&cfg.ConsumeInterval, "block-builder-scheduler.consume-interval", 1*time.Hour, "Interval between consumption cycles.")
	f.DurationVar(&cfg.ConsumeIntervalBuffer, "block-builder-scheduler.consume-interval-buffer", 15*time.Minute, "Extra buffer between subsequent consumption cycles. To avoid small blocks the block-builder consumes until the last hour boundary of the consumption interval, plus the buffer.")
}

func (cfg *Config) Validate() error {
	if err := cfg.Kafka.Validate(); err != nil {
		return err
	}
	if cfg.SchedulingInterval <= 0 {
		return fmt.Errorf("scheduling interval (%d) must be positive", cfg.SchedulingInterval)
	}
	if cfg.ConsumeInterval < 0 {
		return fmt.Errorf("consume interval (%d) cannot be negative", cfg.ConsumeInterval)
	}
	return nil
}
