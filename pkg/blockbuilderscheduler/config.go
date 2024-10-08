// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilderscheduler

import (
	"flag"
	"fmt"
	"time"

	"github.com/go-kit/log"

	"github.com/grafana/mimir/pkg/storage/ingest"
)

type Config struct {
	ConsumerGroup         string        `yaml:"consumer_group"`
	ConsumeInterval       time.Duration `yaml:"consume_interval"`
	ConsumeIntervalBuffer time.Duration `yaml:"consume_interval_buffer"`
	LookbackOnNoCommit    time.Duration `yaml:"lookback_on_no_commit" category:"advanced"`

	// Config parameters defined outside the block-builder config and are injected dynamically.
	Kafka ingest.KafkaConfig `yaml:"-"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	f.StringVar(&cfg.ConsumerGroup, "block-builder.consumer-group", "block-builder", "The Kafka consumer group used to keep track of the consumed offsets for assigned partitions.")
	f.DurationVar(&cfg.ConsumeInterval, "block-builder.consume-interval", time.Hour, "Interval between consumption cycles.")
	f.DurationVar(&cfg.ConsumeIntervalBuffer, "block-builder.consume-interval-buffer", 15*time.Minute, "Extra buffer between subsequent consumption cycles. To avoid small blocks the block-builder consumes until the last hour boundary of the consumption interval, plus the buffer.")
	f.DurationVar(&cfg.LookbackOnNoCommit, "block-builder.lookback-on-no-commit", 12*time.Hour, "How much of the historical records to look back when there is no kafka commit for a partition.")
}

func (cfg *Config) Validate() error {
	if err := cfg.Kafka.Validate(); err != nil {
		return err
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
