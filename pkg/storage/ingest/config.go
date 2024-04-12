// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"errors"
	"flag"
	"fmt"
	"slices"
	"strings"
	"time"
)

const (
	consumeFromLastOffset = "last-offset"
	consumeFromStart      = "start"
	consumeFromEnd        = "end"
)

var (
	ErrMissingKafkaAddress    = errors.New("the Kafka address has not been configured")
	ErrMissingKafkaTopic      = errors.New("the Kafka topic has not been configured")
	ErrInvalidConsumePosition = errors.New("the configured consume position is invalid")

	consumeFromPositionOptions = []string{consumeFromLastOffset, consumeFromStart, consumeFromEnd}
)

type Config struct {
	Enabled     bool        `yaml:"enabled"`
	KafkaConfig KafkaConfig `yaml:"kafka"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "ingest-storage.enabled", false, "True to enable the ingestion via object storage.")

	cfg.KafkaConfig.RegisterFlagsWithPrefix("ingest-storage.kafka", f)
}

// Validate the config.
func (cfg *Config) Validate() error {
	// Skip validation if disabled.
	if !cfg.Enabled {
		return nil
	}

	if err := cfg.KafkaConfig.Validate(); err != nil {
		return err
	}

	return nil
}

// KafkaConfig holds the generic config for the Kafka backend.
type KafkaConfig struct {
	Address      string        `yaml:"address"`
	Topic        string        `yaml:"topic"`
	ClientID     string        `yaml:"client_id"`
	DialTimeout  time.Duration `yaml:"dial_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`

	LastProducedOffsetPollInterval time.Duration `yaml:"last_produced_offset_poll_interval"`
	LastProducedOffsetRetryTimeout time.Duration `yaml:"last_produced_offset_retry_timeout"`

	ConsumeFromPositionAtStartup string        `yaml:"consume_from_position_at_startup"`
	MaxConsumerLagAtStartup      time.Duration `yaml:"max_consumer_lag_at_startup"`

	AutoCreateTopicEnabled           bool `yaml:"auto_create_topic_enabled"`
	AutoCreateTopicDefaultPartitions int  `yaml:"auto_create_topic_default_partitions"`
}

func (cfg *KafkaConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

func (cfg *KafkaConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Address, prefix+".address", "", "The Kafka backend address.")
	f.StringVar(&cfg.Topic, prefix+".topic", "", "The Kafka topic name.")
	f.StringVar(&cfg.ClientID, prefix+".client-id", "", "The Kafka client ID.")
	f.DurationVar(&cfg.DialTimeout, prefix+".dial-timeout", 2*time.Second, "The maximum time allowed to open a connection to a Kafka broker.")
	f.DurationVar(&cfg.WriteTimeout, prefix+".write-timeout", 10*time.Second, "How long to wait for an incoming write request to be successfully committed to the Kafka backend.")

	f.DurationVar(&cfg.LastProducedOffsetPollInterval, prefix+".last-produced-offset-poll-interval", time.Second, "How frequently to poll the last produced offset, used to enforce strong read consistency.")
	f.DurationVar(&cfg.LastProducedOffsetRetryTimeout, prefix+".last-produced-offset-retry-timeout", 10*time.Second, "How long to retry a failed request to get the last produced offset.")

	f.StringVar(&cfg.ConsumeFromPositionAtStartup, prefix+".consume-from-position-at-startup", consumeFromLastOffset, fmt.Sprintf("From which position to start consuming the partition at startup. Supported options: %s.", strings.Join(consumeFromPositionOptions, ", ")))
	f.DurationVar(&cfg.MaxConsumerLagAtStartup, prefix+".max-consumer-lag-at-startup", 15*time.Second, "The maximum tolerated lag before a consumer is considered to have caught up reading from a partition at startup, becomes ACTIVE in the hash ring and passes the readiness check. Set 0 to disable waiting for maximum consumer lag being honored at startup.")
	f.BoolVar(&cfg.AutoCreateTopicEnabled, prefix+".auto-create-topic-enabled", true, "Enable auto-creation of Kafka topic if it doesn't exist.")
	f.IntVar(&cfg.AutoCreateTopicDefaultPartitions, prefix+".auto-create-topic-default-partitions", 0, "When auto-creation of Kafka topic is enabled and this value is positive, Kafka's num.partitions configuration option is set on Kafka brokers with this value when Mimir component that uses Kafka starts. This configuration option specifies the default number of partitions that Kafka broker will use for auto-created topics. Note that this is Kafka-cluster wide setting, and applies to any auto-created topic. If setting of num.partitions fails, Mimir will proceed anyway, but auto-created topic may have incorrect number of partitions.")
}

func (cfg *KafkaConfig) Validate() error {
	if cfg.Address == "" {
		return ErrMissingKafkaAddress
	}
	if cfg.Topic == "" {
		return ErrMissingKafkaTopic
	}
	if !slices.Contains(consumeFromPositionOptions, cfg.ConsumeFromPositionAtStartup) {
		return ErrInvalidConsumePosition
	}

	return nil
}
