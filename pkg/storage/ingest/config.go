// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"errors"
	"flag"
	"time"
)

var (
	ErrMissingKafkaAddress = errors.New("the Kafka address has not been configured")
	ErrMissingKafkaTopic   = errors.New("the Kafka topic has not been configured")
)

type Config struct {
	Enabled bool `yaml:"enabled"`

	KafkaConfig  KafkaConfig  `yaml:",inline"`
	WriterConfig WriterConfig `yaml:",inline"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "ingest-storage.enabled", false, "True to enable the ingestion via object storage.")

	cfg.KafkaConfig.RegisterFlagsWithPrefix("ingest-storage", f)
	cfg.WriterConfig.RegisterFlagsWithPrefix("ingest-storage", f)
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
	Address     string        `yaml:"kafka_address"`
	Topic       string        `yaml:"kafka_topic"`
	ClientID    string        `yaml:"kafka_client_id"`
	DialTimeout time.Duration `yaml:"kafka_dial_timeout"`
}

func (cfg *KafkaConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

func (cfg *KafkaConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Address, prefix+".kafka-address", "", "The Kafka backend address.")
	f.StringVar(&cfg.Topic, prefix+".kafka-topic", "", "The Kafka topic name.")
	f.StringVar(&cfg.ClientID, prefix+".kafka-client-id", "", "The Kafka client ID.")
	f.DurationVar(&cfg.DialTimeout, prefix+".kafka-dial-timeout", 2*time.Second, "The maximum time allowed to open a connection to a Kafka broker.")
}

func (cfg *KafkaConfig) Validate() error {
	if cfg.Address == "" {
		return ErrMissingKafkaAddress
	}
	if cfg.Topic == "" {
		return ErrMissingKafkaTopic
	}

	return nil
}
