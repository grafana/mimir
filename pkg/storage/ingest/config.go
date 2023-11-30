package ingest

import (
	"errors"
	"flag"
)

var (
	ErrMissingKafkaAddress = errors.New("the Kafka address has not been configured")
	ErrMissingKafkaTopic   = errors.New("the Kafka topic has not been configured")
)

type Config struct {
	Enabled bool `yaml:"enabled"`

	KafkaAddress          string `yaml:"kafka_address"`
	KafkaTopic            string `yaml:"kafka_topic"`
	KafkaAvailabilityZone string `yaml:"kafka_availability_zone"` // TODO default may be auto-detected
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "ingest-storage.enabled", false, "True to enable the ingestion via object storage.")

	// Kafka backend.
	f.StringVar(&cfg.KafkaAddress, "ingest-storage.kafka-address", "", "The Kafka backend address.")
	f.StringVar(&cfg.KafkaTopic, "ingest-storage.kafka-topic", "", "The Kafka topic name.")
	f.StringVar(&cfg.KafkaAvailabilityZone, "ingest-storage.kafka-availability-zone", "", "The availability zone of the Kafka backend. Keep it empty if any availability zone is fine, or Kafka is only running in a single availability zone.")
}

// Validate the config.
// TODO unit test
func (cfg *Config) Validate() error {
	// Skip validation if disabled.
	if !cfg.Enabled {
		return nil
	}

	if cfg.KafkaAddress == "" {
		return ErrMissingKafkaAddress
	}
	if cfg.KafkaTopic == "" {
		return ErrMissingKafkaTopic
	}

	return nil
}
