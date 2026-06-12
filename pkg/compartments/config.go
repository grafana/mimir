// SPDX-License-Identifier: AGPL-3.0-only

package compartments

import (
	"errors"
	"flag"
	"fmt"
	"strings"
)

// CompartmentIDPlaceholder is replaced with the read compartment ID when expanding templated
// settings such as the Kafka topic name.
const CompartmentIDPlaceholder = "<compartment-id>"

var (
	ErrInvalidNumCompartments    = errors.New("compartments read.num-compartments must be greater than 0 when compartments are enabled")
	ErrEmptyKafkaTopicFormat     = errors.New("compartments read.kafka-topic-format must not be empty when compartments are enabled")
	ErrKafkaTopicFormatPlacehold = fmt.Errorf("compartments read.kafka-topic-format must contain the %q placeholder", CompartmentIDPlaceholder)
)

// Config holds the configuration for the compartments architecture.
type Config struct {
	Enabled bool       `yaml:"enabled"`
	Read    ReadConfig `yaml:"read"`
}

// ReadConfig holds the configuration of the read compartments.
type ReadConfig struct {
	NumCompartments  int    `yaml:"num_compartments"`
	KafkaTopicFormat string `yaml:"kafka_topic_format"`
}

// RegisterFlags registers the compartments flags.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "compartments.enabled", false, "Whether the compartments architecture is enabled.")
	cfg.Read.RegisterFlagsWithPrefix("compartments.read.", f)
}

// RegisterFlagsWithPrefix registers the read compartments flags with the given prefix.
func (cfg *ReadConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.IntVar(&cfg.NumCompartments, prefix+"num-compartments", 0, "The number of read compartments.")
	f.StringVar(&cfg.KafkaTopicFormat, prefix+"kafka-topic-format", "", fmt.Sprintf("The Kafka topic name template, containing the %q placeholder that is replaced with the read compartment ID (e.g. ingest-rc-%s).", CompartmentIDPlaceholder, CompartmentIDPlaceholder))
}

// Validate returns an error if the config is invalid.
func (cfg *Config) Validate() error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.Read.NumCompartments <= 0 {
		return ErrInvalidNumCompartments
	}
	if cfg.Read.KafkaTopicFormat == "" {
		return ErrEmptyKafkaTopicFormat
	}
	if !strings.Contains(cfg.Read.KafkaTopicFormat, CompartmentIDPlaceholder) {
		return ErrKafkaTopicFormatPlacehold
	}
	return nil
}
