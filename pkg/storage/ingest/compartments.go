// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"errors"
	"flag"
	"fmt"
	"strings"

	"github.com/grafana/mimir/pkg/mimirpb"
)

const (
	compartmentIDPlaceholder      = "<compartment-id>"
	writeCompartmentIDPlaceholder = "<write-compartment-id>"
)

var (
	ErrCompartmentsInvalidNumCompartments = errors.New("compartments num_compartments must be greater than 0 when compartments are enabled")
	ErrCompartmentsEmptyTopicFormat       = errors.New("compartments topic_format must not be empty when compartments are enabled")
)

// CompartmentsConfig holds the configuration for compartments.
type CompartmentsConfig struct {
	Enabled         bool   `yaml:"enabled"`
	NumCompartments int    `yaml:"num_compartments"`
	TopicFormat     string `yaml:"topic_format"`

	// ReadCompartmentID is the read compartment this ingester set serves. Used by null ingesters
	// to determine which partition ring to register in and which topic to consume.
	ReadCompartmentID int `yaml:"read_compartment_id"`

	// WriteKafkaAddressFormat, WriteKafkaSASLUsernameFormat, and WriteKafkaSASLPasswordFormat
	// are templates for per-write-VC Kafka connection parameters. The placeholder
	// writeCompartmentIDPlaceholder is replaced with the write compartment index at runtime.
	// Used by null ingesters to read from each write VC independently.
	WriteKafkaAddressFormat      string `yaml:"write_kafka_address_format"`
	WriteKafkaSASLUsernameFormat string `yaml:"write_kafka_sasl_username_format"`
	WriteKafkaSASLPasswordFormat string `yaml:"write_kafka_sasl_password_format"`
}

// RegisterFlagsWithPrefix registers the flags for CompartmentsConfig with the given prefix.
func (cfg *CompartmentsConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, prefix+"enabled", false, "Whether compartments are enabled. When enabled, series are sharded across multiple Kafka topics based on metric name.")
	f.IntVar(&cfg.NumCompartments, prefix+"num-compartments", 0, "The number of read compartments. Each compartment uses a dedicated Kafka topic.")
	f.StringVar(&cfg.TopicFormat, prefix+"topic-format", "", fmt.Sprintf("The topic name template with a %q placeholder that gets replaced with the compartment ID (e.g. mimir-read-comp-%s).", compartmentIDPlaceholder, compartmentIDPlaceholder))
	f.IntVar(&cfg.ReadCompartmentID, prefix+"read-compartment-id", 0, "The read compartment ID that this ingester set serves. Used by null ingesters to identify which partition ring to register in and which topic to consume.")
	f.StringVar(&cfg.WriteKafkaAddressFormat, prefix+"write-kafka-address-format", "", fmt.Sprintf("Kafka broker address template for write compartment VCs, with a %q placeholder replaced by the write compartment index. Used by null ingesters to read from all write VCs.", writeCompartmentIDPlaceholder))
	f.StringVar(&cfg.WriteKafkaSASLUsernameFormat, prefix+"write-kafka-sasl-username-format", "", fmt.Sprintf("SASL username template for write compartment VCs, with a %q placeholder replaced by the write compartment index.", writeCompartmentIDPlaceholder))
	f.StringVar(&cfg.WriteKafkaSASLPasswordFormat, prefix+"write-kafka-sasl-password-format", "", fmt.Sprintf("SASL password template for write compartment VCs, with a %q placeholder replaced by the write compartment index.", writeCompartmentIDPlaceholder))
}

// Validate returns an error if the config is invalid.
func (cfg *CompartmentsConfig) Validate() error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.NumCompartments <= 0 {
		return ErrCompartmentsInvalidNumCompartments
	}
	if cfg.TopicFormat == "" {
		return ErrCompartmentsEmptyTopicFormat
	}
	return nil
}

// WriteKafkaAddress returns the Kafka broker address for the given write compartment index.
func (cfg *CompartmentsConfig) WriteKafkaAddress(writeCompartmentID int) string {
	return strings.ReplaceAll(cfg.WriteKafkaAddressFormat, writeCompartmentIDPlaceholder, fmt.Sprintf("%d", writeCompartmentID))
}

// WriteKafkaSASLUsername returns the SASL username for the given write compartment index.
func (cfg *CompartmentsConfig) WriteKafkaSASLUsername(writeCompartmentID int) string {
	return strings.ReplaceAll(cfg.WriteKafkaSASLUsernameFormat, writeCompartmentIDPlaceholder, fmt.Sprintf("%d", writeCompartmentID))
}

// WriteKafkaSASLPassword returns the SASL password for the given write compartment index.
func (cfg *CompartmentsConfig) WriteKafkaSASLPassword(writeCompartmentID int) string {
	return strings.ReplaceAll(cfg.WriteKafkaSASLPasswordFormat, writeCompartmentIDPlaceholder, fmt.Sprintf("%d", writeCompartmentID))
}

// CompartmentRouter assigns series to compartments based on a hash of the user ID and metric name.
type CompartmentRouter struct {
	topics []string
}

// NewCompartmentRouter creates a new CompartmentRouter that pre-computes topic names for each compartment.
func NewCompartmentRouter(cfg CompartmentsConfig) *CompartmentRouter {
	topics := make([]string, cfg.NumCompartments)
	for i := range topics {
		topics[i] = strings.ReplaceAll(cfg.TopicFormat, compartmentIDPlaceholder, fmt.Sprintf("%d", i))
	}
	return &CompartmentRouter{
		topics: topics,
	}
}

// CompartmentForMetric returns the compartment index for a given tenant's metric name.
// The metric is assigned to a compartment by hashing userID + metric name
// and taking modulo numCompartments.
func (r *CompartmentRouter) CompartmentForMetric(userID, metricName string) int {
	hash := mimirpb.ShardByMetricName(userID, metricName)
	return int(hash % uint32(len(r.topics)))
}

// TopicForMetric returns the topic for a given tenant's metric name.
func (r *CompartmentRouter) TopicForMetric(userID, metricName string) string {
	return r.topics[r.CompartmentForMetric(userID, metricName)]
}

// NumCompartments returns the number of compartments.
func (r *CompartmentRouter) NumCompartments() int {
	return len(r.topics)
}

// Topic returns the topic for the given compartment index.
func (r *CompartmentRouter) Topic(compartmentID int) string {
	return r.topics[compartmentID]
}
