// SPDX-License-Identifier: AGPL-3.0-only

package forwarding

import (
	"errors"
	"flag"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type Config struct {
	Enabled            bool          `yaml:"enabled" category:"experimental"`
	RequestConcurrency int           `yaml:"request_concurrency" category:"experimental"`
	RequestTimeout     time.Duration `yaml:"request_timeout" category:"experimental"`
	PropagateErrors    bool          `yaml:"propagate_errors" category:"experimental"`

	KafkaTopic            string `yaml:"kafka_topic" category:"experimental"`
	KafkaBrokers          string `yaml:"kafka_brokers" category:"experimental"`
	kafkaBrokers          []string
	KafkaBalancerFunction string `yaml:"kafka_balancer_function" category:"experimental"`
	kafkaBalancer         kafka.Balancer
	KafkaBatchSize        int           `yaml:"kafka_batch_size" category:"experimental"`
	KafkaBatchBytes       int64         `yaml:"kafka_batch_bytes" category:"experimental"`
	KafkaBatchTimeout     time.Duration `yaml:"kafka_batch_timeout" category:"experimental"`
}

func (c *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&c.Enabled, "distributor.forwarding.enabled", false, "Enables the feature to forward certain metrics in remote_write requests, depending on defined rules.")
	f.IntVar(&c.RequestConcurrency, "distributor.forwarding.request-concurrency", 10, "Maximum concurrency at which forwarding requests get performed.")
	f.DurationVar(&c.RequestTimeout, "distributor.forwarding.request-timeout", 2*time.Second, "Timeout for requests to ingestion endpoints to which we forward metrics.")
	f.BoolVar(&c.PropagateErrors, "distributor.forwarding.propagate-errors", true, "If disabled then forwarding requests are always considered to be successful, errors are ignored.")
	f.StringVar(&c.KafkaTopic, "distributor.forwarding.kafka-topic", "aggregations", "Kafka topic to which metrics are forwarded.")
	f.StringVar(&c.KafkaBrokers, "distributor.forwarding.kafka-brokers", "localhost:9092", "Kafka brokers to which metrics are forwarded, separated by \",\".")
	f.StringVar(&c.KafkaBalancerFunction, "distributor.forwarding.kafka-balancer-function", "murmur2", "Hash function to use for sharding among Kafka partitions, must be either \"murmur3\" or \"crc32\".")
	f.IntVar(&c.KafkaBatchSize, "distributor.forwarding.kafka-batch-size", 100, "Maximum number of messages to send in a single Kafka batch.")
	f.Int64Var(&c.KafkaBatchBytes, "distributor.forwarding.kafka-batch-bytes", 1048576, "Maximum number of bytes to send in a single Kafka batch.")
	f.DurationVar(&c.KafkaBatchTimeout, "distributor.forwarding.kafka-batch-timeout", 1*time.Second, "Maximum time to wait before sending a Kafka batch.")
}

func (c *Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	if c.RequestConcurrency < 1 {
		return errors.New("distributor.forwarding.request-concurrency must be greater than 0")
	}

	if len(c.KafkaTopic) == 0 {
		return errors.New("distributor.forwarding.kafka-topic must be set")
	}

	if len(c.KafkaBrokers) == 0 {
		return errors.New("distributor.forwarding.kafka-brokers must be set")
	}
	c.kafkaBrokers = strings.Split(c.KafkaBrokers, ",")

	if c.KafkaBalancerFunction == "murmur2" {
		c.kafkaBalancer = kafka.Murmur2Balancer{}
	} else if c.KafkaBalancerFunction == "crc32" {
		c.kafkaBalancer = kafka.CRC32Balancer{}
	} else {
		return errors.New("distributor.forwarding.kafka-balancer-function must be either \"murmur3\" or \"crc32\"")
	}

	return nil
}
