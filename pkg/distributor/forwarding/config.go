// SPDX-License-Identifier: AGPL-3.0-only

package forwarding

import (
	"errors"
	"flag"
	"strings"
	"time"
)

type Config struct {
	Enabled            bool          `yaml:"enabled" category:"experimental"`
	RequestConcurrency int           `yaml:"request_concurrency" category:"experimental"`
	RequestTimeout     time.Duration `yaml:"request_timeout" category:"experimental"`
	PropagateErrors    bool          `yaml:"propagate_errors" category:"experimental"`

	KafkaTopic   string `yaml:"kafka_topic" category:"experimental"`
	KafkaBrokers string `yaml:"kafka_brokers" category:"experimental"`
	kafkaBrokers []string
}

func (c *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&c.Enabled, "distributor.forwarding.enabled", false, "Enables the feature to forward certain metrics in remote_write requests, depending on defined rules.")
	f.IntVar(&c.RequestConcurrency, "distributor.forwarding.request-concurrency", 10, "Maximum concurrency at which forwarding requests get performed.")
	f.DurationVar(&c.RequestTimeout, "distributor.forwarding.request-timeout", 2*time.Second, "Timeout for requests to ingestion endpoints to which we forward metrics.")
	f.BoolVar(&c.PropagateErrors, "distributor.forwarding.propagate-errors", true, "If disabled then forwarding requests are always considered to be successful, errors are ignored.")
	f.StringVar(&c.KafkaTopic, "distributor.forwarding.kafka-topic", "aggregations", "Kafka topic to which metrics are forwarded.")
	f.StringVar(&c.KafkaBrokers, "distributor.forwarding.kafka-brokers", "localhost:9092", "Kafka brokers to which metrics are forwarded, separated by \",\".")
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

	c.kafkaBrokers = strings.Split(c.KafkaBrokers, ",")
	if len(c.kafkaBrokers) == 0 {
		return errors.New("distributor.forwarding.kafka-brokers must be set")
	}

	return nil
}
