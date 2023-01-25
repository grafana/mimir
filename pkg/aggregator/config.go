package aggregator

import (
	"errors"
	"flag"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	KafkaTopic          string `yaml:"kafka_topic" category:"experimental"`
	KafkaBrokers        string `yaml:"kafka_brokers" category:"experimental"`
	kafkaBrokers        []string
	KafkaPartitions     string `yaml:"kafka_partitions" category:"experimental"`
	kafkaPartitions     []int
	AggregationInterval time.Duration `yaml:"aggregation_interval" category:"experimental"`
	AggregationDelay    time.Duration `yaml:"aggregation_delay" category:"experimental"`
}

func (c *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&c.KafkaTopic, "aggregator.kafka-topic", "aggregations", "Kafka topic to which metrics are forwarded.")
	f.StringVar(&c.KafkaBrokers, "aggregator.kafka-brokers", "localhost:9092", "Kafka brokers to which metrics are forwarded, separated by \",\".")
	f.StringVar(&c.KafkaPartitions, "aggregator.kafka-partitions", "0", "Kafka partitions to t consume metrics from, separated by \",\". Can be a list of integers or ranges of integers (e.g. 0-3,5,7-10).")
	f.DurationVar(&c.AggregationInterval, "aggregator.aggregation-interval", time.Minute, "Interval at which to generate aggregated series.")
	f.DurationVar(&c.AggregationDelay, "aggregator.aggregation-delay", 90*time.Second, "Delay until aggregation is performed, this is the time window which clients have to send us their raw samples.")

}

func (c *Config) Validate() error {
	if len(c.KafkaTopic) == 0 {
		return errors.New("aggregator.kafka-topic must be set")
	}

	c.kafkaBrokers = strings.Split(c.KafkaBrokers, ",")
	if len(c.kafkaBrokers) == 0 {
		return errors.New("aggregator.kafka-brokers must be set")
	}

	kafkaPartitions := strings.Split(c.KafkaPartitions, ",")
	if len(kafkaPartitions) == 0 {
		return errors.New("aggregator.kafka-partitions must be set")
	}
	for _, partition := range kafkaPartitions {
		if strings.Contains(partition, "-") {
			fromTo := strings.Split(partition, "-")
			if len(fromTo) != 2 {
				return errors.New("aggregator.kafka-partitions must be a list of integers or ranges of integers")
			}
			from, err := strconv.Atoi(fromTo[0])
			if err != nil {
				return errors.New("aggregator.kafka-partitions must be a list of integers or ranges of integers")
			}
			to, err := strconv.Atoi(fromTo[1])
			if err != nil {
				return errors.New("aggregator.kafka-partitions must be a list of integers or ranges of integers")
			}

			for ; from <= to; from++ {
				c.kafkaPartitions = append(c.kafkaPartitions, from)
			}
		} else {
			partitionInt, err := strconv.Atoi(partition)
			if err != nil {
				return errors.New("aggregator.kafka-partitions must be a list of integers")
			}
			c.kafkaPartitions = append(c.kafkaPartitions, partitionInt)
		}
	}

	return nil
}
