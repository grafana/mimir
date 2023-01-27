package aggregator

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"regexp"
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
	FlushInterval       time.Duration `yaml:"flush_interval" category:"experimental"`
	ResultChanSize      int           `yaml:"result_chan_size" category:"experimental"`

	KafkaPartitionsTotal   int
	KafkaPartitionsReaders int
	KafkaReaderID          string
}

func (c *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&c.KafkaTopic, "aggregator.kafka-topic", "aggregations", "Kafka topic to which metrics are forwarded.")
	f.StringVar(&c.KafkaBrokers, "aggregator.kafka-brokers", "localhost:9092", "Kafka brokers to which metrics are forwarded, separated by \",\".")
	f.StringVar(&c.KafkaPartitions, "aggregator.kafka-partitions", "0", "Kafka partitions to t consume metrics from, separated by \",\". Can be a list of integers or ranges of integers (e.g. 0-3,5,7-10). If not set, computed from -aggregator.kafka-partitions-total and -aggregator.kafka-partitions-readers.")
	f.DurationVar(&c.AggregationInterval, "aggregator.aggregation-interval", time.Minute, "Interval at which to generate aggregated series.")
	f.DurationVar(&c.AggregationDelay, "aggregator.aggregation-delay", 90*time.Second, "Delay until aggregation is performed, this is the time window which clients have to send us their raw samples.")
	f.DurationVar(&c.FlushInterval, "aggregator.flush-interval", time.Second, "Interval at which to flush aggregated samples.")
	f.IntVar(&c.ResultChanSize, "aggregator.result-chan-size", 1000, "Size of the channel used to send aggregated samples into batches.")

	f.IntVar(&c.KafkaPartitionsTotal, "aggregator.kafka-partitions-total", 0, "Total number of Kafka partitions. Used to compute which partitions this aggregator should be reading.")
	f.IntVar(&c.KafkaPartitionsReaders, "aggregator.kafka-partitions-readers", 0, "Total number of aggregators. Used to compute which partitions this aggregator should be reading.")
	hostname, _ := os.Hostname()
	f.StringVar(&c.KafkaReaderID, "aggregator.kafka-reader-id", hostname, "ID of this aggregator in a form <prefix>-<id>, used to compute partitions to read. ID starts from 0.")
}

func (c *Config) Validate() error {
	if len(c.KafkaTopic) == 0 {
		return errors.New("aggregator.kafka-topic must be set")
	}

	c.kafkaBrokers = strings.Split(c.KafkaBrokers, ",")
	if len(c.kafkaBrokers) == 0 {
		return errors.New("aggregator.kafka-brokers must be set")
	}

	if len(c.KafkaPartitions) == 0 {
		if c.KafkaPartitionsTotal <= 0 || c.KafkaPartitionsReaders <= 0 || c.KafkaReaderID == "" {
			return errors.New("either aggregator.kafka-partitions or (aggregator.kafka-partitions-total and aggregator.kafka-partitions-readers) must be set")
		}

		// Parse ID
		m := regexp.MustCompile("^.*-(\\d+)$").FindStringSubmatch(c.KafkaReaderID)
		if len(m) != 2 {
			return fmt.Errorf("aggregator.kafka-reader-id doesn't match <prefix>-<id>: %s", c.KafkaReaderID)
		}

		id, err := strconv.Atoi(m[1])
		if err != nil {
			return fmt.Errorf("aggregator.kafka-reader-id doesn't match <prefix>-<id>: %s: %w", c.KafkaReaderID, err)
		}

		if id < 0 || id >= c.KafkaPartitionsReaders {
			return fmt.Errorf("invalid aggregator.kafka-reader-id: %s (readers: %d)", c.KafkaReaderID, c.KafkaPartitionsReaders)
		}

		partitionsPerReader := make([][]int, c.KafkaPartitionsReaders)
		for p := 0; p < c.KafkaPartitionsTotal; p++ {
			r := p % c.KafkaPartitionsReaders
			partitionsPerReader[r] = append(partitionsPerReader[r], p)
		}
		c.kafkaPartitions = partitionsPerReader[id]
	} else {
		for _, partition := range strings.Split(c.KafkaPartitions, ",") {
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
	}

	return nil
}
