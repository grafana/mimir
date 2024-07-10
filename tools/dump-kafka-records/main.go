// SPDX-License-Identifier: AGPL-3.0-only
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/alertmanager/matchers/parse"
	"github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/common/model"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

type Config struct {
	Kafka                 ingest.KafkaConfig
	PartitionID           int
	Offset                int64
	FilterTenantID        string
	FilterSeriesLabels    string
	FilterSampleTimestamp int64
}

func (c *Config) RegisterFlags(f *flag.FlagSet) {
	c.Kafka.RegisterFlagsWithPrefix("kafka", f)
	f.IntVar(&c.PartitionID, "partition-id", 0, "Kafka partition ID")
	f.Int64Var(&c.Offset, "offset", 0, "Kafka offset to start reading from (use -2 for beginning of partition, -1 for end of partition)")
	f.StringVar(&c.FilterTenantID, "filter-tenant-id", "", "Tenant ID to filter (empty string to not filter by tenant)")
	f.StringVar(&c.FilterSeriesLabels, "filter-series-labels", "", "Labels matcher to use to filter series (empty to not filter by series labels)")
	f.Int64Var(&c.FilterSampleTimestamp, "filter-sample-timestamp", 0, "Sample timestamp to filter (0 to not filter by timestamp)")
}

func main() {
	// Parse CLI flags.
	cfg := &Config{}
	cfg.RegisterFlags(flag.CommandLine)
	if err := flag.CommandLine.Parse(os.Args[1:]); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	// Override some config.
	cfg.Kafka.AutoCreateTopicEnabled = false
	cfg.Kafka.AutoCreateTopicDefaultPartitions = 0 // Disabled

	// Create Kafka client.
	at := kgo.NewOffset()
	at.At(cfg.Offset)

	logger := log.NewLogfmtLogger(os.Stdout)
	client, err := newKafkaClient(cfg, at, logger)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	if err := readFromKafka(context.Background(), client, cfg); err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
}

func newKafkaClient(cfg *Config, at kgo.Offset, logger log.Logger) (*kgo.Client, error) {
	const fetchMaxBytes = 100_000_000

	opts := append(
		ingest.CommonKafkaClientOptions(cfg.Kafka, nil, logger),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			cfg.Kafka.Topic: {int32(cfg.PartitionID): at},
		}),
		kgo.FetchMinBytes(1),
		kgo.FetchMaxBytes(fetchMaxBytes),
		kgo.FetchMaxWait(5*time.Second),
		kgo.FetchMaxPartitionBytes(50_000_000),

		// BrokerMaxReadBytes sets the maximum response size that can be read from
		// Kafka. This is a safety measure to avoid OOMing on invalid responses.
		// franz-go recommendation is to set it 2x FetchMaxBytes.
		kgo.BrokerMaxReadBytes(2*fetchMaxBytes),
	)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "creating kafka client")
	}

	return client, nil
}

func readFromKafka(ctx context.Context, client *kgo.Client, cfg *Config) error {
	var seriesLabelMatchers labels.Matchers

	// Parse the label matchers to use to filter series (if configured)
	if cfg.FilterSeriesLabels != "" {
		var err error

		seriesLabelMatchers, err = parse.Matchers(cfg.FilterSeriesLabels)
		if err != nil {
			return err
		}
	}

	for {
		fetches := client.PollFetches(ctx)
		if fetches.Err() != nil {
			return fetches.Err()
		}

		var returnErr error

		fetches.EachRecord(func(record *kgo.Record) {
			// Skip any tenant different than the one we want to filter.
			if cfg.FilterTenantID != "" && string(record.Key) != cfg.FilterTenantID {
				return
			}

			req := &mimirpb.WriteRequest{}
			if err := req.Unmarshal(record.Value); err != nil {
				returnErr = errors.Wrap(err, "failed to unmarshal write request")
				return
			}

			matches := false

			for _, series := range req.Timeseries {
				if matchSeriesFilters(series, seriesLabelMatchers, cfg.FilterSampleTimestamp) {
					matches = true
					break
				}
			}

			if matches {
				data, err := json.Marshal(req)
				if err != nil {
					returnErr = errors.Wrap(err, "failed to marshal write request to JSON")
					return
				}

				fmt.Println(string(data))
			}
		})

		if returnErr != nil {
			return returnErr
		}
	}
}

func matchSeriesFilters(series mimirpb.PreallocTimeseries, matchers labels.Matchers, timestamp int64) bool {
	if len(matchers) > 0 {
		if !matchers.Matches(fromLabelAdaptersToLabelSet(series.Labels)) {
			return false
		}
	}

	if timestamp > 0 {
		found := false

		for _, sample := range series.Samples {
			if timestamp == sample.TimestampMs {
				found = true
			}
		}

		if !found {
			return false
		}
	}

	return true
}

func fromLabelAdaptersToLabelSet(from []mimirpb.LabelAdapter) model.LabelSet {
	set := make(model.LabelSet, len(from))

	for _, l := range from {
		set[model.LabelName(l.Name)] = model.LabelValue(l.Value)
	}

	return set
}
