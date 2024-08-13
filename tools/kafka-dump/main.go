// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/go-jose/go-jose/v3/json"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
	"go.uber.org/atomic"
)

type key int

const (
	originalOffsetKey key = iota
)

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	cfg := struct {
		topic             string
		brokers           string
		partition         int
		skipFirst         int
		mode              string
		exportOffsetStart int64
		exportMaxRecords  int
		fileName          string
	}{}

	flag.StringVar(&cfg.topic, "topic", "mimir", "Kafka topic to dump")
	flag.StringVar(&cfg.brokers, "brokers", "localhost:9092", "Kafka brokers")
	flag.IntVar(&cfg.partition, "partition", 0, "Kafka partition to dump or import into")
	flag.IntVar(&cfg.skipFirst, "skipFirst", 0, "Skip until input record with offset N")
	flag.StringVar(&cfg.mode, "mode", "import", "Mode to run in: import or export")
	flag.Int64Var(&cfg.exportOffsetStart, "exportOffsetStart", 0, "Offset to start exporting from")
	flag.IntVar(&cfg.exportMaxRecords, "exportMaxRecords", 1_000_000, "Maximum number of records to export")
	flag.StringVar(&cfg.fileName, "file", "-", "File to read from or write to. - for stdin/stdout")

	// Parse the CLI arguments.
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	_, _ = fmt.Fprintf(os.Stderr, "Importing to topic %q via brokers %q partition %d\n", cfg.topic, cfg.brokers, cfg.partition)
	var file io.ReadWriter
	if cfg.fileName == "-" {
		switch cfg.mode {
		case "export":
			file = os.Stdout
		case "import":
			file = os.Stdin
		}
	} else {
		explicitFile, err := os.Open(cfg.fileName)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed to open file %s: %v\n", cfg.fileName, err)
			return
		}
		defer explicitFile.Close()
		file = explicitFile
	}

	if cfg.mode == "import" {
		if err := doImport(file, cfg.topic, cfg.brokers, cfg.partition, cfg.skipFirst); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed to import: %v\n", err)
			return
		}
	} else if cfg.mode == "export" {
		if err := doExport(file, cfg.topic, cfg.brokers, cfg.partition, cfg.exportOffsetStart, cfg.exportMaxRecords); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed to export: %v\n", err)
			return
		}
	} else {
		_, _ = fmt.Fprintf(os.Stderr, "unknown mode %q\n", cfg.mode)
	}
}

func doExport(output io.Writer, topicName string, broker string, partition int, offset int64, maxRecords int) error {
	client, err := kgo.NewClient(
		kgo.WithHooks(kprom.NewMetrics("franz_go", kprom.Registerer(prometheus.DefaultRegisterer))),
		kgo.SeedBrokers(broker),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topicName: {int32(partition): kgo.NewOffset().At(offset)}},
		),
	)
	if err != nil {
		return fmt.Errorf("failed to create kafka client: %w", err)
	}
	defer client.Close()
	err = testClient(client)
	if err != nil {
		return fmt.Errorf("failed to test client: %w", err)
	}
	go func() {
		for {
			time.Sleep(time.Second)
			fmt.Fprintf(os.Stderr, "produced records: %d, offset %d\n", recordCount.Load(), consumedOffset.Load())
		}
	}()

	encoder := json.NewEncoder(output)

	for recordCount.Load() < int64(maxRecords) {
		fetches := client.PollFetches(context.Background())
		if err != nil {
			return fmt.Errorf("failed to fetch records: %w", err)
		}
		fetches.EachRecord(func(record *kgo.Record) {
			if recordCount.Inc() > int64(maxRecords) {
				return
			}
			consumedOffset.Store(record.Offset)
			err = encoder.Encode(record)
			if err != nil {
				panic(fmt.Sprintf("encoding offset %d: %v", record.Offset, err))
			}
		})
	}
	return nil
}

var (
	recordCount          = &atomic.Int64{}
	consumedOffset       = &atomic.Int64{}
	recordsTooLarge      = &atomic.Int64{}
	corruptedJSONRecords = &atomic.Int64{}
)

func doImport(from io.Reader, topic, broker string, partition, skipUntil int) error {
	// create kafka client
	client, err := kgo.NewClient(
		kgo.WithHooks(kprom.NewMetrics("franz_go", kprom.Registerer(prometheus.DefaultRegisterer))),
		kgo.SeedBrokers(broker),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.DisableIdempotentWrite(),
		kgo.BrokerMaxWriteBytes(268_435_456),
		kgo.MaxBufferedBytes(268_435_456),
	)
	if err != nil {
		return fmt.Errorf("failed to create kafka client: %w", err)
	}

	err = testClient(client)
	if err != nil {
		return fmt.Errorf("failed to test client: %w", err)
	}
	go func() {
		for {
			time.Sleep(time.Second)
			fmt.Printf("produced items: %d, of those skipped because too large: %d, buffered records: %d, buffered bytes: %d\n", recordCount.Load(), recordsTooLarge.Load(), client.BufferedProduceRecords(), client.BufferedProduceBytes())
		}
	}()

	separator := bufio.NewScanner(from)
	separator.Buffer(make([]byte, 10_000_000), 10_000_000) // 10MB buffer because we can have large records

	for recordsIdx := 0; separator.Scan(); recordsIdx++ {
		item := separator.Bytes()
		record := &kgo.Record{}
		err = json.Unmarshal(item, record)
		if err != nil {
			corruptedJSONRecords.Inc()
			_, _ = fmt.Fprintf(os.Stderr, "corrupted JSON record %d: %v\n", recordsIdx, err)
			continue
		}
		if record.Offset < int64(skipUntil) {
			continue
		}
		record.Topic = topic
		record.Partition = int32(partition)
		record.Context = context.WithValue(context.Background(), originalOffsetKey, record.Offset)

		client.Produce(context.Background(), record, func(record *kgo.Record, err error) {
			recordCount.Inc()
			if errors.Is(err, kerr.MessageTooLarge) {
				recordsTooLarge.Inc()
				return
			}
			if err != nil {
				panic(fmt.Sprintf("failed to produce record with offset %d: %v", record.Context.Value(originalOffsetKey), err))
			}
		})
	}
	fmt.Println("waiting for produce to finish")
	err = client.Flush(context.Background())
	if err != nil {
		return fmt.Errorf("failed to flush records: %w", err)
	}
	if separator.Err() != nil {
		return fmt.Errorf("separator scan failed: %w", separator.Err())
	}
	return nil
}

func testClient(client *kgo.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := client.Ping(ctx)
	if err != nil {
		return fmt.Errorf("failed to ping kafka: %w", err)
	}
	return nil
}
