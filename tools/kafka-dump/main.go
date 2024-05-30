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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
	"go.uber.org/atomic"
)

func main() {

	topic := flag.String("topic", "mimir", "Kafka topic to dump")
	brokers := flag.String("brokers", "localhost:9092", "Kafka brokers")
	partition := flag.Int("partition", 0, "Kafka partition to dump or import into")
	skipFirst := flag.Int("skipFirst", 0, "Skip until input record with offset N")
	mode := flag.String("mode", "import", "Mode to run in: import or export")
	exportOffsetStart := flag.Int64("exportOffsetStart", 0, "Offset to start exporting from")
	exportMaxRecords := flag.Int("exportMaxRecords", 1_000_000, "Maximum number of records to export")
	fileName := flag.String("file", "-", "File to read from or write to. - for stdin/stdout")
	flag.Parse()

	_, _ = fmt.Fprintf(os.Stderr, "Importing to topic %q via brokers %q partition %d\n", *topic, *brokers, *partition)
	var file io.ReadWriter
	if *fileName == "-" {
		switch *mode {
		case "export":
			file = os.Stdout
		case "import":
			file = os.Stdin
		}
	} else {
		explicitFile, err := os.Open(*fileName)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed to open file %s: %v\n", *fileName, err)
			return
		}
		defer explicitFile.Close()
		file = explicitFile
	}

	if *mode == "import" {
		if err := doImport(file, *topic, *brokers, *partition, *skipFirst); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed to import: %v\n", err)
			return
		}
	} else if *mode == "export" {
		if err := doExport(file, *topic, *brokers, *partition, *exportOffsetStart, *exportMaxRecords); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "failed to export: %v\n", err)
			return
		}
	} else {
		_, _ = fmt.Fprintf(os.Stderr, "unknown mode %q\n", *mode)
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
		record.Context = context.WithValue(context.Background(), "original_offset", record.Offset)

		client.Produce(context.Background(), record, func(record *kgo.Record, err error) {
			recordCount.Inc()
			if errors.Is(err, kerr.MessageTooLarge) {
				recordsTooLarge.Inc()
				return
			}
			if err != nil {
				panic(fmt.Sprintf("failed to produce record with offset %d: %v", record.Context.Value("original_offset"), err))
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
