// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type DumpCommand struct {
	topic             string
	partition         int
	skipFirst         int
	exportOffsetStart int64
	exportMaxRecords  int
	inOutFile         *os.File
	getKafkaClient    func() *kgo.Client
	printer           Printer
}

// Register is used to register the command to a parent command.
func (c *DumpCommand) Register(app *kingpin.Application, getKafkaClient func() *kgo.Client, printer Printer) {
	c.getKafkaClient = getKafkaClient
	c.printer = printer

	cmd := app.Command("dump", "Dump Kafka topic contents")
	cmd.Flag("topic", "Kafka topic to dump").Required().StringVar(&c.topic)
	cmd.Flag("partition", "Kafka partition to dump or import into").Required().IntVar(&c.partition)
	cmd.Flag("skip-first", "Skip until input record with offset N").Default("0").IntVar(&c.skipFirst)
	cmd.Flag("offset", "Offset to start exporting from").Default("0").Int64Var(&c.exportOffsetStart)
	cmd.Flag("export-max-records", "Maximum number of records to export").Default("1000000").IntVar(&c.exportMaxRecords)
	cmd.Flag("file", "File to read from or write to.").Required().OpenFileVar(&c.inOutFile, os.O_RDWR|os.O_CREATE, 0600)

	cmd.Command("import", "Import records from a file into a Kafka topic").Action(c.doImport)
	cmd.Command("export", "Export records from a Kafka topic into a file").Action(c.doExport)
	cmd.Command("print", "Print the write requests inside records dumped using this tool").Action(c.doPrint)
}

type key int

const (
	originalOffsetKey key = iota
)

func (c *DumpCommand) doExport(*kingpin.ParseContext) error {
	var (
		recordCount    = &atomic.Int64{}
		consumedOffset = &atomic.Int64{}
	)

	client := c.getKafkaClient()
	client.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		c.topic: {int32(c.partition): kgo.NewOffset().At(c.exportOffsetStart)}},
	)

	// Print the number of consumed records both when the export is in progress and at the end.
	go func() {
		for {
			time.Sleep(time.Second)
			c.printer.PrintLine(fmt.Sprintf("consumed records: %d, offset %d", recordCount.Load(), consumedOffset.Load()))
		}
	}()
	defer func() {
		c.printer.PrintLine(fmt.Sprintf("consumed records: %d, offset %d", recordCount.Load(), consumedOffset.Load()))
	}()

	encoder := json.NewEncoder(c.inOutFile)

	lastAvailableOffset := int64(-1)
	for recordCount.Load() < int64(c.exportMaxRecords) {
		fetches := client.PollFetches(context.Background())
		if err := fetches.Err(); err != nil {
			return fmt.Errorf("failed to fetch records: %w", err)
		}
		fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
			lastAvailableOffset = max(lastAvailableOffset, partition.HighWatermark-1)
		})
		var err error
		fetches.EachRecord(func(record *kgo.Record) {
			if recordCount.Inc() > int64(c.exportMaxRecords) {
				return
			}
			consumedOffset.Store(record.Offset)
			encodeErr := encoder.Encode(record)
			if encodeErr != nil {
				err = fmt.Errorf("encoding offset %d: %v", record.Offset, encodeErr)
			}
		})
		if err != nil {
			return err
		}
		if lastAvailableOffset >= 0 && consumedOffset.Load() >= lastAvailableOffset {
			c.printer.PrintLine("reached high watermark before max records")
			break
		}
	}
	return nil
}

func (c *DumpCommand) doImport(*kingpin.ParseContext) error {
	var (
		recordCount          = &atomic.Int64{}
		recordsTooLarge      = &atomic.Int64{}
		corruptedJSONRecords = &atomic.Int64{}
	)

	client := c.getKafkaClient()

	go func() {
		for {
			time.Sleep(time.Second)
			c.printer.PrintLine(fmt.Sprintf(
				"produced items: %d, of those skipped because too large: %d, buffered records: %d, buffered bytes: %d",
				recordCount.Load(), recordsTooLarge.Load(), client.BufferedProduceRecords(), client.BufferedProduceBytes()),
			)
		}
	}()

	produceErr := atomic.NewError(nil)

	parseErr := c.parseDumpFile(
		func(_ int, record *kgo.Record) {
			client.Produce(context.Background(), record, func(record *kgo.Record, err error) {
				recordCount.Inc()
				if errors.Is(err, kerr.MessageTooLarge) {
					recordsTooLarge.Inc()
					return
				}
				if err != nil {
					produceErr.Store(fmt.Errorf("failed to produce record with offset %d: %v", record.Context.Value(originalOffsetKey), err))
				}
			})
		},
		func(recordIdx int, err error) {
			corruptedJSONRecords.Inc()
			c.printer.PrintLine(fmt.Sprintf("corrupted JSON record %d: %v", recordIdx, err))
		})

	c.printer.PrintLine("waiting for produce to finish")
	err := client.Flush(context.Background())
	if err != nil {
		return fmt.Errorf("failed to flush records: %w", err)
	}

	if parseErr != nil {
		return fmt.Errorf("failed to parse dump file: %w", parseErr)
	}
	if err = produceErr.Load(); err != nil {
		return err
	}
	return nil
}

func (c *DumpCommand) doPrint(*kingpin.ParseContext) error {
	return c.parseDumpFile(
		func(recordIdx int, record *kgo.Record) {
			req := mimirpb.WriteRequest{}
			err := req.Unmarshal(record.Value)
			if err != nil {
				c.printer.PrintLine(fmt.Sprintf("failed to unmarshal write request from record %d: %v", recordIdx, err))
				return
			}

			// Print the time series in the write request.
			c.printer.PrintLine(fmt.Sprintf("Record #%d (offset: %d)", recordIdx, record.Offset))
			for _, series := range req.Timeseries {
				for _, sample := range series.Samples {
					c.printer.PrintLine(fmt.Sprintf("%s %d %f",
						mimirpb.FromLabelAdaptersToLabels(series.Labels).String(),
						sample.TimestampMs,
						sample.Value,
					))
				}
			}
			c.printer.PrintLine("")
		},
		func(recordIdx int, err error) {
			c.printer.PrintLine(fmt.Sprintf("corrupted JSON record %d: %v", recordIdx, err))
		})
}

func (c *DumpCommand) parseDumpFile(onRecordParsed func(recordIdx int, record *kgo.Record), onRecordCorrupted func(recordIdx int, err error)) error {
	separator := bufio.NewScanner(c.inOutFile)
	separator.Buffer(make([]byte, 10_000_000), 10_000_000) // 10MB buffer because we can have large records

	for recordIdx := 0; separator.Scan(); recordIdx++ {
		item := separator.Bytes()
		record := &kgo.Record{}
		err := json.Unmarshal(item, record)
		if err != nil {
			onRecordCorrupted(recordIdx, err)
			continue
		}
		if record.Offset < int64(c.skipFirst) {
			continue
		}
		record.Topic = c.topic
		record.Partition = int32(c.partition)
		record.Context = context.WithValue(context.Background(), originalOffsetKey, record.Offset)

		onRecordParsed(recordIdx, record)
	}

	if separator.Err() != nil {
		return fmt.Errorf("separator scan failed: %w", separator.Err())
	}

	return nil
}
