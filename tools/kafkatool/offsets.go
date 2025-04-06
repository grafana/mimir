// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"fmt"

	"github.com/alecthomas/kingpin/v2"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// OffsetsCommand allows inspecting offsets for a given topic.
type OffsetsCommand struct {
	getKafkaClient func() *kgo.Client
	printer        Printer

	topic      string
	offsetType string
	millis     int64
}

// Register is used to register the command to a parent command.
func (c *OffsetsCommand) Register(app *kingpin.Application, getKafkaClient func() *kgo.Client, printer Printer) {
	c.getKafkaClient = getKafkaClient
	c.printer = printer

	cmd := app.Command("offsets", "Describes offsets for a given topic and partition.")
	listOffsetsCmd := cmd.Command("list", "List all offsets of a given topic and partition.").Action(c.listOffsets)
	listOffsetsCmd.Flag("topic", "Kafka topic to dump").Required().StringVar(&c.topic)
	listOffsetsCmd.Flag("type", "offset type to list").Default("end").EnumVar(&c.offsetType, "start", "committed", "end", "after_ms")
	listOffsetsCmd.Flag("millis", "millisecond timestamp to list offsets after when --type=after_ms").Int64Var(&c.millis)
}

func (c *OffsetsCommand) listOffsets(_ *kingpin.ParseContext) error {
	if c.offsetType == "after_ms" && c.millis == 0 {
		return fmt.Errorf("--millis is required when --type=after_ms")
	}
	if c.millis != 0 && c.offsetType != "after_ms" {
		return fmt.Errorf("--millis is only valid when --type=after_ms")
	}

	client := c.getKafkaClient()
	adm := kadm.NewClient(client)

	offsets, err := fetchOffsets(adm, c.topic, c.offsetType, c.millis)
	if err != nil {
		return err
	}

	// Sort topic and partitions to get a stable output.
	for _, entry := range offsets.Offsets().Sorted() {
		c.printer.PrintLine(fmt.Sprintf("Topic: %s \tPartition: %d \tOffset: %d", entry.Topic, entry.Partition, entry.At))
	}

	return nil
}

func fetchOffsets(adm *kadm.Client, topic string, t string, milli int64) (kadm.ListedOffsets, error) {
	var offs kadm.ListedOffsets
	var err error

	switch t {
	case "start":
		offs, err = adm.ListStartOffsets(context.Background(), topic)
	case "end":
		offs, err = adm.ListEndOffsets(context.Background(), topic)
	case "committed":
		offs, err = adm.ListCommittedOffsets(context.Background(), topic)
	case "after_ms":
		offs, err = adm.ListOffsetsAfterMilli(context.Background(), milli, topic)
	}
	if err != nil {
		return nil, err
	}
	if offs.Error() != nil {
		return nil, offs.Error()
	}

	return offs, nil
}
