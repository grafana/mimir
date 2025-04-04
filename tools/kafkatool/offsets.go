// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"fmt"

	"github.com/alecthomas/kingpin/v2"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ConsumerdddfdGroupCommand manages Kafka consumer groups.
type OffsetsCommand struct {
	getKafkaClient func() *kgo.Client
	printer        Printer

	topic       string
	partitionID int32
	offsetType  string
	milli       int64
}

// Register is used to register the command to a parent command.
func (c *OffsetsCommand) Register(app *kingpin.Application, getKafkaClient func() *kgo.Client, printer Printer) {
	c.getKafkaClient = getKafkaClient
	c.printer = printer

	cmd := app.Command("offsets", "Describes offsets for a given topic and partition.")
	listOffsetsCmd := cmd.Command("list", "List all offsets of a given topic and partition.").Action(c.listOffsets)
	listOffsetsCmd.Flag("topic", "Kafka topic to dump").Required().StringVar(&c.topic)
	listOffsetsCmd.Flag("partition", "Kafka partition to dump or import into").Required().Int32Var(&c.partitionID)
	listOffsetsCmd.Flag("type", "offset type to list").Default("end").EnumVar(&c.offsetType, "start", "committed", "end", "after_ms")
	listOffsetsCmd.Flag("milli", "millisecond timestamp to list offsets after").Int64Var(&c.milli)
}

func (c *OffsetsCommand) listOffsets(_ *kingpin.ParseContext) error {
	client := c.getKafkaClient()
	adm := kadm.NewClient(client)

	offsets, err := fetchOffsets(adm, c.topic, c.offsetType, c.milli)
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
