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

	group       string
	topic       string
	partitionID int32
}

// Register is used to register the command to a parent command.
func (c *OffsetsCommand) Register(app *kingpin.Application, getKafkaClient func() *kgo.Client, printer Printer) {
	c.getKafkaClient = getKafkaClient
	c.printer = printer

	cmd := app.Command("offsets", "Describes offsets for a given topic and partition.")

	listOffsetsCmd := cmd.Command("list-offsets", "List all offsets of a given topic and partition.").Action(c.listOffsets)
	listOffsetsCmd.Flag("group", "Consumer group name.").Required().StringVar(&c.group)

}

func (c *OffsetsCommand) listOffsets(_ *kingpin.ParseContext) error {
	client := c.getKafkaClient()
	adm := kadm.NewClient(client)

	offsets, err := fetchConsumerGroupOffsets(adm, c.group)
	if err != nil {
		return err
	}

	// Sort topic and partitions to get a stable output.
	for _, entry := range offsets.Sorted() {
		c.printer.PrintLine(fmt.Sprintf("Topic: %s \tPartition: %d \tOffset: %d", entry.Topic, entry.Partition, entry.Offset.At))
	}

	return nil
}

func fetchConsumerGroupOffsets(adm *kadm.Client, group string) (kadm.OffsetResponses, error) {

	adm.ListEndOffsets()
	offsets, err := adm.FetchOffsets(context.Background(), group)
	if err != nil {
		return nil, err
	}
	if offsets.Error() != nil {
		return nil, offsets.Error()
	}

	return offsets, nil
}

func commitConsumerGroupOffset(adm *kadm.Client, group, topic string, partitionID int32, offset int64, printer Printer) error {
	// Commit the offset.
	toCommit := kadm.Offsets{}
	toCommit.AddOffset(topic, partitionID, offset, -1)

	committed, err := adm.CommitOffsets(context.Background(), group, toCommit)
	if err != nil {
		return err
	} else if !committed.Ok() {
		return committed.Error()
	}

	committedOffset, _ := committed.Lookup(topic, partitionID)
	printer.PrintLine(fmt.Sprintf("successfully committed offset %d for consumer group %s, topic %s and partition %d", committedOffset.At, group, topic, partitionID))

	return nil
}
