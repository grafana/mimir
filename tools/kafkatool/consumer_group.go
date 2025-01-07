// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"fmt"

	"github.com/alecthomas/kingpin/v2"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ConsumerGroupCommand manages Kafka consumer groups.
type ConsumerGroupCommand struct {
	getKafkaClient func() *kgo.Client
	printer        Printer

	group       string
	topic       string
	partitionID int32
	offset      int64
	copyToGroup string
}

// Register is used to register the command to a parent command.
func (c *ConsumerGroupCommand) Register(app *kingpin.Application, getKafkaClient func() *kgo.Client, printer Printer) {
	c.getKafkaClient = getKafkaClient
	c.printer = printer

	cmd := app.Command("consumer-group", "Manages a single consumer group.")

	listOffsetsCmd := cmd.Command("list-offsets", "List all offsets of a given consumer group.").Action(c.listOffsets)
	listOffsetsCmd.Flag("group", "Consumer group name.").Required().StringVar(&c.group)

	commitOffsetCmd := cmd.Command("commit-offset", "Commits an offset for a given topic and partition.").Action(c.commitOffset)
	commitOffsetCmd.Flag("group", "Consumer group name.").Required().StringVar(&c.group)
	commitOffsetCmd.Flag("topic", "Topic.").Required().StringVar(&c.topic)
	commitOffsetCmd.Flag("partition", "Partition ID.").Required().Int32Var(&c.partitionID)
	commitOffsetCmd.Flag("offset", "The offset to commit.").Required().Int64Var(&c.offset)

	copyOffsetCmd := cmd.Command("copy-offset", "Copies an offset for a given topic and partition from a consumer group to another.").Action(c.copyOffset)
	copyOffsetCmd.Flag("from-group", "Consumer group from which the offset should be copied.").Required().StringVar(&c.group)
	copyOffsetCmd.Flag("to-group", "Consumer group to which the offset should be copied.").Required().StringVar(&c.copyToGroup)
	copyOffsetCmd.Flag("topic", "Topic.").Required().StringVar(&c.topic)
	copyOffsetCmd.Flag("partition", "Partition ID.").Required().Int32Var(&c.partitionID)
}

func (c *ConsumerGroupCommand) listOffsets(_ *kingpin.ParseContext) error {
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

func (c *ConsumerGroupCommand) commitOffset(_ *kingpin.ParseContext) error {
	client := c.getKafkaClient()
	adm := kadm.NewClient(client)

	// Commit the offset.
	return commitConsumerGroupOffset(adm, c.group, c.topic, c.partitionID, c.offset, c.printer)
}

func (c *ConsumerGroupCommand) copyOffset(_ *kingpin.ParseContext) error {
	client := c.getKafkaClient()
	adm := kadm.NewClient(client)

	// Fetch the offset from the source consumer group.
	offsets, err := fetchConsumerGroupOffsets(adm, c.group)
	if err != nil {
		return err
	}

	offset, ok := offsets.Lookup(c.topic, c.partitionID)
	if !ok {
		return fmt.Errorf("unable to find offset for consumer group %s topic %s and partition %d", c.group, c.topic, c.partitionID)
	}

	// Commit the offset.
	return commitConsumerGroupOffset(adm, c.copyToGroup, c.topic, c.partitionID, offset.At, c.printer)
}

func fetchConsumerGroupOffsets(adm *kadm.Client, group string) (kadm.OffsetResponses, error) {
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
