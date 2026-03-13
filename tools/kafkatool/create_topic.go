// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/alecthomas/kingpin/v2"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// CreateTopicCommand creates a Kafka topic.
type CreateTopicCommand struct {
	getKafkaClient func() *kgo.Client
	printer        Printer

	topic         string
	numPartitions int
}

// Register is used to register the command to a parent command.
func (c *CreateTopicCommand) Register(app *kingpin.Application, getKafkaClient func() *kgo.Client, printer Printer) {
	c.getKafkaClient = getKafkaClient
	c.printer = printer

	cmd := app.Command("create-topic", "Creates a Kafka topic.").Action(c.createTopic)
	cmd.Flag("topic", "Name of the Kafka topic to create.").Required().StringVar(&c.topic)
	cmd.Flag("num-partitions", "Number of partitions for the topic.").Required().IntVar(&c.numPartitions)
}

func (c *CreateTopicCommand) createTopic(_ *kingpin.ParseContext) error {
	client := c.getKafkaClient()
	adm := kadm.NewClient(client)

	// Check if the topic already exists.
	if _, err := getTopicDetails(adm, c.topic); err == nil {
		return fmt.Errorf("topic %s already exists", c.topic)
	} else if !errors.Is(err, errTopicNotFound) {
		return err
	}

	// Create the topic. Replication factor -1 means use broker default.
	_, err := adm.CreateTopic(context.Background(), int32(c.numPartitions), -1, nil, c.topic)
	if err != nil {
		return err
	}

	c.printer.PrintLine(fmt.Sprintf("Created topic %s with %d partitions", c.topic, c.numPartitions))
	return nil
}
