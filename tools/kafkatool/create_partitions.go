// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"fmt"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// CreatePartitionsCommand creates Kafka partitions.
type CreatePartitionsCommand struct {
	getKafkaClient func() *kgo.Client
	printer        Printer

	topic         string
	numPartitions int
}

// Register is used to register the command to a parent command.
func (c *CreatePartitionsCommand) Register(app *kingpin.Application, getKafkaClient func() *kgo.Client, printer Printer) {
	c.getKafkaClient = getKafkaClient
	c.printer = printer

	cmd := app.Command("create-partitions", "Creates Kafka partitions to a topic.").Action(c.createPartitions)
	cmd.Flag("topic", "Kafka topic to which partitions should be added.").Required().StringVar(&c.topic)
	cmd.Flag("num-partitions", "Number of additional partitions to add to the topic.").Required().IntVar(&c.numPartitions)
}

func (c *CreatePartitionsCommand) createPartitions(_ *kingpin.ParseContext) error {
	client := c.getKafkaClient()
	adm := kadm.NewClient(client)

	// Get the current number of partitions (just for logging purposes).
	details, err := getTopicDetails(adm, c.topic)
	if err != nil {
		return err
	}

	c.printer.PrintLine(fmt.Sprintf("Number of partitions before creating additional partitions: %d", len(details.Partitions)))

	// Create new partitions.
	responses, err := adm.CreatePartitions(context.Background(), c.numPartitions, c.topic)
	if err != nil {
		return err
	}

	res, err := responses.On(c.topic, nil)
	if err != nil {
		return err
	}

	if res.Err != nil {
		return res.Err
	}

	c.printer.PrintLine(fmt.Sprintf("Created %d additional partitions to topic %s", c.numPartitions, c.topic))

	// Get the current number of partitions (just for logging purposes).
	details, err = getTopicDetails(adm, c.topic)
	if err != nil {
		return err
	}

	c.printer.PrintLine(fmt.Sprintf("Number of partitions after creating additional partitions: %d", len(details.Partitions)))
	return nil
}

func getTopicDetails(adm *kadm.Client, topic string) (kadm.TopicDetail, error) {
	topics, err := adm.ListTopics(context.Background(), topic)
	if err != nil {
		return kadm.TopicDetail{}, errors.Wrap(err, "failed to list topics")
	}

	if topics.Error() != nil {
		return kadm.TopicDetail{}, errors.Wrap(topics.Error(), "failed to list topics")
	}

	details, ok := topics[topic]
	if !ok {
		return kadm.TopicDetail{}, errors.New("unable to requested find topic")
	}

	return details, nil
}
