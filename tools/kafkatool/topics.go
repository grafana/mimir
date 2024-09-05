package main

import (
	"context"
	"fmt"

	"github.com/alecthomas/kingpin/v2"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type TopicsCommand struct {
	getKafkaClient func() *kgo.Client
	printer        Printer

	topic             string
	numPartitions     int32
	replicationFactor int16

	create   bool
	list     bool
	describe bool
}

func (c *TopicsCommand) Register(app *kingpin.Application, getKafkaClient func() *kgo.Client, printer Printer) {
	c.getKafkaClient = getKafkaClient
	c.printer = printer

	cmd := app.Command("topic", "Create, list, or describe Kafka topic(s).").Action(c.topicCommand)
	cmd.Flag("topic", "The name of the Kafka topic.").StringVar(&c.topic)
	cmd.Flag("num-partitions", "The number of partitions to create the topic with.").Int32Var(&c.numPartitions)
	cmd.Flag("replication-factor", "The replication factor to create the topic with.").Int16Var(&c.replicationFactor)
	cmd.Flag("create", "Create the Kafka topic.").BoolVar(&c.create)
	cmd.Flag("list", "List all Kafka topics.").BoolVar(&c.list)
	cmd.Flag("describe", "Describe the Kafka topic.").BoolVar(&c.describe)
}

func (c *TopicsCommand) topicCommand(_ *kingpin.ParseContext) error {
	if c.create {
		return c.createTopic()
	} else if c.describe {
		c.printer.PrintLine("The '--describe' command is still unimplemented.") // TODO
	} else if c.list {
		c.printer.PrintLine("The '--list' command is still unimplemented.") // TODO
	} else {
		c.printer.PrintLine("Must specify either '--create', '--list', or '--describe'")
	}

	return nil
}

func (c *TopicsCommand) createTopic() error {
	if c.topic == "" {
		return fmt.Errorf("missing topic name")
	}
	if c.numPartitions <= 0 {
		return fmt.Errorf("number of partitions must be greater than zero")
	}
	if c.replicationFactor <= 0 {
		return fmt.Errorf("replication factor must be greater than zero")
	}

	adm := kadm.NewClient(c.getKafkaClient())

	resp, err := adm.CreateTopic(context.Background(), c.numPartitions, c.replicationFactor, map[string]*string{}, c.topic)
	if err != nil {
		return err
	}
	if resp.Err != nil {
		return fmt.Errorf("problem creating topic: %w (%s)", resp.Err, resp.ErrMessage)
	}

	c.printer.PrintLine(fmt.Sprintf("Topic %s created with %d partitions and rf=%d", resp.Topic, resp.NumPartitions, resp.ReplicationFactor))

	return nil
}
