// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"fmt"
	"sort"

	"github.com/alecthomas/kingpin/v2"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ListTopicsCommand lists all Kafka topics and their partition counts.
type ListTopicsCommand struct {
	getKafkaClient func() *kgo.Client
	printer        Printer
}

// Register is used to register the command to a parent command.
func (c *ListTopicsCommand) Register(app *kingpin.Application, getKafkaClient func() *kgo.Client, printer Printer) {
	c.getKafkaClient = getKafkaClient
	c.printer = printer

	app.Command("list-topics", "Lists all Kafka topics and their partition counts.").Action(c.listTopics)
}

func (c *ListTopicsCommand) listTopics(_ *kingpin.ParseContext) error {
	client := c.getKafkaClient()
	adm := kadm.NewClient(client)

	topics, err := adm.ListTopics(context.Background())
	if err != nil {
		return err
	}
	if topics.Error() != nil {
		return topics.Error()
	}

	// Sort topic names for deterministic output.
	names := make([]string, 0, len(topics))
	for name := range topics {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		detail := topics[name]
		c.printer.PrintLine(fmt.Sprintf("Topic: %s \tPartitions: %d", name, len(detail.Partitions)))
	}

	return nil
}
