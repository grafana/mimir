// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"fmt"

	"github.com/alecthomas/kingpin/v2"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type BrokersCommand struct {
	getKafkaClient func() *kgo.Client
	printer        Printer

	topic          string
	minPartitionID int32
	maxPartitionID int32
}

// Register is used to register the command to a parent command.
func (c *BrokersCommand) Register(app *kingpin.Application, getKafkaClient func() *kgo.Client, printer Printer) {
	c.getKafkaClient = getKafkaClient
	c.printer = printer

	cmd := app.Command("brokers", "Retrieve information about brokers.")

	listLeadersCmd := cmd.Command("list-leaders-by-partition", "List leader brokers by partition").Action(c.listLeadersByPartition)
	listLeadersCmd.Flag("topic", "Topic.").Required().StringVar(&c.topic)
	listLeadersCmd.Flag("filter-min-partition-id", "Minimum partition ID to show.").Default("-1").Int32Var(&c.minPartitionID)
	listLeadersCmd.Flag("filter-max-partition-id", "Maximum partition ID to show.").Default("-1").Int32Var(&c.maxPartitionID)
}

func (c *BrokersCommand) listLeadersByPartition(_ *kingpin.ParseContext) error {
	client := c.getKafkaClient()
	adm := kadm.NewClient(client)

	// Get topic metadata.
	topics, err := adm.ListTopics(context.Background(), c.topic)
	if err != nil {
		return err
	}
	if topics.Error() != nil {
		return topics.Error()
	}

	// Find all brokers.
	brokers, err := adm.ListBrokers(context.Background())
	if err != nil {
		return err
	}

	// Map broker node IDs to broker info.
	brokersByNodeID := make(map[int32]kadm.BrokerDetail, len(brokers))
	for _, broker := range brokers {
		brokersByNodeID[broker.NodeID] = broker
	}

	// Find the leader broker for each partition.
	for _, topic := range topics {
		for _, partition := range topic.Partitions.Sorted() {
			// Filter by min/max partition ID.
			if c.minPartitionID >= 0 && partition.Partition < c.minPartitionID {
				continue
			}
			if c.maxPartitionID >= 0 && partition.Partition > c.maxPartitionID {
				continue
			}

			if leader, ok := brokersByNodeID[partition.Leader]; ok {
				c.printer.PrintLine(fmt.Sprintf("Partition: %d \tLeader: %s:%d (ID: %d)", partition.Partition, leader.Host, leader.Port, leader.NodeID))
			} else {
				c.printer.PrintLine(fmt.Sprintf("Partition: %d \tLeader: unknown (ID: %d)", partition.Partition, partition.Leader))
			}
		}
	}

	return nil
}
