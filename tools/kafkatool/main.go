// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/alecthomas/kingpin/v2"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

func main() {
	var (
		kafkaAddress      string
		kafkaClientID     string
		kafkaClient       *kgo.Client
		kafkaSASLUsername string
		kafkaSASLPassword string
	)

	app := kingpin.New("kafkatool", "A command-line tool to manage Kafka.")
	app.Flag("kafka-address", "Kafka broker address. Required by commands that contact Kafka; not needed for 'dump print', 'dump analyse', and 'dump find-duplicates' (which only read a local dump file).").StringVar(&kafkaAddress)
	app.Flag("kafka-client-id", "Kafka client ID.").StringVar(&kafkaClientID)
	app.Flag("kafka-sasl-username", "SASL username. SASL plain authentication is enabled when both username and password are set.").StringVar(&kafkaSASLUsername)
	app.Flag("kafka-sasl-password", "SASL password. SASL plain authentication is enabled when both username and password are set.").StringVar(&kafkaSASLPassword)

	// Create the Kafka client before any command is executed, but only when an
	// address is provided. Commands that operate on a local dump file only
	// (dump print/analyse/find-duplicates) never contact Kafka and should not
	// be forced to pass --kafka-address.
	app.Action(func(_ *kingpin.ParseContext) error {
		if kafkaAddress == "" {
			// No Kafka address: leave the client nil. Commands that need a
			// client will fail with a clear error when they call
			// getKafkaClient(); file-only commands proceed normally.
			return nil
		}

		var err error
		var auth sasl.Mechanism

		if kafkaSASLUsername != "" && kafkaSASLPassword != "" {
			auth = plain.Plain(func(_ context.Context) (plain.Auth, error) {
				return plain.Auth{
					User: kafkaSASLUsername,
					Pass: kafkaSASLPassword,
				}, nil
			})
		}

		kafkaClient, err = CreateKafkaClient(kafkaAddress, kafkaClientID, auth)
		return err
	})

	// Ensure to close the Kafka client before exiting.
	defer func() {
		if kafkaClient != nil {
			kafkaClient.Close()
		}
	}()

	printer := &StdoutPrinter{}

	// Function passed to commands to get Kafka client.
	getKafkaClient := func() *kgo.Client {
		if kafkaClient == nil {
			fmt.Fprintln(os.Stderr, "error: --kafka-address is required for this command (it was not provided)")
			os.Exit(1)
		}
		return kafkaClient
	}

	createPartitionsCommand := &CreatePartitionsCommand{}
	createPartitionsCommand.Register(app, getKafkaClient, printer)

	createTopicCommand := &CreateTopicCommand{}
	createTopicCommand.Register(app, getKafkaClient, printer)

	listTopicsCommand := &ListTopicsCommand{}
	listTopicsCommand.Register(app, getKafkaClient, printer)

	consumerGroupCommand := &ConsumerGroupCommand{}
	consumerGroupCommand.Register(app, getKafkaClient, printer)

	clusterMetadataCommand := &BrokersCommand{}
	clusterMetadataCommand.Register(app, getKafkaClient, printer)

	dumpCommand := &DumpCommand{}
	dumpCommand.Register(app, getKafkaClient, printer)

	offsetsCommand := &OffsetsCommand{}
	offsetsCommand.Register(app, getKafkaClient, printer)

	kingpin.MustParse(app.Parse(os.Args[1:]))
}
