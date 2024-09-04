// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"os"

	"github.com/alecthomas/kingpin/v2"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	var (
		kafkaAddress  string
		kafkaClientID string
		saslPlainUser string
		saslPlainPass string
		kafkaClient   *kgo.Client
	)

	app := kingpin.New("kafkatool", "A command-line tool to manage Kafka.")
	app.Flag("kafka-address", "Kafka broker address.").Required().StringVar(&kafkaAddress)
	app.Flag("kafka-client-id", "Kafka client ID.").StringVar(&kafkaClientID)
	app.Flag("kafka-sasl-plain-user", "Username to use when authenticating with Kafka using SASL/PLAIN.").StringVar(&saslPlainUser)
	app.Flag("kafka-sasl-plain-pass", "Password to use when authenticating with Kafka using SASL/PLAIN.").StringVar(&saslPlainPass)

	// Create the Kafka client before any command is executed.
	app.Action(func(_ *kingpin.ParseContext) error {
		var err error
		kafkaClient, err = CreateKafkaClient(kafkaAddress, kafkaClientID, SASLPlainOpts(saslPlainUser, saslPlainPass))
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
		return kafkaClient
	}

	createPartitionsCommand := &CreatePartitionsCommand{}
	createPartitionsCommand.Register(app, getKafkaClient, printer)

	consumerGroupCommand := &ConsumerGroupCommand{}
	consumerGroupCommand.Register(app, getKafkaClient, printer)

	clusterMetadataCommand := &BrokersCommand{}
	clusterMetadataCommand.Register(app, getKafkaClient, printer)

	kingpin.MustParse(app.Parse(os.Args[1:]))
}
