// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"strconv"
	"testing"

	"github.com/alecthomas/kingpin/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestCreateTopicCommand_createTopic(t *testing.T) {
	t.Run("should create a new topic", func(t *testing.T) {
		const (
			seedTopic     = "seed"
			newTopic      = "new-topic"
			numPartitions = 5
		)

		_, clusterAddr := testkafka.CreateCluster(t, 1, seedTopic)
		clientProvider, err := createKafkaClientProvider(t, clusterAddr, "")
		require.NoError(t, err)

		app := newTestApp()
		_, printer := newTestCreateTopicCommand(app, clientProvider)

		_, err = app.Parse([]string{"create-topic", "--topic", newTopic, "--num-partitions", strconv.Itoa(numPartitions)})
		require.NoError(t, err)
		require.Len(t, printer.Lines, 1)
		assert.Equal(t, "Created topic "+newTopic+" with "+strconv.Itoa(numPartitions)+" partitions", printer.Lines[0])

		details, err := getTopicDetails(kadm.NewClient(clientProvider()), newTopic)
		require.NoError(t, err)
		assert.Len(t, details.Partitions, numPartitions)
	})

	t.Run("should fail if topic already exists", func(t *testing.T) {
		const (
			existingTopic = "existing-topic"
			numPartitions = 3
		)

		_, clusterAddr := testkafka.CreateCluster(t, numPartitions, existingTopic)
		clientProvider, err := createKafkaClientProvider(t, clusterAddr, "")
		require.NoError(t, err)

		app := newTestApp()
		newTestCreateTopicCommand(app, clientProvider)

		_, err = app.Parse([]string{"create-topic", "--topic", existingTopic, "--num-partitions", strconv.Itoa(numPartitions)})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "topic "+existingTopic+" already exists")
	})
}

func newTestCreateTopicCommand(app *kingpin.Application, getKafkaClient func() *kgo.Client) (*CreateTopicCommand, *BufferedPrinter) {
	printer := &BufferedPrinter{}

	cmd := &CreateTopicCommand{}
	cmd.Register(app, getKafkaClient, printer)

	return cmd, printer
}
