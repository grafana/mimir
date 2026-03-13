// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"testing"

	"github.com/alecthomas/kingpin/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestListTopicsCommand_listTopics(t *testing.T) {
	t.Run("should list a single topic", func(t *testing.T) {
		const (
			seedTopic     = "test-topic"
			numPartitions = 3
		)

		_, clusterAddr := testkafka.CreateCluster(t, numPartitions, seedTopic)
		clientProvider, err := createKafkaClientProvider(t, clusterAddr, "")
		require.NoError(t, err)

		app := newTestApp()
		_, printer := newTestListTopicsCommand(app, clientProvider)

		_, err = app.Parse([]string{"list-topics"})
		require.NoError(t, err)
		require.Len(t, printer.Lines, 1)
		assert.Contains(t, printer.Lines[0], "Topic: "+seedTopic)
		assert.Contains(t, printer.Lines[0], "Partitions: 3")
	})

	t.Run("should list multiple topics", func(t *testing.T) {
		const (
			seedTopic         = "topic-a"
			seedNumPartitions = 2
			newTopic          = "topic-b"
			newNumPartitions  = 5
		)

		_, clusterAddr := testkafka.CreateCluster(t, seedNumPartitions, seedTopic)
		clientProvider, err := createKafkaClientProvider(t, clusterAddr, "")
		require.NoError(t, err)

		// Create a second topic using the create-topic command.
		app := newTestApp()
		newTestCreateTopicCommand(app, clientProvider)
		_, err = app.Parse([]string{"create-topic", "--topic", newTopic, "--num-partitions", "5"})
		require.NoError(t, err)

		// Now list topics.
		app = newTestApp()
		_, printer := newTestListTopicsCommand(app, clientProvider)
		_, err = app.Parse([]string{"list-topics"})
		require.NoError(t, err)

		require.Len(t, printer.Lines, 2)
		// Topics should be sorted by name.
		assert.Contains(t, printer.Lines[0], "Topic: "+seedTopic)
		assert.Contains(t, printer.Lines[0], "Partitions: 2")
		assert.Contains(t, printer.Lines[1], "Topic: "+newTopic)
		assert.Contains(t, printer.Lines[1], "Partitions: 5")
	})
}

func newTestListTopicsCommand(app *kingpin.Application, getKafkaClient func() *kgo.Client) (*ListTopicsCommand, *BufferedPrinter) {
	printer := &BufferedPrinter{}

	cmd := &ListTopicsCommand{}
	cmd.Register(app, getKafkaClient, printer)

	return cmd, printer
}
