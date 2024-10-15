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

func TestConsumerGroupCommand_commitOffset_copyOffset_listOffsets(t *testing.T) {
	const (
		numPartitions = 3
		topic         = "test"
	)

	_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topic)
	clientProvider, err := createKafkaClientProvider(t, clusterAddr, "")
	require.NoError(t, err)

	app := newTestApp()
	_, printer := newTestConsumerGroupCommand(app, clientProvider)

	// List offsets when there are no consumed offsets committed.
	_, err = app.Parse([]string{"consumer-group", "list-offsets", "--group", "consumer-group-1"})
	require.NoError(t, err)
	require.Len(t, printer.Lines, 0)

	// Commit some offsets.
	printer.Reset()
	_, err = app.Parse([]string{"consumer-group", "commit-offset", "--group", "consumer-group-1", "--topic", topic, "--partition", "1", "--offset", "100"})
	require.NoError(t, err)
	require.Len(t, printer.Lines, 1)
	assert.Contains(t, printer.Lines[0], "successfully committed offset 100")

	printer.Reset()
	_, err = app.Parse([]string{"consumer-group", "commit-offset", "--group", "consumer-group-1", "--topic", topic, "--partition", "2", "--offset", "200"})
	require.NoError(t, err)
	require.Len(t, printer.Lines, 1)
	assert.Contains(t, printer.Lines[0], "successfully committed offset 200")

	printer.Reset()
	_, err = app.Parse([]string{"consumer-group", "commit-offset", "--group", "consumer-group-2", "--topic", topic, "--partition", "1", "--offset", "300"})
	require.NoError(t, err)
	require.Len(t, printer.Lines, 1)
	assert.Contains(t, printer.Lines[0], "successfully committed offset 300")

	// List offsets.
	printer.Reset()
	_, err = app.Parse([]string{"consumer-group", "list-offsets", "--group", "consumer-group-1"})
	require.NoError(t, err)
	require.Len(t, printer.Lines, 2)
	assert.Regexp(t, expectedLineRegexpByComponents("Topic:", topic, "Partition:", "1", "Offset:", "100"), printer.Lines[0])
	assert.Regexp(t, expectedLineRegexpByComponents("Topic:", topic, "Partition:", "2", "Offset:", "200"), printer.Lines[1])

	printer.Reset()
	_, err = app.Parse([]string{"consumer-group", "list-offsets", "--group", "consumer-group-2"})
	require.NoError(t, err)
	require.Len(t, printer.Lines, 1)
	assert.Regexp(t, expectedLineRegexpByComponents("Topic:", topic, "Partition:", "1", "Offset:", "300"), printer.Lines[0])

	// Copy offset from a consumer group to another.
	printer.Reset()
	_, err = app.Parse([]string{"consumer-group", "copy-offset", "--from-group", "consumer-group-2", "--to-group", "consumer-group-1", "--topic", topic, "--partition", "1"})
	require.NoError(t, err)
	require.Len(t, printer.Lines, 1)
	assert.Contains(t, printer.Lines[0], "successfully committed offset 300")

	printer.Reset()
	_, err = app.Parse([]string{"consumer-group", "list-offsets", "--group", "consumer-group-1"})
	require.NoError(t, err)
	require.Len(t, printer.Lines, 2)
	assert.Regexp(t, expectedLineRegexpByComponents("Topic:", topic, "Partition:", "1", "Offset:", "300"), printer.Lines[0])
	assert.Regexp(t, expectedLineRegexpByComponents("Topic:", topic, "Partition:", "2", "Offset:", "200"), printer.Lines[1])
}

func newTestConsumerGroupCommand(app *kingpin.Application, getKafkaClient func() *kgo.Client) (*ConsumerGroupCommand, *BufferedPrinter) {
	printer := &BufferedPrinter{}

	cmd := &ConsumerGroupCommand{}
	cmd.Register(app, getKafkaClient, printer)

	return cmd, printer
}
