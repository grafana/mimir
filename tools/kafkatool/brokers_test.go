// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"net"
	"testing"

	"github.com/alecthomas/kingpin/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestBrokersCommand_listLeadersByPartition(t *testing.T) {
	const (
		numPartitions = 3
		topic         = "test"
	)

	_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topic)
	clientProvider, err := createKafkaClientProvider(t, clusterAddr, "")
	require.NoError(t, err)

	app := newTestApp()
	_, printer := newTestBrokersCommand(app, clientProvider)

	// Get the cluster host.
	clusterHost, _, err := net.SplitHostPort(clusterAddr)
	require.NoError(t, err)

	// List leaders for all partitions.
	_, err = app.Parse([]string{"brokers", "list-leaders-by-partition", "--topic", topic})
	require.NoError(t, err)
	require.Len(t, printer.Lines, 3)
	assert.Regexp(t, expectedLineRegexpByComponents("Partition:", "0", "Leader:", clusterHost), printer.Lines[0])
	assert.Regexp(t, expectedLineRegexpByComponents("Partition:", "1", "Leader:", clusterHost), printer.Lines[1])
	assert.Regexp(t, expectedLineRegexpByComponents("Partition:", "2", "Leader:", clusterHost), printer.Lines[2])

	// List leaders for partitions, filtering by min/max partition ID.
	printer.Reset()
	_, err = app.Parse([]string{"brokers", "list-leaders-by-partition", "--topic", topic, "--filter-min-partition-id", "1", "--filter-max-partition-id", "1"})
	require.NoError(t, err)
	require.Len(t, printer.Lines, 1)
	assert.Regexp(t, expectedLineRegexpByComponents("Partition:", "1", "Leader:", clusterHost), printer.Lines[0])
}

func newTestBrokersCommand(app *kingpin.Application, getKafkaClient func() *kgo.Client) (*BrokersCommand, *BufferedPrinter) {
	printer := &BufferedPrinter{}

	cmd := &BrokersCommand{}
	cmd.Register(app, getKafkaClient, printer)

	return cmd, printer
}
