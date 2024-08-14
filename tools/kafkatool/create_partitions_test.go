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

func TestCreatePartitionsCommand_createPartitions(t *testing.T) {
	const (
		numInitialPartitions    = 3
		numAdditionalPartitions = 2
		topic                   = "test"
	)

	_, clusterAddr := testkafka.CreateCluster(t, numInitialPartitions, topic)
	clientProvider, err := createKafkaClientProvider(t, clusterAddr, "")
	require.NoError(t, err)

	app := newTestApp()
	_, printer := newTestCreatePartitionsCommand(app, clientProvider)

	// Create more partitions.
	_, err = app.Parse([]string{"create-partitions", "--topic", topic, "--num-partitions", strconv.Itoa(numAdditionalPartitions)})
	require.NoError(t, err)
	require.Len(t, printer.Lines, 3)
	assert.Equal(t, "Number of partitions before creating additional partitions: "+strconv.Itoa(numInitialPartitions), printer.Lines[0])
	assert.Equal(t, "Created "+strconv.Itoa(numAdditionalPartitions)+" additional partitions to topic "+topic, printer.Lines[1])
	assert.Equal(t, "Number of partitions after creating additional partitions: "+strconv.Itoa(numInitialPartitions+numAdditionalPartitions), printer.Lines[2])

	details, err := getTopicDetails(kadm.NewClient(clientProvider()), topic)
	require.NoError(t, err)
	assert.Len(t, details.Partitions, numInitialPartitions+numAdditionalPartitions)
}

func newTestCreatePartitionsCommand(app *kingpin.Application, getKafkaClient func() *kgo.Client) (*CreatePartitionsCommand, *BufferedPrinter) {
	printer := &BufferedPrinter{}

	cmd := &CreatePartitionsCommand{}
	cmd.Register(app, getKafkaClient, printer)

	return cmd, printer
}
