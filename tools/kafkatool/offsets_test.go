package main

import (
	"strconv"
	"testing"

	"github.com/alecthomas/kingpin/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestOffsets(t *testing.T) {
	const (
		partitions = 9
		topic      = "topic"
		numRecords = 100
	)

	// Create a Kafka cluster
	kafkaCluster, clusterAddr := testkafka.CreateCluster(t, partitions, topic)
	defer kafkaCluster.Close()

	// Create a Kafka client
	clientProvider, err := createKafkaClientProvider(t, clusterAddr, "")
	require.NoError(t, err)
	client := clientProvider()

	// Generate records and push them to the source topic. Partition p gets 10p records.
	for p := range partitions {
		generateAndPushRecords(t, client, topic, int32(p), p*10)
	}

	app := newTestApp()
	_, printer := newTestOffsetsCommand(app, clientProvider)

	// List offsets.
	printer.Reset()
	_, err = app.Parse([]string{"offsets", "list", "--topic", topic, "--type", "end"})
	require.NoError(t, err)
	require.Len(t, printer.Lines, partitions)

	for p := range partitions {
		assert.Regexp(t, expectedLineRegexpByComponents(
			"Topic:", topic, "Partition:", strconv.Itoa(p), "Offset:", strconv.Itoa(p*10)), printer.Lines[p])
	}
}

func newTestOffsetsCommand(app *kingpin.Application, getKafkaClient func() *kgo.Client) (*OffsetsCommand, *BufferedPrinter) {
	printer := &BufferedPrinter{}

	cmd := &OffsetsCommand{}
	cmd.Register(app, getKafkaClient, printer)

	return cmd, printer
}
