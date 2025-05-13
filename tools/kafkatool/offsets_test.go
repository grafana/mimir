// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"strconv"
	"testing"
	"time"

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

	t.Run("start offsets", func(t *testing.T) {
		printer.Reset()
		_, err = app.Parse([]string{"offsets", "list", "--topic", topic, "--type", offsetTypeStart})
		require.NoError(t, err)
		require.Len(t, printer.Lines, partitions)

		for p := range partitions {
			assert.Regexp(t, expectedLineRegexpByComponents(
				"Topic:", topic, "Partition:", strconv.Itoa(p), "Offset:", "0"), printer.Lines[p])
		}
	})

	t.Run("end offsets", func(t *testing.T) {
		printer.Reset()
		_, err = app.Parse([]string{"offsets", "list", "--topic", topic, "--type", offsetTypeEnd})
		require.NoError(t, err)
		require.Len(t, printer.Lines, partitions)

		for p := range partitions {
			assert.Regexp(t, expectedLineRegexpByComponents(
				"Topic:", topic, "Partition:", strconv.Itoa(p), "Offset:", strconv.Itoa(p*10)), printer.Lines[p])
		}
	})

	t.Run("committed offsets", func(t *testing.T) {
		printer.Reset()
		_, err = app.Parse([]string{"offsets", "list", "--topic", topic, "--type", offsetTypeCommitted})
		require.NoError(t, err)
		require.Len(t, printer.Lines, partitions)

		for p := range partitions {
			assert.Regexp(t, expectedLineRegexpByComponents(
				"Topic:", topic, "Partition:", strconv.Itoa(p), "Offset:", strconv.Itoa(p*10)), printer.Lines[p])
		}
	})

	t.Run("after ms", func(t *testing.T) {
		printer.Reset()
		_, err = app.Parse([]string{"offsets", "list", "--topic", topic, "--type", offsetTypeAfterMs})
		require.Error(t, err)
		require.ErrorContains(t, err, "--millis is required when --type="+offsetTypeAfterMs)

		printer.Reset()
		_, err = app.Parse([]string{"offsets", "list", "--topic", topic, "--type", offsetTypeEnd, "--millis", "1234"})
		require.Error(t, err)
		require.ErrorContains(t, err, "--millis is only valid when --type="+offsetTypeAfterMs)

		printer.Reset()
		tomorrow := time.Now().Add(24 * time.Hour)
		_, err = app.Parse([]string{"offsets", "list", "--topic", topic, "--type", offsetTypeAfterMs, "--millis", fmt.Sprint(tomorrow.UnixMilli())})
		require.NoError(t, err)
		require.Len(t, printer.Lines, partitions)

		for p := range partitions {
			assert.Regexp(t,
				expectedLineRegexpByComponents("Topic:", topic, "Partition:", strconv.Itoa(p), "Offset:", strconv.Itoa(p*10)),
				printer.Lines[p],
				"future timestamp should return the partition's end offset",
			)
		}
	})
}

func newTestOffsetsCommand(app *kingpin.Application, getKafkaClient func() *kgo.Client) (*OffsetsCommand, *BufferedPrinter) {
	printer := &BufferedPrinter{}

	cmd := &OffsetsCommand{}
	cmd.Register(app, getKafkaClient, printer)

	return cmd, printer
}
