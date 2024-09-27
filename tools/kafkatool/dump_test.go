// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestDumpCommand_ExportImport(t *testing.T) {
	const (
		partitionID = 1
		sourceTopic = "source-topic"
		destTopic   = "dest-topic"
		numRecords  = 100
	)

	// Create a Kafka cluster
	kafkaCluster, clusterAddr := testkafka.CreateCluster(t, partitionID+1, sourceTopic)
	defer kafkaCluster.Close()

	// Create a Kafka client
	clientProvider, err := createKafkaClientProvider(t, clusterAddr, "")
	require.NoError(t, err)
	client := clientProvider()

	// Generate records and push them to the source topic
	generatedRecords := generateAndPushRecords(t, client, sourceTopic, partitionID, numRecords)

	// Create temporary files for export and import
	exportFile, err := os.CreateTemp(t.TempDir(), "export_test")
	require.NoError(t, err)
	defer exportFile.Close()

	// Create the DumpCommand
	app := kingpin.New("test", "Test application")
	cmd := &DumpCommand{}
	printer := &BufferedPrinter{}
	t.Cleanup(func() {
		t.Log(strings.Join(printer.Lines, "\n"))
	})
	cmd.Register(app, clientProvider, printer)

	// Create the destination topic
	_, err = kadm.NewClient(clientProvider()).CreateTopic(context.Background(), partitionID+1, 1, nil, destTopic)
	require.NoError(t, err)

	// Export from the source topic
	_, err = app.Parse([]string{
		"dump", "export",
		"--topic", sourceTopic,
		"--partition", fmt.Sprintf("%d", partitionID),
		"--file", exportFile.Name(),
	})
	require.NoError(t, err)

	// Import into the destination topic
	_, err = app.Parse([]string{
		"dump", "import",
		"--topic", destTopic,
		"--partition", fmt.Sprintf("%d", partitionID),
		"--file", exportFile.Name(),
	})
	require.NoError(t, err)

	// Read from Kafka from the destination topic
	importedRecords := consumeRecordsFromKafka(t, client, destTopic, numRecords)

	// Assert that the records are the same ones that were generated in step 1
	assert.Equal(t, len(generatedRecords), len(importedRecords), "Number of imported records should match generated records")
	for i, genRecord := range generatedRecords {
		importedRecord := importedRecords[i]
		assert.Equal(t, genRecord.Value, importedRecord.Value, "Record values should match")
		assert.Equal(t, genRecord.Key, importedRecord.Key, "Record keys should match")
		// Note: We don't compare offsets as they might be different in the new topic
	}
}

func generateAndPushRecords(t *testing.T, client *kgo.Client, topic string, partition int32, numRecords int) []*kgo.Record {
	var generatedRecords []*kgo.Record
	for i := 0; i < numRecords; i++ {
		record := &kgo.Record{
			Topic:     topic,
			Partition: partition,
			Key:       []byte(fmt.Sprintf("key-%d", i)),
			Value:     []byte(fmt.Sprintf("test-value-%d", i)),
		}
		err := client.ProduceSync(context.Background(), record).FirstErr()
		require.NoError(t, err)
		generatedRecords = append(generatedRecords, record)
	}
	return generatedRecords
}

func consumeRecordsFromKafka(t *testing.T, client *kgo.Client, topic string, numRecords int) []*kgo.Record {
	var records []*kgo.Record
	client.AddConsumeTopics(topic)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for len(records) < numRecords {
		fetches := client.PollFetches(ctx)
		require.NoError(t, fetches.Err())
		fetches.EachRecord(func(record *kgo.Record) {
			records = append(records, record)
		})
	}

	return records
}
