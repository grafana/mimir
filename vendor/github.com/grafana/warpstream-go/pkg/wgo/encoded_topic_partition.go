package wgo

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// encodedTopicPartitionRecords is the serialised RecordBatch to produce for one
// Kafka partition: the encoded bytes plus that batch's producer-state counts.
type encodedTopicPartitionRecords struct {
	topic        string
	partition    int32
	encoded      []byte
	encodedStats produceRequestStats
}

// newEncodedTopicPartitionRecords encodes records into a single RecordBatch for
// (topic, partition), capturing that batch's producer-state counts. records must
// be non-empty.
func newEncodedTopicPartitionRecords(topic string, partition int32, records []*kgo.Record) encodedTopicPartitionRecords {
	batch, uncompressed, compressed := encodeBatch(records)
	return encodedTopicPartitionRecords{
		topic:     topic,
		partition: partition,
		encoded:   batch,
		encodedStats: produceRequestStats{
			records:           int64(len(records)),
			batches:           1,
			uncompressedBytes: int64(uncompressed),
			compressedBytes:   int64(compressed),
		},
	}
}

// recordCount returns the number of records in the encoded batch.
func (p encodedTopicPartitionRecords) recordCount() int {
	return int(p.encodedStats.records)
}

// payloadBytes is unsupported and returns 0.
func (p encodedTopicPartitionRecords) payloadBytes() int64 {
	return 0
}

// routedEncodedTopicPartitionRecords is an encodedTopicPartitionRecords plus the
// routing decision: the destination nodeID and that agent's state at routing time.
type routedEncodedTopicPartitionRecords struct {
	encodedTopicPartitionRecords

	nodeID int32

	// nodeState is the State of the Agent nodeID was picked from, captured at
	// routing time (e.g. AgentStateDemoted when nodeID is a demoted agent
	// being probed).
	nodeState AgentState
}

func (p routedEncodedTopicPartitionRecords) getTopicPartition() topicPartition {
	return topicPartition{topic: p.topic, partition: p.partition}
}

func (p routedEncodedTopicPartitionRecords) getNodeID() int32 { return p.nodeID }

// uncompressedWireBytes returns the RecordBatch's wire size with the records
// payload uncompressed (its size before Snappy compression).
func (p routedEncodedTopicPartitionRecords) uncompressedWireBytes() int64 {
	if p.encodedStats.records == 0 {
		return 0
	}
	return recordBatchHeaderBytes + p.encodedStats.uncompressedBytes
}

// splitByMaxBytes returns the item unchanged: an encoded batch is already sized
// to fit batchMaxBytes.
func (p routedEncodedTopicPartitionRecords) splitByMaxBytes(int32) []routedEncodedTopicPartitionRecords {
	return []routedEncodedTopicPartitionRecords{p}
}

// mergeWith combines p and others into a single RecordBatch for the partition by
// decoding every batch, concatenating the records in arrival order, and
// re-encoding once.
//
// Returns p unchanged when others is empty, so non-merged partitions never decode.
// Decoding operates on bytes this client encoded, never the caller's records, so it
// stays race-free.
func (p routedEncodedTopicPartitionRecords) mergeWith(others []routedEncodedTopicPartitionRecords) routedEncodedTopicPartitionRecords {
	if len(others) == 0 {
		return p
	}

	for _, o := range others {
		// Merging items for a different topic, partition, or nodeID is a bug, not
		// a runtime case: fail loud rather than silently combine unrelated batches.
		if p.topic != o.topic || p.partition != o.partition || p.nodeID != o.nodeID {
			panic(fmt.Sprintf("wgo: mergeWith mismatched routing: %s/%d nodeID=%d vs %s/%d nodeID=%d",
				p.topic, p.partition, p.nodeID, o.topic, o.partition, o.nodeID))
		}
	}

	records := decodeBatch(p.encoded)
	for _, o := range others {
		records = append(records, decodeBatch(o.encoded)...)
	}

	return routedEncodedTopicPartitionRecords{
		encodedTopicPartitionRecords: newEncodedTopicPartitionRecords(p.topic, p.partition, records),
		nodeID:                       p.nodeID,
		// Take the latest Add's nodeState: it is the freshest routing-time view of
		// the agent, which is what the hedger trusts to decide probe (zero-delay)
		// hedging.
		nodeState: others[len(others)-1].nodeState,
	}
}

// unrouteEncodedTopicPartitionRecords returns the routing-less encodedTopicPartitionRecords view for
// the DirectProducer chain, which takes the nodeID separately.
func unrouteEncodedTopicPartitionRecords(parts []routedEncodedTopicPartitionRecords) []encodedTopicPartitionRecords {
	out := make([]encodedTopicPartitionRecords, len(parts))
	for i, p := range parts {
		out[i] = p.encodedTopicPartitionRecords
	}
	return out
}

// newMultiRoutedEncodedTopicPartitionRecords stamps each encoded partition with
// the same destination nodeID and shared done callback.
func newMultiRoutedEncodedTopicPartitionRecords(parts []encodedTopicPartitionRecords, nodeID int32, done func(ProduceResult)) []promised[routedEncodedTopicPartitionRecords] {
	out := make([]promised[routedEncodedTopicPartitionRecords], len(parts))
	for i, p := range parts {
		out[i] = promised[routedEncodedTopicPartitionRecords]{
			item: routedEncodedTopicPartitionRecords{encodedTopicPartitionRecords: p, nodeID: nodeID},
			done: done,
		}
	}
	return out
}
