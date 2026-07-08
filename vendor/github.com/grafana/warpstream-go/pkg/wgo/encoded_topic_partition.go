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

// payloadBytes returns 0: an encoded batch retains no caller-owned record
// payload to account for, and the buffered-bytes counter it feeds (on the hedge
// buffer, which exposes no gauge) is never read anyway.
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

// wireBytes returns the encoded batch length; the batch header is already
// included in the bytes.
func (p routedEncodedTopicPartitionRecords) wireBytes() int64 {
	return int64(len(p.encoded))
}

// splitByMaxBytes returns the item unchanged: an encoded batch is already sized
// to fit batchMaxBytes.
func (p routedEncodedTopicPartitionRecords) splitByMaxBytes(int32) []routedEncodedTopicPartitionRecords {
	return []routedEncodedTopicPartitionRecords{p}
}

// mergeWith concatenates other's batch bytes after p's and sums the stats — a
// partition's wire payload may hold several batches back to back — and takes
// other's nodeState. A fresh backing slice is allocated so neither input is mutated.
func (p routedEncodedTopicPartitionRecords) mergeWith(other routedEncodedTopicPartitionRecords) routedEncodedTopicPartitionRecords {
	// Merging items for a different topic, partition, or nodeID is a bug, not a
	// runtime case: fail loud rather than silently combine unrelated batches.
	if p.topic != other.topic || p.partition != other.partition || p.nodeID != other.nodeID {
		panic(fmt.Sprintf("wgo: mergeWith mismatched routing: %s/%d nodeID=%d vs %s/%d nodeID=%d",
			p.topic, p.partition, p.nodeID, other.topic, other.partition, other.nodeID))
	}

	combined := make([]byte, 0, len(p.encoded)+len(other.encoded))
	combined = append(combined, p.encoded...)
	combined = append(combined, other.encoded...)
	p.encoded = combined
	p.encodedStats = p.encodedStats.add(other.encodedStats)
	// Take the latest Add's nodeState: it is the freshest
	// routing-time view of the agent, which is what the hedger
	// trusts to decide probe (zero-delay) hedging.
	p.nodeState = other.nodeState
	return p
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
