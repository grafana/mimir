package wgo

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// topicPartitionRecords is the records to produce for one Kafka partition,
// before wire encoding: a topic, a partition index, and the records to write.
type topicPartitionRecords struct {
	topic     string
	partition int32
	records   []*kgo.Record
}

// recordCount returns the number of records in the group.
func (p topicPartitionRecords) recordCount() int {
	return len(p.records)
}

// payloadBytes returns the producer-side accounting size of the group,
// mirroring franz-go's BufferedProduceBytes. It omits Kafka wire overhead
// (record batch header, varints) so the counter matches what franz-go reports
// for the same records.
func (p topicPartitionRecords) payloadBytes() int64 {
	var n int64
	for _, r := range p.records {
		n += int64(len(r.Key) + len(r.Value))
		for _, h := range r.Headers {
			n += int64(len(h.Key) + len(h.Value))
		}
	}
	return n
}

// routedTopicPartitionRecords is a topicPartitionRecords plus the routing
// decision for it: the destination nodeID and the state of that agent.
type routedTopicPartitionRecords struct {
	topicPartitionRecords

	nodeID int32

	// nodeState is the State of the Agent nodeID was picked from, captured at
	// routing time (e.g. AgentStateDemoted when nodeID is a demoted agent
	// being probed).
	nodeState AgentState
}

func (p routedTopicPartitionRecords) getTopicPartition() topicPartition {
	return topicPartition{topic: p.topic, partition: p.partition}
}

func (p routedTopicPartitionRecords) getNodeID() int32 { return p.nodeID }

// uncompressedWireBytes returns the uncompressed wire size of the group encoded
// as a single standalone RecordBatch.
func (p routedTopicPartitionRecords) uncompressedWireBytes() int64 {
	return multiRecordBatchEstimateBytes(p.records)
}

// splitByMaxBytes splits the records so each returned chunk's standalone
// RecordBatch fits within batchMaxBytes, returning the group unchanged when it
// already fits. Each record's own batch is <= batchMaxBytes (the per-record
// gate enforces this), so every chunk carries at least one record and the split
// terminates.
func (p routedTopicPartitionRecords) splitByMaxBytes(batchMaxBytes int32) []routedTopicPartitionRecords {
	if multiRecordBatchEstimateBytes(p.records) <= int64(batchMaxBytes) {
		return []routedTopicPartitionRecords{p}
	}

	var (
		chunks      [][]*kgo.Record
		currRecords []*kgo.Record
		currBytes   int64
		firstTS     int64
	)
	for _, r := range p.records {
		if len(currRecords) == 0 {
			firstTS = r.Timestamp.UnixMilli()
			currRecords = append(currRecords, r)
			currBytes = recordBatchHeaderBytes + recordEstimateBytes(r, 0, 0)
			continue
		}

		cost := recordEstimateBytes(r, int32(len(currRecords)), r.Timestamp.UnixMilli()-firstTS)
		if currBytes+cost > int64(batchMaxBytes) {
			chunks = append(chunks, currRecords)
			firstTS = r.Timestamp.UnixMilli()
			currRecords = []*kgo.Record{r}
			currBytes = recordBatchHeaderBytes + recordEstimateBytes(r, 0, 0)
			continue
		}

		currRecords = append(currRecords, r)
		currBytes += cost
	}
	chunks = append(chunks, currRecords)

	out := make([]routedTopicPartitionRecords, len(chunks))
	for i, c := range chunks {
		out[i] = p
		out[i].records = c
	}
	return out
}

// mergeWith concatenates others' records after p's (arrival order) and takes the
// last contributor's nodeState. A fresh backing slice is allocated so no input
// is mutated. Returns p unchanged when others is empty.
func (p routedTopicPartitionRecords) mergeWith(others []routedTopicPartitionRecords) routedTopicPartitionRecords {
	if len(others) == 0 {
		return p
	}

	total := len(p.records)
	for _, o := range others {
		// Merging items for a different topic, partition, or nodeID is a bug, not
		// a runtime case: fail loud rather than silently combine unrelated batches.
		if p.topic != o.topic || p.partition != o.partition || p.nodeID != o.nodeID {
			panic(fmt.Sprintf("wgo: mergeWith mismatched routing: %s/%d nodeID=%d vs %s/%d nodeID=%d",
				p.topic, p.partition, p.nodeID, o.topic, o.partition, o.nodeID))
		}
		total += len(o.records)
	}

	combined := make([]*kgo.Record, 0, total)
	combined = append(combined, p.records...)
	for _, o := range others {
		combined = append(combined, o.records...)
	}
	p.records = combined
	// Take the latest Add's nodeState: it is the freshest routing-time view of the
	// agent, which is what the hedger trusts to decide probe (zero-delay) hedging.
	p.nodeState = others[len(others)-1].nodeState
	return p
}
