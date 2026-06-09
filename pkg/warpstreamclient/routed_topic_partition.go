// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

// topicPartitionRecords is the bare "what to produce" data for one Kafka
// partition: a topic, a partition index, and the records to write. It
// carries no routing or completion-handling state; the layers below the
// buffer (the DirectProducer chain) take []topicPartitionRecords as
// input because all they need to do is encode and ship a wire request.
type topicPartitionRecords struct {
	topic     string
	partition int32
	records   []*kgo.Record
}

// recordValueBytes returns the total bytes of every record's Value in the
// group. This is the producer-side accounting unit used for
// BufferedProduceBytes — it intentionally omits Kafka wire overhead (record
// batch header, varints, etc.) so the counter matches what franz-go reports
// for the same records.
func (p *topicPartitionRecords) recordValueBytes() int64 {
	var n int64
	for _, r := range p.records {
		n += int64(len(r.Value))
	}
	return n
}

// routedTopicPartitionRecords is the partition-grouped unit of work flowing
// through the buffer. It is the bare topicPartitionRecords plus the routing
// decision (nodeID + nodeState) and the per-partition completion callback.
//
// done fires exactly once with the partition's terminal ProduceResult: the
// produce attempt's full ProduceResponse, or an error-only result on
// transport-level failure or ctx-cancel. Callers that only care about the
// error split it out via ProduceResult.error(). All records in the group
// share that outcome — per-record fan-out is the concern of the caller that
// built the group (e.g. WarpstreamClient.ProduceSync).
//
// Cross-agent retry is NOT a concern of this type: the Hedger owns the
// "try the next agent" loop internally per call. A routed entry entering
// the buffer always represents one decisive routing decision; if the
// Hedger needs to retry on a different agent, it builds new internal
// state without re-entering the buffer.
//
// Two routedTopicPartitionRecords values are mergeable iff they share
// (topic, partition, nodeID). When the AgentRecordBuffer merges fresh
// Produce calls for the same partition into one wire batch, it appends
// records and chains the two dones into one — preserving every caller's
// completion notification without inflating the wire batch count.
type routedTopicPartitionRecords struct {
	topicPartitionRecords

	done   func(ProduceResult)
	nodeID int32

	// nodeState mirrors the State of the Agent that nodeID was picked
	// from. AgentStateDemoted in particular tells downstream layers
	// (specifically the Hedger) that nodeID is a demoted agent being
	// probed — fire the fallback immediately rather than paying the
	// hedge delay, because a probe is expected to fail.
	nodeState AgentState
}

// newMultiRoutedTopicPartitionRecords stamps each input with the same
// routing decision (nodeID) and shared done callback. Used by callers
// that batch multiple partitions to the same agent in one wave.
func newMultiRoutedTopicPartitionRecords(parts []topicPartitionRecords, nodeID int32, done func(ProduceResult)) []routedTopicPartitionRecords {
	out := make([]routedTopicPartitionRecords, len(parts))
	for i, p := range parts {
		out[i] = routedTopicPartitionRecords{
			topicPartitionRecords: p,
			nodeID:                nodeID,
			done:                  done,
		}
	}
	return out
}

// mergeableWith reports whether other can be merged into p — true iff both
// describe the same (topic, partition) routed to the same nodeID. The
// caller is free to apply additional constraints (e.g. byte caps).
func (p *routedTopicPartitionRecords) mergeableWith(other *routedTopicPartitionRecords) bool {
	return p.topic == other.topic &&
		p.partition == other.partition &&
		p.nodeID == other.nodeID
}

// unrouteTopicPartitionRecords returns the bare topicPartitionRecords
// slice for callers that don't care about routing or done state.
func unrouteTopicPartitionRecords(parts []routedTopicPartitionRecords) []topicPartitionRecords {
	out := make([]topicPartitionRecords, len(parts))
	for i, p := range parts {
		out[i] = p.topicPartitionRecords
	}
	return out
}

// chainDones returns a done that invokes both a and b. Used by the buffer's
// merge path so a merged routed entry carries every contributing caller's
// completion. Either may be nil (no-op).
func chainDones(a, b func(ProduceResult)) func(ProduceResult) {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	default:
		return func(res ProduceResult) {
			a(res)
			b(res)
		}
	}
}
