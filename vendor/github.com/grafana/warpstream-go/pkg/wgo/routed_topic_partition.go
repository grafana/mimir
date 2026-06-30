package wgo

import (
	"sync"

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

// recordPayloadBytes returns the producer-side accounting size of the group,
// mirroring franz-go's BufferedProduceBytes.
//
// It intentionally omits Kafka wire overhead (record batch header, varints,
// etc.) so the counter matches what franz-go reports for the same records.
func (p *topicPartitionRecords) recordPayloadBytes() int64 {
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

// unrouteTopicPartitionRecords returns the bare topicPartitionRecords
// slice for callers that don't care about routing or done state.
func unrouteTopicPartitionRecords(parts []routedTopicPartitionRecords) []topicPartitionRecords {
	out := make([]topicPartitionRecords, len(parts))
	for i, p := range parts {
		out[i] = p.topicPartitionRecords
	}
	return out
}

// promisedRoutedTopicPartitionRecords is a routedTopicPartitionRecords paired
// with a done callback that carries the group's terminal ProduceResult.
type promisedRoutedTopicPartitionRecords struct {
	routedTopicPartitionRecords

	// done fires exactly once with the partition's terminal ProduceResult: a
	// full ProduceResponse on success, or an error result on failure or
	// ctx-cancel — which may still carry a partial response (e.g. the
	// retry-exhausted kgo.ErrRecordTimeout envelope with per-partition entries).
	// Callers that only care about the error split it out via
	// ProduceResult.error(). All records in the group share that outcome —
	// per-record fan-out is the concern of the caller that built the group
	// (e.g. WarpstreamClient.ProduceSync).
	done func(ProduceResult)
}

// newMultiRoutedTopicPartitionRecords stamps each input with the same
// routing decision (nodeID) and shared done callback. Used by callers
// that batch multiple partitions to the same agent in one wave.
func newMultiRoutedTopicPartitionRecords(parts []topicPartitionRecords, nodeID int32, done func(ProduceResult)) []promisedRoutedTopicPartitionRecords {
	out := make([]promisedRoutedTopicPartitionRecords, len(parts))
	for i, p := range parts {
		out[i] = promisedRoutedTopicPartitionRecords{
			routedTopicPartitionRecords: routedTopicPartitionRecords{
				topicPartitionRecords: p,
				nodeID:                nodeID,
			},
			done: done,
		}
	}
	return out
}

// unpromiseRoutedTopicPartitionRecords returns the done-less
// routedTopicPartitionRecords view of each entry, for callers that need the
// routing decision and records but not the completion callback.
func unpromiseRoutedTopicPartitionRecords(parts []promisedRoutedTopicPartitionRecords) []routedTopicPartitionRecords {
	out := make([]routedTopicPartitionRecords, len(parts))
	for i, p := range parts {
		out[i] = p.routedTopicPartitionRecords
	}
	return out
}

// mergePromisedRoutedTopicPartitionRecordsByTopicPartition returns one
// routedTopicPartitionRecords per (topic, partition), concatenating the records
// of entries that share a partition (in arrival order). The merged entry takes
// the last contributor's nodeState — the freshest routing-time classification
// of the agent. This is the done-less wire view handed to a flush; completion is fired
// separately on the original promised entries, so this view drops done. Backing
// arrays of the inputs are never mutated.
func mergePromisedRoutedTopicPartitionRecordsByTopicPartition(entries []promisedRoutedTopicPartitionRecords) []routedTopicPartitionRecords {
	out := make([]routedTopicPartitionRecords, 0, len(entries))
	for _, e := range entries {
		merged := false
		for i := range out {
			if out[i].topic == e.topic && out[i].partition == e.partition {
				combined := make([]*kgo.Record, 0, len(out[i].records)+len(e.records))
				combined = append(combined, out[i].records...)
				combined = append(combined, e.records...)
				out[i].records = combined
				// Take the latest Add's nodeState: it is the freshest
				// routing-time view of the agent, which is what the hedger
				// trusts to decide probe (zero-delay) hedging.
				out[i].nodeState = e.nodeState
				merged = true
				break
			}
		}
		if !merged {
			out = append(out, e.routedTopicPartitionRecords)
		}
	}
	return out
}

// splitPromisedRoutedTopicPartitionRecordsByBatchMaxBytes splits in's records
// so each returned group's standalone RecordBatch fits within batchMaxBytes.
//
// The whole group is returned unchanged when it already fits (the common case).
// When it must be split, the chunks share a fan-in done: in.done fires exactly once,
// after every chunk has resolved, with a failing result if any chunk failed (else a
// success).
//
// Each record's own batch is <= batchMaxBytes (enforced by the per-record gate), so
// every chunk carries at least one record and the split always terminates.
func splitPromisedRoutedTopicPartitionRecordsByBatchMaxBytes(in promisedRoutedTopicPartitionRecords, batchMaxBytes int32) []promisedRoutedTopicPartitionRecords {
	if multiRecordBatchEstimateBytes(in.records) <= int64(batchMaxBytes) {
		return []promisedRoutedTopicPartitionRecords{in}
	}

	var (
		chunks      [][]*kgo.Record
		currRecords []*kgo.Record
		currBytes   int64
		firstTS     int64
	)
	for _, r := range in.records {
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

	// Fan-in done: every chunk shares in's single (topic, partition), so
	// in.done just needs firing once, after all chunks resolve, with the first
	// failing result if any chunk failed (else a success). No merging of the
	// per-chunk responses is needed — in.done resolves only that one partition's
	// outcome, and reporting any chunk's failure triggers a safe whole-partition
	// retry under at-least-once.
	// Capture the done func, not in itself: referencing in.done directly would
	// make the closure retain all of in (notably in.records) until the last
	// chunk completes.
	origDone := in.done
	var (
		mu         sync.Mutex
		remaining  = len(chunks)
		chosen     ProduceResult
		haveChosen bool
		chosenOK   bool
	)
	done := func(res ProduceResult) {
		mu.Lock()
		remaining--
		if !haveChosen {
			chosen = res
			haveChosen = true
			chosenOK = res.error() == nil
		} else if chosenOK && res.error() != nil {
			chosen = res
			chosenOK = false
		}
		last := remaining == 0
		result := chosen
		mu.Unlock()
		if last {
			origDone(result)
		}
	}

	out := make([]promisedRoutedTopicPartitionRecords, len(chunks))
	for i, c := range chunks {
		out[i] = in
		out[i].records = c
		out[i].done = done
	}
	return out
}
