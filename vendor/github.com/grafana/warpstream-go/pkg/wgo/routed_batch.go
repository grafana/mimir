package wgo

import "sync"

// routedBatch is the per-item behavior the generic buffer delegates to: the
// routing key and decision, the accounting sizes, and the split/merge
// operations that differ between unencoded records and pre-encoded batches.
type routedBatch[W any] interface {
	getTopicPartition() topicPartition
	getNodeID() int32
	recordCount() int
	payloadBytes() int64
	wireBytes() int64
	splitByMaxBytes(max int32) []W
	mergeWith(other W) W
}

// Compile-time assertion that both item types satisfy routedBatch. It also marks
// methods reached only through the type parameter as used, which the unused
// linter does not track across generic instantiation.
var (
	_ routedBatch[routedTopicPartitionRecords]        = routedTopicPartitionRecords{}
	_ routedBatch[routedEncodedTopicPartitionRecords] = routedEncodedTopicPartitionRecords{}
)

// promised pairs a routedBatch item with a done callback that fires exactly once
// with the item's terminal ProduceResult: a full ProduceResponse on success, or
// an error result on failure or ctx-cancel (which may still carry a partial
// response). All records in the item share that outcome.
type promised[W routedBatch[W]] struct {
	item W
	done func(ProduceResult)
}

// unpromise returns the done-less item view of each promised entry.
func unpromise[W routedBatch[W]](parts []promised[W]) []W {
	out := make([]W, len(parts))
	for i, p := range parts {
		out[i] = p.item
	}
	return out
}

// splitPromisedRoutedBatchByBatchMaxBytes splits in's item so each returned chunk's
// standalone wire batch fits within batchMaxBytes, returning in unchanged when
// it already fits. When split, the chunks share a fan-in done that fires in's
// done once, after all chunks have resolved.
func splitPromisedRoutedBatchByBatchMaxBytes[W routedBatch[W]](in promised[W], batchMaxBytes int32) []promised[W] {
	chunks := in.item.splitByMaxBytes(batchMaxBytes)
	if len(chunks) <= 1 {
		return []promised[W]{in}
	}

	// Capture the done func, not in itself: referencing in.done directly would
	// make the closure retain all of in until the last chunk completes.
	origDone := in.done
	var (
		mu         sync.Mutex
		remaining  = len(chunks)
		chosen     ProduceResult
		haveChosen bool
		chosenOK   bool
	)
	// The chunks all share in's single (topic, partition), so origDone fires once
	// after every chunk resolves, choosing a failing result over a success.
	// Per-chunk responses need no merging: reporting any failure triggers a safe
	// whole-partition retry under at-least-once.
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

	out := make([]promised[W], len(chunks))
	for i, c := range chunks {
		out[i] = promised[W]{item: c, done: done}
	}
	return out
}

// mergePromisedRoutedBatchByTopicPartition returns one done-less wire item per
// (topic, partition), merging items that share a partition in arrival order via
// W.mergeWith. Backing arrays of the inputs are never mutated.
func mergePromisedRoutedBatchByTopicPartition[W routedBatch[W]](entries []promised[W]) []W {
	out := make([]W, 0, len(entries))
	index := make(map[topicPartition]int, len(entries))
	for _, e := range entries {
		tp := e.item.getTopicPartition()
		if i, ok := index[tp]; ok {
			out[i] = out[i].mergeWith(e.item)
			continue
		}
		index[tp] = len(out)
		out = append(out, e.item)
	}
	return out
}
