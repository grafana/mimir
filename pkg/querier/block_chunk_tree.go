// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/dskit/blob/main/loser/loser.go
// Provenance-includes-license: Apache-2.0

package querier

import (
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func newSeriesChunksIteratorTree(iterators []iteratorWithMaxTime) *seriesChunksIteratorTree {
	nIterators := len(iterators)
	t := seriesChunksIteratorTree{
		nodes: make([]node, nIterators*2),
	}
	for i, it := range iterators {
		t.nodes[i+nIterators].it = it
		t.nodes[i+nIterators].value = it
		t.moveNext(i + nIterators) // Must call Next on each item so that At() has a value.
	}
	if nIterators > 0 {
		t.nodes[0].index = -1 // flag to be initialized on first call to Next().
	}
	return &t
}

// seriesChunksIteratorTree is a loser tree used to merge chunks from different store-gateways.
// This implementation is based on https://github.com/grafana/dskit/blob/main/loser/loser.go.
//
// A loser tree is a binary tree laid out such that nodes N and N+1 have parent N/2.
// We store M leaf nodes in positions M...2M-1, and M-1 internal nodes in positions 1..M-1.
// Node 0 is a special node, containing the winner of the contest.
type seriesChunksIteratorTree struct {
	nodes []node
	err   error
}

type node struct {
	index         int                 // This is the loser for all nodes except the 0th, where it is the winner.
	value         chunkenc.Iterator   // Iterator from the loser node, or winner for node 0.
	it            iteratorWithMaxTime // Only populated for leaf nodes.
	nextValueType chunkenc.ValueType
}

func (t *seriesChunksIteratorTree) moveNext(index int) bool {
	n := &t.nodes[index]

	if typ := n.it.Next(); typ != chunkenc.ValNone {
		n.nextValueType = typ
		return true
	}

	t.onNodeExhausted(n)
	return false
}

func (t *seriesChunksIteratorTree) onNodeExhausted(n *node) {
	n.index = -1
	n.nextValueType = chunkenc.ValNone
	n.value = nil

	if t.err == nil {
		t.err = n.it.Err()
	}
}

func (t *seriesChunksIteratorTree) WinnerIterator() chunkenc.Iterator {
	return t.nodes[t.nodes[0].index].value
}

func (t *seriesChunksIteratorTree) WinnerValueType() chunkenc.ValueType {
	return t.nodes[t.nodes[0].index].nextValueType
}

func (t *seriesChunksIteratorTree) Err() error {
	return t.err
}

func (t *seriesChunksIteratorTree) Next() bool {
	if len(t.nodes) == 0 {
		return false
	}
	if t.nodes[0].index == -1 { // If tree has not been initialized yet, do that.
		t.initialize()
		return t.nodes[t.nodes[0].index].index != -1
	}
	if t.nodes[t.nodes[0].index].index == -1 { // already exhausted
		return false
	}
	if t.moveNext(t.nodes[0].index) {
		t.replayGames(t.nodes[0].index)
	} else {
		t.sequenceEnded(t.nodes[0].index)
	}
	return t.nodes[t.nodes[0].index].index != -1
}

func (t *seriesChunksIteratorTree) Seek(ts int64) {
	// If the tree hasn't been initialized yet, do that now.
	if t.nodes[0].index == -1 {
		t.initialize()
	}

	for {
		winnerIndex := t.nodes[0].index
		n := &t.nodes[winnerIndex]

		if n.value == nil {
			// We've exhausted all iterators. We're done.
			return
		}

		if n.it.maxT < ts {
			// Iterator only contains samples before the desired timestamp.
			// There's no point asking it to seek, as it definitely won't have any samples we're interested in.
			// Mark it as exhausted and move to the next iterator.
			t.onNodeExhausted(n)
			t.sequenceEnded(winnerIndex)
			continue
		}

		if n.it.AtT() >= ts {
			// The iterator with the earliest remaining sample is at or after the timestamp we're looking for.
			// We are done.
			return
		}

		n.nextValueType = n.it.Seek(ts)
		if n.nextValueType == chunkenc.ValNone {
			// Seeking exhausted this iterator. Mark it as exhausted.
			t.onNodeExhausted(n)
			t.sequenceEnded(winnerIndex)
			continue
		}
	}
}

func (t *seriesChunksIteratorTree) initialize() {
	winners := make([]int, len(t.nodes))
	// Initialize leaf nodes as winners to start.
	for i := len(t.nodes) / 2; i < len(t.nodes); i++ {
		winners[i] = i
	}
	for i := len(t.nodes) - 2; i > 0; i -= 2 {
		// At each stage the winners play each other, and we record the loser in the node.
		loser, winner := t.playGame(winners[i], winners[i+1])
		p := parent(i)
		t.nodes[p].index = loser
		t.nodes[p].value = t.nodes[loser].value
		winners[p] = winner
	}
	t.nodes[0].index = winners[1]
	t.nodes[0].value = t.nodes[winners[1]].value
}

// Starting at pos, which is a winner, re-consider all values up to the root.
func (t *seriesChunksIteratorTree) replayGames(pos int) {
	// At the start, pos is a leaf node, and is the winner at that level.
	n := parent(pos)
	for n != 0 {
		// If n.value < pos.value then pos loses.
		// If they are equal, pos wins because n could be a sequence that ended, with value maxval.
		if t.nodes[n].value != nil && t.nodes[n].value.AtT() < t.nodes[pos].value.AtT() {
			loser := pos
			// Record pos as the loser here, and the old loser is the new winner.
			pos = t.nodes[n].index
			t.nodes[n].index = loser
			t.nodes[n].value = t.nodes[loser].value
		}
		n = parent(n)
	}
	// pos is now the winner; store it in node 0.
	t.nodes[0].index = pos
	t.nodes[0].value = t.nodes[pos].value
}

func (t *seriesChunksIteratorTree) sequenceEnded(pos int) {
	// Find the first active sequence which used to lose to it.
	n := parent(pos)
	for n != 0 && t.nodes[t.nodes[n].index].index == -1 {
		n = parent(n)
	}
	if n == 0 {
		// There are no active sequences left
		t.nodes[0].index = pos
		t.nodes[0].value = nil
		return
	}

	// Record pos as the loser here, and the old loser is the new winner.
	loser := pos
	winner := t.nodes[n].index
	t.nodes[n].index = loser
	t.nodes[n].value = t.nodes[loser].value
	t.replayGames(winner)
}

func (t *seriesChunksIteratorTree) playGame(a, b int) (loser, winner int) {
	if t.nodes[a].it.AtT() < t.nodes[b].it.AtT() {
		return b, a
	}
	return a, b
}

func parent(i int) int { return i / 2 }
