// SPDX-License-Identifier: AGPL-3.0-only

package store

import "sync/atomic"

// Tests count what queries read, to check they read no more than what they return. Counting is
// off outside tests: the counters are shared by every query thread.
var countersEnabled bool

var (
	// Series label values that queries looked up to check a matcher.
	labelValueReads    atomic.Uint64
	chunkBoundsDecodes atomic.Uint64
	chunkListDecodes   atomic.Uint64
	// Metric names name matchers were checked against, and cold blocks' label lists computed.
	nameMatchCount    atomic.Uint64
	coldLabelLists    atomic.Uint64
	coldNameMatches   atomic.Uint64
	regexEvaluations  atomic.Uint64
	regexCompiles     atomic.Uint64
	coldShardHashings atomic.Uint64
)

func countLabelValueRead() {
	if countersEnabled {
		labelValueReads.Add(1)
	}
}
