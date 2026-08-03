// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

// sourceHead is the current front record of one Kafka cluster's source, awaiting
// emission by the cross-cluster merge. sourceIndex points back into the merge's sources
// slice so the next record can be pulled from the same source once this head is emitted.
type sourceHead struct {
	rec         *kgo.Record
	clusterID   int
	sourceIndex int
}

// sourceHeadHeap is a min-heap of per-source heads ordered by (record.Timestamp, cluster ID). The
// cluster ID makes the order deterministic when records share a producer timestamp (as happens
// within a single distributor write batch). Offset is not a tie-breaker: mergeSourcesByTimestamp
// holds at most one head per source and each source is a distinct cluster, so any two heads always
// differ by cluster ID and the comparison never reaches offset. Ordering among records from the
// same source instead comes from that source's own offset-ordered delivery.
type sourceHeadHeap []sourceHead

func (h sourceHeadHeap) Len() int { return len(h) }

// Less orders by timestamp ascending, then cluster ID ascending.
func (h sourceHeadHeap) Less(i, j int) bool {
	a, b := h[i].rec, h[j].rec
	if !a.Timestamp.Equal(b.Timestamp) {
		return a.Timestamp.Before(b.Timestamp)
	}
	return h[i].clusterID < h[j].clusterID
}

func (h sourceHeadHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *sourceHeadHeap) Push(x any) { *h = append(*h, x.(sourceHead)) }

func (h *sourceHeadHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
