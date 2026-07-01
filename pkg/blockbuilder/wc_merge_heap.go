// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

// sourceHead is the current front record of one write compartment's source, awaiting
// emission by the cross-compartment merge. sourceIndex points back into the merge's sources
// slice so the next record can be pulled from the same source once this head is emitted.
type sourceHead struct {
	rec         *kgo.Record
	wc          int
	sourceIndex int
}

// sourceHeadHeap is a min-heap of per-source heads ordered by (record.Timestamp, write
// compartment ID, offset). The write compartment ID and offset are deterministic tie-breakers
// so the emit order is stable when records share a producer timestamp, as happens within a
// single distributor write batch.
type sourceHeadHeap []sourceHead

func (h sourceHeadHeap) Len() int { return len(h) }

// Less orders by timestamp ascending, then write compartment ID ascending, then offset ascending.
func (h sourceHeadHeap) Less(i, j int) bool {
	a, b := h[i].rec, h[j].rec
	if !a.Timestamp.Equal(b.Timestamp) {
		return a.Timestamp.Before(b.Timestamp)
	}
	if h[i].wc != h[j].wc {
		return h[i].wc < h[j].wc
	}
	return a.Offset < b.Offset
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
