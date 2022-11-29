// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

type unloadedBatch struct {
	Entries []unloadedBatchEntry
	Stats   *safeQueryStats
}

func newBatch(size int) unloadedBatch {
	return unloadedBatch{
		Entries: make([]unloadedBatchEntry, size),
		Stats:   newSafeQueryStats(),
	}
}

func (b unloadedBatch) len() int {
	return len(b.Entries)
}

type unloadedBatchEntry struct {
	lset   labels.Labels
	chunks []unloadedChunk
}

type unloadedChunk struct {
	BlockID          ulid.ULID
	Ref              chunks.ChunkRef
	MinTime, MaxTime int64
}

func (m unloadedChunk) Compare(b unloadedChunk) int {
	if m.MinTime < b.MinTime {
		return 1
	}
	if m.MinTime > b.MinTime {
		return -1
	}

	// Same min time.
	if m.MaxTime < b.MaxTime {
		return 1
	}
	if m.MaxTime > b.MaxTime {
		return -1
	}
	return 0
}
