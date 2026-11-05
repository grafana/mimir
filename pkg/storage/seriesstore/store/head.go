// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"bytes"
	"fmt"
	"os"
	"slices"
	"time"

	promlabels "github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// freezeScope limits scans to groups containing series that are leaving the head.
type freezeScope struct {
	groups []uint32
}

// forEach visits the series the scope covers, with the shard locked.
func (f *freezeScope) forEach(state *shardState, visit func(*seriesEntry)) {
	t := state.tenant.Load()
	if t == nil {
		return
	}
	if f == nil {
		t.series.forEach(visit)
		return
	}
	for _, group := range f.groups {
		entries := t.series.groups[group].entries
		for index := range entries {
			visit(&entries[index])
		}
	}
}

// A nil scope scans all groups; an empty scope scans none.
func (f *freezeScope) groupsOf() []uint32 {
	if f == nil {
		return nil
	}
	if f.groups == nil {
		return []uint32{}
	}
	return f.groups
}

// pendingFreeze is a cold block of the series that left the head, from what they held when it
// was prepared.
type pendingFreeze struct {
	id      uint64
	frozen  []frozenSeries
	keys    map[labels.Labels]chunkList
	started time.Time
}

// prepareFreeze cuts the open chunks of the series out of the head and takes their data for a
// cold block, or returns nil when there are none. With the shard locked.
func prepareFreeze(state *shardState, tenantID string, scope *freezeScope) *pendingFreeze {
	pending := &pendingFreeze{keys: map[labels.Labels]chunkList{}, started: time.Now()}
	scope.forEach(state, func(entry *seriesEntry) {
		series := &entry.series
		if series.inHead {
			return
		}
		err := cutFloatHead(series, state.disk)
		if err == nil {
			err = cutHistogramHead(series, state.disk)
		}
		if err == nil {
			err = flushOutOfOrder(series, state.disk)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "phase=cold_freeze_error tenant=%s error=%v\n", tenantID, err)
			return
		}
		// A copy: the series may take samples again before the block is installed.
		chunks := slices.Clone(series.chunks)
		pending.keys[entry.labels] = chunks
		// Series without any sample have nothing to keep.
		if !chunks.isEmpty() {
			pending.frozen = append(pending.frozen, frozenSeries{tenant: tenantID, labels: entry.labels, chunks: chunks, nativeHistogram: series.nativeHistogram})
		}
	})
	if len(pending.keys) == 0 {
		return nil
	}
	pending.id = state.cold.nextID
	state.cold.nextID++
	return pending
}

// build writes the block, which needs no shard lock: it only reads the series' copies and the
// chunk files' immutable chunks. A nil block is one with no series to keep.
func (p *pendingFreeze) build(directory string) *coldBlock {
	if len(p.frozen) == 0 {
		return nil
	}
	block, err := buildColdBlock(directory, p.id, p.frozen)
	if err != nil {
		fmt.Fprintf(os.Stderr, "phase=cold_block_error id=%d error=%v\n", p.id, err)
		p.keys = nil
		return block
	}
	return block
}

// installFreeze adds the block and drops its series from memory, with the shard locked. If any
// of them took samples since it was prepared, the block is discarded and they all stay in
// memory, for the next freeze: the block would miss the new samples.
func installFreeze(state *shardState, pending *pendingFreeze, block *coldBlock, scope *freezeScope) (removed map[uint64]struct{}) {
	if pending.keys == nil {
		return nil
	}
	unchanged := 0
	scope.forEach(state, func(entry *seriesEntry) {
		series := &entry.series
		// Most series stay in the head: no need to hash their labels.
		if series.inHead {
			return
		}
		chunks, ok := pending.keys[entry.labels]
		if ok && series.floatHead == nil && series.histogram() == nil && len(series.ooo()) == 0 && bytes.Equal(series.chunks, chunks) {
			unchanged++
		}
	})
	if unchanged != len(pending.keys) {
		fmt.Fprintf(os.Stderr, "phase=cold_block_discarded id=%d series=%d changed=%d\n", pending.id, len(pending.keys), len(pending.keys)-unchanged)
		if block != nil {
			discardColdBlock(block)
		}
		return nil
	}
	if block != nil {
		state.cold.blocks = append(state.cold.blocks, block)
		state.cold.changed()
		layoutChanges.Add(1)
		state.cold.mergeSmallBlocks()
	}
	var builder promlabels.ScratchBuilder
	removed = map[uint64]struct{}{}
	t := state.tenant.Load()
	if t != nil {
		changed := t.series.retainGroups(scope.groupsOf(), func(entry *seriesEntry) bool {
			if entry.series.inHead {
				return true
			}
			_, gone := pending.keys[entry.labels]
			if gone {
				t.uncount(entry, &builder)
				if ref := entry.series.ref; ref != 0 {
					t.byRef.delete(ref)
					removed[ref] = struct{}{}
				}
			}
			return !gone
		})
		reindexGroups(t, changed)
	}
	fmt.Fprintf(os.Stderr, "phase=cold_block_written id=%d series=%d duration_ms=%d\n", pending.id, len(pending.keys), time.Since(pending.started).Milliseconds())
	return removed
}
