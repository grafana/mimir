// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"sync"
	"sync/atomic"

	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// nameShards is which of a tenant's shards hold series of a metric name, in memory or in cold blocks, as of each
// shard's count of name groups and of cold block changes: a select of the name visits only those shards, where it
// visited all of them, which for the many names of a few series were mostly shards without any.
type nameShards struct {
	bits uint64
	// The layoutChanges the bits are as of: while it is the same, no shard's counts changed.
	layout uint64
	// By shard, the counts the bits are as of, and whether the shard had the tenant.
	names, cold []uint64
	known       []bool
}

// nameShardIndex is an engine's nameShards by metric name.
type nameShardIndex struct {
	mu    sync.RWMutex
	names map[string]*nameShards
}

// layoutChanges counts the changes of any shard's names or cold blocks: while it doesn't move, a select uses the bits it
// finds without checking each shard's counts.
var layoutChanges atomic.Uint64

// maxIndexedNames is how many names the index keeps: at this many, they are all dropped, which the names selected
// fill again.
const maxIndexedNames = 1 << 16

// shardsOfName returns the shards a select of the name has to visit, as a bit for each, or false when the engine has
// more shards than bits. A shard whose names or cold blocks changed since is looked at again, with its lock read held,
// which the next select of the name then doesn't.
func (e *Engine) shardsOfName(name string) (uint64, bool) {
	shards := len(e.store.shards)
	if shards > 64 {
		return 0, false
	}
	layout := layoutChanges.Load()
	index := &e.nameShards
	index.mu.RLock()
	cached := index.names[name]
	index.mu.RUnlock()
	if cached != nil && cached.layout == layout {
		return cached.bits, true
	}
	var fresh *nameShards
	for shard := range shards {
		if cached != nil && cached.known[shard] {
			if t := e.tenants[shard].Load(); t != nil && t.series.namesAdded.Load() == cached.names[shard] && e.store.shards[shard].cold.changes.Load() == cached.cold[shard] {
				continue
			}
		}
		if fresh == nil {
			fresh = &nameShards{names: make([]uint64, shards), cold: make([]uint64, shards), known: make([]bool, shards)}
			if cached != nil {
				fresh.bits = cached.bits
				copy(fresh.names, cached.names)
				copy(fresh.cold, cached.cold)
				copy(fresh.known, cached.known)
			}
		}
		present, names, cold, known := e.shardHasName(shard, name)
		bit := uint64(1) << shard
		fresh.bits &^= bit
		if present {
			fresh.bits |= bit
		}
		fresh.names[shard], fresh.cold[shard], fresh.known[shard] = names, cold, known
	}
	if fresh == nil {
		// Unchanged: the next select takes the fast path.
		fresh = &nameShards{bits: cached.bits, names: cached.names, cold: cached.cold, known: cached.known}
	}
	fresh.layout = layout
	index.mu.Lock()
	if index.names == nil || len(index.names) >= maxIndexedNames {
		index.names = map[string]*nameShards{}
	}
	index.names[string([]byte(name))] = fresh
	index.mu.Unlock()
	return fresh.bits, true
}

// shardHasName reports whether the shard holds series of the name, in memory or in a cold block that may, with the
// counts that is as of, and whether the shard has the tenant: without it, the next select looks again.
func (e *Engine) shardHasName(index int, name string) (present bool, names, cold uint64, known bool) {
	shard := e.store.shards[index]
	shard.RLock()
	defer shard.RUnlock()
	// Writers change them with the lock held: as of what the lock lets this read.
	cold = shard.cold.changes.Load()
	t, ok := e.tenantLocked(index)
	if ok {
		names = t.series.namesAdded.Load()
		_, present = t.series.names[name]
	}
	if !present {
		hash := labels.ValueHash(name)
		for _, block := range shard.cold.blocks {
			if table := block.tenant(e.tenantID); table != nil && block.mayHaveName(table, hash) {
				present = true
				break
			}
		}
	}
	return present, names, cold, ok
}

// perShardOfName is perShard over the shards that hold series of the name, when byName, or every shard.
func (e *Engine) perShardOfName(name string, byName bool, query func(index int, t *tenant, disk *chunks.DiskMapper, cold *coldState)) {
	bits, ok := uint64(0), false
	if byName {
		bits, ok = e.shardsOfName(name)
	}
	if !ok {
		e.perShard(query)
		return
	}
	for index, shard := range e.store.shards {
		if bits&(1<<index) == 0 {
			continue
		}
		shard.RLock()
		if t, ok := e.tenantLocked(index); ok {
			query(index, t, shard.disk, shard.cold)
		}
		shard.RUnlock()
	}
}
