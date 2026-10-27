// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"math"
	"sync"
)

// readGuard keeps what queries read in place until the queries that may have it are done: a query enters it before
// it reads and leaves it once its caller is done with what it returned, and what the store would release meanwhile
// waits for the queries that entered before.
type readGuard struct {
	mu sync.Mutex
	// Each release starts a new epoch: the queries of earlier ones may still have what it releases.
	epoch  uint64
	active map[uint64]int
	queue  []guardedRelease
}

type guardedRelease struct {
	epoch   uint64
	release func()
}

// guardChunkFiles keeps the chunk files head compactions remove mapped until the queries that read them are done, so
// that queries return the chunks' data as it is in the files instead of copies.
func (s *Store) guardChunkFiles() {
	for _, shard := range s.shards {
		shard.disk.DeferUnmaps(s.reads.release)
	}
}

// enter returns the epoch the query reads in, for leave.
func (g *readGuard) enter() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.active == nil {
		g.active = map[uint64]int{}
	}
	g.active[g.epoch]++
	return g.epoch
}

func (g *readGuard) leave(epoch uint64) {
	g.mu.Lock()
	if g.active[epoch]--; g.active[epoch] <= 0 {
		delete(g.active, epoch)
	}
	ready := g.readyLocked()
	g.mu.Unlock()
	for _, release := range ready {
		release()
	}
}

// release runs f once the queries that entered before are done, right away when there are none.
func (g *readGuard) release(f func()) {
	g.mu.Lock()
	if len(g.active) == 0 {
		g.mu.Unlock()
		f()
		return
	}
	g.queue = append(g.queue, guardedRelease{epoch: g.epoch, release: f})
	// The queries that enter from now on can't have it.
	g.epoch++
	g.mu.Unlock()
}

// readyLocked returns the releases no query that entered before is still reading for.
func (g *readGuard) readyLocked() []func() {
	oldest := uint64(math.MaxUint64)
	for epoch := range g.active {
		oldest = min(oldest, epoch)
	}
	var ready []func()
	kept := g.queue[:0]
	for _, r := range g.queue {
		if r.epoch < oldest {
			ready = append(ready, r.release)
		} else {
			kept = append(kept, r)
		}
	}
	clear(g.queue[len(kept):])
	g.queue = kept
	return ready
}
