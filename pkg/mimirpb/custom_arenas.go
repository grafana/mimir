// SPDX-License-Identifier: AGPL-3.0-only

//go:build goexperiment.arenas

package mimirpb

import (
	"github.com/grafana/mimir/pkg/util/arena"
	"go.uber.org/atomic"
	"google.golang.org/grpc/mem"
)

// unmarshalSlicePool returns a BufferPool that allocates buffers in an arena.
// Once all allocated buffers are put back in the pool, the arena is freed.
func unmarshalSlicePool(a *arena.Arena) mem.BufferPool {
	return &arenaBufferPool{arena: a}
}

type arenaBufferPool struct {
	arena    *arena.Arena
	bufCount atomic.Int64
}

// Get implements mem.BufferPool.
func (a *arenaBufferPool) Get(length int) *[]byte {
	a.bufCount.Add(1)
	buf := arena.MakeSlice[byte](a.arena, length, length)
	return &buf
}

// Put implements mem.BufferPool.
func (a *arenaBufferPool) Put(*[]byte) {
	count := a.bufCount.Sub(1)
	switch {
	case count == 0:
		a.arena.Free()
	case count < 0:
		panic("arenaBufferPool buffer count below zero")
	}
}

var _ mem.BufferPool = (*arenaBufferPool)(nil)
