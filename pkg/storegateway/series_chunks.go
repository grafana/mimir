// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

// seriesChunksSetIterator is the interface implemented by an iterator returning a sequence of seriesChunksSet.
type seriesChunksSetIterator interface {
	Next() bool
	At() seriesChunksSet
	Err() error
}

// seriesChunksSet holds a set of series, each with its own chunks.
type seriesChunksSet struct {
	series []seriesEntry // this should ideally be its own type that doesn't have the refs

	// chunksReleaser releases the memory used to allocate series chunks.
	chunksReleaser chunksReleaser
}

type chunksReleaser interface {
	// Release the memory used to allocate series chunks.
	Release()
}

func (b *seriesChunksSet) release() {
	if b.chunksReleaser != nil {
		b.chunksReleaser.Release()
		b.chunksReleaser = nil
	}

	b.series = nil
}

func (b seriesChunksSet) len() int {
	return len(b.series)
}
