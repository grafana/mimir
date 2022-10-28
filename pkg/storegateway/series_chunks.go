// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

// seriesChunksSetIterator is the interface implemented by an iterator returning a sequence of seriesChunksSet.
//
//nolint:unused // dead code while we are working on PR 3355
type seriesChunksSetIterator interface {
	Next() bool
	At() seriesChunksSet
	Err() error
}

// seriesChunksSet holds a set of series, each with its own chunks.
//
//nolint:unused // dead code while we are working on PR 3355
type seriesChunksSet struct {
	series []seriesEntry

	// chunksReleaser releases the memory used to allocate series chunks.
	chunksReleaser chunksReleaser
}

//nolint:unused // dead code while we are working on PR 3355
type chunksReleaser interface {
	// Release the memory used to allocate series chunks.
	Release()
}

//nolint:unused // dead code while we are working on PR 3355
func (b *seriesChunksSet) release() {
	if b.chunksReleaser != nil {
		b.chunksReleaser.Release()
		b.chunksReleaser = nil
	}

	b.series = nil
}

//nolint:unused // dead code while we are working on PR 3355
func (b seriesChunksSet) len() int {
	return len(b.series)
}
