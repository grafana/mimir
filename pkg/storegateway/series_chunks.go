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

	bytesReleaser releaser
}

func (b *seriesChunksSet) release() {
	if len(b.series) == 0 {
		// There's nothing to release, just return; this also allows to call release() on a zero-valued seriesChunksSet.
		return
	}

	b.bytesReleaser.Release()

	// Make it harder to do a "use after free".
	b.series = nil
}

func (b seriesChunksSet) len() int {
	return len(b.series)
}
