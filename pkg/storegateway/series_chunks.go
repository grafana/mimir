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

	bytesReleaser releaser
}

//nolint:unused // dead code while we are working on PR 3355
type releaser interface{ Release() }

//nolint:unused // dead code while we are working on PR 3355
func (b *seriesChunksSet) release() {
	if len(b.series) == 0 {
		// There's nothing to release, just return; this also allows to call release() on a zero-valued seriesChunksSet.
		return
	}

	b.bytesReleaser.Release()

	// Make it harder to do a "use after free".
	b.series = nil
}

//nolint:unused // dead code while we are working on PR 3355
func (b seriesChunksSet) len() int {
	return len(b.series)
}
