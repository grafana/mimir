// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

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

type seriesChunksSeriesSet struct {
	from seriesChunksSetIterator

	currSet    seriesChunksSet
	currOffset int
}

func newSeriesChunksSeriesSet(from seriesChunksSetIterator) storepb.SeriesSet {
	return &seriesChunksSeriesSet{
		from: from,
	}
}

// Next advances to the next item. Once the underlying seriesChunksSet has been fully consumed
// (which means the call to Next moves to the next set), the seriesChunksSet is released. This
// means that it's not safe to read from the values returned by At() after Next() is called again.
func (b *seriesChunksSeriesSet) Next() bool {
	b.currOffset++
	if b.currOffset >= b.currSet.len() {
		if !b.from.Next() {
			b.currSet.release()
			return false
		}
		b.currSet.release()
		b.currSet = b.from.At()
		b.currOffset = 0
	}
	return true
}

// At returns the current series. The result from At() MUST not be retained after calling Next()
func (b *seriesChunksSeriesSet) At() (labels.Labels, []storepb.AggrChunk) {
	if b.currOffset >= b.currSet.len() {
		return nil, nil
	}

	return b.currSet.series[b.currOffset].lset, b.currSet.series[b.currOffset].chks
}

func (b *seriesChunksSeriesSet) Err() error {
	return b.from.Err()
}
