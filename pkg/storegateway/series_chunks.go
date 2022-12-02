// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"

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

// preloadedSeriesChunksSet holds the result of preloading the next set. It can either contain
// the preloaded set or an error, but not both.
type preloadedSeriesChunksSet struct {
	set seriesChunksSet
	err error
}

type preloadingSeriesChunkSetIterator struct {
	ctx     context.Context
	from    seriesChunksSetIterator
	current seriesChunksSet

	preloaded chan preloadedSeriesChunksSet
	err       error
}

func newPreloadingSeriesChunkSetIterator(ctx context.Context, preloadedSetsCount int, from seriesChunksSetIterator) *preloadingSeriesChunkSetIterator {
	preloadedSet := &preloadingSeriesChunkSetIterator{
		ctx:       ctx,
		from:      from,
		preloaded: make(chan preloadedSeriesChunksSet, preloadedSetsCount-1), // one will be kept outside the channel when the channel blocks
	}
	go preloadedSet.preload()
	return preloadedSet
}

func (p *preloadingSeriesChunkSetIterator) preload() {
	defer close(p.preloaded)

	for p.from.Next() {
		select {
		case <-p.ctx.Done():
			// If the context is done, we should just stop the preloading goroutine.
			return
		case p.preloaded <- preloadedSeriesChunksSet{set: p.from.At()}:
		}
	}

	if p.from.Err() != nil {
		p.preloaded <- preloadedSeriesChunksSet{err: p.from.Err()}
	}
}

func (p *preloadingSeriesChunkSetIterator) Next() bool {
	preloaded, ok := <-p.preloaded
	if !ok {
		// Iteration reached the end or context has been canceled.
		return false
	}

	p.current = preloaded.set
	p.err = preloaded.err

	return p.err == nil
}

func (p *preloadingSeriesChunkSetIterator) At() seriesChunksSet {
	return p.current
}

func (p *preloadingSeriesChunkSetIterator) Err() error {
	return p.err
}
