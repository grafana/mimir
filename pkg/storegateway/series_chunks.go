// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/pool"
)

// seriesChunksSetIterator is the interface implemented by an iterator returning a sequence of seriesChunksSet.
type seriesChunksSetIterator interface {
	Next() bool
	At() seriesChunksSet
	Err() error
}

// seriesChunksSet holds a set of series, each with its own chunks.
type seriesChunksSet struct {
	series []seriesEntry

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

func newSeriesSetWithChunks(ctx context.Context, chunkReaders bucketChunkReaders, chunksPool pool.Bytes, batches seriesChunkRefsSetIterator, stats *safeQueryStats, iteratorLoadDurations *prometheus.HistogramVec) storepb.SeriesSet {
	var iterator seriesChunksSetIterator
	iterator = newLoadingSeriesChunksSetIterator(chunkReaders, chunksPool, batches, stats)
	iterator = newDurationMeasuringIterator[seriesChunksSet](iterator, iteratorLoadDurations.WithLabelValues("chunks_load"))
	iterator = newPreloadingSetIterator[seriesChunksSet](ctx, 1, iterator)
	// We are measuring the time we wait for a preloaded batch. In an ideal world this is 0 because there's always a preloaded batch waiting.
	// But realistically it will not be. Along with the duration of the chunks_load iterator,
	// we can determine where is the bottleneck in the streaming pipeline.
	iterator = newDurationMeasuringIterator[seriesChunksSet](iterator, iteratorLoadDurations.WithLabelValues("chunks_preloaded"))
	return newSeriesChunksSeriesSet(iterator)
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
type preloadedSeriesChunksSet[T any] struct {
	set T
	err error
}

type genericIterator[V any] interface {
	Next() bool
	At() V
	Err() error
}

type preloadingSetIterator[Set any] struct {
	ctx  context.Context
	from genericIterator[Set]

	current Set

	preloaded chan preloadedSeriesChunksSet[Set]
	err       error
}

func newPreloadingSetIterator[Set any](ctx context.Context, preloadedSetsCount int, from genericIterator[Set]) *preloadingSetIterator[Set] {
	preloadedSet := &preloadingSetIterator[Set]{
		ctx:       ctx,
		from:      from,
		preloaded: make(chan preloadedSeriesChunksSet[Set], preloadedSetsCount-1), // one will be kept outside the channel when the channel blocks
	}
	go preloadedSet.preload()
	return preloadedSet
}

func (p *preloadingSetIterator[Set]) preload() {
	defer close(p.preloaded)

	for p.from.Next() {
		select {
		case <-p.ctx.Done():
			// If the context is done, we should just stop the preloading goroutine.
			return
		case p.preloaded <- preloadedSeriesChunksSet[Set]{set: p.from.At()}:
		}
	}

	if p.from.Err() != nil {
		p.preloaded <- preloadedSeriesChunksSet[Set]{err: p.from.Err()}
	}
}

func (p *preloadingSetIterator[Set]) Next() bool {
	preloaded, ok := <-p.preloaded
	if !ok {
		// Iteration reached the end or context has been canceled.
		return false
	}

	p.current = preloaded.set
	p.err = preloaded.err

	return p.err == nil
}

func (p *preloadingSetIterator[Set]) At() Set {
	return p.current
}

func (p *preloadingSetIterator[Set]) Err() error {
	return p.err
}

type loadingSeriesChunksSetIterator struct {
	chunkReaders bucketChunkReaders
	from         seriesChunkRefsSetIterator
	chunksPool   pool.Bytes
	stats        *safeQueryStats

	current seriesChunksSet
	err     error
}

func newLoadingSeriesChunksSetIterator(chunkReaders bucketChunkReaders, chunksPool pool.Bytes, from seriesChunkRefsSetIterator, stats *safeQueryStats) *loadingSeriesChunksSetIterator {
	return &loadingSeriesChunksSetIterator{
		chunkReaders: chunkReaders,
		from:         from,
		chunksPool:   chunksPool,
		stats:        stats,
	}
}

func (c *loadingSeriesChunksSetIterator) Next() bool {
	if c.err != nil {
		return false
	}

	if !c.from.Next() {
		c.err = c.from.Err()
		return false
	}

	nextUnloaded := c.from.At()

	// This data structure doesn't retain the seriesChunkRefsSet so it can be released once done.
	defer nextUnloaded.release()

	entries := make([]seriesEntry, nextUnloaded.len())
	c.chunkReaders.reset()
	for i, s := range nextUnloaded.series {
		entries[i].lset = s.lset
		entries[i].chks = make([]storepb.AggrChunk, len(s.chunks))

		for j, chunk := range s.chunks {
			entries[i].chks[j].MinTime = chunk.minTime
			entries[i].chks[j].MaxTime = chunk.maxTime

			err := c.chunkReaders.addLoad(chunk.blockID, chunk.ref, i, j)
			if err != nil {
				c.err = errors.Wrap(err, "preloading chunks")
				return false
			}
		}
	}

	// Create a batched memory pool that can be released all at once.
	chunksPool := &pool.BatchBytes{Delegate: c.chunksPool}

	err := c.chunkReaders.load(entries, chunksPool, c.stats)
	if err != nil {
		c.err = errors.Wrap(err, "loading chunks")
		return false
	}
	c.current = seriesChunksSet{
		series:         entries,
		chunksReleaser: chunksPool,
	}
	return true
}

func (c *loadingSeriesChunksSetIterator) At() seriesChunksSet {
	return c.current
}

func (c *loadingSeriesChunksSetIterator) Err() error {
	return c.err
}

type durationMeasuringIterator[Set any] struct {
	from             genericIterator[Set]
	durationObserver prometheus.Observer
}

func newDurationMeasuringIterator[Set any](from genericIterator[Set], durationObserver prometheus.Observer) genericIterator[Set] {
	return &durationMeasuringIterator[Set]{
		from:             from,
		durationObserver: durationObserver,
	}
}

func (m *durationMeasuringIterator[Set]) Next() bool {
	start := time.Now()
	next := m.from.Next()
	m.durationObserver.Observe(time.Since(start).Seconds())
	return next
}

func (m *durationMeasuringIterator[Set]) At() Set {
	return m.from.At()
}

func (m *durationMeasuringIterator[Set]) Err() error {
	return m.from.Err()
}
