package wal

import (
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

type memSeries struct {
	sync.Mutex

	ref    chunks.HeadSeriesRef
	hash   uint64
	lastTs int64

	// TODO(rfratto): this solution below isn't perfect, and there's still
	// the possibility for a series to be deleted before it's
	// completely gone from the WAL. Rather, we should have gc return
	// a "should delete" map and be given a "deleted" map.
	// If a series that is going to be marked for deletion is in the
	// "deleted" map, then it should be deleted instead.
	//
	// The "deleted" map will be populated by the Truncate function.
	// It will be cleared with every call to gc.

	// willDelete marks a series as to be deleted on the next garbage
	// collection. If it receives a write, willDelete is disabled.
	willDelete bool

	// Whether this series has samples waiting to be committed to the WAL
	pendingCommit bool
}

func (s *memSeries) updateTs(ts int64) {
	s.lastTs = ts
	s.willDelete = false
	s.pendingCommit = true
}

func internLabels(lbls labels.Labels) {
	for i, l := range lbls {
		lbls[i].Name = interner.intern(l.Name)
		lbls[i].Value = interner.intern(l.Value)
	}
}

func releaseLabels(ls labels.Labels) {
	for _, l := range ls {
		interner.release(l.Name)
		interner.release(l.Value)
	}
}

// seriesHashmap is a simple hashmap for memSeries by their label set. It is
// built on top of a regular hashmap and holds a slice of series to resolve
// hash collisions. Its methods require the hash to be submitted with it to
// avoid re-computations throughout the code.
//
// This code is copied from the Prometheus TSDB.
type seriesHashmap map[uint64][]*refLabelsPair

type refLabelsPair struct {
	ref    chunks.HeadSeriesRef
	labels labels.Labels
}

func (m seriesHashmap) set(hash uint64, lset labels.Labels, ref chunks.HeadSeriesRef) {
	internLabels(lset)

	pair := refLabelsPair{ref: ref, labels: lset}

	l := m[hash]
	for i, prev := range l {
		if labels.Equal(prev.labels, lset) {
			l[i] = &pair
			return
		}
	}
	m[hash] = append(l, &pair)
}

func (m seriesHashmap) del(hash uint64, ref chunks.HeadSeriesRef) {
	var rem []*refLabelsPair
	for _, s := range m[hash] {
		if s.ref != ref {
			rem = append(rem, s)
		} else {
			releaseLabels(s.labels)
		}
	}
	if len(rem) == 0 {
		delete(m, hash)
	} else {
		m[hash] = rem
	}
}

const (
	// defaultStripeSize is the default number of entries to allocate in the
	// stripeSeries hash map.
	defaultStripeSize = 1 << 14
)

// stripeSeries locks modulo ranges of IDs and hashes to reduce lock contention.
// The locks are padded to not be on the same cache line. Filling the padded space
// with the maps was profiled to be slower – likely due to the additional pointer
// dereferences.
//
// This code is copied from the Prometheus TSDB.
// TODO(jratliff): can we reference this now?
type stripeSeries struct {
	size   int
	series []map[chunks.HeadSeriesRef]*memSeries
	hashes []seriesHashmap
	locks  []stripeLock
}

type stripeLock struct {
	sync.RWMutex
	// Padding to avoid multiple locks being on the same cache line.
	_ [40]byte
}

func newStripeSeries() *stripeSeries {
	stripeSize := defaultStripeSize
	s := &stripeSeries{
		size:   stripeSize,
		series: make([]map[chunks.HeadSeriesRef]*memSeries, stripeSize),
		hashes: make([]seriesHashmap, stripeSize),
		locks:  make([]stripeLock, stripeSize),
	}

	for i := range s.series {
		s.series[i] = map[chunks.HeadSeriesRef]*memSeries{}
	}
	for i := range s.hashes {
		s.hashes[i] = seriesHashmap{}
	}
	return s
}

// gc garbage collects old chunks that are strictly before mint and removes
// series entirely that have no chunks left.
func (s *stripeSeries) gc(mint int64) map[chunks.HeadSeriesRef]struct{} {
	var (
		deleted = map[chunks.HeadSeriesRef]struct{}{}
	)

	// Run through all series and find series that haven't been written to
	// since mint. Mark those series as deleted and store their ID.
	for i := 0; i < s.size; i++ {
		s.locks[i].Lock()

		for _, series := range s.series[i] {
			series.Lock()

			// If the series has received a write after mint, there's still
			// data and it's not completely gone yet.
			if series.lastTs >= mint || series.pendingCommit {
				series.willDelete = false
				series.Unlock()
				continue
			}

			// The series hasn't received any data and *might* be gone, but
			// we want to give it an opportunity to come back before marking
			// it as deleted, so we wait one more GC cycle.
			if !series.willDelete {
				series.willDelete = true
				series.Unlock()
				continue
			}

			// The series is gone entirely. We'll need to delete the label
			// hash (if one exists) so we'll obtain a lock for that too.
			j := int(series.hash) & (s.size - 1)
			if i != j {
				s.locks[j].Lock()
			}

			deleted[series.ref] = struct{}{}
			delete(s.series[i], series.ref)

			s.hashes[j].del(series.hash, series.ref)

			if i != j {
				s.locks[j].Unlock()
			}

			series.Unlock()
		}

		s.locks[i].Unlock()
	}

	return deleted
}

func (s *stripeSeries) getByID(id chunks.HeadSeriesRef) *memSeries {
	i := uint64(id) & uint64(s.size-1)

	s.locks[i].RLock()
	series := s.series[i][id]
	s.locks[i].RUnlock()

	return series
}

func (s *stripeSeries) set(series *memSeries) {
	i := uint64(series.ref) & uint64(s.size-1)

	s.locks[i].Lock()
	s.series[i][series.ref] = series
	s.locks[i].Unlock()
}

func (s *stripeSeries) saveLabels(hash uint64, series *memSeries, lbls labels.Labels) {
	i := hash & uint64(s.size-1)

	s.locks[i].Lock()
	s.hashes[i].set(hash, lbls, series.ref)
	s.locks[i].Unlock()
}

func (s *stripeSeries) iterator() *stripeSeriesIterator {
	return &stripeSeriesIterator{s}
}

// stripeSeriesIterator allows to iterate over series through a channel.
// The channel should always be completely consumed to not leak.
type stripeSeriesIterator struct {
	s *stripeSeries
}

func (it *stripeSeriesIterator) Channel() <-chan *memSeries {
	ret := make(chan *memSeries)

	go func() {
		for i := 0; i < it.s.size; i++ {
			it.s.locks[i].RLock()

			for _, series := range it.s.series[i] {
				series.Lock()

				j := int(series.hash) & (it.s.size - 1)
				if i != j {
					it.s.locks[j].RLock()
				}

				ret <- series

				if i != j {
					it.s.locks[j].RUnlock()
				}
				series.Unlock()
			}

			it.s.locks[i].RUnlock()
		}

		close(ret)
	}()

	return ret
}
