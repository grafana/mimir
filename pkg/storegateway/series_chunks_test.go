// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

// sliceSeriesChunksSetIterator implements seriesChunksSetIterator and
// returns the provided err when the sets are exhausted
type sliceSeriesChunksSetIterator struct {
	current int
	sets    []seriesChunksSet

	err error
}

func newSliceSeriesChunksSetIterator(err error, sets ...seriesChunksSet) seriesChunksSetIterator {
	return &sliceSeriesChunksSetIterator{
		current: -1,
		sets:    sets,
		err:     err,
	}
}

func (s *sliceSeriesChunksSetIterator) Next() bool {
	s.current++
	return s.current < len(s.sets)
}

func (s *sliceSeriesChunksSetIterator) At() seriesChunksSet {
	return s.sets[s.current]
}

func (s *sliceSeriesChunksSetIterator) Err() error {
	if s.current >= len(s.sets) {
		return s.err
	}
	return nil
}
