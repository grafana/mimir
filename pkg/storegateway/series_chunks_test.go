// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

// sliceSeriesChunksSetIterator implements seriesChunksSetIterator and
// returns the provided err when the batches are exhausted
type sliceSeriesChunksSetIterator struct {
	current int
	batches []seriesChunksSet

	err error
}

func newSliceSeriesChunksSetIterator(err error, batches ...seriesChunksSet) seriesChunksSetIterator {
	return &sliceSeriesChunksSetIterator{
		current: -1,
		batches: batches,
		err:     err,
	}
}

func (s *sliceSeriesChunksSetIterator) Next() bool {
	s.current++
	return s.current < len(s.batches)
}

func (s *sliceSeriesChunksSetIterator) At() seriesChunksSet {
	return s.batches[s.current]
}

func (s *sliceSeriesChunksSetIterator) Err() error {
	if s.current >= len(s.batches) {
		return s.err
	}
	return nil
}
