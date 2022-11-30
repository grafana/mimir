// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

// sliceSeriesChunksSetIterator implements seriesChunksSetIterator and
// returns the provided err when the sets are exhausted
//
//nolint:unused // dead code while we are working on PR 3355
type sliceSeriesChunksSetIterator struct {
	current int
	sets    []seriesChunksSet

	err error
}

//nolint:unused // dead code while we are working on PR 3355
func newSliceSeriesChunksSetIterator(err error, sets ...seriesChunksSet) seriesChunksSetIterator {
	return &sliceSeriesChunksSetIterator{
		current: -1,
		sets:    sets,
		err:     err,
	}
}

//nolint:unused // dead code while we are working on PR 3355
func (s *sliceSeriesChunksSetIterator) Next() bool {
	s.current++
	return s.current < len(s.sets)
}

//nolint:unused // dead code while we are working on PR 3355
func (s *sliceSeriesChunksSetIterator) At() seriesChunksSet {
	return s.sets[s.current]
}

//nolint:unused // dead code while we are working on PR 3355
func (s *sliceSeriesChunksSetIterator) Err() error {
	if s.current >= len(s.sets) {
		return s.err
	}
	return nil
}
