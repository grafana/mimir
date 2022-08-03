// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

type InstantSplitterStats struct {
	splitQueries int
}

func NewInstantSplitterStats() *InstantSplitterStats {
	return &InstantSplitterStats{}
}

// AddSplitQueries add num split queries to the counter.
func (s *InstantSplitterStats) AddSplitQueries(num int) {
	s.splitQueries += num
}

// GetSplitQueries returns the number of split queries.
func (s *InstantSplitterStats) GetSplitQueries() int {
	return s.splitQueries
}
