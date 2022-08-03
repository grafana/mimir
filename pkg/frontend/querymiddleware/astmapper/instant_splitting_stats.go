// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

type InstantSplitterStats struct {
	splitQueries           int  // counter of split queries (0 if non-splittable)
	noOpSmallIntervalQuery bool // query is a noop due to a small range interval
	noOpSubquery           bool // query is a subquery (non-splittable)
	noOpNonSplittableQuery bool // query is non-splittable
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

// SetNoOpSmallRangeInterval set query as a no operation due to a small range interval.
func (s *InstantSplitterStats) SetNoOpSmallRangeInterval() {
	s.noOpSmallIntervalQuery = true
}

// GetNoOpSmallIntervalQuery returns whether the query is a no operation due to a small range interval.
func (s *InstantSplitterStats) GetNoOpSmallIntervalQuery() bool {
	if s.GetSplitQueries() > 0 {
		s.noOpSmallIntervalQuery = false
	}
	return s.noOpSmallIntervalQuery
}

// SetNoOpSubquery increment num of no operation queries that are subqueries to the counter.
func (s *InstantSplitterStats) SetNoOpSubquery() {
	s.noOpSubquery = true
}

// GetNoOpSubquery returns whether the query is a subquery.
func (s *InstantSplitterStats) GetNoOpSubquery() bool {
	if s.GetSplitQueries() > 0 {
		s.noOpSubquery = false
	}
	return s.noOpSubquery
}

// GetNoOpNonSplittableQuery returns whether the query is non-splittable.
// Note: if the query results in a noop and both noOpSmallIntervalQuery and noOpSubquery are false,
// then noOpNonSplittableQuery is true
func (s *InstantSplitterStats) GetNoOpNonSplittableQuery() bool {
	if s.splitQueries <= 0 && !s.GetNoOpSmallIntervalQuery() && !s.GetNoOpSubquery() {
		s.noOpNonSplittableQuery = true
	}
	return s.noOpNonSplittableQuery
}
