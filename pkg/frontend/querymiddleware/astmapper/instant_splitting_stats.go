// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

type noOpReason string

// Possible noop reasons
const (
	SmallIntervalNoOpReason = noOpReason("small_interval")
	SubqueryNoOpReason      = noOpReason("subquery")
	NonSplittableNoOpReason = noOpReason("non_splittable")
	noneNoOpReason          = noOpReason("none")
)

type InstantSplitterStats struct {
	splitQueries    int        // counter of split queries (0 if non-splittable)
	noOpQueryReason noOpReason // reason the initial query is a no operation
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

// SetNoOpQueryReason set no operation reason for query.
func (s *InstantSplitterStats) SetNoOpQueryReason(reason noOpReason) {
	if s.isNoOpQueryReasonSet() {
		return
	}
	s.noOpQueryReason = reason
}

// GetNoOpQueryReason returns the reason a query is a no operation.
// If number of split queries is greater than 0, it means the query is splittable
// Otherwise, if no noop reason is set, it means the query is non-splittable
func (s *InstantSplitterStats) GetNoOpQueryReason() noOpReason {
	if s.GetSplitQueries() > 0 {
		return noneNoOpReason
	}
	if s.isNoOpQueryReasonSet() {
		return s.noOpQueryReason
	}
	return NonSplittableNoOpReason
}

func (s *InstantSplitterStats) isNoOpQueryReasonSet() bool {
	return len(s.noOpQueryReason) > 0
}
