// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

type SkippedReason string

// Possible noop reasons
const (
	SkippedReasonSmallInterval = SkippedReason("small-interval")
	SkippedReasonSubquery      = SkippedReason("subquery")
	SkippedReasonNonSplittable = SkippedReason("non-splittable")
	noneNoOpReason             = SkippedReason("none")
)

type InstantSplitterStats struct {
	splitQueries    int           // counter of split queries (0 if non-splittable)
	noOpQueryReason SkippedReason // reason the initial query is a no operation
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
func (s *InstantSplitterStats) SetNoOpQueryReason(reason SkippedReason) {
	if s.isNoOpQueryReasonSet() {
		return
	}
	s.noOpQueryReason = reason
}

// GetNoOpQueryReason returns the reason a query is a no operation.
// If number of split queries is greater than 0, it means the query is splittable
// Otherwise, if no noop reason is set, it means the query is non-splittable
func (s *InstantSplitterStats) GetNoOpQueryReason() SkippedReason {
	if s.GetSplitQueries() > 0 {
		return noneNoOpReason
	}
	if s.isNoOpQueryReasonSet() {
		return s.noOpQueryReason
	}
	return SkippedReasonNonSplittable
}

func (s *InstantSplitterStats) isNoOpQueryReasonSet() bool {
	return len(s.noOpQueryReason) > 0
}
