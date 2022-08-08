// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

type SkippedReason string

// Possible skipped reasons
const (
	SkippedReasonSmallInterval = SkippedReason("small-interval")
	SkippedReasonSubquery      = SkippedReason("subquery")
	SkippedReasonNonSplittable = SkippedReason("non-splittable")
	noneSkippedReason          = SkippedReason("")
)

type InstantSplitterStats struct {
	splitQueries  int           // counter of split queries (0 if non-splittable)
	skippedReason SkippedReason // reason the initial query is a no operation
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

// SetSkippedReason set no operation reason for query.
func (s *InstantSplitterStats) SetSkippedReason(reason SkippedReason) {
	if s.isSkippedReasonSet() {
		return
	}
	s.skippedReason = reason
}

// GetSkippedReason returns the reason a query is a no operation.
// If number of split queries is greater than 0, it means the query is splittable
// Otherwise, if no skipped reason is set, it means the query is non-splittable
func (s *InstantSplitterStats) GetSkippedReason() SkippedReason {
	if s.GetSplitQueries() > 0 {
		return noneSkippedReason
	}
	if s.isSkippedReasonSet() {
		return s.skippedReason
	}
	return SkippedReasonNonSplittable
}

func (s *InstantSplitterStats) isSkippedReasonSet() bool {
	return len(s.skippedReason) > 0
}
