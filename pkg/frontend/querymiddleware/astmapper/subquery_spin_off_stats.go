// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

type SubquerySpinOffMapperStats struct {
	spunOffSubqueries int // counter of subqueries extracted
	downstreamQueries int // counter of downstream queries extracted
}

func NewSubquerySpinOffMapperStats() *SubquerySpinOffMapperStats {
	return &SubquerySpinOffMapperStats{}
}

func (s *SubquerySpinOffMapperStats) AddSpunOffSubquery() {
	s.spunOffSubqueries++
}

func (s *SubquerySpinOffMapperStats) AddDownstreamQuery() {
	s.downstreamQueries++
}

func (s *SubquerySpinOffMapperStats) SpunOffSubqueries() int {
	return s.spunOffSubqueries
}

func (s *SubquerySpinOffMapperStats) DownstreamQueries() int {
	return s.downstreamQueries
}
