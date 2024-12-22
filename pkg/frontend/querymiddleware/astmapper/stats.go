// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

type MapperStats struct {
	shardedQueries    int
	spunOffSubqueries int
}

func NewMapperStats() *MapperStats {
	return &MapperStats{}
}

// AddShardedQueries add num sharded queries to the counter.
func (s *MapperStats) AddShardedQueries(num int) {
	s.shardedQueries += num
}

// GetShardedQueries returns the number of sharded queries.
func (s *MapperStats) GetShardedQueries() int {
	return s.shardedQueries
}

// AddSpunOffSubqueries add num spun off subqueries to the counter.
func (s *MapperStats) AddSpunOffSubquery() {
	s.spunOffSubqueries += 1
}

// GetSpunOffSubqueries returns the number of spun off subqueries.
func (s *MapperStats) GetSpunOffSubqueries() int {
	return s.spunOffSubqueries
}
