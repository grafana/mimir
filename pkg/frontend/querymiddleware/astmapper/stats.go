// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

type MapperStats struct {
	// The number of sharded selectors in the final expression.
	// For example, if the number of shards is 3, then:
	// - If the original query was sum(foo), this would be 3.
	// - If the original query was sum(foo) + sum(bar), this would be 3+3 = 6.
	// - If the original query was avg(foo), this would be 2×3 = 6, as avg(foo) is rewritten as sum(foo) / count(foo).
	shardedQueries int

	// The number of expressions in the original query that can be sharded.
	// This does not consider the number of shards or any additional selectors that are added due to rewriting (eg. for avg()).
	// For example:
	// - For sum(foo), this would be 1.
	// - For sum(foo) + sum(bar), this would be 2.
	// - For avg(foo), this would be 1.
	shardableExpressions int
}

func NewMapperStats() *MapperStats {
	return &MapperStats{}
}

// AddShardedQueries add num sharded queries to the counter.
func (s *MapperStats) AddShardedQueries(num int) {
	s.shardedQueries += num
}

// AddShardableExpressions add num shardable expressions to the counter.
func (s *MapperStats) AddShardableExpressions(num int) {
	s.shardableExpressions += num
}

// GetShardedQueries returns the number of sharded queries.
func (s *MapperStats) GetShardedQueries() int {
	return s.shardedQueries
}

// GetShardableExpressions returns the number of shardable expressions.
func (s *MapperStats) GetShardableExpressions() int {
	return s.shardableExpressions
}
