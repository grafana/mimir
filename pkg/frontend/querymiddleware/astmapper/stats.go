// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

type MapperStats struct {
	// The number of sharded selectors in the final expression.
	// For example, if the number of shards is 3, then:
	// - If the original query was sum(foo), this would be 3.
	// - If the original query was sum(foo) + bar, this would be 0, as the bar selector can't be sharded and so the sum is not sharded.
	// - If the original query was sum(foo) + sum(bar), this would be 3+3 = 6.
	// - If the original query was avg(foo), this would be 2×3 = 6, as avg(foo) is rewritten as sum(foo) / count(foo).
	shardedQueries int
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
