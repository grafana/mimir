// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

type MapperStats struct {
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
