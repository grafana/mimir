// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

import "go.uber.org/atomic"

type MapperStats struct {
	shardedQueries atomic.Int32
}

func NewMapperStats() *MapperStats {
	return &MapperStats{}
}

// AddShardedQueries add num sharded queries to the counter.
func (s *MapperStats) AddShardedQueries(num int) {
	s.shardedQueries.Add(int32(num))
}

// GetShardedQueries returns the number of sharded queries.
func (s *MapperStats) GetShardedQueries() int {
	return int(s.shardedQueries.Load())
}
