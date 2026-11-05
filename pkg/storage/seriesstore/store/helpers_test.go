// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"math"

	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

func init() {
	countersEnabled = true
}

func matcher(kind int32, name, value string) LabelMatcher {
	return LabelMatcher{Type: kind, Name: name, Value: value}
}

// pruneBefore is the engine's retention by time only, of every chunk older than cutoff but the out-of-order head's.
func (e *Engine) pruneBefore(cutoff int64) error {
	return e.prune(&retained{cutoff: cutoff}, math.MinInt64)
}

// push adds a chunk after the others.
func (l *chunkList) push(meta ChunkMeta) {
	reference, minTime := l.last()
	l.pushAfter(meta, reference, minTime)
}

func (b *coldBlock) seriesCount(tenant string) int {
	if index, ok := b.tenants[tenant]; ok {
		return index.seriesCount
	}
	return 0
}

// candidatesCached is candidates, kept by the selector's id for the next time: selector is the matchers the id is of.
func (b *coldBlock) candidatesCached(tenant string, id uint64, selector, matchers []compiledMatcher) []uint32 {
	table := b.tenant(tenant)
	if table == nil {
		return nil
	}
	list, hit := b.candidatesCachedIn(table, tenant, &coldSelector{id: id, matchers: selector}, matchers)
	if hit {
		countColdCandidates(table, 1, 0)
	} else {
		countColdCandidates(table, 0, 1)
	}
	return list
}

// candidatesCachedIn is candidatesCached of the tenant's table of the block, and whether they were kept.
func (b *coldBlock) candidatesCachedIn(table *coldTenantIndex, tenant string, selector *coldSelector, matchers []compiledMatcher) ([]uint32, bool) {
	list, hit, _, _ := b.lookupCandidatesIn(table, tenant, selector, matchers)
	return list, hit
}

func (r *refIndex) len() int {
	return r.used
}

func labelsFromStrings(pairs ...string) labels.Labels {
	sorted := make([][2]string, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		sorted = append(sorted, [2]string{pairs[i], pairs[i+1]})
	}
	return labels.FromSorted(sorted)
}

func comparePairs(a, b [2]string) int {
	if a[0] != b[0] {
		if a[0] < b[0] {
			return -1
		}
		return 1
	}
	if a[1] != b[1] {
		if a[1] < b[1] {
			return -1
		}
		return 1
	}
	return 0
}

// pruneBefore removes the blocks all older than cutoff.
func (c *coldState) pruneBefore(cutoff int64) {
	c.prune(&retained{cutoff: cutoff}, 0)
}

// retain keeps the series keep accepts, and returns the name groups that lost series. A removal costs what the removed
// series and their groups do, not what the whole shard does: the postings keep the ids of the removed series, which no
// entry answers to any more, so they only add candidates that the lookups skip, until rebuildable says to rebuild them.
func (b *seriesByName) retain(keep func(entry *seriesEntry) bool) (changed []uint32) {
	return b.retainGroups(nil, keep)
}

// perShardWithCold runs query on the tenant's series and cold blocks in every shard, one after the
// other on the calling goroutine: like Go's head, a query runs on one thread and concurrent
// queries use the cores. Spreading each small query over the store's threads spent more CPU
// finding work than running it.
func (s *Store) perShardWithCold(tenantID string, query func(t *tenant, disk *chunks.DiskMapper, cold *coldState)) {
	s.perShardIndexed(tenantID, func(_ int, t *tenant, disk *chunks.DiskMapper, cold *coldState) { query(t, disk, cold) })
}

// perShardIndexed is perShardWithCold with the index of the shard.
func (s *Store) perShardIndexed(tenantID string, query func(index int, t *tenant, disk *chunks.DiskMapper, cold *coldState)) {
	timer := startReadTimer(phaseSelect)
	for index, shard := range s.shards {
		shard.RLock()
		at := timer.held()
		if t := shard.tenant.Load(); t != nil {
			query(index, t, shard.disk, shard.cold)
		}
		timer.release(at)
		shard.RUnlock()
	}
}
