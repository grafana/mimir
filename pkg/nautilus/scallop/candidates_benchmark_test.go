// SPDX-License-Identifier: AGPL-3.0-only

package scallop

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

func BenchmarkLargeCellCandidateGeneration(b *testing.B) {
	snapshot := largeCellBenchmarkSnapshot()
	state := stateFromSnapshot(snapshot)
	policy := DefaultPolicy()
	policy.MaxActions = 4

	b.ResetTimer()
	for range b.N {
		_, _ = generateCandidates(state, snapshot, policy)
	}
}

func BenchmarkLargeCellPlan(b *testing.B) {
	snapshot := largeCellBenchmarkSnapshot()
	policy := DefaultPolicy()
	policy.MaxActions = 4

	b.ResetTimer()
	for range b.N {
		if _, err := Plan(snapshot, policy); err != nil {
			b.Fatal(err)
		}
	}
}

func largeCellBenchmarkSnapshot() Snapshot {
	const (
		partitionCount  = 500
		replicaCount    = 100
		tenantCount     = 50
		rangesPerTenant = 5
	)

	partitions := make([]int32, partitionCount)
	owners := make(map[int32]string, partitionCount)
	replicas := make([]string, replicaCount)
	for i := range replicas {
		replicas[i] = fmt.Sprintf("readcache-%d", i)
	}
	for i := range partitions {
		partitions[i] = int32(i)
		owners[int32(i)] = replicas[i%replicaCount]
	}

	entries := make([]assignment.Entry, 0, tenantCount*rangesPerTenant)
	loads := make(map[RangeKey]float64, tenantCount*rangesPerTenant)
	locality := make(map[string]map[string]time.Time, tenantCount)
	at := time.Unix(100, 0)
	for tenant := 0; tenant < tenantCount; tenant++ {
		tenantID := fmt.Sprintf("tenant-%02d", tenant)
		destinations := make([]int32, rangesPerTenant)
		for i := range destinations {
			destinations[i] = int32(tenant + i*replicaCount)
		}
		tenantAssignment := assignment.EvenSplitForTenant(tenantID, destinations)
		for i, entry := range tenantAssignment.Entries {
			entries = append(entries, entry)
			loads[RangeKey{TenantID: tenantID, Range: entry.Range}] = float64((tenant + 1) * (i + 1))
		}
		locality[tenantID] = map[string]time.Time{replicas[tenant]: at}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].TenantID != entries[j].TenantID {
			return entries[i].TenantID < entries[j].TenantID
		}
		return entries[i].Range.Lo < entries[j].Range.Lo
	})

	return Snapshot{
		At:               at,
		Assignment:       &assignment.Assignment{Entries: entries},
		RangeLoads:       loads,
		ActivePartitions: partitions,
		PartitionOwners:  owners,
		ActiveReplicas:   replicas,
		LastHostedAt:     locality,
	}
}
