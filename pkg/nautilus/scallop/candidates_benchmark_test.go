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
	policy.ActionLimits = testActionLimits(4)

	b.ResetTimer()
	for range b.N {
		_, _ = generateCandidates(state, snapshot, policy)
	}
}

func BenchmarkLargeCellPlan(b *testing.B) {
	snapshot := largeCellBenchmarkSnapshot()
	policy := DefaultPolicy()
	policy.ActionLimits = testActionLimits(4)

	b.ResetTimer()
	for range b.N {
		if _, err := Plan(snapshot, policy); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProductionScaleCandidateGeneration(b *testing.B) {
	for _, rangesPerTenant := range []int{4, 64} {
		b.Run(fmt.Sprintf("%dk-ranges", 10*rangesPerTenant), func(b *testing.B) {
			snapshot := productionScaleBenchmarkSnapshot(rangesPerTenant)
			state := stateFromSnapshot(snapshot)
			policy := DefaultPolicy()
			policy.ActionLimits = testActionLimits(1)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, diagnostics := generateCandidates(state, snapshot, policy)
				assertProductionDiscoveryWork(b, diagnostics, policy)
				b.ReportMetric(float64(diagnostics.Work.RangesInspected), "ranges-inspected/op")
				b.ReportMetric(float64(diagnostics.Work.AdjacencyEdgesInspected), "adjacency-inspected/op")
				b.ReportMetric(float64(diagnostics.FullyScored.Total), "delta-candidates/op")
			}
		})
	}
}

func BenchmarkProductionScalePlan(b *testing.B) {
	for _, rangesPerTenant := range []int{4, 64} {
		b.Run(fmt.Sprintf("%dk-ranges", 10*rangesPerTenant), func(b *testing.B) {
			snapshot := productionScaleBenchmarkSnapshot(rangesPerTenant)
			policy := DefaultPolicy()
			policy.ActionLimits = ActionLimits{
				Total:          1,
				Move:           1,
				MovePerTenant:  1,
				Merge:          1,
				MergePerTenant: 1,
				MovePartition:  1,
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				result, err := Plan(snapshot, policy)
				if err != nil {
					b.Fatal(err)
				}
				assertProductionDiscoveryWork(b, result.CandidateSearch, policy)
				b.ReportMetric(float64(result.CandidateSearch.Work.RangesInspected), "ranges-inspected/op")
				b.ReportMetric(float64(result.CandidateSearch.Work.ExactDeltaScores), "exact-deltas/op")
				b.ReportMetric(float64(result.CandidateSearch.Work.CompleteProjections), "projections/op")
			}
		})
	}
}

func assertProductionDiscoveryWork(b *testing.B, diagnostics CandidateSearchDiagnostics, policy Policy) {
	b.Helper()
	limits := policy.CandidateSearch
	maxRanges := (limits.MaxPartitionOffenders + limits.MaxReplicaOffenders) * limits.MaxRangesPerOffender
	if diagnostics.Work.RangesInspected > maxRanges {
		b.Fatalf("inspected %d ranges, limit-derived maximum is %d", diagnostics.Work.RangesInspected, maxRanges)
	}
	maxAdjacency := limits.MaxTenantOffenders * limits.MaxAdjacencyPerTenant
	if diagnostics.Work.AdjacencyEdgesInspected > maxAdjacency {
		b.Fatalf("inspected %d adjacency edges, limit-derived maximum is %d", diagnostics.Work.AdjacencyEdgesInspected, maxAdjacency)
	}
	iterations := max(1, diagnostics.Iterations)
	if diagnostics.FullyScored.Total > iterations*limits.MaxFullyScored {
		b.Fatalf("fully scored %d candidates across %d iterations with a %d per-iteration maximum",
			diagnostics.FullyScored.Total, diagnostics.Iterations, limits.MaxFullyScored)
	}
	if diagnostics.Work.CompleteProjections > iterations {
		b.Fatalf("materialized %d complete projections across %d iterations",
			diagnostics.Work.CompleteProjections, diagnostics.Iterations)
	}
}

func productionScaleBenchmarkSnapshot(rangesPerTenant int) Snapshot {
	const (
		partitionCount = 1000
		replicaCount   = 300
		tenantCount    = 10000
	)
	partitions := make([]int32, partitionCount)
	owners := make(map[int32]string, partitionCount)
	replicas := make([]string, replicaCount)
	for i := range replicas {
		replicas[i] = fmt.Sprintf("readcache-%03d", i)
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
		tenantID := fmt.Sprintf("tenant-%05d", tenant)
		destinations := make([]int32, rangesPerTenant)
		for i := range destinations {
			destinations[i] = int32((tenant*17 + i*31) % partitionCount)
		}
		tenantAssignment := assignment.EvenSplitForTenant(tenantID, destinations)
		for i, entry := range tenantAssignment.Entries {
			entries = append(entries, entry)
			loads[RangeKey{TenantID: tenantID, Range: entry.Range}] =
				float64(1 + (tenant*13+i*7)%101)
		}
		locality[tenantID] = map[string]time.Time{
			owners[destinations[0]]: at,
		}
	}
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
