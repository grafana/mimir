// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"fmt"
	"math"
	"runtime"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

func BenchmarkTenantScale10K_AssignmentSnapshot(b *testing.B) {
	entries, _ := tenantScale10KFixture()
	update := assignmentUpdate{
		entries:    entries,
		reset:      true,
		generation: 42,
		validUntil: time.Date(2026, 9, 18, 12, 5, 0, 0, time.UTC),
	}

	b.Run("construction", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			response := assignmentUpdateToProto(update)
			runtime.KeepAlive(response)
		}
	})

	response := assignmentUpdateToProto(update)
	b.Run("serialization", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			data, err := proto.Marshal(response)
			if err != nil {
				b.Fatal(err)
			}
			runtime.KeepAlive(data)
		}
	})
}

func BenchmarkTenantScale10K_RepresentativeRebalance(b *testing.B) {
	entries, initial := tenantScale10KFixture()
	moved := &assignment.Assignment{Entries: append([]assignment.Entry(nil), initial.Entries...)}
	moved.Entries[len(moved.Entries)/2].PartitionID++
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	baseline := assignment.LogState{Entries: entries, Generation: 1, ValidUntil: now.Add(5 * time.Minute)}

	b.ReportAllocs()
	for range b.N {
		b.StopTimer()
		store := newLogStore()
		store.seedFromState(baseline)
		b.StartTimer()
		if !store.apply(now.Add(time.Minute), moved, 5*time.Minute, time.Minute, time.Hour) {
			b.Fatal("representative tenant move did not change the assignment")
		}
	}
}

func BenchmarkTenantScale10K_RepresentativeSlicer(b *testing.B) {
	_, current := tenantScale10KFixture()
	partitions := make([]int32, 300)
	for i := range partitions {
		partitions[i] = int32(i)
	}
	r := &Rebalancer{}
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)

	b.ReportAllocs()
	for range b.N {
		next, actions := r.runSlicer(current, nil, nil, partitions, nil, now)
		if len(next.Entries) != len(current.Entries) || len(actions) != 0 {
			b.Fatalf("unexpected no-load slicer result: entries=%d actions=%d", len(next.Entries), len(actions))
		}
		runtime.KeepAlive(next)
	}
}

func tenantScale10KFixture() ([]assignment.LogEntry, *assignment.Assignment) {
	const tenantCount = 10_000
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	logEntries := make([]assignment.LogEntry, tenantCount)
	assignmentEntries := make([]assignment.Entry, tenantCount)
	for i := range tenantCount {
		tenantID := fmt.Sprintf("tenant-%05d", i)
		partitionID := int32(i % 300)
		hashRange := assignment.HashRange{Lo: 0, Hi: math.MaxUint32}
		logEntries[i] = assignment.LogEntry{
			TenantID:    tenantID,
			Range:       hashRange,
			PartitionID: partitionID,
			From:        now,
		}
		assignmentEntries[i] = assignment.Entry{
			TenantID:    tenantID,
			Range:       hashRange,
			PartitionID: partitionID,
		}
	}
	return logEntries, &assignment.Assignment{Entries: assignmentEntries}
}
