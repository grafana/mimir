// SPDX-License-Identifier: AGPL-3.0-only

// Package readcacheassignment is the readcache-side analogue of
// pkg/nautilus/assignment. Where pkg/nautilus/assignment leases
// (HashRange -> PartitionID) — i.e. tells the producer which Kafka
// partition each series writes to — this package leases
// (PartitionID -> InstanceID) — i.e. tells the read path which
// readcache instance owns the working set for a given Kafka
// partition.
//
// Schema:
//
//	type LogEntry struct {
//	  PartitionID int32
//	  InstanceID  string
//	  From, To    time.Time
//	}
//
// Lease semantics mirror pkg/nautilus/assignment.Log:
//
//   - Leases are time-boxed and immutable except for preemption.
//   - The rebalancer pre-issues successors before expiry; consumers
//     see continuous coverage as long as the rebalancer keeps
//     publishing.
//   - Preemption clamps the active entry's To to `at` and appends a
//     fresh lease for the new owner.
//
// The departure from pkg/nautilus/assignment is that we allow more
// than one entry to be active for the same PartitionID at the same
// time — this is the "multi-owner" mode the plan calls out as a
// future toggle for ranged failover (a partition can be served by N
// readcache instances in parallel during a transition). For Phase 2
// the rebalancer defaults to one owner per partition; the data
// structure is permissive of multi-owner only insofar as Lookup
// returns the full list.
package readcacheassignment
