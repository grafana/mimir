// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

// ReadcacheQueryStreamCall describes a single readcache QueryStream RPC
// the distributor would issue for one Select: exactly one per
// (partition, owner) pair returned by the readcache assignment log.
type ReadcacheQueryStreamCall struct {
	PartitionID int32
	// Owner is the readcache instance address that would be dialed.
	Owner string
	// InstanceID is the synthetic "owner/p<partition>" routing key used
	// internally to keep each (owner, partition) replication set unique.
	InstanceID string
	// LeaseFrom/LeaseTo bound the readcache ownership lease(s) that put
	// this owner into the fan-out for the query window. When an owner
	// holds several leases in the window these span the union.
	LeaseFrom time.Time
	LeaseTo   time.Time
}

// ReadcachePartitionPlan groups the QueryStream calls for a single
// partition. More than one call means the partition changed readcache
// owners during the query window and each owner holds only the slice it
// ingested while it owned the partition.
type ReadcachePartitionPlan struct {
	PartitionID int32
	Calls       []ReadcacheQueryStreamCall
}

// ReadcacheQueryPlan is the resolved readcache fan-out for one Select
// (a matcher set over a sample-time range). It mirrors exactly what
// getReadcacheReplicationSetsForQuery would produce at query time, but
// without dialing any readcache, so an operator can preview the
// per-partition/per-owner QueryStream lookups a query will make.
type ReadcacheQueryPlan struct {
	// RoutingEnabled reports whether the tenant is actually routed to
	// readcache. When false the querier would use the ingester path and
	// issue none of these calls; the plan is still resolved so an
	// operator can see what the fan-out would be.
	RoutingEnabled bool
	// Unavailable, when non-empty, is the reason the assignment logs
	// could not resolve the query. In production this is the same
	// condition that hard-fails a readcache-routed query (there is no
	// ingester fallback for nautilus-only tenants).
	Unavailable string

	// MetricName / Named describe whether the query is scoped to a
	// single metric name (an exact __name__ matcher), which narrows the
	// fan-out to the partitions overlapping that name's hash range.
	MetricName string
	Named      bool
	HashLo     uint32
	HashHi     uint32

	// From/To is the query's sample-time range; W0/W1 is the padded
	// wall-clock window used to resolve ownership.
	From model.Time
	To   model.Time
	W0   time.Time
	W1   time.Time

	Partitions []ReadcachePartitionPlan
	// TotalCalls is the number of QueryStream RPCs this Select fans out
	// to (Σ owners over partitions).
	TotalCalls int
}

// ExplainReadcacheQuery resolves, without executing anything, the
// readcache QueryStream fan-out the distributor would produce for a
// single Select: a matcher set over the sample-time range [from,to].
// It is the read-path analogue of the write path's plan and is intended
// for the querier's readcache-lookup debug page. It never dials a
// readcache; it only consults the same nautilus and readcache
// assignment-log snapshots that getReadcacheReplicationSetsForQuery
// uses, so the enumerated calls match production routing exactly.
func (d *Distributor) ExplainReadcacheQuery(_ context.Context, userID string, from, to model.Time, matchers []*labels.Matcher) ReadcacheQueryPlan {
	plan := ReadcacheQueryPlan{
		From:           from,
		To:             to,
		RoutingEnabled: d.limits.ReadcacheReadRouting(userID) == validation.ReadcacheReadRoutingNautilus,
	}

	log := d.GetNautilusLog()
	if log == nil {
		plan.Unavailable = "no live nautilus assignment log snapshot is available"
		return plan
	}
	rcLog := d.GetReadcacheLog()
	if rcLog == nil {
		plan.Unavailable = "no live readcache assignment log snapshot is available"
		return plan
	}

	w0, w1 := d.readcacheQueryWindow(userID, from, to)
	plan.W0, plan.W1 = w0, w1

	metricName, named := extractExactMetricName(matchers)
	plan.MetricName, plan.Named = metricName, named

	var partitionIDs []int32
	if named {
		lo, hi := mimirpb.MetricNameHashRange(userID, metricName)
		plan.HashLo, plan.HashHi = lo, hi
		partitionIDs = log.PartitionsOverlappingInterval(w0, w1, lo, hi)
	} else {
		partitionIDs = log.AllPartitionsDuring(w0, w1)
	}
	if len(partitionIDs) == 0 {
		plan.Unavailable = "assignment log resolved no partitions for the query"
		return plan
	}

	plan.Partitions = make([]ReadcachePartitionPlan, 0, len(partitionIDs))
	for _, partID := range partitionIDs {
		// Owners drives the actual fan-out (one QueryStream per owner),
		// matching getReadcacheReplicationSetsForQuery. Lease bounds are
		// looked up separately, purely for display.
		owners := rcLog.OwnersDuring(partID, w0, w1)
		if len(owners) == 0 {
			plan.Unavailable = fmt.Sprintf("partition %d had no readcache owner during the query window", partID)
			plan.Partitions = nil
			plan.TotalCalls = 0
			return plan
		}

		leaseSpanByOwner := make(map[string][2]time.Time, len(owners))
		for _, e := range rcLog.EntriesDuring(partID, w0, w1) {
			span, ok := leaseSpanByOwner[e.InstanceID]
			if !ok {
				leaseSpanByOwner[e.InstanceID] = [2]time.Time{e.From, e.To}
				continue
			}
			if e.From.Before(span[0]) {
				span[0] = e.From
			}
			if e.To.After(span[1]) {
				span[1] = e.To
			}
			leaseSpanByOwner[e.InstanceID] = span
		}

		pp := ReadcachePartitionPlan{PartitionID: partID, Calls: make([]ReadcacheQueryStreamCall, 0, len(owners))}
		for _, owner := range owners {
			call := ReadcacheQueryStreamCall{
				PartitionID: partID,
				Owner:       owner,
				InstanceID:  fmt.Sprintf("%s/p%d", owner, partID),
			}
			if span, ok := leaseSpanByOwner[owner]; ok {
				call.LeaseFrom, call.LeaseTo = span[0], span[1]
			}
			pp.Calls = append(pp.Calls, call)
		}
		plan.TotalCalls += len(pp.Calls)
		plan.Partitions = append(plan.Partitions, pp)
	}

	return plan
}
