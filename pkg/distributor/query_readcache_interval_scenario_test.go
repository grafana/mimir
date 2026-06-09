// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"sort"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
)

// TestReadcacheIntervalRouting_ELI5Scenario is the executable spec for
// interval-aware readcache routing. It encodes the exact worked example
// we designed:
//
//	Tenant "acme": ooo_window = 1h, future_grace = 15m.
//	Metric "temperature" has a fixed hash range H.
//
//	Log 1 (range -> partition):
//	  H -> P7  during wall-clock [08:00, 12:00)
//	  H -> P3  during wall-clock [12:00, now)      // range moved at noon
//
//	Log 2 (partition -> readcache instance):
//	  P7 -> R1 during [06:00, 11:00)
//	  P7 -> R2 during [11:00, now)                 // P7 moved pods at 11:00
//	  P3 -> R2 during [12:00, now)
//
//	Query: metric "temperature" over sample-time [10:30, 12:30].
//
// Both layers are resolved against ONE wall-clock window derived from
// the query's sample range by padding it with the tenant's grace knobs:
//
//	w0 = t0 - future_grace = 10:30 - 15m = 10:15
//	w1 = t1 + ooo_window   = 12:30 +  1h = 13:30
//
// The SAME [w0, w1) is intersected with both logs:
//   - Log 1 -> partitions {P3, P7}
//   - Log 2 -> P7 on {R1, R2}, P3 on {R2}
//
// so the query fans out to distinct (instance, partition) pairs
// {(R1,P7), (R2,P7), (R2,P3)}, i.e. one call to R1 (for P7) and one to
// R2 (for P7 and P3, which R2 self-selects by per-epoch minT/maxT).
func TestReadcacheIntervalRouting_ELI5Scenario(t *testing.T) {
	const (
		userID     = "acme"
		metricName = "temperature"

		oooWindow   = time.Hour
		futureGrace = 15 * time.Minute
		// query-ingesters-within: large enough not to clamp this
		// scenario; included to exercise the clamp arithmetic.
		queryIngestersWithin = 13 * time.Hour

		p7 = int32(7)
		p3 = int32(3)
	)

	// Fixed clock so the window math is exact and reproducible.
	day := func(hh, mm int) time.Time {
		return time.Date(2026, time.June, 9, hh, mm, 0, 0, time.UTC)
	}
	now := day(14, 0)

	// The metric's hash range H. Both range->partition leases below
	// carry a Range that covers H (here exactly H), so only the TIME
	// dimension distinguishes them: at any instant a single partition
	// owns H, but across the window both P7 and P3 did.
	lo, hi := mimirpb.MetricNameHashRange(userID, metricName)
	H := assignment.HashRange{Lo: lo, Hi: hi}

	// Log 1: range -> partition.
	baseLog := assignment.NewLogFromEntries([]assignment.LogEntry{
		{Range: H, PartitionID: p7, From: day(8, 0), To: day(12, 0)},
		{Range: H, PartitionID: p3, From: day(12, 0), To: day(18, 0)},
	})

	// Log 2: partition -> readcache instance. Two epochs of P7 (R1 then
	// R2) plus P3 on R2. The R1->R2 handoff for P7 happens at 11:00.
	rcLog := readcacheassignment.NewLogFromEntries([]readcacheassignment.LogEntry{
		{PartitionID: p7, InstanceID: "R1", From: day(6, 0), To: day(11, 0)},
		{PartitionID: p7, InstanceID: "R2", From: day(11, 0), To: day(18, 0)},
		{PartitionID: p3, InstanceID: "R2", From: day(12, 0), To: day(18, 0)},
	})

	// The query's sample-time range.
	from := model.TimeFromUnixNano(day(10, 30).UnixNano())
	to := model.TimeFromUnixNano(day(12, 30).UnixNano())

	// Step 0: pad the sample range into a single wall-clock window,
	// then clamp the lower bound to the readcache-serving horizon.
	w0 := from.Time().Add(-futureGrace)
	w1 := to.Time().Add(oooWindow)
	if floor := now.Add(-queryIngestersWithin); w0.Before(floor) {
		w0 = floor
	}
	// Compare instants (model.Time.Time() returns the local zone, so a
	// field-wise Equal would spuriously fail across zones).
	require.True(t, w0.Equal(day(10, 15)), "w0 = t0 - future_grace, got %s", w0)
	require.True(t, w1.Equal(day(13, 30)), "w1 = t1 + ooo_window, got %s", w1)

	// Step 1: range -> partition, intersect the window with Log 1.
	gotPartitions := baseLog.PartitionsOverlappingInterval(w0, w1, lo, hi)
	assert.Equal(t, []int32{p3, p7}, gotPartitions,
		"query spanning the noon range move must fan out to both partitions")

	// Step 2 + 3: partition -> instance using the SAME window, then
	// combine into the distinct (instance, partition) fan-out set.
	type pair struct {
		instance  string
		partition int32
	}
	var gotPairs []pair
	instanceSet := map[string]struct{}{}
	for _, p := range gotPartitions {
		owners := rcLog.OwnersDuring(p, w0, w1)
		require.NotEmpty(t, owners, "partition %d resolved no readcache owner in window", p)
		for _, inst := range owners {
			gotPairs = append(gotPairs, pair{instance: inst, partition: p})
			instanceSet[inst] = struct{}{}
		}
	}
	sort.Slice(gotPairs, func(i, j int) bool {
		if gotPairs[i].instance != gotPairs[j].instance {
			return gotPairs[i].instance < gotPairs[j].instance
		}
		return gotPairs[i].partition < gotPairs[j].partition
	})

	wantPairs := []pair{
		{instance: "R1", partition: p7},
		{instance: "R2", partition: p3},
		{instance: "R2", partition: p7},
	}
	assert.Equal(t, wantPairs, gotPairs,
		"fan-out must be exactly (R1,P7), (R2,P7), (R2,P3)")

	// And the distinct readcache instances actually dialed: R1 and R2.
	gotInstances := make([]string, 0, len(instanceSet))
	for inst := range instanceSet {
		gotInstances = append(gotInstances, inst)
	}
	sort.Strings(gotInstances)
	assert.Equal(t, []string{"R1", "R2"}, gotInstances)
}

// TestReadcacheIntervalRouting_NarrowQueryHitsCurrentOwnerOnly is the
// contrast case: a query whose padded window lands entirely after both
// moves resolves to just the current partition (P3) and its current
// owner (R2) - i.e. interval routing degenerates to point-in-time
// routing when nothing relevant moved inside the window.
func TestReadcacheIntervalRouting_NarrowQueryHitsCurrentOwnerOnly(t *testing.T) {
	const (
		userID      = "acme"
		metricName  = "temperature"
		oooWindow   = time.Hour
		futureGrace = 15 * time.Minute
		p7          = int32(7)
		p3          = int32(3)
	)
	day := func(hh, mm int) time.Time {
		return time.Date(2026, time.June, 9, hh, mm, 0, 0, time.UTC)
	}

	lo, hi := mimirpb.MetricNameHashRange(userID, metricName)
	H := assignment.HashRange{Lo: lo, Hi: hi}
	baseLog := assignment.NewLogFromEntries([]assignment.LogEntry{
		{Range: H, PartitionID: p7, From: day(8, 0), To: day(12, 0)},
		{Range: H, PartitionID: p3, From: day(12, 0), To: day(18, 0)},
	})
	rcLog := readcacheassignment.NewLogFromEntries([]readcacheassignment.LogEntry{
		{PartitionID: p7, InstanceID: "R1", From: day(6, 0), To: day(11, 0)},
		{PartitionID: p7, InstanceID: "R2", From: day(11, 0), To: day(18, 0)},
		{PartitionID: p3, InstanceID: "R2", From: day(12, 0), To: day(18, 0)},
	})

	// Sample range [16:00, 16:30] -> window [15:45, 17:30], entirely
	// after the noon range move and the 11:00 pod move.
	from := model.TimeFromUnixNano(day(16, 0).UnixNano())
	to := model.TimeFromUnixNano(day(16, 30).UnixNano())
	w0 := from.Time().Add(-futureGrace)
	w1 := to.Time().Add(oooWindow)

	gotPartitions := baseLog.PartitionsOverlappingInterval(w0, w1, lo, hi)
	require.Equal(t, []int32{p3}, gotPartitions)
	assert.Equal(t, []string{"R2"}, rcLog.OwnersDuring(gotPartitions[0], w0, w1))
}
