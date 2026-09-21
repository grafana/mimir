// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/loadstats"
	util_math "github.com/grafana/mimir/pkg/util/math"
)

// partitionRanges is the per-Kafka-partition hash-range bookkeeping
// used to feed the rebalancer's load signal.
//
// Two range sets are tracked per partition:
//
//   - currentRanges: ranges that the rebalancer last told us this
//     partition owns (via SetHashRanges). Distributors are routing
//     hashes in these ranges to this partition right now.
//
//   - historicalRanges: hash space that USED to be in currentRanges
//     and may still have TSDB head residue from before the move.
//     Disjoint from currentRanges. Each round of head compaction
//     drops some residue; the walker re-tallies on its next tick and
//     drops historical ranges whose count has fallen to zero.
//
// rangeCounts is the walker's view of head series in this partition,
// keyed by ranges drawn from currentRanges ∪ historicalRanges. The
// invariant is Σ rangeCounts == NumSeries(head) over the
// (current ∪ historical) cover, modulo races between the walker and
// concurrent appends/compactions.
//
// This per-partition shape is what makes residue "stay" on the right
// partition after a range moves: residue series in partition P's head
// are bucketed into P's rangeCounts under a P-owned historicalRange,
// and the HashRangeStats RPC emits them as (P, range, count) so the
// rebalancer attributes the residue to P rather than to whichever
// partition is now the current owner of that range.
type partitionRanges struct {
	mu sync.RWMutex

	// tenantRangeState is the legacy empty-tenant bucket. Embedding it
	// keeps empty TenantID a first-class key while preserving the
	// existing package-local helpers used by legacy tests.
	tenantRangeState

	// byTenant isolates ownership, residue, counts, examples, and
	// sample-rate EWMAs for non-empty tenant IDs. Identical numeric
	// ranges owned by different tenants therefore never share state.
	byTenant map[string]*tenantRangeState

	// unknownFirstSeen records the earliest wall clock at which a live
	// partition-0 tenant was observed without configured current
	// ranges. Entries are removed when the tenant becomes configured or
	// no longer has a live TSDB.
	unknownFirstSeen map[string]int64
}

type tenantRangeState struct {
	currentRanges    []assignment.HashRange
	historicalRanges []assignment.HashRange
	rangeCounts      map[assignment.HashRange]int64
	// exampleSeries holds one representative series (rendered via
	// labels.Labels.String()) per range in the working set. Updated
	// from applyWalkResult, scoped to a single Kafka partition's
	// TSDB heads, and only used by the readcache admin page —
	// operators looking at /readcache want to see "what series
	// actually live here?" next to each hash range so they can
	// sanity-check that distributor routing matches what the
	// rebalancer thinks.
	//
	// Walks that pass nil examples leave this map untouched; that
	// keeps cost off the rebalancer's stats RPC path entirely.
	exampleSeries map[assignment.HashRange]string

	// sampleRates is an EWMA of samples-per-second per range. The
	// ingest hot path (recordSampleBatch) buckets each timeseries
	// into the matching current range and bumps that range's
	// EwmaRate; tickSampleRates advances every EwmaRate once per
	// loadstats.TickInterval (15s) so the rate field tracks recent
	// throughput rather than cumulative work.
	//
	// Entries are created for every range that lands in
	// currentRanges (setRanges and applyWalkResult both materialise
	// missing entries) so the hot path does not need to allocate
	// under contention. Entries are GC'd alongside rangeCounts /
	// exampleSeries when a range falls out of currentRanges ∪
	// historicalRanges. Historical ranges keep their EwmaRate so the
	// natural decay continues to be visible to the rebalancer while
	// residue series sit in the head.
	sampleRates map[assignment.HashRange]*util_math.EwmaRate
}

func newPartitionRanges() *partitionRanges {
	return &partitionRanges{
		tenantRangeState: newTenantRangeState(),
		byTenant:         make(map[string]*tenantRangeState),
		unknownFirstSeen: make(map[string]int64),
	}
}

func newTenantRangeState() tenantRangeState {
	return tenantRangeState{
		rangeCounts:   make(map[assignment.HashRange]int64),
		exampleSeries: make(map[assignment.HashRange]string),
		sampleRates:   make(map[assignment.HashRange]*util_math.EwmaRate),
	}
}

// newPartitionRangeEWMA constructs an EwmaRate using the same alpha
// and tick interval as the query-load tracker. Keeping the two
// signals on the same smoothing constants means an operator reading
// both off the admin page can compare them directly without
// mentally rescaling.
func newPartitionRangeEWMA() *util_math.EwmaRate {
	return util_math.NewEWMARate(loadstats.Alpha, loadstats.TickInterval)
}

// setRanges applies a new set of currently-owned ranges. Hash space
// that drops out of currentRanges moves to historicalRanges so the
// walker keeps counting residue there until compaction clears the
// head. Hash space that enters currentRanges is dropped from
// historicalRanges (it's "ours" again, accounted for by the current
// side).
//
// Range boundaries from the rebalancer are preserved: if a single
// range splits across rounds, both halves appear separately so the
// rebalancer can score each independently. Counts for ranges still in
// the working set (current ∪ historical) are retained; counts for
// ranges that fell out of both sets are discarded. It returns true
// when the current range assignment materially changed.
func (pr *partitionRanges) setRanges(newRanges []assignment.HashRange) bool {
	return pr.setRangesForTenant("", newRanges)
}

func (pr *partitionRanges) setRangesForTenant(tenantID string, newRanges []assignment.HashRange) bool {
	sortedNew := sortedNonOverlappingCopy(newRanges)
	tenantID = strings.Clone(tenantID)

	pr.mu.Lock()
	defer pr.mu.Unlock()

	state := pr.stateForTenantLocked(tenantID)
	if state == nil {
		if len(sortedNew) == 0 {
			return false
		}
		created := newTenantRangeState()
		pr.byTenant[tenantID] = &created
		state = &created
	}

	changed := setTenantRangesLocked(state, sortedNew)
	if len(state.currentRanges) > 0 {
		delete(pr.unknownFirstSeen, tenantID)
	}
	pr.deleteTenantStateIfEmptyLocked(tenantID, state)
	return changed
}

func setTenantRangesLocked(state *tenantRangeState, sortedNew []assignment.HashRange) bool {
	changed := !slices.Equal(state.currentRanges, sortedNew)

	// Hash space that this partition just lost.
	removed := rangeDiff(state.currentRanges, sortedNew)
	// Existing historical ranges that the new current re-claims must
	// drop out of historical (they will be tracked on the current side
	// from now on).
	keptHistorical := rangeDiff(state.historicalRanges, sortedNew)
	// New historical = (kept historical) ∪ (just-removed current).
	state.historicalRanges = rangeUnionPreservingBoundaries(keptHistorical, removed)

	state.currentRanges = sortedNew

	// Pre-create EwmaRate entries for every current range so the
	// ingest hot path (recordSampleBatch) never has to allocate or
	// take the write lock to install a new rate. Historical ranges
	// keep their existing entries; ranges that leave both sets are
	// GC'd below.
	for _, r := range state.currentRanges {
		if _, ok := state.sampleRates[r]; !ok {
			state.sampleRates[r] = newPartitionRangeEWMA()
		}
	}

	// Drop counts for ranges that are now in neither set.
	if len(state.rangeCounts) > 0 || len(state.exampleSeries) > 0 || len(state.sampleRates) > 0 {
		live := make(map[assignment.HashRange]struct{}, len(state.currentRanges)+len(state.historicalRanges))
		for _, r := range state.currentRanges {
			live[r] = struct{}{}
		}
		for _, r := range state.historicalRanges {
			live[r] = struct{}{}
		}
		for r := range state.rangeCounts {
			if _, ok := live[r]; !ok {
				delete(state.rangeCounts, r)
			}
		}
		for r := range state.exampleSeries {
			if _, ok := live[r]; !ok {
				delete(state.exampleSeries, r)
			}
		}
		for r := range state.sampleRates {
			if _, ok := live[r]; !ok {
				delete(state.sampleRates, r)
			}
		}
	}
	return changed
}

func (pr *partitionRanges) stateForTenantLocked(tenantID string) *tenantRangeState {
	if tenantID == "" {
		return &pr.tenantRangeState
	}
	return pr.byTenant[tenantID]
}

func (pr *partitionRanges) deleteTenantStateIfEmptyLocked(tenantID string, state *tenantRangeState) {
	if tenantID == "" || len(state.currentRanges) > 0 || len(state.historicalRanges) > 0 {
		return
	}
	if len(state.rangeCounts) == 0 && len(state.exampleSeries) == 0 && len(state.sampleRates) == 0 {
		delete(pr.byTenant, tenantID)
	}
}

// rangesSnapshot returns the current working set of ranges (current ∪
// historical) sorted by Lo. The returned slice is a copy safe to
// retain. The walker uses this as the bucket boundary list for one
// head walk.
func (pr *partitionRanges) rangesSnapshot() []assignment.HashRange {
	return pr.rangesSnapshotForTenant("")
}

func (pr *partitionRanges) rangesSnapshotForTenant(tenantID string) []assignment.HashRange {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	state := pr.stateForTenantLocked(tenantID)
	if state == nil {
		return nil
	}
	return rangesSnapshotLocked(state)
}

func rangesSnapshotLocked(state *tenantRangeState) []assignment.HashRange {
	if len(state.currentRanges) == 0 && len(state.historicalRanges) == 0 {
		return nil
	}
	out := make([]assignment.HashRange, 0, len(state.currentRanges)+len(state.historicalRanges))
	out = append(out, state.currentRanges...)
	out = append(out, state.historicalRanges...)
	sort.Slice(out, func(i, j int) bool { return out[i].Lo < out[j].Lo })
	return out
}

// applyWalkResult merges per-range counts produced by one walk pass
// (which used the snapshot ranges as bucket keys) into the partition's
// rangeCounts. Ranges that walked to zero AND are not in currentRanges
// are dropped from historicalRanges and rangeCounts — residue has
// compacted out of the head.
//
// If examples is non-nil it must be the same length as forRanges and
// holds one representative series per range (rendered via
// labels.Labels.String()) for display on the readcache admin page.
// Empty slots mean the walk found no series in that range; the
// previously-captured example is retained in that case so the admin
// UI stays useful across walks that happen to skip a range. Examples
// for ranges no longer in the working set are GC'd alongside the
// counts.
//
// forRanges must equal what rangesSnapshot returned at the start of
// the walk (same length, same Lo/Hi at each position). Otherwise the
// update is discarded — currentRanges shifted mid-walk and a fresh
// walk on the new snapshot will reconcile.
func (pr *partitionRanges) applyWalkResult(forRanges []assignment.HashRange, counts []int64, examples []string) bool {
	return pr.applyWalkResultForTenant("", forRanges, counts, examples)
}

func (pr *partitionRanges) applyWalkResultForTenant(tenantID string, forRanges []assignment.HashRange, counts []int64, examples []string) bool {
	if len(forRanges) != len(counts) {
		return false
	}
	if examples != nil && len(examples) != len(forRanges) {
		return false
	}

	pr.mu.Lock()
	defer pr.mu.Unlock()

	state := pr.stateForTenantLocked(tenantID)
	if state == nil {
		return len(forRanges) == 0
	}
	applied := applyTenantWalkResultLocked(state, forRanges, counts, examples)
	if applied {
		pr.deleteTenantStateIfEmptyLocked(tenantID, state)
	}
	return applied
}

func applyTenantWalkResultLocked(state *tenantRangeState, forRanges []assignment.HashRange, counts []int64, examples []string) bool {
	// Validate the snapshot still matches.
	snap := rangesSnapshotLocked(state)
	if len(snap) != len(forRanges) {
		return false
	}
	for i := range snap {
		if snap[i] != forRanges[i] {
			return false
		}
	}

	currentSet := make(map[assignment.HashRange]struct{}, len(state.currentRanges))
	for _, r := range state.currentRanges {
		currentSet[r] = struct{}{}
	}

	// Replace counts wholesale. Historical ranges that walked to zero
	// can be GC'd because their residue has compacted out.
	nextCounts := make(map[assignment.HashRange]int64, len(forRanges))
	nextExamples := make(map[assignment.HashRange]string, len(forRanges))
	var newHistorical []assignment.HashRange
	for i, r := range forRanges {
		c := counts[i]
		keep := false
		if _, isCurrent := currentSet[r]; isCurrent {
			// Always retain current ranges, even at zero — the
			// rebalancer needs to see them as part of this
			// partition's footprint.
			nextCounts[r] = c
			keep = true
		} else if c > 0 {
			nextCounts[r] = c
			newHistorical = append(newHistorical, r)
			keep = true
		}
		if !keep {
			continue
		}
		// Prefer the example captured by this walk; fall back to a
		// previously-captured example so a transient empty walk
		// doesn't blank the UI.
		if examples != nil && examples[i] != "" {
			nextExamples[r] = examples[i]
		} else if prev, ok := state.exampleSeries[r]; ok {
			nextExamples[r] = prev
		}
	}
	sort.Slice(newHistorical, func(i, j int) bool { return newHistorical[i].Lo < newHistorical[j].Lo })

	state.historicalRanges = newHistorical
	state.rangeCounts = nextCounts
	state.exampleSeries = nextExamples

	// GC sample-rate EwmaRates for ranges that just fell out of the
	// working set (current ∪ historical). Without this, residue
	// ranges whose head finally compacted would keep their EwmaRate
	// alive forever, slowly leaking memory in proportion to the
	// number of range moves over the lifetime of the pod.
	if len(state.sampleRates) > 0 {
		live := make(map[assignment.HashRange]struct{}, len(state.currentRanges)+len(state.historicalRanges))
		for _, r := range state.currentRanges {
			live[r] = struct{}{}
		}
		for _, r := range state.historicalRanges {
			live[r] = struct{}{}
		}
		for r := range state.sampleRates {
			if _, ok := live[r]; !ok {
				delete(state.sampleRates, r)
			}
		}
	}
	return true
}

// snapshotCounts returns a deep copy of the per-range counts. Used by
// the HashRangeStats RPC. Each entry carries the per-range
// samples-per-second EWMA alongside the active-series count so the
// rebalancer's load map can be keyed by either (or both) without a
// second RPC.
func (pr *partitionRanges) snapshotCounts() []hashRangeCount {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	out := snapshotTenantCountsLocked("", &pr.tenantRangeState)
	tenantIDs := make([]string, 0, len(pr.byTenant))
	for tenantID := range pr.byTenant {
		tenantIDs = append(tenantIDs, tenantID)
	}
	sort.Strings(tenantIDs)
	for _, tenantID := range tenantIDs {
		out = append(out, snapshotTenantCountsLocked(tenantID, pr.byTenant[tenantID])...)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TenantID != out[j].TenantID {
			return out[i].TenantID < out[j].TenantID
		}
		return out[i].Range.Lo < out[j].Range.Lo
	})
	return out
}

func snapshotTenantCountsLocked(tenantID string, state *tenantRangeState) []hashRangeCount {
	if len(state.rangeCounts) == 0 && len(state.currentRanges) == 0 {
		return nil
	}
	// Always emit currentRanges (even if zero) so the rebalancer sees
	// the partition's claimed footprint. Historical entries are only
	// emitted if they have residue (count > 0).
	out := make([]hashRangeCount, 0, len(state.currentRanges)+len(state.historicalRanges))
	seen := make(map[assignment.HashRange]struct{}, len(state.currentRanges))
	for _, r := range state.currentRanges {
		out = append(out, hashRangeCount{
			TenantID:   tenantID,
			Range:      r,
			Count:      state.rangeCounts[r],
			SampleRate: rateOf(state.sampleRates[r]),
		})
		seen[r] = struct{}{}
	}
	for _, r := range state.historicalRanges {
		if _, dup := seen[r]; dup {
			continue
		}
		if c := state.rangeCounts[r]; c > 0 {
			out = append(out, hashRangeCount{
				TenantID:   tenantID,
				Range:      r,
				Count:      c,
				SampleRate: rateOf(state.sampleRates[r]),
			})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Range.Lo < out[j].Range.Lo })
	return out
}

// currentRangesCopy returns a copy of the currently-owned ranges for
// the GetHashRanges RPC.
func (pr *partitionRanges) currentRangesCopy() []assignment.HashRange {
	pr.mu.RLock()
	defer pr.mu.RUnlock()
	out := make([]assignment.HashRange, len(pr.currentRanges))
	copy(out, pr.currentRanges)
	return out
}

type tenantCurrentRanges struct {
	TenantID string
	Ranges   []assignment.HashRange
}

func (pr *partitionRanges) currentRangesByTenant() []tenantCurrentRanges {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	out := make([]tenantCurrentRanges, 0, len(pr.byTenant)+1)
	if len(pr.currentRanges) > 0 {
		out = append(out, tenantCurrentRanges{TenantID: "", Ranges: slices.Clone(pr.currentRanges)})
	}
	for tenantID, state := range pr.byTenant {
		if len(state.currentRanges) == 0 {
			continue
		}
		out = append(out, tenantCurrentRanges{TenantID: tenantID, Ranges: slices.Clone(state.currentRanges)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TenantID < out[j].TenantID })
	return out
}

func (pr *partitionRanges) configuredTenantIDs() []string {
	snap := pr.currentRangesByTenant()
	out := make([]string, len(snap))
	for i := range snap {
		out[i] = snap[i].TenantID
	}
	return out
}

func (pr *partitionRanges) trackedTenantIDs() []string {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	out := make([]string, 0, len(pr.byTenant)+1)
	if len(pr.currentRanges) > 0 || len(pr.historicalRanges) > 0 {
		out = append(out, "")
	}
	for tenantID, state := range pr.byTenant {
		if len(state.currentRanges) > 0 || len(state.historicalRanges) > 0 {
			out = append(out, tenantID)
		}
	}
	sort.Strings(out)
	return out
}

type unknownTenant struct {
	TenantID        string
	PartitionID     int32
	FirstSeenUnixMs int64
}

func (pr *partitionRanges) unknownTenants(liveTenantIDs []string, partitionID int32, observedAt time.Time) []unknownTenant {
	if partitionID != 0 {
		return nil
	}

	pr.mu.Lock()
	defer pr.mu.Unlock()

	live := make(map[string]struct{}, len(liveTenantIDs))
	for _, tenantID := range liveTenantIDs {
		live[tenantID] = struct{}{}
	}

	for tenantID := range pr.unknownFirstSeen {
		state := pr.stateForTenantLocked(tenantID)
		_, stillLive := live[tenantID]
		if !stillLive || (state != nil && len(state.currentRanges) > 0) {
			delete(pr.unknownFirstSeen, tenantID)
		}
	}

	out := make([]unknownTenant, 0, len(liveTenantIDs))
	for _, tenantID := range liveTenantIDs {
		state := pr.stateForTenantLocked(tenantID)
		if state != nil && len(state.currentRanges) > 0 {
			continue
		}
		firstSeen, ok := pr.unknownFirstSeen[tenantID]
		if !ok {
			tenantID = strings.Clone(tenantID)
			firstSeen = observedAt.UnixMilli()
			pr.unknownFirstSeen[tenantID] = firstSeen
		}
		out = append(out, unknownTenant{
			TenantID:        tenantID,
			PartitionID:     partitionID,
			FirstSeenUnixMs: firstSeen,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TenantID < out[j].TenantID })
	return out
}

// adminSnapshot returns the current and historical ranges with their
// per-range counts and example series, split into two slices so the
// admin page can show growth (current) and residue (historical)
// separately. Unlike snapshotCounts, this preserves the current vs
// historical distinction even when residue is non-zero, and
// populates Example with a representative series (rendered via
// labels.Labels.String()) for each range.
func (pr *partitionRanges) adminSnapshot() (current, historical []hashRangeCount) {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	current, historical = appendTenantAdminSnapshotLocked(current, historical, "", &pr.tenantRangeState)
	tenantIDs := make([]string, 0, len(pr.byTenant))
	for tenantID := range pr.byTenant {
		tenantIDs = append(tenantIDs, tenantID)
	}
	sort.Strings(tenantIDs)
	for _, tenantID := range tenantIDs {
		current, historical = appendTenantAdminSnapshotLocked(current, historical, tenantID, pr.byTenant[tenantID])
	}
	return current, historical
}

func appendTenantAdminSnapshotLocked(current, historical []hashRangeCount, tenantID string, state *tenantRangeState) ([]hashRangeCount, []hashRangeCount) {
	for _, r := range state.currentRanges {
		current = append(current, hashRangeCount{
			TenantID:   tenantID,
			Range:      r,
			Count:      state.rangeCounts[r],
			Example:    state.exampleSeries[r],
			SampleRate: rateOf(state.sampleRates[r]),
		})
	}
	for _, r := range state.historicalRanges {
		historical = append(historical, hashRangeCount{
			TenantID:   tenantID,
			Range:      r,
			Count:      state.rangeCounts[r],
			Example:    state.exampleSeries[r],
			SampleRate: rateOf(state.sampleRates[r]),
		})
	}
	return current, historical
}

// recordSampleBatch attributes a Kafka push's per-series sample
// counts to the matching current ranges' EwmaRates. Called once per
// PushToStorageAndReleaseRequest on the ingest hot path; no map
// allocations are required because EwmaRate.Add is internally
// lock-free and pr.sampleRates is mutated only under pr.mu.Lock by
// setRanges / applyWalkResult.
//
// Samples that hash outside every current range are dropped on the
// floor: they're either misrouted (a distributor bug worth tracking
// elsewhere) or in flight across a range-move boundary. Either way
// the rebalancer's per-range signal should reflect what's owned, not
// what arrived.
//
// Each timeseries' contribution is len(Samples) + len(Histograms);
// this matches the existing cortex_readcache_samples_ingested_total
// counter so an operator can compare per-second rates here against
// per-partition rates there for sanity.
func (pr *partitionRanges) recordSampleBatch(userID string, timeseries []mimirpb.PreallocTimeseries) {
	if len(timeseries) == 0 {
		return
	}
	pr.mu.RLock()
	defer pr.mu.RUnlock()
	state := pr.stateForTenantLocked(userID)
	// Empty TenantID is the legacy unscoped assignment. Use it only
	// when this tenant has no scoped state at all, so upgraded
	// readcaches continue to report load while the rebalancer rolls
	// forward to tenant-tagged SetHashRanges requests.
	if state == nil {
		state = &pr.tenantRangeState
	}
	if len(state.currentRanges) == 0 {
		return
	}
	for _, ts := range timeseries {
		n := int64(len(ts.Samples) + len(ts.Histograms))
		if n == 0 {
			continue
		}
		metric := metricNameOf(ts.Labels)
		hash := mimirpb.ShardByMetricNameLocality(userID, metric, ts.Labels)
		// Binary-search this tenant's currentRanges for the tile containing
		// hash. currentRanges is sorted by Lo and non-overlapping (the
		// rebalancer's assignment guarantees this).
		idx := sort.Search(len(state.currentRanges), func(i int) bool {
			return state.currentRanges[i].Lo > hash
		}) - 1
		if idx < 0 {
			continue
		}
		r := state.currentRanges[idx]
		if !r.Contains(hash) {
			continue
		}
		if rate := state.sampleRates[r]; rate != nil {
			rate.Add(n)
		}
	}
}

// tickSampleRates advances every EwmaRate in this partition by one
// tick. Must be called from the per-pod loadStats ticker at
// loadstats.TickInterval cadence; calling more often does not
// corrupt state but does inflate the EWMA bias (the alpha constant
// is set on the assumption of a 15s tick).
//
// Historical ranges are ticked too: distributors no longer route to
// those ranges, so each tick swaps a 0 into the EwmaRate and the
// reported rate decays toward zero. That's the right behaviour —
// the rebalancer should see the rate trail off after a move.
func (pr *partitionRanges) tickSampleRates() {
	// RLock is sufficient: EwmaRate.Tick is internally synchronised
	// and the map itself is read-only here. Concurrent setRanges /
	// applyWalkResult holds the writer side and is what would
	// reshape pr.sampleRates.
	pr.mu.RLock()
	defer pr.mu.RUnlock()
	for _, rate := range pr.sampleRates {
		rate.Tick()
	}
	for _, state := range pr.byTenant {
		for _, rate := range state.sampleRates {
			rate.Tick()
		}
	}
}

// metricNameOf returns the value of the __name__ label, or "" if it
// is absent. Iterating LabelAdapters is cheap (labels are short
// slices) and avoids materialising a labels.Labels just to read one
// value.
func metricNameOf(ls []mimirpb.LabelAdapter) string {
	for i := range ls {
		if ls[i].Name == model.MetricNameLabel {
			return ls[i].Value
		}
	}
	return ""
}

// rateOf returns the current EWMA rate for r, or 0 if no entry
// exists (e.g. a range that's only in historicalRanges and whose
// EwmaRate was already GC'd by applyWalkResult).
func rateOf(r *util_math.EwmaRate) float64 {
	if r == nil {
		return 0
	}
	return r.Rate()
}

// hashRangeCount is the in-process shape of a per-(partition, range)
// load entry emitted by HashRangeStats and rendered by the admin page.
// Partition association is implicit (it's the partition this came
// from); the wire format adds it back. Example is only populated by
// adminSnapshot — the RPC path leaves it empty.
//
// SampleRate is the EWMA of samples-per-second observed on the
// ingest hot path. snapshotCounts emits it on the wire so the
// rebalancer can balance by ingest throughput, not just by head
// cardinality. A zero value can mean either "no samples" or "the
// EwmaRate has not been ticked yet"; the latter is only true for
// the first 15s after a partition is adopted.
type hashRangeCount struct {
	TenantID   string
	Range      assignment.HashRange
	Count      int64
	Example    string
	SampleRate float64
}

// sortedNonOverlappingCopy returns a fresh sorted-by-Lo copy of the
// input ranges. It does not validate non-overlap; callers are expected
// to pass a non-overlapping set (the rebalancer's assignment is
// non-overlapping by construction).
func sortedNonOverlappingCopy(rs []assignment.HashRange) []assignment.HashRange {
	out := make([]assignment.HashRange, len(rs))
	copy(out, rs)
	sort.Slice(out, func(i, j int) bool { return out[i].Lo < out[j].Lo })
	return out
}

// rangeDiff computes the geometric set difference a − b: the portions
// of any range in a that are not covered by any range in b. Both
// inputs must be sorted by Lo and non-overlapping. The output is
// sorted, non-overlapping, and preserves the original a boundaries
// where possible (a single range in a may be split into multiple
// output entries by boundaries from b).
func rangeDiff(a, b []assignment.HashRange) []assignment.HashRange {
	if len(a) == 0 {
		return nil
	}
	if len(b) == 0 {
		out := make([]assignment.HashRange, len(a))
		copy(out, a)
		return out
	}

	var out []assignment.HashRange
	bi := 0
	for _, r := range a {
		// Skip b entries strictly before r.
		for bi < len(b) && b[bi].Hi < r.Lo {
			bi++
		}
		// Walk b entries that overlap r without consuming our pointer
		// (later a entries may also overlap them).
		lo := int64(r.Lo)
		consumed := false
		for bj := bi; bj < len(b) && b[bj].Lo <= r.Hi; bj++ {
			bLo := int64(b[bj].Lo)
			bHi := int64(b[bj].Hi)
			if bLo > lo {
				out = append(out, assignment.HashRange{Lo: uint32(lo), Hi: uint32(bLo - 1)})
			}
			if bHi >= int64(r.Hi) {
				consumed = true
				break
			}
			lo = bHi + 1
		}
		if !consumed && lo <= int64(r.Hi) {
			out = append(out, assignment.HashRange{Lo: uint32(lo), Hi: r.Hi})
		}
	}
	return out
}

// rangeUnionPreservingBoundaries merges two sorted non-overlapping
// range lists. Adjacent or overlapping pairs are NOT coalesced — we
// keep range boundaries the slicer once chose, so the walker still
// reports them as separate buckets (helps the rebalancer correlate
// residue to its previous slicing decisions). The two inputs are
// assumed to be disjoint from each other at the hash-space level; we
// don't try to detect overlaps across the inputs.
func rangeUnionPreservingBoundaries(a, b []assignment.HashRange) []assignment.HashRange {
	if len(a) == 0 {
		out := make([]assignment.HashRange, len(b))
		copy(out, b)
		return out
	}
	if len(b) == 0 {
		out := make([]assignment.HashRange, len(a))
		copy(out, a)
		return out
	}
	merged := make([]assignment.HashRange, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i].Lo <= b[j].Lo {
			merged = append(merged, a[i])
			i++
		} else {
			merged = append(merged, b[j])
			j++
		}
	}
	merged = append(merged, a[i:]...)
	merged = append(merged, b[j:]...)
	return merged
}
