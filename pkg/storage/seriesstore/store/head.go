// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"slices"
	"strings"
	"time"

	promlabels "github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
	"github.com/grafana/mimir/pkg/storage/seriesstore/metrics"
	"github.com/grafana/mimir/pkg/storage/seriesstore/trackers"
)

// HeadReport is a tenant's emulated Go head, as the metrics report it.
type HeadReport = metrics.HeadReport

type estimation struct {
	tenant     string
	count      uint64
	percentage uint64
}

// tenantsToCompactEarly is Mimir's `filterUsersToCompactToReduceInMemorySeries`, from each
// tenant's in-memory and estimated removable series.
func tenantsToCompactEarly(memorySeries uint64, config EarlyHeadCompaction, estimations []estimation) []string {
	var totalReduction uint64
	for _, e := range estimations {
		totalReduction += e.count
	}
	if memorySeries == 0 || totalReduction*100/memorySeries < config.MinReductionPercentage {
		return nil
	}
	target := uint64(0)
	if memorySeries > config.MinInMemorySeries {
		target = memorySeries - config.MinInMemorySeries
	}
	slices.SortStableFunc(estimations, func(a, b estimation) int {
		switch {
		case a.count > b.count:
			return -1
		case a.count < b.count:
			return 1
		}
		return 0
	})
	var (
		sum     uint64
		tenants []string
	)
	for _, e := range estimations {
		if sum < target || e.percentage >= config.MinReductionPercentage {
			tenants = append(tenants, e.tenant)
			sum += e.count
		}
	}
	return tenants
}

type tenantHeadBounds struct {
	headMin     int64
	recompute   bool
	evictBefore int64
	hasEvict    bool
}

// HeadTick updates which series the Go ingester would still hold in its head, which of those this
// partition owns, and, when compact, moves each tenant's head min time like head compaction: while
// the head spans more than 1.5 block ranges, its oldest block range is compacted away.
//
// With trackOwned, a recompute of a tenant's owned series (on a new tenant, changed ranges or
// after head compaction) removes its non-owned series from the active series until their next
// sample, like Mimir's computeOwnedSeries.
func (s *Store) HeadTick(compact, trackOwned bool) []HeadReport {
	freeze := s.freezePending.Swap(false)
	owned := s.ownedRanges.Load()
	bounds := map[string]tenantHeadBounds{}
	home := s.shards[0]
	home.Lock()
	for id, t := range home.tenants {
		var current *TenantRanges
		if owned != nil {
			if ranges, ok := (*owned)[id]; ok {
				current = &ranges
			}
		}
		recompute := trackOwned && current != nil && (t.ownedRecompute || t.ownedRangesSeen == nil || !t.ownedRangesSeen.equal(current))
		if recompute {
			seen := *current
			t.ownedRangesSeen = &seen
			t.ownedRecompute = false
		}
		b := tenantHeadBounds{headMin: t.headMin, recompute: recompute}
		if eviction := s.nonOwnedEviction; eviction != nil && compact && trackOwned {
			lim := &s.overrides.Tenant(id).Limits
			if threshold := lim.EarlyHeadCompactionOwnedSeriesThreshold; threshold > 0 {
				local := s.overrides.LocalLimit(lim, threshold)
				grace, ok := int64(0), true
				switch {
				case local > 0 && t.headSeries >= uint64(local):
					grace = eviction.MinGraceMs
				case eviction.MaxGraceMs > 0:
					grace = eviction.MaxGraceMs
				default:
					ok = false
				}
				if ok {
					b.evictBefore, b.hasEvict = nowMs()-grace-eviction.JitterMs, true
				}
			}
		}
		bounds[id] = b
	}
	home.Unlock()
	perShard := make([][]HeadReport, len(s.shards))
	_ = s.parallel(len(s.shards), func(index int) error {
		state := s.shards[index]
		state.Lock()
		defer state.Unlock()
		reports := make([]HeadReport, 0, len(state.tenants))
		for tenantID, t := range state.tenants {
			b, ok := bounds[tenantID]
			if !ok {
				b = tenantHeadBounds{headMin: math.MinInt64}
			}
			trackNonOwned := trackOwned && s.nonOwnedEviction != nil
			activeCutoff := satSub(nowMs(), s.activeWindowMs)
			nowS := uint32(nowMs() / 1000)
			// Unknown ranges or a tenant the ring has not been asked about yet count as owned, like
			// new series in Go.
			var ranges *TenantRanges
			if owned != nil {
				if r, ok := (*owned)[tenantID]; ok {
					ranges = &r
				}
			}
			report := HeadReport{Tenant: tenantID}
			minTime := int64(math.MaxInt64)
			t.series.forEach(func(entry *seriesEntry) {
				series := &entry.series
				newest, hasNewest := series.newest()
				// The oldest sample is the head's min time only until its first compaction, which
				// sets it for good; looking it up decodes every chunk of every series, each tick.
				if b.headMin == math.MinInt64 {
					if countersEnabled {
						s.oldestScans.Add(1)
					}
					if oldest, ok := series.oldest(); ok {
						minTime = min(minTime, oldest)
					}
				}
				inHead := !series.headEvicted && hasNewest && newest >= b.headMin
				if inHead && series.nonOwnedSinceS != 0 && b.hasEvict && int64(series.nonOwnedSinceS)*1000 <= b.evictBefore {
					series.headEvicted = true
					s.evictEpoch.Add(1)
					series.nonOwnedSinceS = 0
					report.NonOwnedEvicted++
					inHead = false
				}
				switch {
				case !series.inHead && inHead:
					report.SeriesCreated++
				case series.inHead && !inHead:
					report.SeriesRemoved++
				}
				series.inHead = inHead
				if !inHead {
					return
				}
				report.MemorySeries++
				if series.isActive(activeCutoff) {
					report.ActiveSeries++
				}
				isOwned := ranges == nil || (ranges.InShard && rangesInclude(ranges.Ranges, series.ownedHash))
				if isOwned {
					report.OwnedSeries++
				}
				if b.recompute && trackNonOwned {
					// Like addPendingNonOwnedRefs, a series keeps the time it was first found
					// non-owned while it stays so.
					if isOwned {
						series.nonOwnedSinceS = 0
					} else if series.nonOwnedSinceS == 0 {
						series.nonOwnedSinceS = max(nowS, 1)
					}
				}
				if b.recompute && !isOwned {
					// Mimir clears all active series when the tenant owns no ranges, and deletes
					// the non-owned ones otherwise.
					if ranges != nil && ranges.InShard && len(ranges.Ranges) > 0 {
						series.lastIngestedMs = math.MinInt64
					} else {
						series.activeCleared = true
					}
				}
				it := series.chunks.iter()
				for chunk, more := it.next(); more; chunk, more = it.next() {
					if chunk.MaxTime >= b.headMin {
						report.HeadChunks++
					}
				}
				if series.floatHead != nil {
					report.HeadChunks++
				}
				if series.histogram() != nil {
					report.HeadChunks++
				}
				if len(series.ooo()) > 0 {
					report.HeadChunks++
				}
			})
			report.HeadMinTime = minTime
			reports = append(reports, report)
		}
		if freeze {
			freezeOutOfHead(state)
		}
		perShard[index] = reports
		return nil
	})
	merged := map[string]*HeadReport{}
	for _, reports := range perShard {
		for _, report := range reports {
			total, ok := merged[report.Tenant]
			if !ok {
				total = &HeadReport{Tenant: report.Tenant, HeadMinTime: math.MaxInt64}
				merged[report.Tenant] = total
			}
			total.MemorySeries += report.MemorySeries
			total.SeriesCreated += report.SeriesCreated
			total.SeriesRemoved += report.SeriesRemoved
			total.OwnedSeries += report.OwnedSeries
			total.HeadChunks += report.HeadChunks
			total.NonOwnedEvicted += report.NonOwnedEvicted
			total.ActiveSeries += report.ActiveSeries
			total.HeadMinTime = min(total.HeadMinTime, report.HeadMinTime)
		}
	}
	names := make([]string, 0, len(merged))
	for name := range merged {
		names = append(names, name)
	}
	slices.Sort(names)
	checkEarly := s.compactedLastTick.Swap(compact)
	var early map[string]bool
	if config := s.earlyHeadCompaction; config != nil && checkEarly {
		var memorySeries uint64
		for _, report := range merged {
			memorySeries += report.MemorySeries
		}
		if memorySeries >= config.MinInMemorySeries {
			var estimations []estimation
			for _, name := range names {
				report := merged[name]
				if report.MemorySeries == 0 {
					continue
				}
				count := report.MemorySeries - min(report.ActiveSeries, report.MemorySeries)
				estimations = append(estimations, estimation{report.Tenant, count, count * 100 / report.MemorySeries})
			}
			if tenants := tenantsToCompactEarly(memorySeries, *config, estimations); len(tenants) > 0 {
				fmt.Fprintf(os.Stderr, "phase=early_head_compaction in_memory_series=%d tenants=%s\n", memorySeries, strings.Join(tenants, ","))
				early = map[string]bool{}
				for _, tenant := range tenants {
					early[tenant] = true
				}
			}
		}
	}
	forcedMaxTime := satSub(nowMs(), s.activeWindowMs)
	home.Lock()
	result := make([]HeadReport, 0, len(names))
	for _, name := range names {
		report := merged[name]
		t := tenantOf(home.tenants, name)
		t.headSeries = report.MemorySeries
		// A forced compaction up to the idle timeout ago truncates the head right after it.
		if early[name] && t.maxTime != math.MinInt64 {
			truncated := satAdd(min(forcedMaxTime, t.maxTime), 1)
			if t.headMin == math.MinInt64 || truncated > t.headMin {
				t.headMin = truncated
				t.truncatedTo = truncated
				t.ownedRecompute = true
				s.freezePending.Store(true)
			}
		}
		// Like Mimir, an early compaction asks for another owned series recompute.
		if report.NonOwnedEvicted > 0 {
			t.ownedRecompute = true
			s.freezePending.Store(true)
		}
		if compact {
			if t.headMin == math.MinInt64 && report.HeadMinTime != math.MaxInt64 {
				t.headMin = report.HeadMinTime
			}
			for t.maxTime != math.MinInt64 && t.headMin != math.MinInt64 && t.maxTime-t.headMin > chunkRangeMs/2*3 {
				t.headMin = rangeEnd(t.headMin)
				t.truncatedTo = t.headMin
				t.ownedRecompute = true
				s.freezePending.Store(true)
			}
		}
		// Like Go's head, the min time is where it was last truncated, whatever samples remain,
		// and the oldest sample before that.
		if t.headMin != math.MinInt64 {
			report.HeadMinTime = t.headMin
		}
		report.Truncated = t.truncatedTo != math.MinInt64
		report.HeadMaxTime = t.maxTime
		result = append(result, *report)
	}
	home.Unlock()
	return result
}

// freezeOutOfHead moves the series the emulated head no longer holds to a new cold block, like
// Go's head truncation after compacting them into a block. Their open chunks are cut first, so
// the block has all their data; they stay in memory if the block can't be written.
func freezeOutOfHead(state *shardState) {
	if pending := prepareFreeze(state); pending != nil {
		installFreeze(state, pending, pending.build(state.cold.directory))
	}
}

type frozenKey struct {
	tenant string
	labels labels.Labels
}

// pendingFreeze is a cold block of the series that left the head, from what they held when it
// was prepared.
type pendingFreeze struct {
	id      uint64
	frozen  []frozenSeries
	keys    map[frozenKey]chunkList
	started time.Time
}

// prepareFreeze cuts the open chunks of the series out of the head and takes their data for a
// cold block, or returns nil when there are none. With the shard locked.
func prepareFreeze(state *shardState) *pendingFreeze {
	pending := &pendingFreeze{keys: map[frozenKey]chunkList{}, started: time.Now()}
	for tenantID, t := range state.tenants {
		t.series.forEach(func(entry *seriesEntry) {
			series := &entry.series
			if series.inHead {
				return
			}
			err := cutFloatHead(series, state.disk)
			if err == nil {
				err = cutHistogramHead(series, state.disk)
			}
			if err == nil {
				err = flushOutOfOrder(series, state.disk)
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "phase=cold_freeze_error tenant=%s error=%v\n", tenantID, err)
				return
			}
			// A copy: the series may take samples again before the block is installed.
			chunks := slices.Clone(series.chunks)
			pending.keys[frozenKey{tenantID, entry.labels}] = chunks
			// Series without any sample have nothing to keep.
			if !chunks.isEmpty() {
				pending.frozen = append(pending.frozen, frozenSeries{tenant: tenantID, labels: entry.labels, chunks: chunks, nativeHistogram: series.nativeHistogram})
			}
		})
	}
	if len(pending.keys) == 0 {
		return nil
	}
	pending.id = state.cold.nextID
	state.cold.nextID++
	return pending
}

// build writes the block, which needs no shard lock: it only reads the series' copies and the
// chunk files' immutable chunks. A nil block is one with no series to keep.
func (p *pendingFreeze) build(directory string) *coldBlock {
	if len(p.frozen) == 0 {
		return nil
	}
	block, err := buildColdBlock(directory, p.id, p.frozen)
	if err != nil {
		fmt.Fprintf(os.Stderr, "phase=cold_block_error id=%d error=%v\n", p.id, err)
		p.keys = nil
	}
	return block
}

// installFreeze adds the block and drops its series from memory, with the shard locked. If any
// of them took samples since it was prepared, the block is discarded and they all stay in
// memory, for the next freeze: the block would miss the new samples.
func installFreeze(state *shardState, pending *pendingFreeze, block *coldBlock) (removed map[uint64]struct{}) {
	if pending.keys == nil {
		return nil
	}
	unchanged := 0
	for tenantID, t := range state.tenants {
		t.series.forEach(func(entry *seriesEntry) {
			series := &entry.series
			// Most series stay in the head: no need to hash their labels.
			if series.inHead {
				return
			}
			chunks, ok := pending.keys[frozenKey{tenantID, entry.labels}]
			if ok && series.floatHead == nil && series.histogram() == nil && len(series.ooo()) == 0 && bytes.Equal(series.chunks, chunks) {
				unchanged++
			}
		})
	}
	if unchanged != len(pending.keys) {
		fmt.Fprintf(os.Stderr, "phase=cold_block_discarded id=%d series=%d changed=%d\n", pending.id, len(pending.keys), len(pending.keys)-unchanged)
		if block != nil {
			discardColdBlock(block)
		}
		return nil
	}
	if block != nil {
		state.cold.blocks = append(state.cold.blocks, block)
	}
	var builder promlabels.ScratchBuilder
	removed = map[uint64]struct{}{}
	for tenantID, t := range state.tenants {
		changed := t.series.retain(func(entry *seriesEntry) bool {
			if entry.series.inHead {
				return true
			}
			_, gone := pending.keys[frozenKey{tenantID, entry.labels}]
			if gone {
				t.uncount(entry, &builder)
				if ref := entry.series.ref; ref != 0 {
					t.byRef.delete(ref)
					removed[ref] = struct{}{}
				}
			}
			return !gone
		})
		reindexGroups(t, changed)
	}
	fmt.Fprintf(os.Stderr, "phase=cold_block_written id=%d series=%d duration_ms=%d\n", pending.id, len(pending.keys), time.Since(pending.started).Milliseconds())
	return removed
}

// ActiveSeriesReport returns the active series counts per tenant for the ingester's metrics,
// including custom trackers and cost attribution. Tracker matches are cached per series until the
// tenant's trackers change.
func (s *Store) ActiveSeriesReport() []metrics.ActiveSeriesReport {
	now := nowMs()
	cutoff := satSub(now, s.activeWindowMs)
	type costCounts = []map[string]*costCount
	type shardReport struct {
		report metrics.ActiveSeriesReport
		cost   costCounts
	}
	perShard := make([][]shardReport, len(s.shards))
	_ = s.parallel(len(s.shards), func(index int) error {
		state := s.shards[index]
		state.Lock()
		defer state.Unlock()
		reports := make([]shardReport, 0, len(state.tenants))
		for tenantID, t := range state.tenants {
			tenantLimits := s.overrides.Tenant(tenantID)
			custom := tenantLimits.CustomTrackers
			cost := tenantLimits.CostAttribution
			report := metrics.ActiveSeriesReport{Tenant: tenantID, Series: uint64(t.series.len)}
			for _, set := range t.metadata {
				report.Metadata += uint64(len(set))
			}
			for _, name := range custom.Names() {
				report.CustomTrackers = append(report.CustomTrackers, metrics.TrackerCounts{Name: name})
			}
			counts := make(costCounts, len(cost.Trackers))
			for tracker := range counts {
				counts[tracker] = map[string]*costCount{}
			}
			if t.trackers != custom && !t.trackers.Equal(custom) {
				t.trackerGeneration++
			}
			t.trackers = custom
			generation := t.trackerGeneration
			var matched []uint16
			t.series.forEach(func(entry *seriesEntry) {
				series := &entry.series
				if series.lastIngestedMs < cutoff {
					return
				}
				if series.trackerGeneration != uint32(generation) {
					series.setMatchedTrackers(custom.Matching(entry.labels))
					series.trackerGeneration = uint32(generation)
				}
				var buckets uint64
				histogram := uint64(0)
				if series.nativeHistogram {
					histogram, buckets = 1, uint64(series.lastBucketCount)
				}
				seriesCounts := [3]uint64{1, histogram, buckets}
				if !series.activeCleared {
					report.Active++
					report.ActiveNativeHistograms += histogram
					report.ActiveNativeHistogramBuckets += buckets
					matched = series.matchedTrackersInto(matched)
					for _, match := range matched {
						entry := &report.CustomTrackers[match].Counts
						for i := range entry {
							entry[i] += seriesCounts[i]
						}
					}
				}
				for tracker := range cost.Trackers {
					values := cost.Trackers[tracker].Key(entry.labels)
					key := strings.Join(values, "\xff")
					combination, ok := counts[tracker][key]
					if !ok {
						combination = &costCount{values: values}
						counts[tracker][key] = combination
					}
					for i := range combination.counts {
						combination.counts[i] += seriesCounts[i]
					}
				}
			})
			reports = append(reports, shardReport{report, counts})
		}
		perShard[index] = reports
		return nil
	})
	merged := map[string]*shardReport{}
	for _, reports := range perShard {
		for index := range reports {
			report := &reports[index]
			total, ok := merged[report.report.Tenant]
			if !ok {
				merged[report.report.Tenant] = report
				continue
			}
			total.report.Series += report.report.Series
			total.report.Active += report.report.Active
			total.report.ActiveNativeHistograms += report.report.ActiveNativeHistograms
			total.report.ActiveNativeHistogramBuckets += report.report.ActiveNativeHistogramBuckets
			total.report.Metadata += report.report.Metadata
			for i := range total.report.CustomTrackers {
				if i < len(report.report.CustomTrackers) {
					for j := range total.report.CustomTrackers[i].Counts {
						total.report.CustomTrackers[i].Counts[j] += report.report.CustomTrackers[i].Counts[j]
					}
				}
			}
			for tracker := range total.cost {
				if tracker >= len(report.cost) {
					continue
				}
				for key, counts := range report.cost[tracker] {
					existing, ok := total.cost[tracker][key]
					if !ok {
						total.cost[tracker][key] = counts
						continue
					}
					for i := range existing.counts {
						existing.counts[i] += counts.counts[i]
					}
				}
			}
		}
	}
	names := make([]string, 0, len(merged))
	for name := range merged {
		names = append(names, name)
	}
	slices.Sort(names)
	s.exemplarsLock.Lock()
	defer s.exemplarsLock.Unlock()
	s.costAttributionLock.Lock()
	defer s.costAttributionLock.Unlock()
	live := map[costAttributionKey]struct{}{}
	cleanupDue := now-s.costAttributionLastCleanup >= s.costAttributionCleanupMs
	if cleanupDue {
		s.costAttributionLastCleanup = now
	}
	deadline := satSub(now, s.costAttributionEvictionMs)
	reports := make([]metrics.ActiveSeriesReport, 0, len(names))
	for _, name := range names {
		entry := merged[name]
		report := entry.report
		if storage, ok := s.exemplars[report.Tenant]; ok {
			report.Exemplars = uint64(storage.Len())
			report.ExemplarSeries = uint64(storage.SeriesCount())
			report.OldestExemplarMs, report.HasOldest = storage.OldestTimestamp()
		}
		tenantLimits := s.overrides.Tenant(report.Tenant)
		maxCardinality := max(tenantLimits.Limits.MaxCostAttributionCardinality, 0)
		for tracker := range tenantLimits.CostAttribution.Trackers {
			definition := &tenantLimits.CostAttribution.Trackers[tracker]
			var combinations map[string]*costCount
			if tracker < len(entry.cost) {
				combinations = entry.cost[tracker]
			}
			key := costAttributionKey{report.Tenant, definition.Name}
			live[key] = struct{}{}
			state, ok := s.costAttribution[key]
			if !ok {
				state = &costAttributionState{}
				s.costAttribution[key] = state
			}
			// Like Mimir's tracker: overflow once the cardinality exceeds the maximum. The cleanup,
			// every `-cost-attribution.cleanup-interval`, recovers a tracker whose overflow started
			// a cooldown before the eviction deadline if it went back below the maximum, and
			// otherwise restarts the overflow.
			cardinality := len(combinations)
			switch {
			case !state.overflowing && int64(cardinality) > maxCardinality:
				state.overflowing, state.overflowSince = true, now
			case state.overflowing && cleanupDue && satAdd(state.overflowSince, tenantLimits.Limits.CostAttributionCooldownMs) < deadline:
				state.overflowing = int64(cardinality) > maxCardinality
				state.overflowSince = now
			}
			var values []metrics.AttributedValue
			if state.overflowing {
				var total [3]uint64
				for _, counts := range combinations {
					for i := range total {
						total[i] += counts.counts[i]
					}
				}
				overflow := make([]string, len(definition.Labels))
				for i := range overflow {
					overflow[i] = trackers.OverflowValue
				}
				values = []metrics.AttributedValue{{Values: overflow, Counts: total}}
			} else {
				values = make([]metrics.AttributedValue, 0, len(combinations))
				for _, counts := range combinations {
					values = append(values, metrics.AttributedValue{Values: counts.values, Counts: counts.counts})
				}
			}
			slices.SortFunc(values, compareAttributedValues)
			outputs := make([]string, len(definition.Labels))
			for i, label := range definition.Labels {
				outputs[i] = label.Output
			}
			report.CostAttribution = append(report.CostAttribution, metrics.AttributedSeries{
				Tracker:      definition.Name,
				Internal:     definition.Internal,
				OutputLabels: outputs,
				Values:       values,
				Overflow:     state.overflowing,
				Cardinality:  uint64(cardinality),
			})
		}
		reports = append(reports, report)
	}
	for key := range s.costAttribution {
		if _, ok := live[key]; !ok {
			delete(s.costAttribution, key)
		}
	}
	return reports
}

type costCount struct {
	values []string
	counts [3]uint64
}

func compareAttributedValues(a, b metrics.AttributedValue) int {
	if c := slices.Compare(a.Values, b.Values); c != 0 {
		return c
	}
	return slices.Compare(a.Counts[:], b.Counts[:])
}
