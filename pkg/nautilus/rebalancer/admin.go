// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// ActionKind describes what the slicer did to a hash range.
type ActionKind string

const (
	ActionMove     ActionKind = "move"
	ActionMerge    ActionKind = "merge"
	ActionSplit    ActionKind = "split"
	ActionReassign ActionKind = "reassign"
)

// Action records a single slicer operation from a rebalance round.
// Series is populated for ActionMove and records the per-range head
// series count moved off the source; it's both informational (for the
// admin UI) and load-bearing (recordRecentMoves uses it to accumulate
// each source's outstanding CompactionInterval budget).
type Action struct {
	Kind     ActionKind           `json:"kind"`
	Range    assignment.HashRange `json:"range"`
	FromPart int32                `json:"from_partition,omitempty"`
	ToPart   int32                `json:"to_partition,omitempty"`
	Series   int64                `json:"series,omitempty"`
	Detail   string               `json:"detail,omitempty"`
}

// RoundLog captures the summary stats from one rebalance round. All
// "L" fields are partition-level head-series values (max over owner
// ingesters for each partition), matching the denominator of
// cortex_ingester_memory_series.
type RoundLog struct {
	Time           time.Time `json:"time"`
	TotalL         int64     `json:"total_l"`
	MeanL          int64     `json:"mean_l"`
	MaxL           int64     `json:"max_l"`
	MinL           int64     `json:"min_l"`
	ImbalanceRatio float64   `json:"imbalance_ratio"`
	NumEntries     int       `json:"num_entries"`
	NumPartitions  int       `json:"num_partitions"`
	MovedFraction  float64   `json:"moved_fraction"`
	Actions        []Action  `json:"actions"`
}

const maxRoundLogs = 20

// rangeStatsView mirrors loadMap for one (partition, range) pair: the
// raw head series count the reporter (ingester or readcache)
// attributed to that pair. Keyed by the partition the series belong
// to, so the admin page can attribute residue to the partition that
// has it rather than to whichever partition currently owns the range.
type rangeStatsView struct {
	Series int64
}

// adminState stores the data needed to render the admin page and to
// serve per-round Trace JSON for external verification tools. Traces
// hold both the lightweight Round summary (for HTML) and the full
// inputs/outputs needed for deterministic replay.
type adminState struct {
	mu             sync.RWMutex
	traces         []Trace
	lastStats      map[partitionRangeKey]rangeStatsView
	lastPartitionL map[int32]int64
	lastMovable    map[int32]int64

	// lastReadcacheRound is the most recent readcache slicer round's
	// output. Empty before the first round, or when the readcache
	// slicer is disabled.
	lastReadcacheRound readcacheRoundView
}

// readcacheRoundView is the admin-side snapshot of a single
// readcache slicer round. Used by the trace JSON and (eventually)
// the HTML rounds page.
type readcacheRoundView struct {
	PerInstance  map[string]float64        `json:"per_instance"`
	PerPartition []readcachePartitionEntry `json:"per_partition"`
	Moves        []readcacheMove           `json:"moves"`
}

type readcachePartitionEntry struct {
	PartitionID  int32  `json:"partition_id"`
	CurrentOwner string `json:"current_owner"`
	PlannedOwner string `json:"planned_owner"`
}

// setLastReadcachePlan snapshots the most recent readcache slicer
// round into the admin state. Called from runReadcacheSlicer after
// the plan has been applied to the store.
func (s *adminState) setLastReadcachePlan(plan readcachePlan, currentOwner map[int32]string) {
	view := readcacheRoundView{
		PerInstance:  make(map[string]float64, len(plan.LoadByInstance)),
		PerPartition: make([]readcachePartitionEntry, 0, len(plan.Assignment.Entries)),
		Moves:        append([]readcacheMove(nil), plan.Moves...),
	}
	for inst, l := range plan.LoadByInstance {
		view.PerInstance[inst] = l
	}
	for _, e := range plan.Assignment.Entries {
		view.PerPartition = append(view.PerPartition, readcachePartitionEntry{
			PartitionID:  e.PartitionID,
			CurrentOwner: currentOwner[e.PartitionID],
			PlannedOwner: e.InstanceID,
		})
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastReadcacheRound = view
}

func (s *adminState) addTrace(tr Trace) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.traces = append(s.traces, tr)
	if len(s.traces) > maxRoundLogs {
		s.traces = s.traces[len(s.traces)-maxRoundLogs:]
	}
}

// setLastStats snapshots the current loadMap, partition L map, and
// derived movable budgets into the admin state. The movable budget
// rendered here intentionally mirrors what Phase 3 would compute
// from partitionLByPID (head series count) — even though Phase 3
// itself now balances on sample rate. The admin page still wants
// the cardinality-based view for cluster-state debugging; a future
// patch will surface a parallel sample-rate panel.
func (s *adminState) setLastStats(
	lm *loadMap,
	partitionLByPID map[int32]int64,
	recentMoves map[int32][]moveRecord,
	activePartitions []int32,
) {
	stats := make(map[partitionRangeKey]rangeStatsView, len(lm.series))
	for k, n := range lm.series {
		stats[k] = rangeStatsView{Series: n}
	}

	var totalL int64
	for _, pid := range activePartitions {
		totalL += partitionLByPID[pid]
	}
	var meanL int64
	if len(activePartitions) > 0 {
		meanL = totalL / int64(len(activePartitions))
	}

	partitionLCopy := make(map[int32]int64, len(partitionLByPID))
	movable := make(map[int32]int64, len(partitionLByPID))
	for _, pid := range activePartitions {
		l := partitionLByPID[pid]
		partitionLCopy[pid] = l
		var sumRecent int64
		for _, m := range recentMoves[pid] {
			sumRecent += m.series
		}
		above := l - meanL - sumRecent
		if above < 0 {
			above = 0
		}
		movable[pid] = above
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastStats = stats
	s.lastPartitionL = partitionLCopy
	s.lastMovable = movable
}

// snapshot returns the current admin state for rendering. Returns
// the lightweight Round summaries only — the full Trace inputs are
// served separately via the JSON endpoints to keep HTML rendering
// cheap.
func (s *adminState) snapshotReadcacheRound() (readcacheRoundView, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.lastReadcacheRound.PerPartition) == 0 && len(s.lastReadcacheRound.PerInstance) == 0 {
		return readcacheRoundView{}, false
	}
	view := readcacheRoundView{
		PerInstance:  make(map[string]float64, len(s.lastReadcacheRound.PerInstance)),
		PerPartition: append([]readcachePartitionEntry(nil), s.lastReadcacheRound.PerPartition...),
		Moves:        append([]readcacheMove(nil), s.lastReadcacheRound.Moves...),
	}
	for inst, load := range s.lastReadcacheRound.PerInstance {
		view.PerInstance[inst] = load
	}
	return view, true
}

func (s *adminState) snapshot() ([]RoundLog, map[partitionRangeKey]rangeStatsView, map[int32]int64, map[int32]int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rounds := make([]RoundLog, len(s.traces))
	for i, tr := range s.traces {
		rounds[i] = tr.Round
	}

	stats := make(map[partitionRangeKey]rangeStatsView, len(s.lastStats))
	for k, v := range s.lastStats {
		stats[k] = v
	}
	partitionL := make(map[int32]int64, len(s.lastPartitionL))
	for k, v := range s.lastPartitionL {
		partitionL[k] = v
	}
	movable := make(map[int32]int64, len(s.lastMovable))
	for k, v := range s.lastMovable {
		movable[k] = v
	}
	return rounds, stats, partitionL, movable
}

// traceSnapshot returns a copy of all currently-buffered traces in
// chronological order (oldest first). Use traceAt for single-round
// lookups by display index.
func (s *adminState) traceSnapshot() []Trace {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Trace, len(s.traces))
	copy(out, s.traces)
	return out
}

// traceAt returns the trace at the given display index (0 = newest)
// and true if found, or zero/false if out of range.
func (s *adminState) traceAt(displayIdx int) (Trace, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if displayIdx < 0 || displayIdx >= len(s.traces) {
		return Trace{}, false
	}
	// Display index 0 = newest = last in chronological slice.
	internalIdx := len(s.traces) - 1 - displayIdx
	return s.traces[internalIdx], true
}

// partitionView is the data for one partition row in the admin page.
type partitionView struct {
	PartitionID   int32
	InstanceID    string
	InstanceAddr  string
	MemorySeries  int64 // L_pid, matches cortex_ingester_memory_series (max over owners)
	MovableSeries int64 // max(0, L_pid - meanL) - sumRecentMoves(pid)
	OwnedSeries   int64 // Σ per-range series on this partition (from lm.series)
	HashSpacePct  float64
	NumRanges     int
	Ranges        []rangeView
}

// rangeView is the data for one hash range in the admin page.
type rangeView struct {
	Lo         uint32
	Hi         uint32
	Series     int64
	SizePct    float64
	LastAction ActionKind
}

// readcacheReplicaView is one readcache pod and the Kafka partitions
// it currently owns according to the readcache assignment log.
type readcacheReplicaView struct {
	InstanceID   string
	InstanceAddr string
	Load         float64 // slicer load from the most recent readcache round (0 if unknown)
	Partitions   []int32 // sorted partition IDs owned by this replica
}

// adminPageData is the full data structure passed to the template.
type adminPageData struct {
	GeneratedAt        string
	TotalMemorySeries  int64 // Σ L_pid across partitions
	TotalOwnedSeries   int64 // Σ per-range series, sanity-check against TotalMemorySeries
	AboveAverage       int64 // Σ max(0, L_pid - meanL)
	MeanL              int64
	MaxL               int64
	MinL               int64
	ImbalanceRatio     float64
	NumPartitions      int
	NumEntries         int
	MovedFraction      float64
	CompactionInterval time.Duration
	Partitions         []partitionView
	Rounds             []RoundLog
	HeatmapData        string

	ReadcacheConfigured bool
	ReadcacheReplicas   []readcacheReplicaView
	ReadcacheLastRound  readcacheRoundView
	ReadcacheHasRound   bool
}

func (r *Rebalancer) buildAdminPageData() adminPageData {
	rounds, lastStats, lastPartitionL, lastMovable := r.admin.snapshot()
	current := r.store.latestActiveAssignment(time.Now())

	data := adminPageData{
		GeneratedAt:        time.Now().UTC().Format(time.RFC3339),
		CompactionInterval: r.cfg.CompactionInterval,
	}

	if current == nil {
		return data
	}

	// Compute last-round action lookups.
	lastActions := make(map[assignment.HashRange]ActionKind)
	if len(rounds) > 0 {
		for _, a := range rounds[len(rounds)-1].Actions {
			lastActions[a.Range] = a.Kind
		}
	}

	// Build partition views.
	partMap := make(map[int32]*partitionView)
	hashSpaceTotal := float64(uint64(math.MaxUint32) + 1)
	var totalOwnedSeries int64

	for _, e := range current.Entries {
		pv, ok := partMap[e.PartitionID]
		if !ok {
			pv = &partitionView{PartitionID: e.PartitionID}
			partMap[e.PartitionID] = pv
		}

		stat := lastStats[partitionRangeKey{partitionID: e.PartitionID, hr: e.Range}]
		sizePct := float64(e.Range.Size()) / hashSpaceTotal * 100

		action := lastActions[e.Range]

		pv.Ranges = append(pv.Ranges, rangeView{
			Lo:         e.Range.Lo,
			Hi:         e.Range.Hi,
			Series:     stat.Series,
			SizePct:    sizePct,
			LastAction: action,
		})
		pv.OwnedSeries += stat.Series
		pv.HashSpacePct += sizePct
		pv.NumRanges++
		totalOwnedSeries += stat.Series
	}

	// Partitions that don't currently own any hash ranges but still
	// report a non-zero L_pid (or exist in the ring) need rows too,
	// otherwise the table and the header stats silently exclude:
	//   - "zombie" partitions that the slicer has drained but whose
	//     TSDB head series haven't been compacted away yet, and
	//   - partitions that exist in the ring but haven't received any
	//     assignment yet (e.g. freshly scaled-up replicas).
	// Without this, max_l / min_l / imbalance on the dashboard disagree
	// with what the JSON trace reports.
	for pid := range lastPartitionL {
		if _, ok := partMap[pid]; !ok {
			partMap[pid] = &partitionView{PartitionID: pid}
		}
	}

	// Resolve instance IDs from the partition ring.
	pRing := r.partitionRing.PartitionRing()
	instances, _ := r.ingesterRing.GetAllHealthy(0)
	idToAddr := make(map[string]string)
	for _, inst := range instances.Instances {
		idToAddr[inst.GetId()] = inst.Addr
	}

	var totalMemorySeries int64
	var maxL, minL int64
	minL = math.MaxInt64
	for pid, pv := range partMap {
		owners := pRing.PartitionOwnerIDs(pid)
		if len(owners) > 0 {
			pv.InstanceID = owners[0]
			pv.InstanceAddr = idToAddr[owners[0]]
		}
		pv.MemorySeries = lastPartitionL[pid]
		pv.MovableSeries = lastMovable[pid]
		totalMemorySeries += pv.MemorySeries
		if pv.MemorySeries > maxL {
			maxL = pv.MemorySeries
		}
		if pv.MemorySeries < minL {
			minL = pv.MemorySeries
		}
	}
	if minL == math.MaxInt64 {
		minL = 0
	}

	// Sort partitions by ID, and within each partition sort ranges by
	// descending head series count so the largest contributors surface
	// first (with deterministic tie-break by hash range).
	partitions := make([]partitionView, 0, len(partMap))
	for _, pv := range partMap {
		sort.Slice(pv.Ranges, func(i, j int) bool {
			if pv.Ranges[i].Series != pv.Ranges[j].Series {
				return pv.Ranges[i].Series > pv.Ranges[j].Series
			}
			return pv.Ranges[i].Lo < pv.Ranges[j].Lo
		})
		partitions = append(partitions, *pv)
	}
	// Sort partitions by descending L (memory series) so the heaviest
	// — and most operationally interesting — surface at the top. Tie-break
	// by partition ID for deterministic ordering.
	sort.Slice(partitions, func(i, j int) bool {
		if partitions[i].MemorySeries != partitions[j].MemorySeries {
			return partitions[i].MemorySeries > partitions[j].MemorySeries
		}
		return partitions[i].PartitionID < partitions[j].PartitionID
	})

	numPartitions := len(partitions)
	var meanL int64
	if numPartitions > 0 {
		meanL = totalMemorySeries / int64(numPartitions)
	}
	// Per the Slicer paper, load imbalance is defined as max / mean.
	// 1.0 means perfectly balanced.
	imbalance := 0.0
	if meanL > 0 {
		imbalance = float64(maxL) / float64(meanL)
	}

	var aboveAverage int64
	for _, p := range partitions {
		if p.MemorySeries > meanL {
			aboveAverage += p.MemorySeries - meanL
		}
	}

	// Build heatmap: 256 buckets across the hash space, weighted by
	// per-range head series so the visualization matches what the
	// slicer sees.
	const heatmapBuckets = 256
	heatmap := make([]float64, heatmapBuckets)
	bucketSize := (uint64(math.MaxUint32) + 1) / uint64(heatmapBuckets)
	for _, e := range current.Entries {
		stat := lastStats[partitionRangeKey{partitionID: e.PartitionID, hr: e.Range}]
		if stat.Series == 0 {
			continue
		}
		startBucket := uint64(e.Range.Lo) / bucketSize
		endBucket := uint64(e.Range.Hi) / bucketSize
		if endBucket >= heatmapBuckets {
			endBucket = heatmapBuckets - 1
		}
		bucketsSpanned := endBucket - startBucket + 1
		perBucket := float64(stat.Series) / float64(bucketsSpanned)
		for b := startBucket; b <= endBucket; b++ {
			heatmap[b] += perBucket
		}
	}
	heatmapJSON, _ := json.Marshal(heatmap)

	// Use latest round stats if available.
	movedFraction := 0.0
	if len(rounds) > 0 {
		movedFraction = rounds[len(rounds)-1].MovedFraction
	}

	// Reverse rounds for display (newest first).
	reversedRounds := make([]RoundLog, len(rounds))
	for i, rl := range rounds {
		reversedRounds[len(rounds)-1-i] = rl
	}

	data.TotalMemorySeries = totalMemorySeries
	data.TotalOwnedSeries = totalOwnedSeries
	data.AboveAverage = aboveAverage
	data.MeanL = meanL
	data.MaxL = maxL
	data.MinL = minL
	data.ImbalanceRatio = imbalance
	data.NumPartitions = numPartitions
	data.NumEntries = len(current.Entries)
	data.MovedFraction = movedFraction
	data.Partitions = partitions
	data.Rounds = reversedRounds
	data.HeatmapData = string(heatmapJSON)

	data.ReadcacheConfigured = r.readcacheConfigured()
	if data.ReadcacheConfigured {
		data.ReadcacheReplicas = r.buildReadcacheReplicaViews()
		data.ReadcacheLastRound, data.ReadcacheHasRound = r.admin.snapshotReadcacheRound()
	}

	return data
}

func (r *Rebalancer) readcacheConfigured() bool {
	return r.readcachePool != nil || r.readcacheRing != nil || r.cfg.ReadcacheSlicer.Enabled || len(r.cfg.ReadcacheSlicer.Instances) > 0
}

// buildReadcacheReplicaViews groups the live readcache assignment log
// by instance ID and enriches with ring addresses and slicer load.
func (r *Rebalancer) buildReadcacheReplicaViews() []readcacheReplicaView {
	now := time.Now()

	partitionsByInstance := make(map[string][]int32)
	// Use ActiveAt (currently-serving leases), not LiveEntries, which
	// also includes pre-issued future leases and inflates per-replica
	// partition counts during handoffs.
	for _, e := range r.readcacheStore.snapshot() {
		if !e.ActiveAt(now) {
			continue
		}
		partitionsByInstance[e.InstanceID] = append(partitionsByInstance[e.InstanceID], e.PartitionID)
	}

	instanceSet := make(map[string]struct{})
	for inst := range partitionsByInstance {
		instanceSet[inst] = struct{}{}
	}
	for _, inst := range r.activeReadcacheInstances() {
		instanceSet[inst] = struct{}{}
	}

	loadByInstance := make(map[string]float64)
	if round, ok := r.admin.snapshotReadcacheRound(); ok {
		loadByInstance = round.PerInstance
	}

	idToAddr := make(map[string]string)
	if r.readcacheRing != nil {
		if set, err := r.readcacheRing.GetAllHealthy(readcacheRingOp); err == nil {
			for _, inst := range set.Instances {
				idToAddr[inst.Id] = inst.Addr
			}
		}
	}

	replicas := make([]readcacheReplicaView, 0, len(instanceSet))
	for inst := range instanceSet {
		pids := partitionsByInstance[inst]
		sort.Slice(pids, func(i, j int) bool { return pids[i] < pids[j] })
		replicas = append(replicas, readcacheReplicaView{
			InstanceID:   inst,
			InstanceAddr: idToAddr[inst],
			Load:         loadByInstance[inst],
			Partitions:   pids,
		})
	}
	sort.Slice(replicas, func(i, j int) bool {
		if len(replicas[i].Partitions) != len(replicas[j].Partitions) {
			return len(replicas[i].Partitions) > len(replicas[j].Partitions)
		}
		return replicas[i].InstanceID < replicas[j].InstanceID
	})
	return replicas
}

// ServeHTTP dispatches all requests under the rebalancer's admin
// route prefix:
//
//	GET  /                       → HTML dashboard (default)
//	GET  /rounds.json            → list of recent round summaries
//	GET  /rounds/{idx}.json      → full Trace for one round
//	                               (idx 0 = newest, up to maxRoundLogs-1)
//
// The JSON endpoints exist so external tools — including AI
// verification agents — can fetch a round's complete inputs and
// outputs and replay them locally via ReplayTrace to confirm slicer
// determinism and check invariants.
func (r *Rebalancer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// Trim the registered prefix so this handler only sees its own
	// sub-path. The route registration uses a path prefix; the
	// actual prefix value is whatever modules.go registered, which
	// we discover by stripping anything up through the last
	// "/nautilus/rebalancer" segment.
	sub := strings.TrimPrefix(req.URL.Path, adminPathPrefix)
	switch {
	case sub == "" || sub == "/":
		r.serveAdminHTML(w)
	case sub == "/rounds.json":
		r.serveRoundsList(w)
	case strings.HasPrefix(sub, "/rounds/") && strings.HasSuffix(sub, ".json"):
		idxStr := strings.TrimSuffix(strings.TrimPrefix(sub, "/rounds/"), ".json")
		r.serveRoundTrace(w, idxStr)
	default:
		http.NotFound(w, req)
	}
}

// adminPathPrefix is the URL prefix under which the rebalancer's
// admin handlers are mounted (see modules.go). Kept here so URL
// dispatch in ServeHTTP and link generation in the HTML template
// stay in sync.
const adminPathPrefix = "/nautilus/rebalancer"

func (r *Rebalancer) serveAdminHTML(w http.ResponseWriter) {
	data := r.buildAdminPageData()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := adminTemplate.Execute(w, data); err != nil {
		http.Error(w, fmt.Sprintf("template error: %v", err), http.StatusInternalServerError)
	}
}

func (r *Rebalancer) serveRoundsList(w http.ResponseWriter) {
	traces := r.admin.traceSnapshot()
	// Reverse so the API returns newest-first, matching the indices
	// used by /rounds/{idx}.json.
	rounds := make([]RoundLog, len(traces))
	for i, tr := range traces {
		rounds[len(traces)-1-i] = tr.Round
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(struct {
		SlicerVersion string     `json:"slicer_version"`
		Rounds        []RoundLog `json:"rounds"`
	}{SlicerVersion: SlicerVersion, Rounds: rounds}); err != nil {
		http.Error(w, fmt.Sprintf("encode error: %v", err), http.StatusInternalServerError)
	}
}

func (r *Rebalancer) serveRoundTrace(w http.ResponseWriter, idxStr string) {
	idx, err := strconv.Atoi(idxStr)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid round index %q", idxStr), http.StatusBadRequest)
		return
	}
	tr, ok := r.admin.traceAt(idx)
	if !ok {
		http.NotFound(w, nil)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(tr); err != nil {
		http.Error(w, fmt.Sprintf("encode error: %v", err), http.StatusInternalServerError)
	}
}
