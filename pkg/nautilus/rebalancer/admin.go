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
type Action struct {
	Kind     ActionKind           `json:"kind"`
	Range    assignment.HashRange `json:"range"`
	FromPart int32                `json:"from_partition,omitempty"`
	ToPart   int32                `json:"to_partition,omitempty"`
	Detail   string               `json:"detail,omitempty"`
}

// RoundLog captures the actions and summary stats from one rebalance round.
type RoundLog struct {
	Time           time.Time `json:"time"`
	TotalLoad      float64   `json:"total_load"`
	MeanPartLoad   float64   `json:"mean_partition_load"`
	MaxPartLoad    float64   `json:"max_partition_load"`
	MinPartLoad    float64   `json:"min_partition_load"`
	ImbalanceRatio float64   `json:"imbalance_ratio"`
	NumEntries     int       `json:"num_entries"`
	NumPartitions  int       `json:"num_partitions"`
	MovedFraction  float64   `json:"moved_fraction"`
	Actions        []Action  `json:"actions"`
}

const maxRoundLogs = 20

// rangeStatsView mirrors loadMap.stats for one range: raw signals plus
// the combined weighted load value used for slicer decisions.
type rangeStatsView struct {
	Samples float64
	Series  int64
	Load    float64
}

// adminState stores the data needed to render the admin page and to
// serve per-round Trace JSON for external verification tools. Traces
// hold both the lightweight Round summary (for HTML) and the full
// inputs/outputs needed for deterministic replay.
type adminState struct {
	mu               sync.RWMutex
	traces           []Trace
	lastStats        map[assignment.HashRange]rangeStatsView
	lastOrphanSeries map[int32]int64
	lastOrphanLoad   map[int32]float64
}

func (s *adminState) addTrace(tr Trace) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.traces = append(s.traces, tr)
	if len(s.traces) > maxRoundLogs {
		s.traces = s.traces[len(s.traces)-maxRoundLogs:]
	}
}

// setLastStats snapshots the current loadMap into the admin state.
// Captures all per-range raw signals plus the combined load and the
// per-partition orphan series count and orphan-derived load.
func (s *adminState) setLastStats(lm *loadMap) {
	stats := make(map[assignment.HashRange]rangeStatsView, len(lm.stats))
	for hr, raw := range lm.stats {
		stats[hr] = rangeStatsView{
			Samples: raw.samples,
			Series:  raw.series,
			Load:    lm.load(hr),
		}
	}

	orphanSeries := make(map[int32]int64, len(lm.partitionOrphans))
	orphanLoad := make(map[int32]float64, len(lm.partitionOrphans))
	for pid, n := range lm.partitionOrphans {
		orphanSeries[pid] = n
		orphanLoad[pid] = lm.partitionOrphanLoad(pid)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastStats = stats
	s.lastOrphanSeries = orphanSeries
	s.lastOrphanLoad = orphanLoad
}

// snapshot returns the current admin state for rendering. Returns
// the lightweight Round summaries only — the full Trace inputs are
// served separately via the JSON endpoints to keep HTML rendering
// cheap.
func (s *adminState) snapshot() ([]RoundLog, map[assignment.HashRange]rangeStatsView, map[int32]int64, map[int32]float64) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rounds := make([]RoundLog, len(s.traces))
	for i, tr := range s.traces {
		rounds[i] = tr.Round
	}

	stats := make(map[assignment.HashRange]rangeStatsView, len(s.lastStats))
	for k, v := range s.lastStats {
		stats[k] = v
	}
	orphanSeries := make(map[int32]int64, len(s.lastOrphanSeries))
	for k, v := range s.lastOrphanSeries {
		orphanSeries[k] = v
	}
	orphanLoad := make(map[int32]float64, len(s.lastOrphanLoad))
	for k, v := range s.lastOrphanLoad {
		orphanLoad[k] = v
	}
	return rounds, stats, orphanSeries, orphanLoad
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
	PartitionID  int32
	InstanceID   string
	InstanceAddr string
	TotalLoad    float64
	TotalSamples float64
	TotalSeries  int64
	OrphanSeries int64
	OrphanLoad   float64
	HashSpacePct float64
	NumRanges    int
	Ranges       []rangeView
}

// rangeView is the data for one hash range in the admin page.
type rangeView struct {
	Lo         uint32
	Hi         uint32
	Load       float64
	Samples    float64
	Series     int64
	SizePct    float64
	LastAction ActionKind
}

// adminPageData is the full data structure passed to the template.
type adminPageData struct {
	GeneratedAt    string
	TotalLoad      float64
	TotalSamples   float64
	TotalSeries    int64
	TotalOrphan    int64
	MeanPartLoad   float64
	MaxPartLoad    float64
	MinPartLoad    float64
	ImbalanceRatio float64
	NumPartitions  int
	NumEntries     int
	MovedFraction  float64
	Partitions     []partitionView
	Rounds         []RoundLog
	HeatmapData    string

	// Configuration echo.
	WeightSeries  float64
	WeightSamples float64
}

func (r *Rebalancer) buildAdminPageData() adminPageData {
	rounds, lastStats, lastOrphanSeries, lastOrphanLoad := r.admin.snapshot()
	current := r.store.latest()

	data := adminPageData{
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		WeightSeries:  r.cfg.LoadWeightSeries,
		WeightSamples: r.cfg.LoadWeightSamples,
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
	var totalLoad, totalSamples float64
	var totalSeries int64

	for _, e := range current.Entries {
		pv, ok := partMap[e.PartitionID]
		if !ok {
			pv = &partitionView{PartitionID: e.PartitionID}
			partMap[e.PartitionID] = pv
		}

		stat := lastStats[e.Range]
		sizePct := float64(e.Range.Size()) / hashSpaceTotal * 100

		action := lastActions[e.Range]

		pv.Ranges = append(pv.Ranges, rangeView{
			Lo:         e.Range.Lo,
			Hi:         e.Range.Hi,
			Load:       stat.Load,
			Samples:    stat.Samples,
			Series:     stat.Series,
			SizePct:    sizePct,
			LastAction: action,
		})
		pv.TotalLoad += stat.Load
		pv.TotalSamples += stat.Samples
		pv.TotalSeries += stat.Series
		pv.HashSpacePct += sizePct
		pv.NumRanges++
		totalLoad += stat.Load
		totalSamples += stat.Samples
		totalSeries += stat.Series
	}

	// Resolve instance IDs from the partition ring.
	pRing := r.partitionRing.PartitionRing()
	instances, _ := r.ingesterRing.GetAllHealthy(0)
	idToAddr := make(map[string]string)
	for _, inst := range instances.Instances {
		idToAddr[inst.GetId()] = inst.Addr
	}

	var totalOrphan int64
	for pid, pv := range partMap {
		owners := pRing.PartitionOwnerIDs(pid)
		if len(owners) > 0 {
			pv.InstanceID = owners[0]
			pv.InstanceAddr = idToAddr[owners[0]]
		}
		pv.OrphanSeries = lastOrphanSeries[pid]
		pv.OrphanLoad = lastOrphanLoad[pid]
		pv.TotalLoad += pv.OrphanLoad
		totalOrphan += pv.OrphanSeries
		totalLoad += pv.OrphanLoad
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
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].PartitionID < partitions[j].PartitionID
	})

	numPartitions := len(partitions)
	meanPartLoad := 0.0
	maxPartLoad := 0.0
	minPartLoad := math.MaxFloat64
	if numPartitions > 0 {
		meanPartLoad = totalLoad / float64(numPartitions)
		for _, p := range partitions {
			if p.TotalLoad > maxPartLoad {
				maxPartLoad = p.TotalLoad
			}
			if p.TotalLoad < minPartLoad {
				minPartLoad = p.TotalLoad
			}
		}
	}
	if minPartLoad == math.MaxFloat64 {
		minPartLoad = 0
	}
	imbalance := 0.0
	if meanPartLoad > 0 {
		imbalance = (maxPartLoad - minPartLoad) / meanPartLoad
	}

	// Build heatmap: 256 buckets across the hash space, weighted by
	// combined load so the visualization matches what the slicer sees.
	const heatmapBuckets = 256
	heatmap := make([]float64, heatmapBuckets)
	bucketSize := (uint64(math.MaxUint32) + 1) / uint64(heatmapBuckets)
	for _, e := range current.Entries {
		stat := lastStats[e.Range]
		startBucket := uint64(e.Range.Lo) / bucketSize
		endBucket := uint64(e.Range.Hi) / bucketSize
		if endBucket >= heatmapBuckets {
			endBucket = heatmapBuckets - 1
		}
		bucketsSpanned := endBucket - startBucket + 1
		loadPerBucket := stat.Load / float64(bucketsSpanned)
		for b := startBucket; b <= endBucket; b++ {
			heatmap[b] += loadPerBucket
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

	data.TotalLoad = totalLoad
	data.TotalSamples = totalSamples
	data.TotalSeries = totalSeries
	data.TotalOrphan = totalOrphan
	data.MeanPartLoad = meanPartLoad
	data.MaxPartLoad = maxPartLoad
	data.MinPartLoad = minPartLoad
	data.ImbalanceRatio = imbalance
	data.NumPartitions = numPartitions
	data.NumEntries = len(current.Entries)
	data.MovedFraction = movedFraction
	data.Partitions = partitions
	data.Rounds = reversedRounds
	data.HeatmapData = string(heatmapJSON)

	return data
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
