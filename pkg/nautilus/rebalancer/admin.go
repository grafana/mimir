// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// ActionKind describes what the slicer did to a hash range.
type ActionKind string

const (
	ActionMove       ActionKind = "move"
	ActionMerge      ActionKind = "merge"
	ActionSplit      ActionKind = "split"
	ActionReassign   ActionKind = "reassign"
)

// Action records a single slicer operation from a rebalance round.
type Action struct {
	Kind      ActionKind         `json:"kind"`
	Range     assignment.HashRange `json:"range"`
	FromPart  int32              `json:"from_partition,omitempty"`
	ToPart    int32              `json:"to_partition,omitempty"`
	Detail    string             `json:"detail,omitempty"`
}

// RoundLog captures the actions and summary stats from one rebalance round.
type RoundLog struct {
	Time          time.Time `json:"time"`
	TotalLoad     float64   `json:"total_load"`
	MeanPartLoad  float64   `json:"mean_partition_load"`
	MaxPartLoad   float64   `json:"max_partition_load"`
	MinPartLoad   float64   `json:"min_partition_load"`
	ImbalanceRatio float64  `json:"imbalance_ratio"`
	NumEntries    int       `json:"num_entries"`
	NumPartitions int       `json:"num_partitions"`
	MovedFraction float64   `json:"moved_fraction"`
	Actions       []Action  `json:"actions"`
}

const maxRoundLogs = 20

// adminState stores the data needed to render the admin page.
type adminState struct {
	mu        sync.RWMutex
	rounds    []RoundLog
	lastRates map[assignment.HashRange]float64
}

func (s *adminState) addRound(rl RoundLog) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rounds = append(s.rounds, rl)
	if len(s.rounds) > maxRoundLogs {
		s.rounds = s.rounds[len(s.rounds)-maxRoundLogs:]
	}
}

func (s *adminState) setLastRates(rates map[assignment.HashRange]float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastRates = rates
}

// snapshot returns the current admin state for rendering.
func (s *adminState) snapshot() ([]RoundLog, map[assignment.HashRange]float64) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rounds := make([]RoundLog, len(s.rounds))
	copy(rounds, s.rounds)

	rates := make(map[assignment.HashRange]float64, len(s.lastRates))
	for k, v := range s.lastRates {
		rates[k] = v
	}
	return rounds, rates
}

// partitionView is the data for one partition row in the admin page.
type partitionView struct {
	PartitionID   int32
	InstanceID    string
	InstanceAddr  string
	TotalLoad     float64
	HashSpacePct  float64
	NumRanges     int
	Ranges        []rangeView
}

// rangeView is the data for one hash range in the admin page.
type rangeView struct {
	Lo           uint32
	Hi           uint32
	Load         float64
	SizePct      float64
	LastAction   ActionKind
}

// adminPageData is the full data structure passed to the template.
type adminPageData struct {
	GeneratedAt    string
	TotalLoad      float64
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
}

func (r *Rebalancer) buildAdminPageData() adminPageData {
	rounds, lastRates := r.admin.snapshot()
	current := r.store.latest()

	data := adminPageData{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
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
	var totalLoad float64

	for _, e := range current.Entries {
		pv, ok := partMap[e.PartitionID]
		if !ok {
			pv = &partitionView{PartitionID: e.PartitionID}
			partMap[e.PartitionID] = pv
		}

		rate := lastRates[e.Range]
		sizePct := float64(e.Range.Size()) / hashSpaceTotal * 100

		action, _ := lastActions[e.Range]

		pv.Ranges = append(pv.Ranges, rangeView{
			Lo:         e.Range.Lo,
			Hi:         e.Range.Hi,
			Load:       rate,
			SizePct:    sizePct,
			LastAction: action,
		})
		pv.TotalLoad += rate
		pv.HashSpacePct += sizePct
		pv.NumRanges++
		totalLoad += rate
	}

	// Resolve instance IDs from the partition ring.
	pRing := r.partitionRing.PartitionRing()
	instances, _ := r.ingesterRing.GetAllHealthy(0)
	idToAddr := make(map[string]string)
	for _, inst := range instances.Instances {
		idToAddr[inst.GetId()] = inst.Addr
	}

	for pid, pv := range partMap {
		owners := pRing.PartitionOwnerIDs(pid)
		if len(owners) > 0 {
			pv.InstanceID = owners[0]
			pv.InstanceAddr = idToAddr[owners[0]]
		}
	}

	// Sort partitions by ID.
	partitions := make([]partitionView, 0, len(partMap))
	for _, pv := range partMap {
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

	// Build heatmap: 256 buckets across the hash space.
	const heatmapBuckets = 256
	heatmap := make([]float64, heatmapBuckets)
	bucketSize := (uint64(math.MaxUint32) + 1) / uint64(heatmapBuckets)
	for _, e := range current.Entries {
		rate := lastRates[e.Range]
		startBucket := uint64(e.Range.Lo) / bucketSize
		endBucket := uint64(e.Range.Hi) / bucketSize
		if endBucket >= heatmapBuckets {
			endBucket = heatmapBuckets - 1
		}
		bucketsSpanned := endBucket - startBucket + 1
		ratePerBucket := rate / float64(bucketsSpanned)
		for b := startBucket; b <= endBucket; b++ {
			heatmap[b] += ratePerBucket
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

func (r *Rebalancer) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	data := r.buildAdminPageData()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := adminTemplate.Execute(w, data); err != nil {
		http.Error(w, fmt.Sprintf("template error: %v", err), http.StatusInternalServerError)
	}
}
