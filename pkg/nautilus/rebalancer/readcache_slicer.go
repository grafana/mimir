// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"flag"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/grafana/dskit/flagext"

	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
)

// ReadcacheSlicerConfig configures the second slicer round that
// balances (partition -> readcache instance). It is embedded into the
// rebalancer Config to keep the public flag surface flat.
type ReadcacheSlicerConfig struct {
	// Enabled gates the second slicer round on/off.
	Enabled bool `yaml:"enabled"`

	// Instances is the static list of readcache instance IDs the
	// rebalancer can assign partitions to. Once the readcache ring is
	// wired up (a follow-up patch) this becomes redundant and is
	// replaced by ring.GetReplicationSet. For Phase 2B it is the
	// canonical source of "active readcache instances".
	Instances flagext.StringSliceCSV `yaml:"instances"`

	// Alpha and Beta are the per-partition load weights:
	//
	//     load(pid) = Alpha * SamplesEWMA(pid) + Beta * QuerySamplesEWMA(pid)
	//
	// Alpha measures ingest pressure (samples-per-second flowing into
	// the partition's TSDB head, the same signal the first-tier
	// slicer balances on), Beta measures query CPU/IO pressure (cost
	// of serving reads). The slicer balances total load per readcache
	// instance, so two pods with the same Σ Alpha*write_rate +
	// Beta*query_rate are considered evenly loaded.
	Alpha float64 `yaml:"alpha"`
	Beta  float64 `yaml:"beta"`

	// MovementBudget is the maximum fraction of total load that may
	// be moved in a single round, expressed as a fraction in [0, 1].
	// Similar to Config.MovementBudget but applied to the
	// partition->instance mapping rather than to the hash space.
	MovementBudget float64 `yaml:"movement_budget"`

	// MoveCooldown is the minimum time between consecutive moves of
	// the same partition. Per-partition anti-flap guard.
	MoveCooldown time.Duration `yaml:"move_cooldown"`

	// RoundInterval is the minimum wall-clock interval between
	// consecutive runs of the readcache slicer round (the
	// partition->instance tier). When > 0, the rebalancer's main
	// loop still ticks at the tier-1 cadence (every
	// MinRebalanceInterval) but the tier-2 round only fires if at
	// least RoundInterval has elapsed since the last successful
	// fire, OR the active instance set changed since the last fire
	// (which would orphan partitions if we waited). When 0 (the
	// default), the tier-2 round runs every rebalance tick — the
	// pre-decoupling behavior.
	//
	// Rationale: every tier-2 move forces the destination
	// readcache's per-partition EWMA to start from zero, so the
	// next tier-1 round sees the partition's reported sample rate
	// drop to zero even though writes are still flowing. Spacing
	// tier-2 rounds out lets the destination EWMA settle (≈ 2-3
	// EWMA half-lives) before tier-1 next consults its rate.
	// The EWMA half-life is currently ~1 min. A longer production
	// interval may still be appropriate to absorb fleet and traffic
	// churn rather than merely waiting for the EWMA to settle.
	RoundInterval time.Duration `yaml:"round_interval"`
}

// RegisterFlagsWithPrefix registers the slicer's flags on f under
// the given prefix (typically "nautilus-rebalancer.readcache-slicer.").
func (cfg *ReadcacheSlicerConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, prefix+"enabled", false, "Enable the second slicer round that balances partition->readcache-instance mappings.")
	f.Var(&cfg.Instances, prefix+"instances", "Comma-separated list of readcache instance IDs the rebalancer may assign partitions to. Replaced by ring-based discovery in a follow-up patch.")
	f.Float64Var(&cfg.Alpha, prefix+"alpha", 1.0, "Weight applied to per-partition samples-per-second EWMA (write rate) in the load function. This is the same signal the first-tier slicer balances ranges on.")
	f.Float64Var(&cfg.Beta, prefix+"beta", 0.0, "Weight applied to per-partition query samples EWMA (read rate) in the load function. Zero (the default) makes the slicer balance write pressure only; set to a non-zero value once query-load telemetry is reliable.")
	f.Float64Var(&cfg.MovementBudget, prefix+"movement-budget", 0.10, "Maximum fraction of total load that may be moved between instances in a single round.")
	f.DurationVar(&cfg.MoveCooldown, prefix+"move-cooldown", 5*time.Minute, "Minimum time between consecutive moves of the same partition.")
	f.DurationVar(&cfg.RoundInterval, prefix+"round-interval", 0, "Minimum wall-clock interval between consecutive readcache slicer rounds. When >0, the tier-1 (hash-range) slicer still runs every rebalance tick, but the tier-2 (partition->readcache) slicer only fires after this interval has elapsed since the last successful fire (or if the active readcache instance set changed, to avoid orphaning partitions on scale-down/restart). Decouples tier-2 churn from tier-1 cadence so destination readcaches can fully build their per-partition EWMAs before tier-1 next consults them. 0 (the default) preserves the legacy behavior of running both tiers on every tick.")
}

// readcachePlanInput is the input the rebalancer collects each round
// for the readcache slicer.
type readcachePlanInput struct {
	// Active partition IDs the cluster is currently consuming.
	partitions []int32

	// loadByPartition is the weighted load per partition (alpha *
	// samples_ewma + beta * query_samples_ewma) used by the slicer.
	loadByPartition map[int32]float64

	// instances is the set of readcache instance IDs eligible to own
	// partitions.
	instances []string

	// currentOwner is the existing single-owner mapping
	// (partition -> instance ID), or "" if no entry was active in the
	// log at planning time. The slicer prefers to keep partitions on
	// their current owner unless rebalancing is needed.
	currentOwner map[int32]string

	// recentlyMoved is the set of partitions whose move-cooldown has
	// not yet expired. Excluded from this round's movable set.
	recentlyMoved map[int32]struct{}

	// excludedTargets is the set of instance IDs that must NOT be
	// chosen as a destination this round. The canonical case is an
	// instance whose HashRangeStats RPC failed during stats
	// collection: it aggregates as zero load (collectRatesFromReadcaches
	// skips it), which would otherwise make it the apparent
	// lightestInstance and turn a flaky/unreachable pod into the
	// round's pile target. Excluded instances keep the partitions
	// they already own (pass 1) but cannot receive new ones (pass 2
	// cold placement and pass 3 rebalance). If every eligible instance
	// is excluded the exclusion is ignored, since partitions must land
	// somewhere.
	excludedTargets map[string]struct{}
}

// readcachePlan represents the slicer's output.
type readcachePlan struct {
	// Assignment is the proposed (partition -> instance) mapping.
	Assignment *readcacheassignment.Assignment

	// Moves lists the partitions that changed owner. Used for trace,
	// metrics, and to populate the cooldown map.
	Moves []readcacheMove

	// LoadByInstance is the post-plan total load per instance,
	// included for trace and metrics.
	LoadByInstance map[string]float64
}

type readcacheMove struct {
	PartitionID int32
	From, To    string
	Load        float64
	// Reason is a short human-readable explanation of why the
	// slicer chose to move this partition. Populated by
	// planReadcacheAssignment so the admin page and trace can
	// answer "why did P_n move from rc-x to rc-y?" without
	// re-running the round.
	Reason string
}

// planReadcacheAssignment runs the second slicer round and returns
// the proposed mapping. The algorithm is intentionally simple for
// Phase 2B:
//
//  1. Compute total load.
//  2. Target per-instance load = total / N_instances.
//  3. Walk currently-overloaded instances in descending load order;
//     migrate their heaviest *movable* partition to the
//     currently-lightest instance until the source is at or below
//     target, or no more movable partitions remain.
//  4. Stop once movedLoad / totalLoad exceeds MovementBudget.
//
// Cooldowns gate which partitions are movable. Partitions not in the
// current log get assigned to whichever instance is lightest at the
// time, which is the cold-start path.
func planReadcacheAssignment(cfg ReadcacheSlicerConfig, in readcachePlanInput) readcachePlan {
	if len(in.instances) == 0 {
		return readcachePlan{Assignment: &readcacheassignment.Assignment{}, LoadByInstance: map[string]float64{}}
	}

	// Seed the proposed mapping with the current owner where known,
	// or "unassigned" otherwise. The unassigned partitions are
	// assigned in step 1 below by picking the lightest instance.
	proposed := make(map[int32]string, len(in.partitions))
	loadByInstance := make(map[string]float64, len(in.instances))
	for _, inst := range in.instances {
		loadByInstance[inst] = 0
	}

	// Sort partitions by ID for deterministic iteration (mostly for
	// test stability).
	sort.Slice(in.partitions, func(i, j int) bool { return in.partitions[i] < in.partitions[j] })

	// Pass 1: keep the current owner if it's in the eligible
	// instance set; otherwise leave unassigned for pass 2 to place.
	instanceSet := make(map[string]struct{}, len(in.instances))
	for _, inst := range in.instances {
		instanceSet[inst] = struct{}{}
	}
	var unassigned []int32
	for _, pid := range in.partitions {
		curr := in.currentOwner[pid]
		if _, ok := instanceSet[curr]; curr != "" && ok {
			proposed[pid] = curr
			loadByInstance[curr] += in.loadByPartition[pid]
		} else {
			unassigned = append(unassigned, pid)
		}
	}

	// Pass 2: assign unassigned partitions to the lightest instance.
	for _, pid := range unassigned {
		target := lightestInstance(loadByInstance, in.instances, in.excludedTargets)
		proposed[pid] = target
		loadByInstance[target] += in.loadByPartition[pid]
	}

	// Pass 3: rebalance. Compute mean and migrate from over-target
	// instances to under-target instances until movement budget is
	// exhausted or all instances are within target.
	var totalLoad float64
	for _, l := range loadByInstance {
		totalLoad += l
	}
	target := totalLoad / float64(len(in.instances))

	var moves []readcacheMove
	movedLoad := 0.0
	moveBudgetAbs := cfg.MovementBudget * totalLoad

	for movedLoad < moveBudgetAbs {
		src := heaviestInstance(loadByInstance, in.instances)
		dst := lightestInstance(loadByInstance, in.instances, in.excludedTargets)
		if src == dst {
			break
		}
		if loadByInstance[src] <= target {
			break
		}
		// Find the heaviest movable partition currently owned by src.
		var bestPID int32 = -1
		var bestLoad float64 = -1
		for _, pid := range in.partitions {
			if proposed[pid] != src {
				continue
			}
			if _, cooling := in.recentlyMoved[pid]; cooling {
				continue
			}
			if l := in.loadByPartition[pid]; l > bestLoad {
				bestLoad = l
				bestPID = pid
			}
		}
		if bestPID < 0 {
			break
		}
		// Don't make things worse: skip if moving lands dst above src.
		if loadByInstance[dst]+bestLoad >= loadByInstance[src] {
			break
		}
		from := proposed[bestPID]
		srcLoadBefore := loadByInstance[from]
		dstLoadBefore := loadByInstance[dst]
		proposed[bestPID] = dst
		loadByInstance[from] -= bestLoad
		loadByInstance[dst] += bestLoad
		moves = append(moves, readcacheMove{
			PartitionID: bestPID,
			From:        from,
			To:          dst,
			Load:        bestLoad,
			Reason: fmt.Sprintf(
				"src %s over target by %.1f (load=%.1f, target=%.1f); dst %s at %.1f; partition load=%.1f",
				from, srcLoadBefore-target, srcLoadBefore, target, dst, dstLoadBefore, bestLoad,
			),
		})
		movedLoad += bestLoad
	}

	// Build the output assignment, sorted by partition ID for
	// determinism.
	out := &readcacheassignment.Assignment{Entries: make([]readcacheassignment.AssignmentEntry, 0, len(in.partitions))}
	for _, pid := range in.partitions {
		out.Entries = append(out.Entries, readcacheassignment.AssignmentEntry{PartitionID: pid, InstanceID: proposed[pid]})
	}

	return readcachePlan{Assignment: out, Moves: moves, LoadByInstance: loadByInstance}
}

// lightestInstance returns the eligible instance with the lowest
// load, breaking ties on instance ID for determinism. Instances in
// `excluded` are skipped as destinations (e.g. an instance whose
// stats RPC failed this round). If every instance is excluded the
// exclusion is ignored — partitions must be placed somewhere, so we
// fall back to choosing among all instances.
func lightestInstance(loadByInstance map[string]float64, instances []string, excluded map[string]struct{}) string {
	var best string
	var bestLoad float64
	first := true
	// Iterate in a deterministic order so ties resolve consistently.
	sorted := append([]string(nil), instances...)
	sort.Strings(sorted)
	for _, inst := range sorted {
		if _, skip := excluded[inst]; skip {
			continue
		}
		l := loadByInstance[inst]
		if first || l < bestLoad {
			best = inst
			bestLoad = l
			first = false
		}
	}
	// Every instance was excluded: fall back to the unfiltered pick so
	// we never return "" and drop partitions on the floor.
	if first {
		return lightestInstance(loadByInstance, instances, nil)
	}
	return best
}

func heaviestInstance(loadByInstance map[string]float64, instances []string) string {
	var best string
	var bestLoad float64
	first := true
	sorted := append([]string(nil), instances...)
	sort.Strings(sorted)
	for _, inst := range sorted {
		l := loadByInstance[inst]
		if first || l > bestLoad {
			best = inst
			bestLoad = l
			first = false
		}
	}
	return best
}

// readcacheMoveCooldowns is the cool-down map used to suppress
// repeated moves of the same partition within Config.MoveCooldown.
type readcacheMoveCooldowns map[int32]time.Time

// stillCooling reports the subset of partitions whose cooldown has
// not yet expired at `at`.
func (c readcacheMoveCooldowns) stillCooling(at time.Time) map[int32]struct{} {
	out := make(map[int32]struct{}, len(c))
	for pid, until := range c {
		if until.After(at) {
			out[pid] = struct{}{}
		}
	}
	return out
}

// extendForMoves records cooldowns for the partitions just moved.
func (c readcacheMoveCooldowns) extendForMoves(at time.Time, dur time.Duration, moves []readcacheMove) {
	until := at.Add(dur)
	for _, m := range moves {
		c[m.PartitionID] = until
	}
}

// prune removes cooldowns whose expiry is already in the past.
func (c readcacheMoveCooldowns) prune(at time.Time) {
	for pid, until := range c {
		if !until.After(at) {
			delete(c, pid)
		}
	}
}

// String returns a one-line summary of the cooldown set, useful in
// debug logs.
func (c readcacheMoveCooldowns) String() string {
	if len(c) == 0 {
		return "{}"
	}
	pids := make([]int, 0, len(c))
	for pid := range c {
		pids = append(pids, int(pid))
	}
	sort.Ints(pids)
	var sb strings.Builder
	sb.WriteString("{")
	for i, pid := range pids {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(strings.TrimSpace(c[int32(pid)].String()))
	}
	sb.WriteString("}")
	return sb.String()
}
