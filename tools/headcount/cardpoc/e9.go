// SPDX-License-Identifier: AGPL-3.0-only

package cardpoc

import (
	"encoding/binary"
	"sort"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/tools/headcount/model"
)

// TopNEntry is one metric name and a series count for it.
type TopNEntry struct {
	Name  string
	Count int
}

// E9Result is RunE9's outcome: which metric names the two-round
// threshold protocol found above its own guaranteed-correct threshold,
// checked against the population's own truth, plus how many of them a
// naive (single-round, no cross-gateway dedup) ranking would have missed
// entirely.
type E9Result struct {
	K, TopN   int
	Threshold int
	// Above are the names RunE9 found with a true count over Threshold,
	// sorted by count, descending.
	Above []TopNEntry
	// Truth is the same computed independently from the population, over
	// every metric name in the snapshot, not just the protocol's
	// candidates: this is what actually tests whether Threshold and the
	// candidate set the protocol built were wide enough.
	Truth []TopNEntry
	// NaiveMisses are names in Truth that a naive per-source ranking
	// (round 1 only, no cross-gateway dedup) excludes from its own
	// top-TopN list.
	NaiveMisses []string
}

// Pass reports whether Above exactly matches Truth, name for name and
// count for count.
func (r E9Result) Pass() bool {
	if len(r.Above) != len(r.Truth) {
		return false
	}
	want := make(map[string]int, len(r.Truth))
	for _, e := range r.Truth {
		want[e.Name] = e.Count
	}
	for _, e := range r.Above {
		if want[e.Name] != e.Count {
			return false
		}
	}
	return true
}

// RunE9 simulates K store-gateways over bucketDir's blocks (assigned by a
// hash of the block ID) and runs a two-round threshold protocol for
// the top topN metric names by series count:
//
//   - Round 1: each gateway reports its own local top-topN names and
//     counts (exact within that gateway's own blocks).
//   - Threshold: every gateway that didn't report a name gave it, at
//     most, that gateway's own topN-th-place count (otherwise it would
//     have made that gateway's own list) -- so a name outside every
//     round-1 list can't have a true count above the sum of every
//     gateway's cutoff. That sum is Threshold: the point above which the
//     protocol can guarantee it has seen every name that belongs there.
//   - Round 2: for each round-1 candidate, fetch its exact count from
//     every gateway (not just those that listed it) and union them,
//     giving the true global count.
func RunE9(bucketDir string, pop *model.Model, k, topN int) (E9Result, error) {
	blocks, err := LoadBlocks(bucketDir)
	if err != nil {
		return E9Result{}, err
	}
	gateways := assignGateways(blocks, k)

	candidates := map[string]bool{}
	naiveScore := map[string]int{}
	threshold := 0
	for _, gw := range gateways {
		top, cutoff := localTopN(gw, topN)
		for _, e := range top {
			candidates[e.Name] = true
			naiveScore[e.Name] += e.Count
		}
		threshold += cutoff
	}

	var above []TopNEntry
	for name := range candidates {
		if c := len(hashesForName(blocks, name)); c > threshold {
			above = append(above, TopNEntry{name, c})
		}
	}
	sortDesc(above)

	allNames := map[string]bool{}
	for _, b := range blocks {
		for name := range b.SeriesByName {
			allNames[name] = true
		}
	}
	cfg := pop.Config()
	start, end := cfg.Start.UnixMilli(), cfg.End.UnixMilli()
	var truthAbove []TopNEntry
	for name := range allNames {
		matcher := labels.MustNewMatcher(labels.MatchEqual, "__name__", name)
		if c := pop.Truth([]*labels.Matcher{matcher}, start, end); c > threshold {
			truthAbove = append(truthAbove, TopNEntry{name, c})
		}
	}
	sortDesc(truthAbove)

	naiveTop := topNames(naiveScore, topN)
	var misses []string
	for _, e := range truthAbove {
		if !naiveTop[e.Name] {
			misses = append(misses, e.Name)
		}
	}

	return E9Result{K: k, TopN: topN, Threshold: threshold, Above: above, Truth: truthAbove, NaiveMisses: misses}, nil
}

// assignGateways splits blocks into k groups by their ID, simulating each
// group as one store-gateway's assigned blocks.
func assignGateways(blocks []Block, k int) [][]Block {
	gateways := make([][]Block, k)
	for _, b := range blocks {
		h := binary.BigEndian.Uint64(b.ID[:8])
		i := h % uint64(k)
		gateways[i] = append(gateways[i], b)
	}
	return gateways
}

// localTopN returns blocks' own exact top-n names by series count
// (deduplicated within blocks, since a name's series can span more than
// one of them), and the n-th (lowest) count among them -- 0 if there are
// fewer than n names at all.
func localTopN(blocks []Block, n int) ([]TopNEntry, int) {
	byName := map[string]map[uint64]struct{}{}
	for _, b := range blocks {
		for name, hashes := range b.SeriesByName {
			set := byName[name]
			if set == nil {
				set = map[uint64]struct{}{}
				byName[name] = set
			}
			for _, h := range hashes {
				set[h] = struct{}{}
			}
		}
	}

	entries := make([]TopNEntry, 0, len(byName))
	for name, set := range byName {
		entries = append(entries, TopNEntry{name, len(set)})
	}
	sortDesc(entries)

	if n > len(entries) {
		n = len(entries)
	}
	top := entries[:n]
	cutoff := 0
	if n > 0 {
		cutoff = top[n-1].Count
	}
	return top, cutoff
}

func sortDesc(entries []TopNEntry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Count != entries[j].Count {
			return entries[i].Count > entries[j].Count
		}
		return entries[i].Name < entries[j].Name // deterministic tiebreak.
	})
}

// topNames returns the set of the n highest-scoring names in scores.
func topNames(scores map[string]int, n int) map[string]bool {
	entries := make([]TopNEntry, 0, len(scores))
	for name, score := range scores {
		entries = append(entries, TopNEntry{name, score})
	}
	sortDesc(entries)

	if n > len(entries) {
		n = len(entries)
	}
	out := make(map[string]bool, n)
	for _, e := range entries[:n] {
		out[e.Name] = true
	}
	return out
}
