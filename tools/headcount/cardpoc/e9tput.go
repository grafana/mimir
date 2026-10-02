// SPDX-License-Identifier: AGPL-3.0-only

package cardpoc

// E9 TPUT: the same top-N question as E9 with a TPUT-shaped threshold protocol,
// with and without cross-gateway dedup of shared series.

import (
	"github.com/grafana/mimir/tools/headcount/model"
)

// E9TPUTResult is RunE9TPUT's outcome for one gateway count K.
type E9TPUTResult struct {
	K, TopN int
	// Dedup is set when T came from deduplicated counts of the round-1
	// candidates and the answer from a third round of exact counts.
	// Unset, the protocol runs as a sum of what the gateways report.
	Dedup bool

	// Threshold is T: the N-th largest round-1 partial sum, or with Dedup
	// the N-th largest exact count among the round-1 candidates. A
	// gateway reports a name in round 2 when its local count times K is
	// at least T.
	Threshold int

	// Answer is the protocol's top N, with the counts it would return.
	Answer []TopNEntry
	// Truth is the population's own top N over every name.
	Truth []TopNEntry

	// Round1Entries and Round2Entries are the (gateway, name, count)
	// entries sent in each round. ExactNames is how many names needed
	// an exact count across every gateway (round 1 and round 3 with
	// Dedup, none without).
	Round1Entries, Round2Entries, ExactNames int
}

// Pass reports whether Answer equals Truth, name for name and count for
// count.
func (r E9TPUTResult) Pass() bool {
	if len(r.Answer) != len(r.Truth) {
		return false
	}
	for i := range r.Answer {
		if r.Answer[i] != r.Truth[i] {
			return false
		}
	}
	return true
}

// Missed returns the names in Truth that are not in Answer.
func (r E9TPUTResult) Missed() []string {
	got := map[string]bool{}
	for _, e := range r.Answer {
		got[e.Name] = true
	}
	var out []string
	for _, e := range r.Truth {
		if !got[e.Name] {
			out = append(out, e.Name)
		}
	}
	return out
}

// RunE9TPUT simulates K store-gateways over bucketDir's blocks (assigned by
// a hash of the block ID, as in RunE9) and runs the threshold protocol in
// the shape of TPUT (Cao and Wang, 2004) for the top topN metric names:
//
//   - Round 1: each gateway reports its local top topN names with local
//     counts, deduplicated within its own blocks.
//   - T is the topN-th largest partial sum of those reports. A name's
//     total is at least its partial sum only when gateways share no
//     series; when they do, a partial sum can exceed the total.
//   - Round 2: each gateway reports every name whose local count is at
//     least T/K. If a name's total is at least T, some gateway holds at
//     least T/K of it, when totals are sums over gateways.
//   - Without dedup, the answer is the top topN names by the sum of their
//     round-2 reports. With dedup, T is instead the topN-th largest exact
//     (hash-union) count among the round-1 candidates, which is a true
//     lower bound on the topN-th total whatever the overlap, and a third
//     round fetches exact counts for every round-2 name.
func RunE9TPUT(bucketDir string, pop *model.Model, k, topN int, dedup bool) (E9TPUTResult, error) {
	blocks, err := LoadBlocks(bucketDir)
	if err != nil {
		return E9TPUTResult{}, err
	}
	gateways := assignGateways(blocks, k)
	local := make([]map[string]int, len(gateways))
	for i, gw := range gateways {
		local[i] = gatewayCounts(gw)
	}
	res := E9TPUTResult{K: k, TopN: topN, Dedup: dedup}

	// Round 1.
	partial := map[string]int{}
	for i, gw := range gateways {
		top, _ := localTopN(gw, topN)
		res.Round1Entries += len(top)
		for _, e := range top {
			partial[e.Name] += local[i][e.Name]
		}
	}

	exact := map[string]int{}
	exactCount := func(name string) int {
		if c, ok := exact[name]; ok {
			return c
		}
		c := len(hashesForName(blocks, name))
		exact[name] = c
		return c
	}

	var t1 []TopNEntry
	for name, sum := range partial {
		if dedup {
			sum = exactCount(name)
		}
		t1 = append(t1, TopNEntry{name, sum})
	}
	sortDesc(t1)
	if len(t1) >= topN {
		res.Threshold = t1[topN-1].Count
	}

	// Round 2.
	reported := map[string]int{}
	for _, counts := range local {
		for name, c := range counts {
			if c*k >= res.Threshold {
				res.Round2Entries++
				reported[name] += c
			}
		}
	}

	var answer []TopNEntry
	for name, sum := range reported {
		if dedup {
			sum = exactCount(name)
		}
		answer = append(answer, TopNEntry{name, sum})
	}
	sortDesc(answer)
	res.Answer = answer[:min(topN, len(answer))]
	if dedup {
		res.ExactNames = len(exact)
	}

	var truth []TopNEntry
	for name, c := range populationTruthByName(pop) {
		truth = append(truth, TopNEntry{name, c})
	}
	sortDesc(truth)
	res.Truth = truth[:min(topN, len(truth))]
	return res, nil
}

// gatewayCounts returns every metric name's series count across blocks,
// deduplicated within them, as one store-gateway would report it.
func gatewayCounts(blocks []Block) map[string]int {
	sets := map[string]map[uint64]struct{}{}
	for _, b := range blocks {
		for name, hashes := range b.SeriesByName {
			set := sets[name]
			if set == nil {
				set = map[uint64]struct{}{}
				sets[name] = set
			}
			for _, h := range hashes {
				set[h] = struct{}{}
			}
		}
	}
	out := make(map[string]int, len(sets))
	for name, set := range sets {
		out[name] = len(set)
	}
	return out
}
