// SPDX-License-Identifier: AGPL-3.0-only

package explore

import (
	"math/rand/v2"
)

// exhaustiveLimit is the largest number of combinations for which every combination is explored.
// Above it, the explorer only guarantees that every pair of values is explored.
const exhaustiveLimit = 1024

type dimension struct {
	name   string
	values []string
}

// explorer maps slot numbers to combinations of dimension values. Slots are grouped into
// cycles of equal length. Each cycle covers every value, and every pair of values across
// dimensions, at least once.
type explorer struct {
	seed uint64
	dims []dimension
	// rows is the covering order shared by all cycles. Each row holds one value index per dimension.
	rows [][]int

	cachedPerm *cyclePermutation
}

// cyclePermutation reorders the rows and relabels the values of each dimension for one cycle.
// Relabeling values keeps the pairwise coverage of rows, while making each cycle explore a
// different set of combinations.
type cyclePermutation struct {
	cycle  int64
	rows   []int
	values [][]int
}

func newExplorer(seed uint64, dims []dimension) *explorer {
	sizes := make([]int, len(dims))
	for i, d := range dims {
		sizes[i] = len(d.values)
	}
	rng := newRand(seed, 0)

	var rows [][]int
	if product(sizes) <= exhaustiveLimit {
		rows = allCombinations(sizes)
	} else {
		rows = pairwiseCombinations(sizes, rng)
	}
	return &explorer{seed: seed, dims: dims, rows: rows}
}

// cycleLength returns the number of slots in each cycle.
func (e *explorer) cycleLength() int64 {
	return int64(len(e.rows))
}

// combination returns the value of each dimension for the given slot, in the order of dimensions.
func (e *explorer) combination(slot int64) []string {
	cycle, pos := slot/e.cycleLength(), slot%e.cycleLength()
	if pos < 0 {
		cycle, pos = cycle-1, pos+e.cycleLength()
	}
	perm := e.permutation(cycle)

	row := e.rows[perm.rows[pos]]
	out := make([]string, len(e.dims))
	for d, dim := range e.dims {
		out[d] = dim.values[perm.values[d][row[d]]]
	}
	return out
}

func (e *explorer) permutation(cycle int64) *cyclePermutation {
	if e.cachedPerm != nil && e.cachedPerm.cycle == cycle {
		return e.cachedPerm
	}
	rng := newRand(e.seed, uint64(cycle)+1)
	perm := &cyclePermutation{
		cycle:  cycle,
		rows:   rng.Perm(len(e.rows)),
		values: make([][]int, len(e.dims)),
	}
	for d, dim := range e.dims {
		perm.values[d] = rng.Perm(len(dim.values))
	}
	e.cachedPerm = perm
	return perm
}

func product(sizes []int) int {
	p := 1
	for _, s := range sizes {
		p *= s
		if p > exhaustiveLimit {
			return p
		}
	}
	return p
}

func allCombinations(sizes []int) [][]int {
	rows := [][]int{make([]int, len(sizes))}
	for d, size := range sizes {
		next := make([][]int, 0, len(rows)*size)
		for _, row := range rows {
			for v := range size {
				r := append([]int(nil), row...)
				r[d] = v
				next = append(next, r)
			}
		}
		rows = next
	}
	return rows
}

// pairwiseCombinations greedily builds rows until every pair of values across two dimensions
// appears in at least one row. Each row starts from an uncovered pair, so every row covers at
// least one new pair and the loop terminates.
func pairwiseCombinations(sizes []int, rng *rand.Rand) [][]int {
	pairs := newPairSet(sizes)
	var rows [][]int
	for pairs.remaining > 0 {
		row := make([]int, len(sizes))
		for d := range row {
			row[d] = -1
		}
		d1, v1, d2, v2 := pairs.firstUncovered(rng.IntN(len(pairs.covered)))
		row[d1], row[d2] = v1, v2

		for _, d := range rng.Perm(len(sizes)) {
			if row[d] >= 0 {
				continue
			}
			best, bestScore := -1, -1
			for _, v := range rng.Perm(sizes[d]) {
				if score := pairs.newlyCovered(row, d, v); score > bestScore {
					best, bestScore = v, score
				}
			}
			row[d] = best
		}
		pairs.cover(row)
		rows = append(rows, row)
	}
	return rows
}

type pairSet struct {
	sizes []int
	// offsets[d1][d2] is the index of the first pair of dimensions d1 and d2, for d1 < d2.
	offsets   [][]int
	covered   []bool
	remaining int
}

func newPairSet(sizes []int) *pairSet {
	p := &pairSet{sizes: sizes, offsets: make([][]int, len(sizes))}
	n := 0
	for d1 := range sizes {
		p.offsets[d1] = make([]int, len(sizes))
		for d2 := d1 + 1; d2 < len(sizes); d2++ {
			p.offsets[d1][d2] = n
			n += sizes[d1] * sizes[d2]
		}
	}
	p.covered = make([]bool, n)
	p.remaining = n
	return p
}

func (p *pairSet) index(d1, v1, d2, v2 int) int {
	if d1 > d2 {
		d1, v1, d2, v2 = d2, v2, d1, v1
	}
	return p.offsets[d1][d2] + v1*p.sizes[d2] + v2
}

// firstUncovered returns the first uncovered pair at or after index start, wrapping around.
func (p *pairSet) firstUncovered(start int) (d1, v1, d2, v2 int) {
	for i := range p.covered {
		idx := (start + i) % len(p.covered)
		if p.covered[idx] {
			continue
		}
		for d1 = range p.sizes {
			for d2 = d1 + 1; d2 < len(p.sizes); d2++ {
				off := idx - p.offsets[d1][d2]
				if off >= 0 && off < p.sizes[d1]*p.sizes[d2] {
					return d1, off / p.sizes[d2], d2, off % p.sizes[d2]
				}
			}
		}
	}
	panic("no uncovered pair left")
}

// newlyCovered returns how many uncovered pairs setting dimension d to value v would cover,
// given the dimensions already set in row.
func (p *pairSet) newlyCovered(row []int, d, v int) int {
	n := 0
	for other, ov := range row {
		if other != d && ov >= 0 && !p.covered[p.index(d, v, other, ov)] {
			n++
		}
	}
	return n
}

func (p *pairSet) cover(row []int) {
	for d1 := range row {
		for d2 := d1 + 1; d2 < len(row); d2++ {
			idx := p.index(d1, row[d1], d2, row[d2])
			if !p.covered[idx] {
				p.covered[idx] = true
				p.remaining--
			}
		}
	}
}

// newRand returns a deterministic random number generator for the given seed and stream.
func newRand(seed, stream uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, stream))
}

// mix deterministically combines values into a well-distributed 64-bit hash.
func mix(values ...uint64) uint64 {
	h := uint64(0x9e3779b97f4a7c15)
	for _, v := range values {
		h ^= v + 0x9e3779b97f4a7c15 + (h << 6) + (h >> 2)
		h = splitmix64(h)
	}
	return h
}

func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}
