// SPDX-License-Identifier: AGPL-3.0-only

package explore

import (
	"fmt"
	"math/rand/v2"
	"slices"
)

type dimension struct {
	name   string
	values []string
}

// exclusion is a pair of values of two dimensions that are never written together. The protocol of
// a write counts as a dimension.
type exclusion struct {
	dim1, value1, dim2, value2 string
}

// explorer picks the protocol and the series of each write.
type explorer struct {
	protocols  []Protocol
	writeSizes []int
	// families holds, for each protocol, the series of each family that the protocol can write, in
	// the order of the definition. Families that the protocol can't write any series of are left out.
	families map[Protocol][]familySeries
}

// familySeries holds the dimensions of a family and the series of the family that a protocol can
// write, in the order of their dimension values.
type familySeries struct {
	dims   []dimension
	series []Series
}

func newExplorer(seed uint64, def Definition, exclusions []exclusion) (*explorer, error) {
	e := &explorer{protocols: def.Protocols, writeSizes: def.WriteSizes, families: map[Protocol][]familySeries{}}
	for _, dims := range def.families() {
		series := map[Protocol][]Series{}
		for _, values := range allCombinations(dims) {
			s, err := newSeries(seed, dims, values)
			if err != nil {
				return nil, err
			}
			for _, p := range def.Protocols {
				if allowed(exclusions, p, s) {
					series[p] = append(series[p], s)
				}
			}
		}
		for _, p := range def.Protocols {
			if len(series[p]) > 0 {
				e.families[p] = append(e.families[p], familySeries{dims: dims, series: series[p]})
			}
		}
	}
	for _, p := range def.Protocols {
		if len(e.families[p]) == 0 {
			return nil, fmt.Errorf("protocol %s can't write any combination of dimension values", p)
		}
	}
	return e, nil
}

// allSeries returns every series that the protocol can write.
func (e *explorer) allSeries(protocol Protocol) []Series {
	var series []Series
	for _, f := range e.families[protocol] {
		series = append(series, f.series...)
	}
	return series
}

// allowed returns whether the protocol can write the series.
func allowed(exclusions []exclusion, protocol Protocol, s Series) bool {
	values := map[string]string{labelProtocol: string(protocol)}
	for _, l := range s.Dimensions {
		values[l.Name] = l.Value
	}
	for _, ex := range exclusions {
		if values[ex.dim1] == ex.value1 && values[ex.dim2] == ex.value2 {
			return false
		}
	}
	return true
}

// pick returns the protocol of a write and the distinct series it writes. choose returns a random
// integer in [0, n). Each series of the write is drawn in two steps: a family, with the same chance
// for every family that has series left to pick, and then the value of each dimension of the
// family in turn.
func (e *explorer) pick(choose func(n int) int) (Protocol, []Series) {
	protocol := e.protocols[draw(choose, len(e.protocols))]
	size := e.writeSizes[draw(choose, len(e.writeSizes))]
	families := e.families[protocol]
	left := make([][]Series, len(families))
	for i, f := range families {
		left[i] = slices.Clone(f.series)
	}
	var picked []Series
	for len(picked) < size {
		var candidates []int
		for i := range left {
			if len(left[i]) > 0 {
				candidates = append(candidates, i)
			}
		}
		if len(candidates) == 0 {
			break
		}
		f := candidates[draw(choose, len(candidates))]
		i := drawSeries(choose, families[f].dims, left[f])
		picked = append(picked, left[f][i])
		left[f] = slices.Delete(left[f], i, i+1)
	}
	return protocol, picked
}

// drawSeries draws one of series, which have the given dimensions, and returns its index. It draws
// the value of each dimension in turn, among the values of the series that have the values drawn
// before, so values that can't be written with the values drawn before aren't offered.
func drawSeries(choose func(n int) int, dims []dimension, series []Series) int {
	candidates := make([]int, len(series))
	for i := range candidates {
		candidates[i] = i
	}
	for d, dim := range dims {
		hasValue := func(v string) func(int) bool {
			return func(i int) bool { return series[i].Dimensions[d].Value == v }
		}
		var values []string
		for _, v := range dim.values {
			if slices.ContainsFunc(candidates, hasValue(v)) {
				values = append(values, v)
			}
		}
		has := hasValue(values[draw(choose, len(values))])
		candidates = slices.DeleteFunc(candidates, func(i int) bool { return !has(i) })
	}
	return candidates[0]
}

// draw returns a random integer in [0, n), without calling choose when there is a single choice.
func draw(choose func(n int) int, n int) int {
	if n == 1 {
		return 0
	}
	return choose(n)
}

// numSeries returns the number of distinct series that some protocol can write.
func (e *explorer) numSeries() int {
	seen := map[string]bool{}
	for _, p := range e.protocols {
		for _, s := range e.allSeries(p) {
			seen[s.String()] = true
		}
	}
	return len(seen)
}

// allCombinations returns every combination of dimension values, in the order of dims.
func allCombinations(dims []dimension) [][]string {
	combinations := [][]string{nil}
	for _, d := range dims {
		next := make([][]string, 0, len(combinations)*len(d.values))
		for _, c := range combinations {
			for _, v := range d.values {
				next = append(next, append(slices.Clip(c), v))
			}
		}
		combinations = next
	}
	return combinations
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
