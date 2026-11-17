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
	// series holds, for each protocol, every series that the protocol can write.
	series map[Protocol][]Series
}

func newExplorer(seed uint64, def Definition, exclusions []exclusion) (*explorer, error) {
	dims := def.dimensions()
	e := &explorer{protocols: def.Protocols, writeSizes: def.WriteSizes, series: map[Protocol][]Series{}}
	for _, values := range allCombinations(dims) {
		s, err := newSeries(seed, dims, values)
		if err != nil {
			return nil, err
		}
		for _, p := range def.Protocols {
			if allowed(exclusions, p, s) {
				e.series[p] = append(e.series[p], s)
			}
		}
	}
	for _, p := range def.Protocols {
		if len(e.series[p]) == 0 {
			return nil, fmt.Errorf("protocol %s can't write any combination of dimension values", p)
		}
	}
	return e, nil
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
// integer in [0, n).
func (e *explorer) pick(choose func(n int) int) (Protocol, []Series) {
	protocol := e.protocols[choose(len(e.protocols))]
	size := e.writeSizes[choose(len(e.writeSizes))]
	candidates := slices.Clone(e.series[protocol])
	size = min(size, len(candidates))
	for i := range size {
		j := i + choose(len(candidates)-i)
		candidates[i], candidates[j] = candidates[j], candidates[i]
	}
	return protocol, candidates[:size]
}

// numSeries returns the number of distinct series that some protocol can write.
func (e *explorer) numSeries() int {
	seen := map[string]bool{}
	for _, series := range e.series {
		for _, s := range series {
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
