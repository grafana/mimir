// Package mpchash implements a multi-probe consistent hashing strategy
/*
http://arxiv.org/pdf/1505.00062.pdf
*/
package mpchash

import (
	"math"
	"sort"
)

// Multi selects buckets with a multi-probe consistent hash
type Multi struct {
	buckets []string
	seeds   [2]uint64
	hashf   func(b []byte, s uint64) uint64
	k       int

	bmap map[uint64]string

	// We store sorted slices of hashes by bit prefix
	bhashes     [][]uint64
	prefixmask  uint64
	prefixshift uint64
}

// New returns a new multi-probe hasher.  The hash function h is used with the two seeds to generate k different probes.
func New(buckets []string, h func(b []byte, s uint64) uint64, seeds [2]uint64, k int) *Multi {

	m := &Multi{
		buckets: make([]string, len(buckets)),
		hashf:   h,
		seeds:   seeds,
		bmap:    make(map[uint64]string, len(buckets)),
		k:       k,
	}

	copy(m.buckets, buckets)

	const desiredCollisionRate = 6
	prefixlen := len(buckets) / desiredCollisionRate
	psize := ilog2(prefixlen)

	m.prefixmask = ((1 << psize) - 1) << (64 - psize)
	m.prefixshift = 64 - psize

	m.bhashes = make([][]uint64, 1<<psize)

	for _, b := range buckets {
		h := m.hashf([]byte(b), 0)
		prefix := (h & m.prefixmask) >> m.prefixshift

		m.bhashes[prefix] = append(m.bhashes[prefix], h)
		m.bmap[h] = b
	}

	for _, v := range m.bhashes {
		sort.Sort(uint64Slice(v))
	}

	return m
}

// Hash returns the bucket for a given key
func (m *Multi) Hash(key string) string {
	bkey := []byte(key)

	minDistance := uint64(math.MaxUint64)

	var minhash uint64

	h1 := m.hashf(bkey, m.seeds[0])
	h2 := m.hashf(bkey, m.seeds[1])

	for i := 0; i < m.k; i++ {
		hash := h1 + uint64(i)*h2
		prefix := (hash & m.prefixmask) >> m.prefixshift

		var node uint64
	FOUND:
		for {
			uints := m.bhashes[prefix]

			for _, v := range uints {
				if hash < v {
					node = v
					break FOUND
				}
			}

			prefix++
			if prefix == uint64(len(m.bhashes)) {
				prefix = 0
				// wrapped -- take the first node hash we can find
				for uints = nil; uints == nil; prefix++ {
					uints = m.bhashes[prefix]
				}

				node = uints[0]
				break FOUND
			}
		}

		distance := node - hash
		if distance < minDistance {
			minDistance = distance
			minhash = node
		}
	}

	return m.bmap[minhash]
}

type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// integer log base 2
func ilog2(v int) uint64 {
	var r uint64
	for ; v != 0; v >>= 1 {
		r++
	}
	return r
}
