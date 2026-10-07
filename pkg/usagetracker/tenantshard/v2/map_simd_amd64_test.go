// SPDX-License-Identifier: AGPL-3.0-only

//go:build amd64.v3 && !nosimd

package v2

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex_Match(t *testing.T) {
	m := &index{0x12, 0x34, 0x56, 0xc9, 0xc9, 0x77, 0x77, 0x77, 0xc9, empty, spillmark, 0xc8, 0xca, 0x49, 0xc9, 0xff}
	require.Equal(t, bitset(0b0100_0001_0001_1000), m.match(0xc9))
	require.Equal(t, bitset(0b0000_0010_0000_0000), m.match(empty))
	require.Zero(t, m.match(0x42))
}

func TestNextMatch(t *testing.T) {
	b := bitset(0xffff)
	for i := range uint32(groupSize) {
		require.Equal(t, i, nextMatch(&b))
		require.Equal(t, bitset(0xffff<<(i+1)), b)
	}
}

func TestMatchRandom(t *testing.T) {
	// Unlike the SWAR version, the vector version of match is exact, so it can be checked byte by byte.
	r := rand.New(rand.NewSource(1))
	for range 10_000 {
		var idx index
		for j := range idx {
			// Use few distinct values, so that a prefix often shows up in several slots.
			idx[j] = prefix(r.Intn(8)) * 37
		}
		p := idx[r.Intn(groupSize)]

		var want []uint32
		for j, v := range idx {
			if v == p {
				want = append(want, uint32(j))
			}
		}
		require.Equal(t, want, slots(idx.match(p)), "index %v, prefix %d", idx, p)
	}
}
