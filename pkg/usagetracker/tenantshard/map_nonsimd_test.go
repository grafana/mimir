// SPDX-License-Identifier: AGPL-3.0-only

package tenantshard

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex_Match(t *testing.T) {
	m := &index{0x12, 0x34, 0x56, 0xc9, 0xc9, 0x77, 0x77, 0x77}
	p := prefix(0xc9)
	require.Equal(t, bitset(0x0000008080000000), m.match(p))
}

func TestFindZeroBytes(t *testing.T) {
	require.Equal(t, bitset(0x0000000000000000), findZeroBytes(castUint64(&index{7, 8, 9, 10, 11, 12, 13, 14})))
	require.Equal(t, bitset(0x8080808080808080), findZeroBytes(castUint64(&index{0, 0, 0, 0, 0, 0, 0, 0})))
	require.Equal(t, bitset(0x8000000000000080), findZeroBytes(castUint64(&index{0, 11, 12, 13, 14, 15, 16, 0})))
	// Tombstones are a special case that also count as zero bytes.
	require.Equal(t, bitset(0x0000000000000000), findZeroBytes(castUint64(&index{tombstone, tombstone, tombstone, tombstone, tombstone, tombstone, tombstone, tombstone})))
}

func TestNextMatch(t *testing.T) {
	b := bitset(0x8080808080808080)
	require.Equal(t, uint32(0), nextMatch(&b))
	require.Equal(t, bitset(0x8080808080808000), b)
	require.Equal(t, uint32(1), nextMatch(&b))
	require.Equal(t, bitset(0x8080808080800000), b)
	require.Equal(t, uint32(2), nextMatch(&b))
	require.Equal(t, bitset(0x8080808080000000), b)
	require.Equal(t, uint32(3), nextMatch(&b))
	require.Equal(t, bitset(0x8080808000000000), b)
	require.Equal(t, uint32(4), nextMatch(&b))
	require.Equal(t, bitset(0x8080800000000000), b)
	require.Equal(t, uint32(5), nextMatch(&b))
	require.Equal(t, bitset(0x8080000000000000), b)
	require.Equal(t, uint32(6), nextMatch(&b))
	require.Equal(t, bitset(0x8000000000000000), b)
	require.Equal(t, uint32(7), nextMatch(&b))
	require.Equal(t, bitset(0x0000000000000000), b)
}
