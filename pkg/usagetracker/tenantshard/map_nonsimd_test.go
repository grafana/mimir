package tenantshard

import (
	"testing"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
	"github.com/stretchr/testify/require"
)

func TestMapNonsimd(t *testing.T) {
	m := New(100)
	for i := 0; i < 100; i++ {
		m.Put(uint64(i), clock.Minutes(i), nil, nil, false)
	}
	require.Equal(t, 100, m.Count())
}

func TestHasZeroByte(t *testing.T) {
	require.Equal(t, bitset(0x0000000000000000), hasZeroByte(castUint64(&index{7, 8, 9, 10, 11, 12, 13, 14})))
	require.Equal(t, bitset(0x8080808080808080), hasZeroByte(castUint64(&index{0, 0, 0, 0, 0, 0, 0, 0})))
	require.Equal(t, bitset(0x8000000000000080), hasZeroByte(castUint64(&index{0, 11, 12, 13, 14, 15, 16, 0})))
	// Tombstones are a special case that also count as zero bytes.
	require.Equal(t, bitset(0x0000000000000000), hasZeroByte(castUint64(&index{tombstone, tombstone, tombstone, tombstone, tombstone, tombstone, tombstone, tombstone})))
}
