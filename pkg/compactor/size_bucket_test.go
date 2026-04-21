// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSizeBucket(t *testing.T) {
	cases := []struct {
		bytes uint64
		want  string
	}{
		{0, "xs"},
		{1, "xs"},
		{100<<20 - 1, "xs"},
		{100 << 20, "s"},
		{1<<30 - 1, "s"},
		{1 << 30, "m"},
		{4<<30 - 1, "m"},
		{4 << 30, "l"},
		{16<<30 - 1, "l"},
		{16 << 30, "xl"},
		{64<<30 - 1, "xl"},
		{64 << 30, "xxl"},
		{1 << 40, "xxl"},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, SizeBucket(c.bytes), "bytes=%d", c.bytes)
	}
}

func TestAllSizeBuckets_CoverAllValues(t *testing.T) {
	// Every value SizeBucket can return must appear in AllSizeBuckets, otherwise metric
	// pre-initialization would miss a time series.
	all := map[string]bool{}
	for _, b := range AllSizeBuckets() {
		all[b] = true
	}
	samples := []uint64{0, 100 << 20, 1 << 30, 4 << 30, 16 << 30, 64 << 30, 1 << 50}
	for _, b := range samples {
		assert.True(t, all[SizeBucket(b)], "bucket %q for %d bytes missing from AllSizeBuckets", SizeBucket(b), b)
	}
}
