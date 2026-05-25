// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestComputeRateZeroExclusions_RateZeroAndLPositive(t *testing.T) {
	rates := map[int32]float64{1: 100, 2: 0, 3: 50}
	l := map[int32]int64{1: 1000, 2: 5000, 3: 0}
	got := computeRateZeroExclusions(rates, l, []int32{1, 2, 3})
	assert.Equal(t, map[int32]bool{2: true}, got,
		"only pid 2 (rate=0, L>0) should be excluded")
}

func TestComputeRateZeroExclusions_EmptyPartitionNotExcluded(t *testing.T) {
	// rate=0 AND L=0 is a truly empty partition (e.g. freshly
	// created or fully merged-away); it must remain a valid
	// destination so the slicer can fill it at cold-start.
	rates := map[int32]float64{1: 0}
	l := map[int32]int64{1: 0}
	got := computeRateZeroExclusions(rates, l, []int32{1})
	assert.Nil(t, got, "rate=0 && L=0 must not be excluded")
}

func TestComputeRateZeroExclusions_NonZeroRateNeverExcluded(t *testing.T) {
	// Any positive rate, no matter how small, means the readcache
	// is reporting — the slicer can trust the signal.
	rates := map[int32]float64{1: 0.0001}
	l := map[int32]int64{1: 1_000_000}
	got := computeRateZeroExclusions(rates, l, []int32{1})
	assert.Nil(t, got, "rate>0 must never be excluded")
}

func TestComputeRateZeroExclusions_MissingFromMaps(t *testing.T) {
	// activePartitions lists a pid that is present in neither
	// map. Go's zero-value lookup gives rate=0, L=0, which the
	// "L > 0" guard rejects — so the pid is not excluded.
	got := computeRateZeroExclusions(map[int32]float64{}, map[int32]int64{}, []int32{42})
	assert.Nil(t, got)
}

func TestComputeRateZeroExclusions_NegativeLTreatedAsEmpty(t *testing.T) {
	// L is int64 by type, but defensively: a negative value
	// (corrupt data) is not a positive series count and so the
	// partition must not be excluded.
	rates := map[int32]float64{1: 0}
	l := map[int32]int64{1: -5}
	got := computeRateZeroExclusions(rates, l, []int32{1})
	assert.Nil(t, got)
}

func TestComputeRateZeroExclusions_NoActivePartitions(t *testing.T) {
	got := computeRateZeroExclusions(
		map[int32]float64{1: 0, 2: 0},
		map[int32]int64{1: 100, 2: 200},
		nil,
	)
	assert.Nil(t, got, "no active partitions means nothing to exclude")
}

func TestComputeRateZeroExclusions_MultipleExcluded(t *testing.T) {
	rates := map[int32]float64{1: 0, 2: 0, 3: 0, 4: 100, 5: 0}
	l := map[int32]int64{1: 10, 2: 0, 3: 50, 4: 0, 5: 99}
	got := computeRateZeroExclusions(rates, l, []int32{1, 2, 3, 4, 5})
	assert.Equal(t, map[int32]bool{1: true, 3: true, 5: true}, got,
		"all partitions with rate=0 && L>0 must be excluded; pid 2 (L=0) and pid 4 (rate>0) must not")
}
