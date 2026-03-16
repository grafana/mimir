package main

import (
	"math"
	"sort"
)

const hashSpaceSize = float64(uint64(1) << 32)

type HashRange struct {
	Lo uint32
	Hi uint32 // exclusive; 0 means 2^32 (end of space)
}

func (h HashRange) Size() uint64 {
	if h.Hi > h.Lo {
		return uint64(h.Hi) - uint64(h.Lo)
	}
	if h.Hi == 0 {
		return uint64(math.MaxUint32) - uint64(h.Lo) + 1
	}
	return 0
}

func (h HashRange) FractionOfSpace() float64 {
	return float64(h.Size()) / hashSpaceSize
}

func (h HashRange) Split() (HashRange, HashRange) {
	mid := h.Lo + uint32(h.Size()/2)
	return HashRange{h.Lo, mid}, HashRange{mid, h.Hi}
}

func (a HashRange) CanMergeWith(b HashRange) bool {
	return (a.Hi == b.Lo && a.Hi != 0) || (b.Hi == a.Lo && b.Hi != 0)
}

func MergeRanges(a, b HashRange) HashRange {
	if a.Hi == b.Lo {
		return HashRange{a.Lo, b.Hi}
	}
	return HashRange{b.Lo, a.Hi}
}

type MetricInfo struct {
	Name            string
	IngestionWeight float64 // samples/sec/series
	QueryWeight     float64 // queries/sec targeting this metric
}

type Series struct {
	MetricIndex int
	Hash        uint32
}

type Partition struct {
	ID         int32
	HashRanges map[HashRange]struct{}
}

type Ingester struct {
	ID         int
	Partitions map[int32]struct{}
}

type CatchUpEntry struct {
	PartitionID    int32
	OldIngesterID  int
	NewIngesterID  int
	RemainingSteps int
	ReplayLoad     float64
}

type Stats struct {
	Min, Max, Mean, P1, P5, P50, P99, Stddev, MaxMean float64
}

func computeStats(values []float64) Stats {
	n := len(values)
	if n == 0 {
		return Stats{}
	}
	sorted := make([]float64, n)
	copy(sorted, values)
	sort.Float64s(sorted)

	sum := 0.0
	for _, v := range sorted {
		sum += v
	}
	mean := sum / float64(n)

	pctIdx := func(pct float64) int {
		idx := int(math.Ceil(float64(n)*pct)) - 1
		if idx >= n {
			idx = n - 1
		}
		if idx < 0 {
			idx = 0
		}
		return idx
	}

	sumSqDiff := 0.0
	for _, v := range sorted {
		d := v - mean
		sumSqDiff += d * d
	}

	maxMean := 0.0
	if mean > 0 {
		maxMean = sorted[n-1] - mean
	}

	return Stats{
		Min:     sorted[0],
		Max:     sorted[n-1],
		Mean:    mean,
		P1:      sorted[pctIdx(0.01)],
		P5:      sorted[pctIdx(0.05)],
		P50:     sorted[pctIdx(0.50)],
		P99:     sorted[pctIdx(0.99)],
		Stddev:  math.Sqrt(sumSqDiff / float64(n)),
		MaxMean: maxMean,
	}
}

func normalizeByTotal(vs []float64) []float64 {
	total := sumFloat64(vs)
	if total == 0 {
		return vs
	}
	out := make([]float64, len(vs))
	for i, v := range vs {
		out[i] = v / total
	}
	return out
}

func sumFloat64(vs []float64) float64 {
	s := 0.0
	for _, v := range vs {
		s += v
	}
	return s
}

func meanFloat64(vs []float64) float64 {
	if len(vs) == 0 {
		return 0
	}
	return sumFloat64(vs) / float64(len(vs))
}

func maxIndex(vs []float64) int {
	idx := 0
	for i, v := range vs {
		if v > vs[idx] {
			idx = i
		}
	}
	return idx
}

func minIndex(vs []float64) int {
	idx := 0
	for i, v := range vs {
		if v < vs[idx] {
			idx = i
		}
	}
	return idx
}

func computeMaxMean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	max := values[0]
	sum := values[0]
	for _, v := range values[1:] {
		if v > max {
			max = v
		}
		sum += v
	}
	mean := sum / float64(len(values))
	if mean == 0 {
		return 0
	}
	return max / mean
}
