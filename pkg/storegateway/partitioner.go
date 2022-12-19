// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storegateway/partitioner.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type gapBasedPartitioner struct {
	maxGapBytes uint64

	// Metrics.
	requestedBytes  prometheus.Counter
	requestedRanges prometheus.Counter
	expandedBytes   prometheus.Counter
	expandedRanges  prometheus.Counter
	extendedRanges  prometheus.Counter
}

func newGapBasedPartitioner(maxGapBytes uint64, reg prometheus.Registerer) *gapBasedPartitioner {
	return &gapBasedPartitioner{
		maxGapBytes: maxGapBytes,
		requestedBytes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_partitioner_requested_bytes_total",
			Help: "Total size of byte ranges required to fetch from the storage before they are extended with maxGapBytes.",
		}),
		expandedBytes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_partitioner_expanded_bytes_total",
			Help: "Total size of byte ranges required to fetch from the storage after they are extended with maxGapBytes.",
		}),
		requestedRanges: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_partitioner_requested_ranges_total",
			Help: "Total number of byte ranges required to fetch from the storage before they are passed to the partitioner.",
		}),
		expandedRanges: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_partitioner_expanded_ranges_total",
			Help: "Total number of byte ranges returned by the partitioner after they've been combined together to reduce the number of bucket API calls.",
		}),
		extendedRanges: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_partitioner_extended_ranges_total",
			Help: "Total number of byte ranges that were not overlapping but were joined because they were closer than maxGapBytes.",
		}),
	}
}

// Partition partitions length entries into n <= length ranges that cover all
// input ranges by combining entries that are separated by reasonably small gaps.
// It is used to combine multiple small ranges from object storage into bigger, more efficient/cheaper ones.
func (g *gapBasedPartitioner) Partition(length int, rng func(int) (uint64, uint64)) []Part {
	// Calculate the size of requested ranges.
	requestedBytes := uint64(0)
	for i := 0; i < length; i++ {
		start, end := rng(i)
		requestedBytes += end - start
	}

	// Run the upstream partitioner to compute the actual ranges that will be fetched.
	parts, stats := g.partition(length, rng)

	// Calculate the size of ranges that will be fetched.
	expandedBytes := uint64(0)
	for _, p := range parts {
		expandedBytes += p.End - p.Start
	}

	g.requestedBytes.Add(float64(expandedBytes - stats.overextendedRange))
	g.expandedBytes.Add(float64(expandedBytes))
	g.requestedRanges.Add(float64(length))
	g.expandedRanges.Add(float64(len(parts)))
	g.extendedRanges.Add(float64(stats.extendedNonOverlappingRanges))

	return parts
}

type partitionStats struct {
	extendedNonOverlappingRanges int
	overextendedRange            uint64
}

func (g *gapBasedPartitioner) partition(length int, rng func(int) (uint64, uint64)) (parts []Part, stats partitionStats) {
	j := 0
	k := 0
	for k < length {
		j = k
		k++

		p := Part{}
		p.Start, p.End = rng(j)

		// Keep growing the range until the end or we encounter a large gap.
		for ; k < length; k++ {
			s, e := rng(k)

			if p.End >= s {
				// The start of the next range overlaps with the current range's end, so we can merge them.
			} else if p.End+g.maxGapBytes >= s {
				// We can afford to fill a gap between the current range's end and the next range's start.
				// We do so, but we also keep track of how much of it we do.
				stats.extendedNonOverlappingRanges++
				stats.overextendedRange += s - p.End
			} else {
				break
			}

			if p.End <= e {
				p.End = e
			}
		}
		p.ElemRng = [2]int{j, k}
		parts = append(parts, p)
	}
	return
}
