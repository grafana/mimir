// Included-from-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket.go
// Included-from-license: Apache-2.0
// Included-from-copyright: The Thanos Authors.

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
}

func newGapBasedPartitioner(maxGapBytes uint64, reg prometheus.Registerer) *gapBasedPartitioner {
	return &gapBasedPartitioner{
		maxGapBytes: maxGapBytes,
		requestedBytes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_partitioner_requested_bytes_total",
			Help: "Total size of byte ranges required to fetch from the storage before they are passed to the partitioner.",
		}),
		expandedBytes: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_partitioner_expanded_bytes_total",
			Help: "Total size of byte ranges returned by the partitioner after they've been combined together to reduce the number of bucket API calls.",
		}),
		requestedRanges: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_partitioner_requested_ranges_total",
			Help: "Total number of byte ranges required to fetch from the storage before they are passed to the partitioner.",
		}),
		expandedRanges: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_partitioner_expanded_ranges_total",
			Help: "Total number of byte ranges returned by the partitioner after they've been combined together to reduce the number of bucket API calls.",
		}),
	}
}

// Partition partitions length entries into n <= length ranges that cover all
// input ranges by combining entries that are separated by reasonably small gaps.
// It is used to combine multiple small ranges from object storage into bigger, more efficient/cheaper ones.
func (p *gapBasedPartitioner) Partition(length int, rng func(int) (uint64, uint64)) []Part {
	// Calculate the size of requested ranges.
	requestedBytes := uint64(0)
	for i := 0; i < length; i++ {
		start, end := rng(i)
		requestedBytes += end - start
	}

	// Run the upstream partitioner to compute the actual ranges that will be fetched.
	parts := p.partition(length, rng)

	// Calculate the size of ranges that will be fetched.
	expandedBytes := uint64(0)
	for _, p := range parts {
		expandedBytes += p.End - p.Start
	}

	p.requestedBytes.Add(float64(requestedBytes))
	p.expandedBytes.Add(float64(expandedBytes))
	p.requestedRanges.Add(float64(length))
	p.expandedRanges.Add(float64(len(parts)))

	return parts
}

func (g *gapBasedPartitioner) partition(length int, rng func(int) (uint64, uint64)) (parts []Part) {
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

			if p.End+g.maxGapBytes < s {
				break
			}

			if p.End <= e {
				p.End = e
			}
		}
		p.ElemRng = [2]int{j, k}
		parts = append(parts, p)
	}
	return parts
}
