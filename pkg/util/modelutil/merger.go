// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/merger.go
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/strutil/merge.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
// Provenance-includes-copyright: The Thanos Authors.

package modelutil

import (
	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// MergeSampleSets merges and dedupes two sets of already sorted sample pairs.
func MergeSampleSets(a, b []model.SamplePair) []model.SamplePair {
	result := make([]model.SamplePair, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i].Timestamp < b[j].Timestamp {
			result = append(result, a[i])
			i++
		} else if a[i].Timestamp > b[j].Timestamp {
			result = append(result, b[j])
			j++
		} else {
			result = append(result, a[i])
			i++
			j++
		}
	}
	// Add the rest of a or b. One of them is empty now.
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}

// MergeNSampleSets merges and dedupes n sets of already sorted sample pairs.
func MergeNSampleSets(sampleSets ...[]model.SamplePair) []model.SamplePair {
	l := len(sampleSets)
	switch l {
	case 0:
		return []model.SamplePair{}
	case 1:
		return sampleSets[0]
	}

	n := l / 2
	left := MergeNSampleSets(sampleSets[:n]...)
	right := MergeNSampleSets(sampleSets[n:]...)
	return MergeSampleSets(left, right)
}

// MergeHistogramSets merges and dedupes two sets of already sorted histograms.
func MergeHistogramSets(a, b []mimirpb.Histogram) []mimirpb.Histogram {
	result := make([]mimirpb.Histogram, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i].GetTimestamp() < b[j].GetTimestamp() {
			result = append(result, a[i])
			i++
		} else if a[i].GetTimestamp() > b[j].GetTimestamp() {
			result = append(result, b[j])
			j++
		} else {
			result = append(result, a[i])
			i++
			j++
		}
	}
	// Add the rest of a or b. One of them is empty now.
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}

// MergeNHistogramSets merges and dedupes n sets of already sorted histograms.
func MergeNHistogramSets(sampleSets ...[]mimirpb.Histogram) []mimirpb.Histogram {
	l := len(sampleSets)
	switch l {
	case 0:
		return []mimirpb.Histogram{}
	case 1:
		return sampleSets[0]
	}

	n := l / 2
	left := MergeNHistogramSets(sampleSets[:n]...)
	right := MergeNHistogramSets(sampleSets[n:]...)
	return MergeHistogramSets(left, right)
}
