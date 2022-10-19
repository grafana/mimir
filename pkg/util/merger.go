// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/merger.go
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/strutil/merge.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
// Provenance-includes-copyright: The Thanos Authors.

package util

import (
	"github.com/prometheus/common/model"
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

// MergeSlices merges a set of sorted string slices into a single ones
// while removing all duplicates.
func MergeSlices(a ...[]string) []string {
	if len(a) == 0 {
		return nil
	}
	if len(a) == 1 {
		return a[0]
	}
	l := len(a) / 2
	return mergeTwoStringSlices(MergeSlices(a[:l]...), MergeSlices(a[l:]...))
}

func mergeTwoStringSlices(a, b []string) []string {
	maxl := len(a)
	if len(b) > len(a) {
		maxl = len(b)
	}
	res := make([]string, 0, maxl*10/9)

	for len(a) > 0 && len(b) > 0 {
		if a[0] == b[0] {
			res = append(res, a[0])
			a, b = a[1:], b[1:]
		} else if a[0] < b[0] {
			res = append(res, a[0])
			a = a[1:]
		} else {
			res = append(res, b[0])
			b = b[1:]
		}
	}
	// Append all remaining elements.
	res = append(res, a...)
	res = append(res, b...)
	return res
}
