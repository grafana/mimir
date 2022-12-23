// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/query_range.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimirpb

type GenericSamplePair interface {
	Sample | SampleHistogramPair | Histogram
	GetTimestampVal() int64
}

func (m Sample) GetTimestampVal() int64 {
	return m.TimestampMs
}

func (m SampleHistogramPair) GetTimestampVal() int64 {
	return m.Timestamp
}

func (m Histogram) GetTimestampVal() int64 {
	return m.Timestamp
}
