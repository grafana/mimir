// SPDX-License-Identifier: AGPL-3.0-only

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
