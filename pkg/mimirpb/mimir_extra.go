// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/query_range.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimirpb

import (
	jsoniter "github.com/json-iterator/go"

	"github.com/prometheus/common/model"
)

var (
	json = jsoniter.Config{
		EscapeHTML:             false, // No HTML in our responses.
		SortMapKeys:            true,
		ValidateJsonRawMessage: true,
	}.Froze()
)

func (vs *SampleHistogramPair) UnmarshalJSON(b []byte) error {
	s := model.SampleHistogramPair{}
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	vs.Timestamp = int64(s.Timestamp)
	buckets := make([]*HistogramBucket, len(s.Histogram.Buckets))
	for i, bucket := range s.Histogram.Buckets {
		buckets[i] = &HistogramBucket{
			Boundaries: int32(bucket.Boundaries),
			Lower:      float64(bucket.Lower),
			Upper:      float64(bucket.Upper),
			Count:      uint64(bucket.Count),
		}
	}
	vs.Histogram = &SampleHistogram{
		Count:   uint64(s.Histogram.Count),
		Sum:     float64(s.Histogram.Sum),
		Buckets: buckets,
	}
	return nil
}

func (vs SampleHistogramPair) MarshalJSON() ([]byte, error) {
	buckets := make(model.HistogramBuckets, len(vs.Histogram.Buckets))
	for i, bucket := range vs.Histogram.Buckets {
		buckets[i] = &model.HistogramBucket{
			Boundaries: int(bucket.Boundaries),
			Lower:      model.FloatString(bucket.Lower),
			Upper:      model.FloatString(bucket.Upper),
			Count:      model.IntString(bucket.Count),
		}
	}
	s := model.SampleHistogramPair{
		Timestamp: model.Time(vs.Timestamp),
		Histogram: model.SampleHistogram{
			Count:   model.IntString(vs.Histogram.Count),
			Sum:     model.FloatString(vs.Histogram.Sum),
			Buckets: buckets,
		},
	}
	return json.Marshal(s)
}
