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
	h := FromPromCommonToMimirSampleHistogram(s.Histogram)
	vs.Histogram = &h
	return nil
}

func (vs SampleHistogramPair) MarshalJSON() ([]byte, error) {
	s := model.SampleHistogramPair{
		Timestamp: model.Time(vs.Timestamp),
		Histogram: FromMimirSampleToPromCommonHistogram(*vs.Histogram),
	}
	return json.Marshal(s)
}
