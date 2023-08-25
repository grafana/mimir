// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/storepb/custom.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storepb

import (
	"fmt"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func NewSeriesResponse(series *Series) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_Series{
			Series: series,
		},
	}
}

func NewHintsSeriesResponse(hints *types.Any) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_Hints{
			Hints: hints,
		},
	}
}

func NewStatsResponse(indexBytesFetched int) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_Stats{
			Stats: &Stats{FetchedIndexBytes: uint64(indexBytesFetched)},
		},
	}
}

func NewStreamingSeriesResponse(series *StreamingSeriesBatch) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_StreamingSeries{
			StreamingSeries: series,
		},
	}
}

func NewStreamingChunksResponse(series *StreamingChunksBatch) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_StreamingChunks{
			StreamingChunks: series,
		},
	}
}

func NewStreamingChunksEstimate(estimatedChunks uint64) *SeriesResponse {
	return &SeriesResponse{
		Result: &SeriesResponse_StreamingChunksEstimate{
			StreamingChunksEstimate: &StreamingChunksEstimate{
				EstimatedChunkCount: estimatedChunks,
			},
		},
	}
}

type emptySeriesSet struct{}

func (emptySeriesSet) Next() bool                       { return false }
func (emptySeriesSet) At() (labels.Labels, []AggrChunk) { return labels.EmptyLabels(), nil }
func (emptySeriesSet) Err() error                       { return nil }

// EmptySeriesSet returns a new series set that contains no series.
func EmptySeriesSet() SeriesSet {
	return emptySeriesSet{}
}

// SeriesSet is a set of series and their corresponding chunks.
// The set is sorted by the label sets. Chunks may be overlapping or expected of order.
type SeriesSet interface {
	Next() bool
	At() (labels.Labels, []AggrChunk)
	Err() error
}

// PromMatchersToMatchers returns proto matchers from Prometheus matchers.
// NOTE: It allocates memory.
func PromMatchersToMatchers(ms ...*labels.Matcher) ([]LabelMatcher, error) {
	res := make([]LabelMatcher, 0, len(ms))
	for _, m := range ms {
		var t LabelMatcher_Type

		switch m.Type {
		case labels.MatchEqual:
			t = LabelMatcher_EQ
		case labels.MatchNotEqual:
			t = LabelMatcher_NEQ
		case labels.MatchRegexp:
			t = LabelMatcher_RE
		case labels.MatchNotRegexp:
			t = LabelMatcher_NRE
		default:
			return nil, errors.Errorf("unrecognized matcher type %d", m.Type)
		}
		res = append(res, LabelMatcher{Type: t, Name: m.Name, Value: m.Value})
	}
	return res, nil
}

// MatchersToPromMatchers returns Prometheus matchers from proto matchers.
// NOTE: It allocates memory.
func MatchersToPromMatchers(ms ...LabelMatcher) ([]*labels.Matcher, error) {
	res := make([]*labels.Matcher, 0, len(ms))
	for _, m := range ms {
		var t labels.MatchType

		switch m.Type {
		case LabelMatcher_EQ:
			t = labels.MatchEqual
		case LabelMatcher_NEQ:
			t = labels.MatchNotEqual
		case LabelMatcher_RE:
			t = labels.MatchRegexp
		case LabelMatcher_NRE:
			t = labels.MatchNotRegexp
		default:
			return nil, errors.Errorf("unrecognized label matcher type %d", m.Type)
		}
		m, err := labels.NewMatcher(t, m.Name, m.Value)
		if err != nil {
			return nil, err
		}
		res = append(res, m)
	}
	return res, nil
}

// PromMatchersToString converts prometheus label matchers to string format.
// String should be parsable as a valid PromQL query metric selector.
func PromMatchersToString(ms ...*labels.Matcher) string {
	var res string
	for i, m := range ms {
		res += m.String()
		if i < len(ms)-1 {
			res += ", "
		}
	}
	return "{" + res + "}"
}

func (m *LabelMatcher) PromString() string {
	return fmt.Sprintf("%s%s%q", m.Name, m.Type.PromString(), m.Value)
}

func (x LabelMatcher_Type) PromString() string {
	typeToStr := map[LabelMatcher_Type]string{
		LabelMatcher_EQ:  "=",
		LabelMatcher_NEQ: "!=",
		LabelMatcher_RE:  "=~",
		LabelMatcher_NRE: "!~",
	}
	if str, ok := typeToStr[x]; ok {
		return str
	}
	panic("unknown match type")
}

// PromLabels return Prometheus labels.Labels without extra allocation.
func (m *Series) PromLabels() labels.Labels {
	return mimirpb.FromLabelAdaptersToLabels(m.Labels)
}
