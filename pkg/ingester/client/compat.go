// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/client/compat.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// ToQueryRequest builds a QueryRequest proto.
func ToQueryRequest(from, to model.Time, matchers []*labels.Matcher) (*QueryRequest, error) {
	ms, err := ToLabelMatchers(matchers)
	if err != nil {
		return nil, err
	}

	return &QueryRequest{
		StartTimestampMs: int64(from),
		EndTimestampMs:   int64(to),
		Matchers:         ms,
	}, nil
}

// FromQueryRequest unpacks a QueryRequest proto.
func FromQueryRequest(req *QueryRequest) (model.Time, model.Time, []*labels.Matcher, error) {
	matchers, err := FromLabelMatchers(req.Matchers)
	if err != nil {
		return 0, 0, nil, err
	}
	from := model.Time(req.StartTimestampMs)
	to := model.Time(req.EndTimestampMs)
	return from, to, matchers, nil
}

// ToExemplarQueryRequest builds an ExemplarQueryRequest proto.
func ToExemplarQueryRequest(from, to model.Time, matchers ...[]*labels.Matcher) (*ExemplarQueryRequest, error) {
	var reqMatchers []*LabelMatchers
	for _, m := range matchers {
		ms, err := ToLabelMatchers(m)
		if err != nil {
			return nil, err
		}
		reqMatchers = append(reqMatchers, &LabelMatchers{ms})
	}

	return &ExemplarQueryRequest{
		StartTimestampMs: int64(from),
		EndTimestampMs:   int64(to),
		Matchers:         reqMatchers,
	}, nil
}

// FromExemplarQueryRequest unpacks a ExemplarQueryRequest proto.
func FromExemplarQueryRequest(req *ExemplarQueryRequest) (int64, int64, [][]*labels.Matcher, error) {
	var result [][]*labels.Matcher
	for _, m := range req.Matchers {
		matchers, err := FromLabelMatchers(m.Matchers)
		if err != nil {
			return 0, 0, nil, err
		}
		result = append(result, matchers)
	}

	return req.StartTimestampMs, req.EndTimestampMs, result, nil
}

// ToMetricsForLabelMatchersRequest builds a MetricsForLabelMatchersRequest proto
func ToMetricsForLabelMatchersRequest(from, to model.Time, hints *storage.SelectHints, matchers []*labels.Matcher) (*MetricsForLabelMatchersRequest, error) {
	ms, err := ToLabelMatchers(matchers)
	if err != nil {
		return nil, err
	}

	var limit int64
	if hints != nil && hints.Limit > 0 {
		limit = int64(hints.Limit)
	}
	return &MetricsForLabelMatchersRequest{
		StartTimestampMs: int64(from),
		EndTimestampMs:   int64(to),
		MatchersSet:      []*LabelMatchers{{Matchers: ms}},
		Limit:            limit,
	}, nil
}

// FromMetricsForLabelMatchersRequest unpacks a MetricsForLabelMatchersRequest proto.
func FromMetricsForLabelMatchersRequest(req *MetricsForLabelMatchersRequest) (*storage.SelectHints, [][]*labels.Matcher, error) {
	matchersSet := make([][]*labels.Matcher, 0, len(req.MatchersSet))
	for _, matchers := range req.MatchersSet {
		matchers, err := FromLabelMatchers(matchers.Matchers)
		if err != nil {
			return nil, nil, err
		}
		matchersSet = append(matchersSet, matchers)
	}
	hints := &storage.SelectHints{
		Limit: int(req.Limit),
	}
	return hints, matchersSet, nil
}

// FromMetricsForLabelMatchersResponse unpacks a MetricsForLabelMatchersResponse proto
func FromMetricsForLabelMatchersResponse(resp *MetricsForLabelMatchersResponse) []labels.Labels {
	metrics := []labels.Labels{}
	for _, m := range resp.Metric {
		metrics = append(metrics, mimirpb.FromLabelAdaptersToLabels(m.Labels))
	}
	return metrics
}

// ToLabelValuesRequest builds a LabelValuesRequest proto
func ToLabelValuesRequest(labelName model.LabelName, from, to model.Time, hints *storage.LabelHints, matchers []*labels.Matcher) (*LabelValuesRequest, error) {
	ms, err := ToLabelMatchers(matchers)
	if err != nil {
		return nil, err
	}

	var limit int64
	if hints != nil && hints.Limit > 0 {
		limit = int64(hints.Limit)
	}
	return &LabelValuesRequest{
		LabelName:        string(labelName),
		StartTimestampMs: int64(from),
		EndTimestampMs:   int64(to),
		Matchers:         &LabelMatchers{Matchers: ms},
		Limit:            limit,
	}, nil
}

// FromLabelValuesRequest unpacks a LabelValuesRequest proto
func FromLabelValuesRequest(req *LabelValuesRequest) (string, int64, int64, *storage.LabelHints, []*labels.Matcher, error) {
	var err error
	var hints *storage.LabelHints
	var matchers []*labels.Matcher

	if req.Matchers != nil {
		matchers, err = FromLabelMatchers(req.Matchers.Matchers)
		if err != nil {
			return "", 0, 0, nil, nil, err
		}
	}

	if req.Limit > 0 {
		hints = &storage.LabelHints{Limit: int(req.Limit)}
	}

	return req.LabelName, req.StartTimestampMs, req.EndTimestampMs, hints, matchers, nil
}

// ToLabelNamesRequest builds a LabelNamesRequest proto
func ToLabelNamesRequest(from, to model.Time, hints *storage.LabelHints, matchers []*labels.Matcher) (*LabelNamesRequest, error) {
	ms, err := ToLabelMatchers(matchers)
	if err != nil {
		return nil, err
	}

	var limit int64
	if hints != nil && hints.Limit > 0 {
		limit = int64(hints.Limit)
	}

	return &LabelNamesRequest{
		StartTimestampMs: int64(from),
		EndTimestampMs:   int64(to),
		Matchers:         &LabelMatchers{Matchers: ms},
		Limit:            limit,
	}, nil
}

// FromLabelNamesRequest unpacks a LabelNamesRequest proto
func FromLabelNamesRequest(req *LabelNamesRequest) (int64, int64, *storage.LabelHints, []*labels.Matcher, error) {
	var err error
	var hints *storage.LabelHints
	var matchers []*labels.Matcher
	if req.Matchers != nil {
		matchers, err = FromLabelMatchers(req.Matchers.Matchers)
		if err != nil {
			return 0, 0, nil, nil, err
		}
	}

	if req.Limit != 0 {
		hints = &storage.LabelHints{Limit: int(req.Limit)}
	}

	return req.StartTimestampMs, req.EndTimestampMs, hints, matchers, nil
}

func ToActiveSeriesRequest(matchers []*labels.Matcher) (*ActiveSeriesRequest, error) {
	ms, err := ToLabelMatchers(matchers)
	if err != nil {
		return nil, err
	}
	return &ActiveSeriesRequest{Matchers: ms}, nil
}

func ToLabelMatchers(matchers []*labels.Matcher) ([]*LabelMatcher, error) {
	result := make([]*LabelMatcher, 0, len(matchers))
	for _, matcher := range matchers {
		var mType MatchType
		switch matcher.Type {
		case labels.MatchEqual:
			mType = EQUAL
		case labels.MatchNotEqual:
			mType = NOT_EQUAL
		case labels.MatchRegexp:
			mType = REGEX_MATCH
		case labels.MatchNotRegexp:
			mType = REGEX_NO_MATCH
		default:
			return nil, fmt.Errorf("invalid matcher type")
		}
		result = append(result, &LabelMatcher{
			Type:  mType,
			Name:  matcher.Name,
			Value: matcher.Value,
		})
	}
	return result, nil
}

func FromLabelMatchers(matchers []*LabelMatcher) ([]*labels.Matcher, error) {
	result := make([]*labels.Matcher, 0, len(matchers))
	for _, matcher := range matchers {
		var mtype labels.MatchType
		switch matcher.Type {
		case EQUAL:
			mtype = labels.MatchEqual
		case NOT_EQUAL:
			mtype = labels.MatchNotEqual
		case REGEX_MATCH:
			mtype = labels.MatchRegexp
		case REGEX_NO_MATCH:
			mtype = labels.MatchNotRegexp
		default:
			return nil, fmt.Errorf("invalid matcher type")
		}
		matcher, err := labels.NewMatcher(mtype, matcher.Name, matcher.Value)
		if err != nil {
			return nil, err
		}
		result = append(result, matcher)
	}
	return result, nil
}
