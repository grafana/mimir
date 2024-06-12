// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"golang.org/x/exp/slices"
)

type CountMethod string

const (
	InMemoryMethod CountMethod = "inmemory"
	ActiveMethod   CountMethod = "active"
)

const (
	minLimit           = 0
	maxLimit           = 500
	defaultLimit       = 20
	defaultCountMethod = InMemoryMethod

	stringParamSeparator = rune(0)
	stringValueSeparator = rune(1)
)

type LabelNamesRequest struct {
	Matchers    []*labels.Matcher
	CountMethod CountMethod
	Limit       int
}

// Strings returns a full representation of the request. The returned string can be
// used to uniquely identify the request.
func (r *LabelNamesRequest) String() string {
	b := strings.Builder{}

	// Add matchers.
	for idx, matcher := range r.Matchers {
		if idx > 0 {
			b.WriteRune(stringValueSeparator)
		}
		b.WriteString(matcher.String())
	}

	// Add count method.
	b.WriteRune(stringParamSeparator)
	b.WriteString(string(r.CountMethod))

	// Add limit.
	b.WriteRune(stringParamSeparator)
	b.WriteString(strconv.Itoa(r.Limit))

	return b.String()
}

// DecodeLabelNamesRequest decodes the input http.Request into a LabelNamesRequest.
// The input http.Request can either be a GET or POST with URL-encoded parameters.
func DecodeLabelNamesRequest(r *http.Request) (*LabelNamesRequest, error) {
	if err := r.ParseForm(); err != nil {
		return nil, err
	}

	return DecodeLabelNamesRequestFromValues(r.Form)
}

// DecodeLabelNamesRequestFromValues is like DecodeLabelNamesRequest but takes url.Values in input.
func DecodeLabelNamesRequestFromValues(values url.Values) (*LabelNamesRequest, error) {
	var (
		parsed = &LabelNamesRequest{}
		err    error
	)

	parsed.Matchers, err = extractSelector(values)
	if err != nil {
		return nil, err
	}

	parsed.Limit, err = extractLimit(values)
	if err != nil {
		return nil, err
	}

	parsed.CountMethod, err = extractCountMethod(values)
	if err != nil {
		return nil, err
	}

	return parsed, nil
}

type LabelValuesRequest struct {
	LabelNames  []model.LabelName
	Matchers    []*labels.Matcher
	CountMethod CountMethod
	Limit       int
}

// Strings returns a full representation of the request. The returned string can be
// used to uniquely identify the request.
func (r *LabelValuesRequest) String() string {
	b := strings.Builder{}

	// Add label names.
	for idx, name := range r.LabelNames {
		if idx > 0 {
			b.WriteRune(stringValueSeparator)
		}
		b.WriteString(string(name))
	}

	// Add matchers.
	b.WriteRune(stringParamSeparator)
	for idx, matcher := range r.Matchers {
		if idx > 0 {
			b.WriteRune(stringValueSeparator)
		}
		b.WriteString(matcher.String())
	}

	// Add count method.
	b.WriteRune(stringParamSeparator)
	b.WriteString(string(r.CountMethod))

	// Add limit.
	b.WriteRune(stringParamSeparator)
	b.WriteString(strconv.Itoa(r.Limit))

	return b.String()
}

// DecodeLabelValuesRequest decodes the input http.Request into a LabelValuesRequest.
// The input http.Request can either be a GET or POST with URL-encoded parameters.
func DecodeLabelValuesRequest(r *http.Request) (*LabelValuesRequest, error) {
	if err := r.ParseForm(); err != nil {
		return nil, err
	}

	return DecodeLabelValuesRequestFromValues(r.Form)
}

// DecodeLabelValuesRequestFromValues is like DecodeLabelValuesRequest but takes url.Values in input.
func DecodeLabelValuesRequestFromValues(values url.Values) (*LabelValuesRequest, error) {
	var (
		parsed = &LabelValuesRequest{}
		err    error
	)

	parsed.LabelNames, err = extractLabelNames(values)
	if err != nil {
		return nil, err
	}

	parsed.Matchers, err = extractSelector(values)
	if err != nil {
		return nil, err
	}

	parsed.Limit, err = extractLimit(values)
	if err != nil {
		return nil, err
	}

	parsed.CountMethod, err = extractCountMethod(values)
	if err != nil {
		return nil, err
	}

	return parsed, nil
}

// extractSelector parses and gets selector query parameter containing a single matcher
func extractSelector(values url.Values) (matchers []*labels.Matcher, err error) {
	selectorParams := values["selector"]
	if len(selectorParams) == 0 {
		return nil, nil
	}
	if len(selectorParams) > 1 {
		return nil, fmt.Errorf("multiple 'selector' params are not allowed")
	}
	matchers, err = parser.ParseMetricSelector(selectorParams[0])
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse selector")
	}

	// Ensure stable sorting (improves query results cache hit ratio).
	slices.SortFunc(matchers, func(a, b *labels.Matcher) int {
		switch {
		case a.Name < b.Name:
			return -1
		case a.Name > b.Name:
			return 1
		case a.Type < b.Type:
			return -1
		case a.Type > b.Type:
			return 1
		case a.Value < b.Value:
			return -1
		case a.Value > b.Value:
			return 1
		default:
			return 0
		}
	})

	return matchers, nil
}

// extractLimit parses and validates request param `limit` if it's defined, otherwise returns default value.
func extractLimit(values url.Values) (limit int, err error) {
	limitParams := values["limit"]
	if len(limitParams) == 0 {
		return defaultLimit, nil
	}
	if len(limitParams) > 1 {
		return 0, fmt.Errorf("multiple 'limit' params are not allowed")
	}
	limit, err = strconv.Atoi(limitParams[0])
	if err != nil {
		return 0, err
	}
	if limit < minLimit {
		return 0, fmt.Errorf("'limit' param cannot be less than '%v'", minLimit)
	}
	if limit > maxLimit {
		return 0, fmt.Errorf("'limit' param cannot be greater than '%v'", maxLimit)
	}
	return limit, nil
}

// extractLabelNames parses and gets label_names query parameter containing an array of label values
func extractLabelNames(values url.Values) ([]model.LabelName, error) {
	labelNamesParams := values["label_names[]"]
	if len(labelNamesParams) == 0 {
		return nil, fmt.Errorf("'label_names[]' param is required")
	}

	labelNames := make([]model.LabelName, 0, len(labelNamesParams))
	for _, labelNameParam := range labelNamesParams {
		labelName := model.LabelName(labelNameParam)
		if !labelName.IsValid() {
			return nil, fmt.Errorf("invalid 'label_names' param '%v'", labelNameParam)
		}
		labelNames = append(labelNames, labelName)
	}

	// Ensure stable sorting (improves query results cache hit ratio).
	slices.Sort(labelNames)

	return labelNames, nil
}

// extractCountMethod parses and validates request param `count_method` if it's defined, otherwise returns default value.
func extractCountMethod(values url.Values) (countMethod CountMethod, err error) {
	countMethodParams := values["count_method"]
	if len(countMethodParams) == 0 {
		return defaultCountMethod, nil
	}
	switch CountMethod(countMethodParams[0]) {
	case ActiveMethod:
		return ActiveMethod, nil
	case InMemoryMethod:
		return InMemoryMethod, nil
	default:
		return "", fmt.Errorf("invalid 'count_method' param '%v'. valid options are: [%s]", countMethodParams[0], strings.Join([]string{string(ActiveMethod), string(InMemoryMethod)}, ","))
	}
}

type ActiveSeriesRequest struct {
	Matchers []*labels.Matcher
}

type ActiveMetricWithBucketCount struct {
	Metric         string  `json:"metric"`
	SeriesCount    uint64  `json:"series_count"`
	BucketCount    uint64  `json:"bucket_count"`
	AvgBucketCount float64 `json:"avg_bucket_count"`
	MinBucketCount uint64  `json:"min_bucket_count"`
	MaxBucketCount uint64  `json:"max_bucket_count"`
}

func (m *ActiveMetricWithBucketCount) UpdateAverage() {
	m.AvgBucketCount = float64(m.BucketCount) / float64(m.SeriesCount)
}

type ActiveNativeHistogramMetricsResponse struct {
	Data   []ActiveMetricWithBucketCount `json:"data"`
	Status string                        `json:"status,omitempty"`
	Error  string                        `json:"error,omitempty"`
}

// DecodeActiveSeriesRequest decodes the input http.Request into an ActiveSeriesRequest.
func DecodeActiveSeriesRequest(r *http.Request) (*ActiveSeriesRequest, error) {
	if err := r.ParseForm(); err != nil {
		return nil, err
	}

	return DecodeActiveSeriesRequestFromValues(r.Form)
}

// DecodeActiveSeriesRequestFromValues is like DecodeActiveSeriesRequest but takes an url.Values parameter.
func DecodeActiveSeriesRequestFromValues(values url.Values) (*ActiveSeriesRequest, error) {
	var (
		parsed = &ActiveSeriesRequest{}
		err    error
	)

	if !values.Has("selector") {
		return nil, fmt.Errorf("missing 'selector' parameter")
	}

	parsed.Matchers, err = extractSelector(values)
	if err != nil {
		return nil, err
	}

	return parsed, nil
}

// String returns a string representation that uniquely identifies the request.
func (r *ActiveSeriesRequest) String() string {
	b := strings.Builder{}

	for idx, matcher := range r.Matchers {
		if idx > 0 {
			b.WriteRune(stringValueSeparator)
		}
		b.WriteString(matcher.String())
	}

	return b.String()
}
