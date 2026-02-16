// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"cmp"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/util/promqlext"
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
func DecodeLabelNamesRequest(r *http.Request, tenantMaxLimit int) (*LabelNamesRequest, error) {
	if err := r.ParseForm(); err != nil {
		return nil, err
	}

	return DecodeLabelNamesRequestFromValuesWithTenantMaxLimit(r.Form, tenantMaxLimit)
}

// DecodeLabelNamesRequestFromValues is like DecodeLabelNamesRequest but takes url.Values in input.
func DecodeLabelNamesRequestFromValues(values url.Values) (*LabelNamesRequest, error) {
	return DecodeLabelNamesRequestFromValuesWithTenantMaxLimit(values, 0)
}

// DecodeLabelNamesRequestFromValuesWithTenantMaxLimit is like DecodeLabelNamesRequestFromValues but also accepts the tenantMaxLimit
func DecodeLabelNamesRequestFromValuesWithTenantMaxLimit(values url.Values, tenantMaxLimit int) (*LabelNamesRequest, error) {
	var (
		parsed = &LabelNamesRequest{}
		err    error
	)

	parsed.Matchers, err = extractSelector(values)
	if err != nil {
		return nil, err
	}

	parsed.Limit, err = extractLimit(values, tenantMaxLimit)
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
func DecodeLabelValuesRequest(r *http.Request, tenantMaxLimit int) (*LabelValuesRequest, error) {
	if err := r.ParseForm(); err != nil {
		return nil, err
	}

	return DecodeLabelValuesRequestFromValuesWithTenantMaxLimit(r.Form, tenantMaxLimit)
}

// DecodeLabelValuesRequestFromValues is like DecodeLabelValuesRequest but takes url.Values in input.
func DecodeLabelValuesRequestFromValues(values url.Values) (*LabelValuesRequest, error) {
	return DecodeLabelValuesRequestFromValuesWithTenantMaxLimit(values, 0)
}

// DecodeLabelValuesRequestFromValuesWithTenantMaxLimit is like DecodeLabelValuesRequestFromValues but also accepts the tenantMaxLimit
func DecodeLabelValuesRequestFromValuesWithTenantMaxLimit(values url.Values, tenantMaxLimit int) (*LabelValuesRequest, error) {
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

	parsed.Limit, err = extractLimit(values, tenantMaxLimit)
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
	p := promqlext.NewExperimentalParser()
	matchers, err = p.ParseMetricSelector(selectorParams[0])
	if err != nil {
		return nil, fmt.Errorf("failed to parse selector: %w", err)
	}

	// Ensure stable sorting (improves query results cache hit ratio).
	slices.SortFunc(matchers, func(a, b *labels.Matcher) int {
		return cmp.Or(
			strings.Compare(a.Name, b.Name),
			cmp.Compare(a.Type, b.Type),
			strings.Compare(a.Value, b.Value),
		)
	})

	return matchers, nil
}

// extractLimit parses and validates request param `limit` if it's defined, otherwise returns default value.
func extractLimit(values url.Values, tenantMaxLimit int) (limit int, err error) {
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

	// Use the tenant-specific limit from Overrides if available
	if tenantMaxLimit == 0 {
		tenantMaxLimit = maxLimit
	}

	if limit > tenantMaxLimit {
		return 0, fmt.Errorf("'limit' param cannot be greater than '%v'", tenantMaxLimit)
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
	for _, labelName := range labelNamesParams {
		if !model.UTF8Validation.IsValidLabelName(labelName) {
			return nil, fmt.Errorf("invalid 'label_names' param '%v'", labelName)
		}
		labelNames = append(labelNames, model.LabelName(labelName))
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
