// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"golang.org/x/exp/slices"
)

const (
	RequestTypeLabelNames  = RequestType(iota)
	RequestTypeLabelValues = RequestType(iota)

	minLimit     = 0
	maxLimit     = 500
	defaultLimit = 20

	stringParamSeparator = rune(0)
	stringValueSeparator = rune(1)
)

type RequestType int

type LabelNamesRequest struct {
	Matchers []*labels.Matcher
	Limit    int
}

// Strings returns a full representation of the request. This returned string can be
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

	// Add limit.
	b.WriteRune(stringParamSeparator)
	b.WriteString(strconv.Itoa(r.Limit))

	return b.String()
}

func (r *LabelNamesRequest) RequestType() RequestType {
	return RequestTypeLabelNames
}

// DecodeLabelNamesRequest decodes the input http.Request into a LabelNamesRequest.
// The input http.Request can either be a GET or POST.
func DecodeLabelNamesRequest(r *http.Request) (*LabelNamesRequest, error) {
	var (
		parsed = &LabelNamesRequest{}
		err    error
	)

	err = r.ParseForm()
	if err != nil {
		return nil, err
	}

	parsed.Matchers, err = extractSelector(r)
	if err != nil {
		return nil, err
	}

	parsed.Limit, err = extractLimit(r)
	if err != nil {
		return nil, err
	}

	return parsed, nil
}

type LabelValuesRequest struct {
	LabelNames []model.LabelName
	Matchers   []*labels.Matcher
	Limit      int
}

// Strings returns a full representation of the request. This returned string can be
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

	// Add limit.
	b.WriteRune(stringParamSeparator)
	b.WriteString(strconv.Itoa(r.Limit))

	return b.String()
}

func (r *LabelValuesRequest) RequestType() RequestType {
	return RequestTypeLabelValues
}

// DecodeLabelValuesRequest decodes the input http.Request into a LabelValuesRequest.
// The input http.Request can either be a GET or POST.
func DecodeLabelValuesRequest(r *http.Request) (*LabelValuesRequest, error) {
	var (
		parsed = &LabelValuesRequest{}
		err    error
	)

	if err = r.ParseForm(); err != nil {
		return nil, err
	}

	parsed.LabelNames, err = extractLabelNames(r)
	if err != nil {
		return nil, err
	}

	parsed.Matchers, err = extractSelector(r)
	if err != nil {
		return nil, err
	}

	parsed.Limit, err = extractLimit(r)
	if err != nil {
		return nil, err
	}

	return parsed, nil
}

// extractSelector parses and gets selector query parameter containing a single matcher
func extractSelector(r *http.Request) (matchers []*labels.Matcher, err error) {
	selectorParams := r.Form["selector"]
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
	slices.SortFunc(matchers, func(a, b *labels.Matcher) bool {
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		if a.Type != b.Type {
			return a.Type < b.Type
		}
		return a.Value < b.Value
	})

	return matchers, nil
}

// extractLimit parses and validates request param `limit` if it's defined, otherwise returns default value.
func extractLimit(r *http.Request) (limit int, err error) {
	limitParams := r.Form["limit"]
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
func extractLabelNames(r *http.Request) ([]model.LabelName, error) {
	labelNamesParams := r.Form["label_names[]"]
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
