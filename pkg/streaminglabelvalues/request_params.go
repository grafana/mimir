package streaminglabelvalues

import (
	"fmt"
	apierror "github.com/grafana/mimir/pkg/api/error"
	"math"
	"net/url"
	"strconv"
	"strings"
)

type SearchResult struct {
	Name string `json:"name"`
}

type SortBy int
type SortDirection int

type Operator int

const (
	None SortBy = iota
	Alpha
	Score
)

const (
	Asc SortDirection = iota
	Desc
)

const (
	Or Operator = iota
	And
)

const (
	MatcherParam                = "match[]"
	SearchMetricNamesPathSuffix = "/api/v1/search/metric_names"
	SearchLabelNamesPathSuffix  = "/api/v1/search/label_names"
	SearchLabelValuesPathSuffix = "/api/v1/search/label_values"
)

type RequestParser struct {
	reqValues url.Values
}

func NewRequestParser(req url.Values) *RequestParser {
	return &RequestParser{reqValues: req}
}

func (p *RequestParser) asInt(param string, min, max, defaultIfNotSet int) (int, error) {
	if v := p.reqValues.Get(param); v != "" {
		i, err := strconv.Atoi(v)
		if err != nil {
			return 0, apierror.New(apierror.TypeBadData, fmt.Sprintf("invalid %s - expected an int but got %s", param, v))
		}
		if i < min || i > max {
			return 0, apierror.New(apierror.TypeBadData, fmt.Sprintf("invalid %s - expected an int in range [%d-%d] but got %d", param, min, max, i))
		}
		return i, nil
	}
	return defaultIfNotSet, nil
}

func (p *RequestParser) asBool(param string, defaultIfNotSet bool) (bool, error) {
	if v := p.reqValues.Get(param); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return false, apierror.New(apierror.TypeBadData, fmt.Sprintf("invalid %s - expected a bool but got %s", param, v))
		}
		return b, nil
	}
	return defaultIfNotSet, nil
}

func (p *RequestParser) Operator() (Operator, error) {
	v := p.reqValues.Get("operator")
	switch v {
	case "", "or":
		return Or, nil
	case "and":
		return And, nil
	default:
		return Or, apierror.New(apierror.TypeBadData, fmt.Sprintf("invalid operator - expected one of and, or but got %s", v))
	}
}

// FuzzThreshold returns the fuzz_threshold query parameter as an integer in [0, 100].
func (p *RequestParser) FuzzThreshold() (int, error) {
	v, err := p.asInt("fuzz_threshold", 0, 100, 0) // default to disabled
	if err != nil {
		return 0, err
	}
	return v, nil
}

// Search returns the search[] query parameter values.
func (p *RequestParser) Search() []string {
	return p.reqValues["search"]
}

// LabelName returns the label_name query parameter.
func (p *RequestParser) LabelName(isExpected bool) (string, error) {
	v := p.reqValues.Get("label_name")
	v = strings.TrimSpace(v)
	if v == "" && !isExpected {
		return "", nil
	}
	if v == "" {
		return "", apierror.New(apierror.TypeBadData, "missing required parameter label_name")
	}
	if !isExpected {
		return "", apierror.New(apierror.TypeBadData, "not supported parameter label_name")
	}
	return v, nil
}

// FuzzAlgorithm returns the fuzz_alg query parameter.
func (p *RequestParser) FuzzAlgorithm() (string, error) {
	v := p.reqValues.Get("fuzz_alg")
	switch v {
	case "":
		return "jaro", nil
	case "jaro", "jarowinkler":
		return v, nil
	default:
		return "", apierror.New(apierror.TypeBadData, fmt.Sprintf("invalid fuzz_alg - expected jaro or jarowinkler but got %s", v))
	}
}

// CaseSensitive returns the case_sensitive query parameter.
func (p *RequestParser) CaseSensitive() (bool, error) {
	return p.asBool("case_sensitive", false)
}

// BatchSize returns the batch_size query parameter.
func (p *RequestParser) BatchSize() (int, error) {
	return p.asInt("batch_size", 1, math.MaxInt, 1000)
}

// Limit returns the limit query parameter.
func (p *RequestParser) Limit() (int, error) {
	return p.asInt("limit", 0, math.MaxInt, 0)
}

// SortBy returns the sort_by query parameter as a SortBy enum value.
func (p *RequestParser) SortBy() (SortBy, error) {
	v := p.reqValues.Get("sort_by")
	switch v {
	case "", "none":
		return None, nil
	case "alpha":
		return Alpha, nil
	case "score":
		return Score, nil
	default:
		return None, apierror.New(apierror.TypeBadData, fmt.Sprintf("invalid sort_by - expected one of alpha, score, none but got %s", v))
	}
}

// SortDir returns the sort_dir query parameter as a SortDirection enum value.
func (p *RequestParser) SortDir() (SortDirection, error) {
	v := p.reqValues.Get("sort_dir")
	switch v {
	case "", "asc":
		return Asc, nil
	case "desc", "dsc":
		return Desc, nil
	default:
		return Asc, apierror.New(apierror.TypeBadData, fmt.Sprintf("invalid sort_dir - expected one of asc, dsc but got %s", v))
	}
}
