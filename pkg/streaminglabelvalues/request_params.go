package streaminglabelvalues

import (
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"

	apierror "github.com/grafana/mimir/pkg/api/error"
	mimirstorage "github.com/grafana/mimir/pkg/storage"
)

const (
	SearchAlgJaroWinkler = "jarowinkler"
	SearchAlgSubsequence = "subsequence"
)

type SearchResult struct {
	Name  string  `json:"name"`
	Score float64 `json:"score,omitempty"`
}

type SearchResultFactory func(result mimirstorage.SearchResult) SearchResult

func NewSearchResultFactory(includeScore bool) SearchResultFactory {
	if includeScore {
		return func(result mimirstorage.SearchResult) SearchResult {
			return SearchResult{Name: result.Value, Score: result.Score}
		}
	}
	return func(result mimirstorage.SearchResult) SearchResult {
		return SearchResult{Name: result.Value}
	}
}

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
	case "", "jaro":
		return SearchAlgJaroWinkler, nil
	case SearchAlgSubsequence, SearchAlgJaroWinkler:
		return v, nil
	default:
		return "", apierror.New(apierror.TypeBadData, fmt.Sprintf("invalid fuzz_alg - expected subsequence or jarowinkler but got %s", v))
	}
}

// CaseSensitive returns the case_sensitive query parameter.
func (p *RequestParser) CaseSensitive() (bool, error) {
	return p.asBool("case_sensitive", false)
}
func (p *RequestParser) IncludeScore() (bool, error) {
	return p.asBool("include_score", false)
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
func (p *RequestParser) SortBy() (mimirstorage.SortBy, error) {
	v := p.reqValues.Get("sort_by")
	switch v {
	case "", "none":
		return mimirstorage.None, nil
	case "alpha":
		return mimirstorage.Alpha, nil
	case "score":
		return mimirstorage.Score, nil
	default:
		return mimirstorage.None, apierror.New(apierror.TypeBadData, fmt.Sprintf("invalid sort_by - expected one of alpha, score, none but got %s", v))
	}
}

// SortDir returns the sort_dir query parameter as a SortDirection enum value.
func (p *RequestParser) SortDir() (mimirstorage.SortDirection, error) {
	v := p.reqValues.Get("sort_dir")
	switch v {
	case "", "asc":
		return mimirstorage.Asc, nil
	case "desc", "dsc":
		return mimirstorage.Desc, nil
	default:
		return mimirstorage.Asc, apierror.New(apierror.TypeBadData, fmt.Sprintf("invalid sort_dir - expected one of asc, dsc but got %s", v))
	}
}
