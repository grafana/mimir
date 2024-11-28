// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/query_range.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"bytes"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	v1 "github.com/prometheus/prometheus/web/api/v1"

	"github.com/grafana/mimir/pkg/mimirpb"
)

var (
	json = jsoniter.Config{
		EscapeHTML:             false, // No HTML in our responses.
		SortMapKeys:            true,
		ValidateJsonRawMessage: true,
	}.Froze()
)

// newEmptyPrometheusResponse returns an empty successful Prometheus query range response.
func newEmptyPrometheusResponse() *PrometheusResponse {
	return &PrometheusResponse{
		Status: statusSuccess,
		Data: &PrometheusData{
			ResultType: model.ValMatrix.String(),
			Result:     []SampleStream{},
		},
	}
}

type PrometheusRangeQueryRequest struct {
	path    string
	headers []*PrometheusHeader
	start   int64
	end     int64
	step    int64
	// lookback is set at query engine level rather than per request, but required to know minT and maxT
	lookbackDelta time.Duration
	queryExpr     parser.Expr
	minT          int64
	maxT          int64

	// ID of the request used to correlate downstream requests and responses.
	id      int64
	options Options
	// hints that could be optionally attached to the request to pass down the stack.
	// These hints can be used to optimize the query execution.
	hints *Hints
}

func NewPrometheusRangeQueryRequest(
	urlPath string,
	headers []*PrometheusHeader,
	start, end, step int64,
	lookbackDelta time.Duration,
	queryExpr parser.Expr,
	options Options,
	hints *Hints,
) *PrometheusRangeQueryRequest {
	r := &PrometheusRangeQueryRequest{
		path:          urlPath,
		headers:       headers,
		start:         start,
		end:           end,
		step:          step,
		lookbackDelta: lookbackDelta,
		queryExpr:     queryExpr,
		minT:          start,
		maxT:          end,
		options:       options,
		hints:         hints,
	}
	return r.updateMinMaxT()
}

func (r *PrometheusRangeQueryRequest) updateMinMaxT() *PrometheusRangeQueryRequest {
	if r.queryExpr == nil {
		// Protect against panics.
		r.minT, r.maxT = r.start, r.end
	} else {
		r.minT, r.maxT = decodeQueryMinMaxTime(
			r.queryExpr, r.start, r.end, r.step, r.lookbackDelta,
		)
	}
	return r
}

func (r *PrometheusRangeQueryRequest) GetID() int64 {
	return r.id
}

func (r *PrometheusRangeQueryRequest) GetPath() string {
	return r.path
}

func (r *PrometheusRangeQueryRequest) GetHeaders() []*PrometheusHeader {
	return r.headers
}

func (r *PrometheusRangeQueryRequest) GetStart() int64 {
	return r.start
}

func (r *PrometheusRangeQueryRequest) GetEnd() int64 {
	return r.end
}

func (r *PrometheusRangeQueryRequest) GetStep() int64 {
	return r.step
}

func (r *PrometheusRangeQueryRequest) GetQuery() string {
	if r.queryExpr != nil {
		return r.queryExpr.String()
	}
	return ""
}

func (r *PrometheusRangeQueryRequest) GetLookbackDelta() time.Duration {
	return r.lookbackDelta
}

// GetMinT returns the minimum timestamp in milliseconds of data to be queried,
// as determined from the start timestamp and any range vector or offset in the query.
func (r *PrometheusRangeQueryRequest) GetMinT() int64 {
	return r.minT
}

// GetMaxT returns the maximum timestamp in milliseconds of data to be queried,
// as determined from the end timestamp and any offset in the query.
func (r *PrometheusRangeQueryRequest) GetMaxT() int64 {
	return r.maxT
}

func (r *PrometheusRangeQueryRequest) GetOptions() Options {
	return r.options
}

func (r *PrometheusRangeQueryRequest) GetHints() *Hints {
	return r.hints
}

// WithID clones the current `PrometheusRangeQueryRequest` with the provided ID.
func (r *PrometheusRangeQueryRequest) WithID(id int64) (MetricsQueryRequest, error) {
	newRequest := *r
	newRequest.headers = cloneHeaders(r.headers)
	newRequest.id = id
	return &newRequest, nil
}

// WithStartEnd clones the current `PrometheusRangeQueryRequest` with a new `start` and `end` timestamp.
func (r *PrometheusRangeQueryRequest) WithStartEnd(start int64, end int64) (MetricsQueryRequest, error) {
	newRequest := *r
	newRequest.headers = cloneHeaders(r.headers)
	newRequest.start = start
	newRequest.end = end
	return (&newRequest).updateMinMaxT(), nil
}

// WithQuery clones the current `PrometheusRangeQueryRequest` with a new query; returns error if query parse fails.
func (r *PrometheusRangeQueryRequest) WithQuery(query string) (MetricsQueryRequest, error) {
	queryExpr, err := parser.ParseExpr(query)
	if err != nil {
		return nil, err
	}

	newRequest := *r
	newRequest.headers = cloneHeaders(r.headers)
	newRequest.queryExpr = queryExpr
	return (&newRequest).updateMinMaxT(), nil
}

// WithHeaders clones the current `PrometheusRangeQueryRequest` with new headers.
func (r *PrometheusRangeQueryRequest) WithHeaders(headers []*PrometheusHeader) (MetricsQueryRequest, error) {
	newRequest := *r
	newRequest.headers = cloneHeaders(headers)
	return &newRequest, nil
}

// WithExpr clones the current `PrometheusRangeQueryRequest` with a new query expression.
func (r *PrometheusRangeQueryRequest) WithExpr(queryExpr parser.Expr) (MetricsQueryRequest, error) {
	newRequest := *r
	newRequest.headers = cloneHeaders(r.headers)
	newRequest.queryExpr = queryExpr
	return (&newRequest).updateMinMaxT(), nil
}

// WithTotalQueriesHint clones the current `PrometheusRangeQueryRequest` with an
// added Hint value for TotalQueries.
func (r *PrometheusRangeQueryRequest) WithTotalQueriesHint(totalQueries int32) (MetricsQueryRequest, error) {
	newRequest := *r
	newRequest.headers = cloneHeaders(r.headers)
	if newRequest.hints == nil {
		newRequest.hints = &Hints{TotalQueries: totalQueries}
	} else {
		*newRequest.hints = *(r.hints)
		newRequest.hints.TotalQueries = totalQueries
	}
	return &newRequest, nil
}

// WithEstimatedSeriesCountHint clones the current `PrometheusRangeQueryRequest`
// with an added Hint value for EstimatedCardinality.
func (r *PrometheusRangeQueryRequest) WithEstimatedSeriesCountHint(count uint64) (MetricsQueryRequest, error) {
	newRequest := *r
	newRequest.headers = cloneHeaders(r.headers)
	if newRequest.hints == nil {
		newRequest.hints = &Hints{
			CardinalityEstimate: &EstimatedSeriesCount{count},
		}
	} else {
		*newRequest.hints = *(r.hints)
		newRequest.hints.CardinalityEstimate = &EstimatedSeriesCount{count}
	}
	return &newRequest, nil
}

// AddSpanTags writes the current `PrometheusRangeQueryRequest` parameters to the specified span tags
// ("attributes" in OpenTelemetry parlance).
func (r *PrometheusRangeQueryRequest) AddSpanTags(sp opentracing.Span) {
	sp.SetTag("query", r.GetQuery())
	sp.SetTag("start", timestamp.Time(r.GetStart()).String())
	sp.SetTag("end", timestamp.Time(r.GetEnd()).String())
	sp.SetTag("step_ms", r.GetStep())
}

type PrometheusInstantQueryRequest struct {
	path    string
	headers []*PrometheusHeader
	time    int64
	// lookback is set at query engine level rather than per request, but required to know minT and maxT
	lookbackDelta time.Duration
	queryExpr     parser.Expr
	minT          int64
	maxT          int64

	// ID of the request used to correlate downstream requests and responses.
	id      int64
	options Options
	// hints that could be optionally attached to the request to pass down the stack.
	// These hints can be used to optimize the query execution.
	hints *Hints
}

func NewPrometheusInstantQueryRequest(
	urlPath string,
	headers []*PrometheusHeader,
	time int64,
	lookbackDelta time.Duration,
	queryExpr parser.Expr,
	options Options,
	hints *Hints,
) *PrometheusInstantQueryRequest {
	r := &PrometheusInstantQueryRequest{
		path:          urlPath,
		headers:       headers,
		time:          time,
		lookbackDelta: lookbackDelta,
		queryExpr:     queryExpr,
		minT:          time,
		maxT:          time,
		options:       options,
		hints:         hints,
	}
	return r.updateMinMaxT()
}

func (r *PrometheusInstantQueryRequest) updateMinMaxT() *PrometheusInstantQueryRequest {
	if r.queryExpr == nil {
		// Protect against panics.
		r.minT, r.maxT = r.time, r.time
	} else {
		r.minT, r.maxT = decodeQueryMinMaxTime(
			r.queryExpr, r.time, r.time, 0, r.lookbackDelta,
		)
	}
	return r
}

func (r *PrometheusInstantQueryRequest) GetID() int64 {
	return r.id
}

func (r *PrometheusInstantQueryRequest) GetPath() string {
	return r.path
}

func (r *PrometheusInstantQueryRequest) GetHeaders() []*PrometheusHeader {
	return r.headers
}

func (r *PrometheusInstantQueryRequest) GetTime() int64 {
	return r.time
}

func (r *PrometheusInstantQueryRequest) GetQuery() string {
	if r.queryExpr != nil {
		return r.queryExpr.String()
	}
	return ""
}

func (r *PrometheusInstantQueryRequest) GetLookbackDelta() time.Duration {
	return r.lookbackDelta
}

func (r *PrometheusInstantQueryRequest) GetStart() int64 {
	return r.GetTime()
}

func (r *PrometheusInstantQueryRequest) GetEnd() int64 {
	return r.GetTime()
}

func (r *PrometheusInstantQueryRequest) GetStep() int64 {
	return 0
}

// GetMinT returns the minimum timestamp in milliseconds of data to be queried,
// as determined from the start timestamp and any range vector or offset in the query.
func (r *PrometheusInstantQueryRequest) GetMinT() int64 {
	return r.minT
}

// GetMaxT returns the maximum timestamp in milliseconds of data to be queried,
// as determined from the end timestamp and any offset in the query.
func (r *PrometheusInstantQueryRequest) GetMaxT() int64 {
	return r.maxT
}

func (r *PrometheusInstantQueryRequest) GetOptions() Options {
	return r.options
}

func (r *PrometheusInstantQueryRequest) GetHints() *Hints {
	return r.hints
}

func (r *PrometheusInstantQueryRequest) WithID(id int64) (MetricsQueryRequest, error) {
	newRequest := *r
	newRequest.headers = cloneHeaders(r.headers)
	newRequest.id = id
	return &newRequest, nil
}

// WithStartEnd clones the current `PrometheusInstantQueryRequest` with a new `time` timestamp.
func (r *PrometheusInstantQueryRequest) WithStartEnd(time int64, _ int64) (MetricsQueryRequest, error) {
	newRequest := *r
	newRequest.headers = cloneHeaders(r.headers)
	newRequest.time = time
	return (&newRequest).updateMinMaxT(), nil
}

// WithQuery clones the current `PrometheusInstantQueryRequest` with a new query; returns error if query parse fails.
func (r *PrometheusInstantQueryRequest) WithQuery(query string) (MetricsQueryRequest, error) {
	queryExpr, err := parser.ParseExpr(query)
	if err != nil {
		return nil, err
	}

	newRequest := *r
	newRequest.headers = cloneHeaders(r.headers)
	newRequest.queryExpr = queryExpr
	return (&newRequest).updateMinMaxT(), nil
}

// WithHeaders clones the current `PrometheusRangeQueryRequest` with new headers.
func (r *PrometheusInstantQueryRequest) WithHeaders(headers []*PrometheusHeader) (MetricsQueryRequest, error) {
	newRequest := *r
	newRequest.headers = cloneHeaders(headers)
	return &newRequest, nil
}

// WithExpr clones the current `PrometheusInstantQueryRequest` with a new query expression.
func (r *PrometheusInstantQueryRequest) WithExpr(queryExpr parser.Expr) (MetricsQueryRequest, error) {
	newRequest := *r
	newRequest.headers = cloneHeaders(r.headers)
	newRequest.queryExpr = queryExpr
	return (&newRequest).updateMinMaxT(), nil
}

func (r *PrometheusInstantQueryRequest) WithTotalQueriesHint(totalQueries int32) (MetricsQueryRequest, error) {
	newRequest := *r
	newRequest.headers = cloneHeaders(r.headers)
	if newRequest.hints == nil {
		newRequest.hints = &Hints{TotalQueries: totalQueries}
	} else {
		*newRequest.hints = *(r.hints)
		newRequest.hints.TotalQueries = totalQueries
	}
	return &newRequest, nil
}

func (r *PrometheusInstantQueryRequest) WithEstimatedSeriesCountHint(count uint64) (MetricsQueryRequest, error) {
	newRequest := *r
	newRequest.headers = cloneHeaders(r.headers)
	if newRequest.hints == nil {
		newRequest.hints = &Hints{
			CardinalityEstimate: &EstimatedSeriesCount{count},
		}
	} else {
		*newRequest.hints = *(r.hints)
		newRequest.hints.CardinalityEstimate = &EstimatedSeriesCount{count}
	}
	return &newRequest, nil
}

// AddSpanTags writes query information about the current `PrometheusInstantQueryRequest`
// to a span's tag ("attributes" in OpenTelemetry parlance).
func (r *PrometheusInstantQueryRequest) AddSpanTags(sp opentracing.Span) {
	sp.SetTag("query", r.GetQuery())
	sp.SetTag("time", timestamp.Time(r.GetTime()).String())
}

type Hints struct {
	// Total number of queries that are expected to be executed to serve the original request.
	TotalQueries int32
	// Estimated total number of series that a request might return.
	CardinalityEstimate *EstimatedSeriesCount
}

func (h *Hints) GetCardinalityEstimate() *EstimatedSeriesCount {
	return h.CardinalityEstimate
}

func (h *Hints) GetTotalQueries() int32 {
	return h.TotalQueries
}

func (h *Hints) GetEstimatedSeriesCount() uint64 {
	if x := h.GetCardinalityEstimate(); x != nil {
		return x.EstimatedSeriesCount
	}
	return 0
}

type EstimatedSeriesCount struct {
	EstimatedSeriesCount uint64
}

func (r *PrometheusLabelNamesQueryRequest) GetLabelName() string {
	return ""
}

func (r *PrometheusSeriesQueryRequest) GetLabelName() string {
	return ""
}

func (r *PrometheusLabelNamesQueryRequest) GetStartOrDefault() int64 {
	if r.GetStart() == 0 {
		return v1.MinTime.UnixMilli()
	}
	return r.GetStart()
}

func (r *PrometheusLabelNamesQueryRequest) GetEndOrDefault() int64 {
	if r.GetEnd() == 0 {
		return v1.MaxTime.UnixMilli()
	}
	return r.GetEnd()
}

func (r *PrometheusLabelValuesQueryRequest) GetStartOrDefault() int64 {
	if r.GetStart() == 0 {
		return v1.MinTime.UnixMilli()
	}
	return r.GetStart()
}

func (r *PrometheusLabelValuesQueryRequest) GetEndOrDefault() int64 {
	if r.GetEnd() == 0 {
		return v1.MaxTime.UnixMilli()
	}
	return r.GetEnd()
}

func (r *PrometheusSeriesQueryRequest) GetStartOrDefault() int64 {
	if r.GetStart() == 0 {
		return v1.MinTime.UnixMilli()
	}
	return r.GetStart()
}

func (r *PrometheusSeriesQueryRequest) GetEndOrDefault() int64 {
	if r.GetEnd() == 0 {
		return v1.MaxTime.UnixMilli()
	}
	return r.GetEnd()
}

func (r *PrometheusLabelNamesQueryRequest) GetHeaders() []*PrometheusHeader {
	return r.Headers
}

func (r *PrometheusLabelValuesQueryRequest) GetHeaders() []*PrometheusHeader {
	return r.Headers
}

func (r *PrometheusSeriesQueryRequest) GetHeaders() []*PrometheusHeader {
	return r.Headers
}

// WithLabelName clones the current `PrometheusLabelNamesQueryRequest` with a new label name param.
func (r *PrometheusLabelNamesQueryRequest) WithLabelName(string) (LabelsQueryRequest, error) {
	panic("PrometheusLabelNamesQueryRequest.WithLabelName is not implemented")
}

// WithLabelName clones the current `PrometheusLabelValuesQueryRequest` with a new label name param.
func (r *PrometheusLabelValuesQueryRequest) WithLabelName(name string) (LabelsQueryRequest, error) {
	newRequest := *r
	newRequest.Path = labelValuesPathSuffix.ReplaceAllString(r.Path, `/api/v1/label/`+name+`/values`)
	newRequest.LabelName = name
	return &newRequest, nil
}

// WithLabelName clones the current `PrometheusSeriesQueryRequest` with a new label name param.
func (r *PrometheusSeriesQueryRequest) WithLabelName(string) (LabelsQueryRequest, error) {
	panic("PrometheusSeriesQueryRequest.WithLabelName is not implemented")
}

// WithLabelMatcherSets clones the current `PrometheusLabelNamesQueryRequest` with new label matcher sets.
func (r *PrometheusLabelNamesQueryRequest) WithLabelMatcherSets(labelMatcherSets []string) (LabelsQueryRequest, error) {
	newRequest := *r
	newRequest.LabelMatcherSets = make([]string, len(labelMatcherSets))
	copy(newRequest.LabelMatcherSets, labelMatcherSets)
	return &newRequest, nil
}

// WithLabelMatcherSets clones the current `PrometheusLabelValuesQueryRequest` with new label matcher sets.
func (r *PrometheusLabelValuesQueryRequest) WithLabelMatcherSets(labelMatcherSets []string) (LabelsQueryRequest, error) {
	newRequest := *r
	newRequest.LabelMatcherSets = make([]string, len(labelMatcherSets))
	copy(newRequest.LabelMatcherSets, labelMatcherSets)
	return &newRequest, nil
}

// WithLabelMatcherSets clones the current `PrometheusSeriesQueryRequest` with new label matcher sets.
func (r *PrometheusSeriesQueryRequest) WithLabelMatcherSets(labelMatcherSets []string) (LabelsQueryRequest, error) {
	newRequest := *r
	newRequest.LabelMatcherSets = make([]string, len(labelMatcherSets))
	copy(newRequest.LabelMatcherSets, labelMatcherSets)
	return &newRequest, nil
}

// WithHeaders clones the current `PrometheusLabelNamesQueryRequest` with new headers.
func (r *PrometheusLabelNamesQueryRequest) WithHeaders(headers []*PrometheusHeader) (LabelsQueryRequest, error) {
	newRequest := *r
	newRequest.Headers = cloneHeaders(headers)
	return &newRequest, nil
}

// WithHeaders clones the current `PrometheusLabelValuesQueryRequest` with new headers.
func (r *PrometheusLabelValuesQueryRequest) WithHeaders(headers []*PrometheusHeader) (LabelsQueryRequest, error) {
	newRequest := *r
	newRequest.Headers = cloneHeaders(headers)
	return &newRequest, nil
}

// WithHeaders clones the current `PrometheusSeriesQueryRequest` with new headers.
func (r *PrometheusSeriesQueryRequest) WithHeaders(headers []*PrometheusHeader) (LabelsQueryRequest, error) {
	newRequest := *r
	newRequest.Headers = cloneHeaders(headers)
	return &newRequest, nil
}

// AddSpanTags writes query information about the current `PrometheusLabelNamesQueryRequest`
// to a span's tag ("attributes" in OpenTelemetry parlance).
func (r *PrometheusLabelNamesQueryRequest) AddSpanTags(sp opentracing.Span) {
	sp.SetTag("matchers", fmt.Sprintf("%v", r.GetLabelMatcherSets()))
	sp.SetTag("start", timestamp.Time(r.GetStart()).String())
	sp.SetTag("end", timestamp.Time(r.GetEnd()).String())
}

// AddSpanTags writes query information about the current `PrometheusLabelNamesQueryRequest`
// to a span's tag ("attributes" in OpenTelemetry parlance).
func (r *PrometheusLabelValuesQueryRequest) AddSpanTags(sp opentracing.Span) {
	sp.SetTag("label", fmt.Sprintf("%v", r.GetLabelName()))
	sp.SetTag("matchers", fmt.Sprintf("%v", r.GetLabelMatcherSets()))
	sp.SetTag("start", timestamp.Time(r.GetStart()).String())
	sp.SetTag("end", timestamp.Time(r.GetEnd()).String())
}

// AddSpanTags writes query information about the current `PrometheusSeriesQueryRequest`
// to a span's tag ("attributes" in OpenTelemetry parlance).
func (r *PrometheusSeriesQueryRequest) AddSpanTags(sp opentracing.Span) {
	sp.SetTag("matchers", fmt.Sprintf("%v", r.GetLabelMatcherSets()))
	sp.SetTag("start", timestamp.Time(r.GetStart()).String())
	sp.SetTag("end", timestamp.Time(r.GetEnd()).String())
}

type PrometheusLabelNamesQueryRequest struct {
	Path  string
	Start int64
	End   int64
	// LabelMatcherSets is a repeated field here in order to enable the representation
	// of labels queries which have not yet been split; the prometheus querier code
	// will eventually split requests like `?match[]=up&match[]=process_start_time_seconds{job="prometheus"}`
	// into separate queries, one for each matcher set
	LabelMatcherSets []string
	// ID of the request used to correlate downstream requests and responses.
	ID int64
	// Limit the number of label names returned. A value of 0 means no limit
	Limit   uint64
	Headers []*PrometheusHeader
}

func (r *PrometheusLabelNamesQueryRequest) GetPath() string {
	return r.Path
}

func (r *PrometheusLabelNamesQueryRequest) GetStart() int64 {
	return r.Start
}

func (r *PrometheusLabelNamesQueryRequest) GetEnd() int64 {
	return r.End
}

func (r *PrometheusLabelNamesQueryRequest) GetLabelMatcherSets() []string {
	return r.LabelMatcherSets
}

func (r *PrometheusLabelNamesQueryRequest) GetID() int64 {
	return r.ID
}

func (r *PrometheusLabelNamesQueryRequest) GetLimit() uint64 {
	return r.Limit
}

type PrometheusLabelValuesQueryRequest struct {
	Path      string
	LabelName string
	Start     int64
	End       int64
	// LabelMatcherSets is a repeated field here in order to enable the representation
	// of labels queries which have not yet been split; the prometheus querier code
	// will eventually split requests like `?match[]=up&match[]=process_start_time_seconds{job="prometheus"}`
	// into separate queries, one for each matcher set
	LabelMatcherSets []string
	// ID of the request used to correlate downstream requests and responses.
	ID int64
	// Limit the number of label values returned. A value of 0 means no limit.
	Limit   uint64
	Headers []*PrometheusHeader
}

func (r *PrometheusLabelValuesQueryRequest) GetLabelName() string {
	return r.LabelName

}

func (r *PrometheusLabelValuesQueryRequest) GetStart() int64 {
	return r.Start
}

func (r *PrometheusLabelValuesQueryRequest) GetEnd() int64 {
	return r.End
}

func (r *PrometheusLabelValuesQueryRequest) GetLabelMatcherSets() []string {
	return r.LabelMatcherSets
}

func (r *PrometheusLabelValuesQueryRequest) GetID() int64 {
	return r.ID
}

func (r *PrometheusLabelValuesQueryRequest) GetLimit() uint64 {
	return r.Limit
}

type PrometheusSeriesQueryRequest struct {
	Path  string
	Start int64
	End   int64
	// LabelMatcherSets is a repeated field here in order to enable the representation
	// of labels queries which have not yet been split; the prometheus querier code
	// will eventually split requests like `?match[]=up&match[]=process_start_time_seconds{job="prometheus"}`
	// into separate queries, one for each matcher set
	LabelMatcherSets []string
	// ID of the request used to correlate downstream requests and responses.
	ID int64
	// Limit the number of label names returned. A value of 0 means no limit
	Limit   uint64
	Headers []*PrometheusHeader
}

func (r *PrometheusSeriesQueryRequest) GetPath() string {
	return r.Path
}

func (r *PrometheusSeriesQueryRequest) GetStart() int64 {
	return r.Start
}

func (r *PrometheusSeriesQueryRequest) GetEnd() int64 {
	return r.End
}

func (r *PrometheusSeriesQueryRequest) GetLabelMatcherSets() []string {
	return r.LabelMatcherSets
}

func (r *PrometheusSeriesQueryRequest) GetID() int64 {
	return r.ID
}

func (r *PrometheusSeriesQueryRequest) GetLimit() uint64 {
	return r.Limit
}

type PrometheusLabelsResponse struct {
	Status    string              `json:"status"`
	Data      []string            `json:"data"`
	ErrorType string              `json:"errorType,omitempty"`
	Error     string              `json:"error,omitempty"`
	Headers   []*PrometheusHeader `json:"-"`
}

func (m *PrometheusLabelsResponse) GetHeaders() []*PrometheusHeader {
	if m != nil {
		return m.Headers
	}
	return nil
}

func (m *PrometheusLabelsResponse) Reset()         { *m = PrometheusLabelsResponse{} }
func (*PrometheusLabelsResponse) ProtoMessage()    {}
func (m *PrometheusLabelsResponse) String() string { return fmt.Sprintf("%+v", *m) }

type SeriesData map[string]string

func (d *SeriesData) String() string { return fmt.Sprintf("%+v", *d) }

type PrometheusSeriesResponse struct {
	Status    string              `json:"status"`
	Data      []SeriesData        `json:"data"`
	ErrorType string              `json:"errorType,omitempty"`
	Error     string              `json:"error,omitempty"`
	Headers   []*PrometheusHeader `json:"-"`
}

func (m *PrometheusSeriesResponse) GetHeaders() []*PrometheusHeader {
	if m != nil {
		return m.Headers
	}
	return nil
}

func (m *PrometheusSeriesResponse) Reset()         { *m = PrometheusSeriesResponse{} }
func (*PrometheusSeriesResponse) ProtoMessage()    {}
func (m *PrometheusSeriesResponse) String() string { return fmt.Sprintf("%+v", *m) }

func (d *PrometheusData) UnmarshalJSON(b []byte) error {
	v := struct {
		Type   model.ValueType    `json:"resultType"`
		Result stdjson.RawMessage `json:"result"`
	}{}

	err := json.Unmarshal(b, &v)
	if err != nil {
		return err
	}
	d.ResultType = v.Type.String()
	switch v.Type {
	case model.ValString:
		var sss stringSampleStreams
		if err := json.Unmarshal(v.Result, &sss); err != nil {
			return err
		}
		d.Result = sss
		return nil

	case model.ValScalar:
		var sss scalarSampleStreams
		if err := json.Unmarshal(v.Result, &sss); err != nil {
			return err
		}
		d.Result = sss
		return nil

	case model.ValVector:
		var vss []vectorSampleStream
		if err := json.Unmarshal(v.Result, &vss); err != nil {
			return err
		}
		d.Result = fromVectorSampleStreams(vss)
		return nil

	case model.ValMatrix:
		return json.Unmarshal(v.Result, &d.Result)

	default:
		return fmt.Errorf("unsupported value type %q", v.Type)
	}
}

func (d *PrometheusData) MarshalJSON() ([]byte, error) {
	if d == nil {
		return []byte("null"), nil
	}

	switch d.ResultType {
	case model.ValString.String():
		return json.Marshal(struct {
			Type   model.ValueType     `json:"resultType"`
			Result stringSampleStreams `json:"result"`
		}{
			Type:   model.ValString,
			Result: d.Result,
		})

	case model.ValScalar.String():
		return json.Marshal(struct {
			Type   model.ValueType     `json:"resultType"`
			Result scalarSampleStreams `json:"result"`
		}{
			Type:   model.ValScalar,
			Result: d.Result,
		})

	case model.ValVector.String():
		return json.Marshal(struct {
			Type   model.ValueType      `json:"resultType"`
			Result []vectorSampleStream `json:"result"`
		}{
			Type:   model.ValVector,
			Result: asVectorSampleStreams(d.Result),
		})

	case model.ValMatrix.String():
		type plain *PrometheusData
		return json.Marshal(plain(d))

	default:
		return nil, fmt.Errorf("can't marshal prometheus result type %q", d.ResultType)
	}
}

type stringSampleStreams []SampleStream

func (sss stringSampleStreams) MarshalJSON() ([]byte, error) {
	if len(sss) != 1 {
		return nil, fmt.Errorf("string sample streams should have exactly one stream, got %d", len(sss))
	}
	ss := sss[0]
	if len(ss.Labels) != 1 || ss.Labels[0].Name != "value" {
		return nil, fmt.Errorf("string sample stream should have exactly one label called value, got %d: %v", len(ss.Labels), ss.Labels)
	}
	l := ss.Labels[0]

	if len(ss.Samples) != 1 {
		return nil, fmt.Errorf("string sample stream should have exactly one sample, got %d", len(ss.Samples))
	}
	s := ss.Samples[0]

	return json.Marshal(model.String{Value: l.Value, Timestamp: model.Time(s.TimestampMs)})
}

func (sss *stringSampleStreams) UnmarshalJSON(b []byte) error {
	var sv model.String
	if err := json.Unmarshal(b, &sv); err != nil {
		return err
	}
	*sss = []SampleStream{{
		Labels:  []mimirpb.LabelAdapter{{Name: "value", Value: sv.Value}},
		Samples: []mimirpb.Sample{{TimestampMs: int64(sv.Timestamp)}},
	}}
	return nil
}

type scalarSampleStreams []SampleStream

func (sss scalarSampleStreams) MarshalJSON() ([]byte, error) {
	if len(sss) != 1 {
		return nil, fmt.Errorf("scalar sample streams should have exactly one stream, got %d", len(sss))
	}
	ss := sss[0]
	if len(ss.Samples) != 1 {
		return nil, fmt.Errorf("scalar sample stream should have exactly one sample, got %d", len(ss.Samples))
	}
	s := ss.Samples[0]
	return json.Marshal(model.Scalar{
		Timestamp: model.Time(s.TimestampMs),
		Value:     model.SampleValue(s.Value),
	})
}

func (sss *scalarSampleStreams) UnmarshalJSON(b []byte) error {
	var sv model.Scalar
	if err := json.Unmarshal(b, &sv); err != nil {
		return err
	}
	*sss = []SampleStream{{
		Samples: []mimirpb.Sample{{TimestampMs: int64(sv.Timestamp), Value: float64(sv.Value)}},
	}}
	return nil
}

// asVectorSampleStreams converts a slice of SampleStream into a slice of vectorSampleStream.
// This can be done as vectorSampleStream is defined as a SampleStream.
func asVectorSampleStreams(ss []SampleStream) []vectorSampleStream {
	return *(*[]vectorSampleStream)(unsafe.Pointer(&ss))
}

// fromVectorSampleStreams is the inverse of asVectorSampleStreams.
func fromVectorSampleStreams(vss []vectorSampleStream) []SampleStream {
	return *(*[]SampleStream)(unsafe.Pointer(&vss))
}

type vectorSampleStream SampleStream

func (vs *vectorSampleStream) UnmarshalJSON(b []byte) error {
	s := model.Sample{}
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	if s.Histogram != nil {
		return errors.New("cannot unmarshal native histogram from JSON")
	}

	*vs = vectorSampleStream{
		Labels:  mimirpb.FromMetricsToLabelAdapters(s.Metric),
		Samples: []mimirpb.Sample{{TimestampMs: int64(s.Timestamp), Value: float64(s.Value)}},
	}
	return nil
}

func (vs vectorSampleStream) MarshalJSON() ([]byte, error) {
	if (len(vs.Samples) == 1) == (len(vs.Histograms) == 1) { // not XOR
		return nil, fmt.Errorf("vector sample stream should have exactly one sample or one histogram, got %d samples and %d histograms", len(vs.Samples), len(vs.Histograms))
	}
	var sample model.Sample
	if len(vs.Samples) == 1 {
		sample = model.Sample{
			Metric:    mimirpb.FromLabelAdaptersToMetric(vs.Labels),
			Timestamp: model.Time(vs.Samples[0].TimestampMs),
			Value:     model.SampleValue(vs.Samples[0].Value),
		}
	} else {
		sample = model.Sample{
			Metric:    mimirpb.FromLabelAdaptersToMetric(vs.Labels),
			Timestamp: model.Time(vs.Histograms[0].TimestampMs),
			Histogram: mimirpb.FromFloatHistogramToPromHistogram(vs.Histograms[0].Histogram.ToPrometheusModel()),
		}
	}
	return json.Marshal(sample)
}

// UnmarshalJSON implements json.Unmarshaler.
func (s *SampleStream) UnmarshalJSON(data []byte) error {
	var stream struct {
		Metric     model.Metric                  `json:"metric"`
		Values     []mimirpb.Sample              `json:"values"`
		Histograms []mimirpb.SampleHistogramPair `json:"histograms"`
	}
	if err := json.Unmarshal(data, &stream); err != nil {
		return err
	}
	s.Labels = mimirpb.FromMetricsToLabelAdapters(stream.Metric)
	if len(stream.Values) > 0 {
		s.Samples = stream.Values
	}
	if len(stream.Histograms) > 0 {
		return fmt.Errorf("cannot unmarshal native histograms from JSON, but stream contains %d histograms", len(stream.Histograms))
	}
	return nil
}

// MarshalJSON implements json.Marshaler.
func (s *SampleStream) MarshalJSON() ([]byte, error) {
	var histograms []mimirpb.SampleHistogramPair
	if len(s.Histograms) > 0 {
		histograms = make([]mimirpb.SampleHistogramPair, len(s.Histograms))
	}

	for i, h := range s.Histograms {
		histograms[i] = mimirpb.SampleHistogramPair{
			Timestamp: h.TimestampMs,
			Histogram: mimirpb.FromFloatHistogramToSampleHistogram(h.Histogram.ToPrometheusModel()),
		}
	}

	stream := struct {
		Metric     model.Metric                  `json:"metric"`
		Values     []mimirpb.Sample              `json:"values,omitempty"`
		Histograms []mimirpb.SampleHistogramPair `json:"histograms,omitempty"`
	}{
		Metric:     mimirpb.FromLabelAdaptersToMetric(s.Labels),
		Values:     s.Samples,
		Histograms: histograms,
	}

	return json.Marshal(stream)
}

type byFirstTime []*PrometheusResponse

func (a byFirstTime) Len() int           { return len(a) }
func (a byFirstTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byFirstTime) Less(i, j int) bool { return a[i].minTime() < a[j].minTime() }

func (resp *PrometheusResponse) minTime() int64 {
	result := resp.Data.Result
	if len(result) == 0 {
		return -1
	}
	if len(result[0].Samples) == 0 {
		return -1
	}
	return result[0].Samples[0].TimestampMs
}

// EncodeCachedHTTPResponse encodes the input http.Response into CachedHTTPResponse.
// The input res.Body is replaced in this function, so that it can be safely consumed again.
func EncodeCachedHTTPResponse(cacheKey string, res *http.Response) (*CachedHTTPResponse, error) {
	// Read the response.
	body, err := readResponseBody(res)
	if err != nil {
		return nil, err
	}

	// Since we've already consumed the response Body we have to replace it on the response,
	// otherwise the caller will get a response with a closed Body.
	res.Body = io.NopCloser(bytes.NewBuffer(body))

	// When preallocating the slice we assume that header as 1 value (which is the most common case).
	headers := make([]*CachedHTTPHeader, 0, len(res.Header))
	for name, values := range res.Header {
		for _, value := range values {
			headers = append(headers, &CachedHTTPHeader{
				Name:  name,
				Value: value,
			})
		}
	}

	return &CachedHTTPResponse{
		CacheKey:   cacheKey,
		StatusCode: int32(res.StatusCode),
		Body:       body,
		Headers:    headers,
	}, nil
}

func DecodeCachedHTTPResponse(res *CachedHTTPResponse) *http.Response {
	headers := http.Header{}
	for _, header := range res.Headers {
		headers[header.Name] = append(headers[header.Name], header.Value)
	}

	return &http.Response{
		StatusCode:    int(res.StatusCode),
		Body:          io.NopCloser(bytes.NewReader(res.Body)),
		Header:        headers,
		ContentLength: int64(len(res.Body)),
	}
}

// cloneHeaders makes a deep copy of the passed headers parameters.
// This function is designed to be used in MetricsQueryRequest cloning method to avoid modifying
// original request headers, which may cause undesired side effects during the processing in the middleware chain.
func cloneHeaders(headers []*PrometheusHeader) []*PrometheusHeader {
	if headers == nil {
		return nil
	}
	cp := make([]*PrometheusHeader, len(headers))
	for i, h := range headers {
		cp[i] = &PrometheusHeader{Name: h.Name, Values: slices.Clone(h.Values)}
	}
	return cp
}
