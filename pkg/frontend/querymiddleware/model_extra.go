// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/query_range.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"slices"
	"time"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/jsonutil"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/mimirpb"
)

var (
	json = jsoniter.Config{
		EscapeHTML:             false, // No HTML in our responses.
		SortMapKeys:            true,
		ValidateJsonRawMessage: true,
	}.Froze()
)

func init() {
	jsoniter.RegisterTypeEncoderFunc("querymiddleware.PrometheusData", prometheusDataJsoniterEncode, func(unsafe.Pointer) bool { return false })
	jsoniter.RegisterTypeDecoderFunc("querymiddleware.PrometheusData", prometheusDataJsoniterDecode)
}

// NewEmptyPrometheusResponse returns an empty successful Prometheus query range response.
func NewEmptyPrometheusResponse() *PrometheusResponse {
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
	// stats controls query engine stats collection for the request.
	stats string
}

func NewPrometheusRangeQueryRequest(
	urlPath string,
	headers []*PrometheusHeader,
	start, end, step int64,
	lookbackDelta time.Duration,
	queryExpr parser.Expr,
	options Options,
	hints *Hints,
	stats string,
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
		stats:         stats,
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

func (r *PrometheusRangeQueryRequest) GetClonedParsedQuery() (parser.Expr, error) {
	if r.queryExpr == nil {
		return nil, errRequestNoQuery
	}

	return astmapper.CloneExpr(r.queryExpr)
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

func (r *PrometheusRangeQueryRequest) GetLookbackDelta() time.Duration {
	return r.lookbackDelta
}

func (r *PrometheusRangeQueryRequest) GetStats() string {
	return r.stats
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

func (r *PrometheusRangeQueryRequest) WithStats(stats string) (MetricsQueryRequest, error) {
	newRequest := *r
	newRequest.headers = cloneHeaders(r.headers)
	newRequest.stats = stats
	return &newRequest, nil
}

// AddSpanTags writes the current `PrometheusRangeQueryRequest` parameters to the specified span tags
// ("attributes" in OpenTelemetry parlance).
func (r *PrometheusRangeQueryRequest) AddSpanTags(sp trace.Span) {
	sp.SetAttributes(
		attribute.String("query", r.GetQuery()),
		attribute.String("start", timestamp.Time(r.GetStart()).String()),
		attribute.String("end", timestamp.Time(r.GetEnd()).String()),
		attribute.Int64("step_ms", r.GetStep()),
	)
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
	// stats controls stats collection for the request.
	stats string
}

func NewPrometheusInstantQueryRequest(
	urlPath string,
	headers []*PrometheusHeader,
	time int64,
	lookbackDelta time.Duration,
	queryExpr parser.Expr,
	options Options,
	hints *Hints,
	stats string,
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
		stats:         stats,
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

func (r *PrometheusInstantQueryRequest) GetClonedParsedQuery() (parser.Expr, error) {
	if r.queryExpr == nil {
		return nil, errRequestNoQuery
	}

	return astmapper.CloneExpr(r.queryExpr)
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

func (r *PrometheusInstantQueryRequest) GetLookbackDelta() time.Duration {
	return r.lookbackDelta
}

func (r *PrometheusInstantQueryRequest) GetStats() string {
	return r.stats
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

func (r *PrometheusInstantQueryRequest) WithStats(stats string) (MetricsQueryRequest, error) {
	newRequest := *r
	newRequest.headers = cloneHeaders(r.headers)
	newRequest.stats = stats
	return &newRequest, nil
}

// AddSpanTags writes query information about the current `PrometheusInstantQueryRequest`
// to a span's tag ("attributes" in OpenTelemetry parlance).
func (r *PrometheusInstantQueryRequest) AddSpanTags(sp trace.Span) {
	sp.SetAttributes(
		attribute.String("query", r.GetQuery()),
		attribute.String("time", timestamp.Time(r.GetTime()).String()),
	)
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
func (r *PrometheusLabelNamesQueryRequest) WithLabelName(string) (LabelsSeriesQueryRequest, error) {
	panic("PrometheusLabelNamesQueryRequest.WithLabelName is not implemented")
}

// WithLabelName clones the current `PrometheusLabelValuesQueryRequest` with a new label name param.
func (r *PrometheusLabelValuesQueryRequest) WithLabelName(name string) (LabelsSeriesQueryRequest, error) {
	newRequest := *r
	newRequest.Path = labelValuesPathSuffix.ReplaceAllString(r.Path, `/api/v1/label/`+name+`/values`)
	newRequest.LabelName = name
	return &newRequest, nil
}

// WithLabelName clones the current `PrometheusSeriesQueryRequest` with a new label name param.
func (r *PrometheusSeriesQueryRequest) WithLabelName(string) (LabelsSeriesQueryRequest, error) {
	panic("PrometheusSeriesQueryRequest.WithLabelName is not implemented")
}

// WithLabelMatcherSets clones the current `PrometheusLabelNamesQueryRequest` with new label matcher sets.
func (r *PrometheusLabelNamesQueryRequest) WithLabelMatcherSets(labelMatcherSets []string) (LabelsSeriesQueryRequest, error) {
	newRequest := *r
	newRequest.LabelMatcherSets = make([]string, len(labelMatcherSets))
	copy(newRequest.LabelMatcherSets, labelMatcherSets)
	return &newRequest, nil
}

// WithLabelMatcherSets clones the current `PrometheusLabelValuesQueryRequest` with new label matcher sets.
func (r *PrometheusLabelValuesQueryRequest) WithLabelMatcherSets(labelMatcherSets []string) (LabelsSeriesQueryRequest, error) {
	newRequest := *r
	newRequest.LabelMatcherSets = make([]string, len(labelMatcherSets))
	copy(newRequest.LabelMatcherSets, labelMatcherSets)
	return &newRequest, nil
}

// WithLabelMatcherSets clones the current `PrometheusSeriesQueryRequest` with new label matcher sets.
func (r *PrometheusSeriesQueryRequest) WithLabelMatcherSets(labelMatcherSets []string) (LabelsSeriesQueryRequest, error) {
	newRequest := *r
	newRequest.LabelMatcherSets = make([]string, len(labelMatcherSets))
	copy(newRequest.LabelMatcherSets, labelMatcherSets)
	return &newRequest, nil
}

// WithHeaders clones the current `PrometheusLabelNamesQueryRequest` with new headers.
func (r *PrometheusLabelNamesQueryRequest) WithHeaders(headers []*PrometheusHeader) (LabelsSeriesQueryRequest, error) {
	newRequest := *r
	newRequest.Headers = cloneHeaders(headers)
	return &newRequest, nil
}

// WithHeaders clones the current `PrometheusLabelValuesQueryRequest` with new headers.
func (r *PrometheusLabelValuesQueryRequest) WithHeaders(headers []*PrometheusHeader) (LabelsSeriesQueryRequest, error) {
	newRequest := *r
	newRequest.Headers = cloneHeaders(headers)
	return &newRequest, nil
}

// WithHeaders clones the current `PrometheusSeriesQueryRequest` with new headers.
func (r *PrometheusSeriesQueryRequest) WithHeaders(headers []*PrometheusHeader) (LabelsSeriesQueryRequest, error) {
	newRequest := *r
	newRequest.Headers = cloneHeaders(headers)
	return &newRequest, nil
}

// AddSpanTags writes query information about the current `PrometheusLabelNamesQueryRequest`
// to a span's tag ("attributes" in OpenTelemetry parlance).
func (r *PrometheusLabelNamesQueryRequest) AddSpanTags(sp trace.Span) {
	sp.SetAttributes(
		attribute.String("matchers", fmt.Sprintf("%v", r.GetLabelMatcherSets())),
		attribute.String("start", timestamp.Time(r.GetStart()).String()),
		attribute.String("end", timestamp.Time(r.GetEnd()).String()),
	)
}

// AddSpanTags writes query information about the current `PrometheusLabelNamesQueryRequest`
// to a span's tag ("attributes" in OpenTelemetry parlance).
func (r *PrometheusLabelValuesQueryRequest) AddSpanTags(sp trace.Span) {
	sp.SetAttributes(
		attribute.String("label", r.GetLabelName()),
		attribute.String("matchers", fmt.Sprintf("%v", r.GetLabelMatcherSets())),
		attribute.String("start", timestamp.Time(r.GetStart()).String()),
		attribute.String("end", timestamp.Time(r.GetEnd()).String()),
	)
}

// AddSpanTags writes query information about the current `PrometheusSeriesQueryRequest`
// to a span's tag ("attributes" in OpenTelemetry parlance).
func (r *PrometheusSeriesQueryRequest) AddSpanTags(sp trace.Span) {
	sp.SetAttributes(
		attribute.String("matchers", fmt.Sprintf("%v", r.GetLabelMatcherSets())),
		attribute.String("start", timestamp.Time(r.GetStart()).String()),
		attribute.String("end", timestamp.Time(r.GetEnd()).String()),
	)
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
	Warnings  []string            `json:"warnings,omitempty"`
	Infos     []string            `json:"infos,omitempty"`
}

func (m *PrometheusLabelsResponse) Close() {
	// Nothing to do
}

func (m *PrometheusLabelsResponse) GetPrometheusResponse() (*PrometheusResponse, bool) {
	return nil, false
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
	Warnings  []string            `json:"warnings,omitempty"`
	Infos     []string            `json:"infos,omitempty"`
}

func (m *PrometheusSeriesResponse) Close() {
	// Nothing to do
}

func (m *PrometheusSeriesResponse) GetPrometheusResponse() (*PrometheusResponse, bool) {
	return nil, false
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

func prometheusDataJsoniterDecode(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	d := (*PrometheusData)(ptr)
	v := struct {
		Type   model.ValueType `json:"resultType"`
		Result jsoniter.Any    `json:"result"`
	}{}

	iter.ReadVal(&v)
	if iter.Error != nil {
		return
	}
	d.ResultType = v.Type.String()
	switch v.Type {
	case model.ValString:
		var sv model.String
		v.Result.ToVal(&sv)
		d.Result = []SampleStream{{
			Labels:  []mimirpb.LabelAdapter{{Name: "value", Value: sv.Value}},
			Samples: []mimirpb.Sample{{TimestampMs: int64(sv.Timestamp)}},
		}}

	case model.ValScalar:
		var sv model.Scalar
		v.Result.ToVal(&sv)
		d.Result = []SampleStream{{
			Samples: []mimirpb.Sample{{TimestampMs: int64(sv.Timestamp), Value: float64(sv.Value)}},
		}}

	case model.ValVector:
		var vss []vectorSample
		v.Result.ToVal(&vss)
		var errString string
		d.Result, errString = fromVectorSamples(vss)
		if errString != "" {
			iter.ReportError("PrometheusData", errString)
		}

	case model.ValMatrix:
		v.Result.ToVal(&d.Result)

	default:
		iter.ReportError("PrometheusData", fmt.Sprintf("unsupported value type %q", v.Type))
	}
}

func prometheusDataJsoniterEncode(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	d := (*PrometheusData)(ptr)
	stream.WriteObjectStart()
	stream.WriteObjectField(`resultType`)
	stream.WriteString(d.ResultType)
	stream.WriteMore()
	stream.WriteObjectField(`result`)
	switch d.ResultType {
	case model.ValString.String():
		if err := validateStringSampleStream(d.Result); err != nil {
			stream.Error = err
			return
		}
		stringSampleEncode(d.Result, stream)
	case model.ValScalar.String():
		if err := validateScalarSampleStream(d.Result); err != nil {
			stream.Error = err
			return
		}
		scalarSampleEncode(d.Result, stream)
	case model.ValVector.String():
		if err := validateVectorSampleStream(d.Result); err != nil {
			stream.Error = err
			return
		}
		vectorSampleStreamEncode(d.Result, stream)
	case model.ValMatrix.String():
		stream.WriteVal(d.Result)

	default:
		stream.Error = fmt.Errorf("can't marshal prometheus result type %q", d.ResultType)
	}
	stream.WriteObjectEnd()
}

func validateStringSampleStream(sss []SampleStream) error {
	if len(sss) != 1 {
		return fmt.Errorf("string sample streams should have exactly one stream, got %d", len(sss))
	}
	ss := sss[0]
	if len(ss.Labels) != 1 || ss.Labels[0].Name != "value" {
		return fmt.Errorf("string sample stream should have exactly one label called value, got %d: %v", len(ss.Labels), ss.Labels)
	}
	if len(ss.Samples) != 1 {
		return fmt.Errorf("string sample stream should have exactly one sample, got %d", len(ss.Samples))
	}
	return nil
}

// A string sample is written like `[12345678, "hello"]`
func stringSampleEncode(sss []SampleStream, stream *jsoniter.Stream) {
	stream.WriteArrayStart()
	stream.WriteRaw(model.Time(sss[0].Samples[0].TimestampMs).String())
	stream.WriteMore()
	stream.WriteString(sss[0].Labels[0].Value)
	stream.WriteArrayEnd()
}

func validateScalarSampleStream(sss []SampleStream) error {
	if len(sss) != 1 {
		return fmt.Errorf("scalar sample streams should have exactly one stream, got %d", len(sss))
	}
	ss := sss[0]
	if len(ss.Samples) != 1 {
		return fmt.Errorf("scalar sample stream should have exactly one sample, got %d", len(ss.Samples))
	}
	return nil
}

// A scalar sample is written like `[12345678, "42"]`
func scalarSampleEncode(sss []SampleStream, stream *jsoniter.Stream) {
	s := sss[0].Samples[0]
	stream.WriteArrayStart()
	stream.WriteRaw(model.Time(s.TimestampMs).String())
	stream.WriteMore()
	jsonutil.MarshalFloat(s.Value, stream)
	stream.WriteArrayEnd()
}

type vectorSample struct {
	Labels    []mimirpb.LabelAdapter `json:"metric"`
	Value     model.SamplePair       `json:"value"`
	Histogram *model.SampleHistogram `json:"histogram"`
}

func fromVectorSamples(vss []vectorSample) ([]SampleStream, string) {
	ret := make([]SampleStream, len(vss))
	for i, s := range vss {
		if s.Histogram != nil {
			return nil, "cannot unmarshal native histogram from JSON"
		}
		ret[i] = SampleStream{
			Labels:  s.Labels,
			Samples: []mimirpb.Sample{{TimestampMs: int64(s.Value.Timestamp), Value: float64(s.Value.Value)}},
		}
	}
	return ret, ""
}

func validateVectorSampleStream(vss []SampleStream) error {
	for _, vs := range vss {
		if (len(vs.Samples) == 1) == (len(vs.Histograms) == 1) { // not XOR
			return fmt.Errorf("vector sample stream should have exactly one sample or one histogram, got %d samples and %d histograms", len(vs.Samples), len(vs.Histograms))
		}
	}
	return nil
}

// Encode a slice of SampleStream objects which each hold just one data point.
func vectorSampleStreamEncode(vss []SampleStream, stream *jsoniter.Stream) {
	stream.WriteArrayStart()
	for i, vs := range vss {
		if i > 0 {
			stream.WriteMore()
		}
		stream.WriteObjectStart()
		stream.WriteObjectField(`metric`)
		mimirpb.LabelAdaptersEncode(vs.Labels, stream)
		stream.WriteMore()
		if len(vs.Samples) == 1 {
			stream.WriteObjectField(`value`)
			mimirpb.SampleJsoniterEncode(vs.Samples[0], stream)
		} else {
			stream.WriteObjectField(`histogram`)
			mimirpb.HistogramJsoniterEncode(vs.Histograms[0], stream)
		}
		stream.WriteObjectEnd()
	}
	stream.WriteArrayEnd()
}

func (resp *PrometheusResponse) Close() {
	// Nothing to do
}

func (resp *PrometheusResponse) GetPrometheusResponse() (*PrometheusResponse, bool) {
	return resp, true
}

type PrometheusResponseWithFinalizer struct {
	*PrometheusResponse
	finalizer func()
}

func (resp *PrometheusResponseWithFinalizer) Close() {
	resp.finalizer()
}

func (resp *PrometheusResponseWithFinalizer) GetPrometheusResponse() (*PrometheusResponse, bool) {
	return resp.PrometheusResponse, true
}

// Creates a deep clone
func (resp *PrometheusResponse) Clone() *PrometheusResponse {
	respClone := &PrometheusResponse{
		Status:    resp.Status,
		Data:      proto.Clone(resp.Data).(*PrometheusData),
		ErrorType: resp.ErrorType,
		Error:     resp.Error,
		Headers:   make([]*PrometheusHeader, len(resp.Headers)),
		Warnings:  make([]string, len(resp.Warnings)),
		Infos:     make([]string, len(resp.Infos)),
	}
	copy(respClone.Warnings, resp.Warnings)
	copy(respClone.Infos, resp.Infos)
	for i, header := range resp.Headers {
		respClone.Headers[i] = proto.Clone(header).(*PrometheusHeader)
	}
	return respClone
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
