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
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/timestamp"
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
	Path    string
	Start   int64
	End     int64
	Step    int64
	Timeout time.Duration
	Query   string
	Options Options
	// ID of the request used to correlate downstream requests and responses.
	ID int64
	// Hints that could be optionally attached to the request to pass down the stack.
	// These hints can be used to optimize the query execution.
	Hints *Hints
}

func (r *PrometheusRangeQueryRequest) GetPath() string {
	return r.Path
}

func (r *PrometheusRangeQueryRequest) GetStart() int64 {
	return r.Start
}

func (r *PrometheusRangeQueryRequest) GetEnd() int64 {
	return r.End
}

func (r *PrometheusRangeQueryRequest) GetStep() int64 {
	return r.Step
}

func (r *PrometheusRangeQueryRequest) GetTimeout() time.Duration {
	return r.Timeout
}

func (r *PrometheusRangeQueryRequest) GetQuery() string {
	return r.Query
}

func (r *PrometheusRangeQueryRequest) GetOptions() Options {
	return r.Options
}

func (r *PrometheusRangeQueryRequest) GetID() int64 {
	return r.ID
}

func (r *PrometheusRangeQueryRequest) GetHints() *Hints {
	return r.Hints
}

type PrometheusInstantQueryRequest struct {
	Path    string
	Time    int64
	Query   string
	Options Options
	// ID of the request used to correlate downstream requests and responses.
	ID int64
	// Hints that could be optionally attached to the request to pass down the stack.
	// These hints can be used to optimize the query execution.
	Hints *Hints
}

func (r *PrometheusInstantQueryRequest) GetPath() string {
	return r.Path
}

func (r *PrometheusInstantQueryRequest) GetTime() int64 {
	return r.Time
}

func (r *PrometheusInstantQueryRequest) GetQuery() string {
	return r.Query
}

func (r *PrometheusInstantQueryRequest) GetOptions() Options {
	return r.Options
}

func (r *PrometheusInstantQueryRequest) GetID() int64 {
	return r.ID
}

func (r *PrometheusInstantQueryRequest) GetHints() *Hints {
	return r.Hints
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

// WithID clones the current `PrometheusRangeQueryRequest` with the provided ID.
func (r *PrometheusRangeQueryRequest) WithID(id int64) MetricsQueryRequest {
	newRequest := *r
	newRequest.ID = id
	return &newRequest
}

// WithStartEnd clones the current `PrometheusRangeQueryRequest` with a new `start` and `end` timestamp.
func (r *PrometheusRangeQueryRequest) WithStartEnd(start int64, end int64) MetricsQueryRequest {
	newRequest := *r
	newRequest.Start = start
	newRequest.End = end
	return &newRequest
}

// WithQuery clones the current `PrometheusRangeQueryRequest` with a new query.
func (r *PrometheusRangeQueryRequest) WithQuery(query string) MetricsQueryRequest {
	newRequest := *r
	newRequest.Query = query
	return &newRequest
}

// WithTotalQueriesHint clones the current `PrometheusRangeQueryRequest` with an
// added Hint value for TotalQueries.
func (r *PrometheusRangeQueryRequest) WithTotalQueriesHint(totalQueries int32) MetricsQueryRequest {
	newRequest := *r
	if newRequest.Hints == nil {
		newRequest.Hints = &Hints{TotalQueries: totalQueries}
	} else {
		*newRequest.Hints = *(r.Hints)
		newRequest.Hints.TotalQueries = totalQueries
	}
	return &newRequest
}

// WithEstimatedSeriesCountHint clones the current `PrometheusRangeQueryRequest`
// with an added Hint value for EstimatedCardinality.
func (r *PrometheusRangeQueryRequest) WithEstimatedSeriesCountHint(count uint64) MetricsQueryRequest {
	newRequest := *r
	if newRequest.Hints == nil {
		newRequest.Hints = &Hints{
			CardinalityEstimate: &EstimatedSeriesCount{count},
		}
	} else {
		*newRequest.Hints = *(r.Hints)
		newRequest.Hints.CardinalityEstimate = &EstimatedSeriesCount{count}
	}
	return &newRequest
}

// AddSpanTags writes the current `PrometheusRangeQueryRequest` parameters to the specified span tags
// ("attributes" in OpenTelemetry parlance).
func (r *PrometheusRangeQueryRequest) AddSpanTags(sp opentracing.Span) {
	sp.SetTag("query", r.GetQuery())
	sp.SetTag("start", timestamp.Time(r.GetStart()).String())
	sp.SetTag("end", timestamp.Time(r.GetEnd()).String())
	sp.SetTag("step_ms", r.GetStep())
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

func (r *PrometheusInstantQueryRequest) WithID(id int64) MetricsQueryRequest {
	newRequest := *r
	newRequest.ID = id
	return &newRequest
}

func (r *PrometheusInstantQueryRequest) WithStartEnd(startTime int64, _ int64) MetricsQueryRequest {
	newRequest := *r
	newRequest.Time = startTime
	return &newRequest
}

func (r *PrometheusInstantQueryRequest) WithQuery(s string) MetricsQueryRequest {
	newRequest := *r
	newRequest.Query = s
	return &newRequest
}

func (r *PrometheusInstantQueryRequest) WithTotalQueriesHint(totalQueries int32) MetricsQueryRequest {
	newRequest := *r
	if newRequest.Hints == nil {
		newRequest.Hints = &Hints{TotalQueries: totalQueries}
	} else {
		*newRequest.Hints = *(r.Hints)
		newRequest.Hints.TotalQueries = totalQueries
	}
	return &newRequest
}

func (r *PrometheusInstantQueryRequest) WithEstimatedSeriesCountHint(count uint64) MetricsQueryRequest {
	newRequest := *r
	if newRequest.Hints == nil {
		newRequest.Hints = &Hints{
			CardinalityEstimate: &EstimatedSeriesCount{count},
		}
	} else {
		*newRequest.Hints = *(r.Hints)
		newRequest.Hints.CardinalityEstimate = &EstimatedSeriesCount{count}
	}
	return &newRequest
}

// AddSpanTags writes query information about the current `PrometheusInstantQueryRequest`
// to a span's tag ("attributes" in OpenTelemetry parlance).
func (r *PrometheusInstantQueryRequest) AddSpanTags(sp opentracing.Span) {
	sp.SetTag("query", r.GetQuery())
	sp.SetTag("time", timestamp.Time(r.GetTime()).String())
}

func (r *PrometheusLabelNamesQueryRequest) GetLabelName() string {
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

type PrometheusLabelNamesQueryRequest struct {
	Path  string
	Start int64
	End   int64
	// labelMatcherSets is a repeated field here in order to enable the representation
	// of labels queries which have not yet been split; the prometheus querier code
	// will eventually split requests like `?match[]=up&match[]=process_start_time_seconds{job="prometheus"}`
	// into separate queries, one for each matcher set
	LabelMatcherSets []string
	// ID of the request used to correlate downstream requests and responses.
	ID int64
	// Limit the number of label names returned. A value of 0 means no limit
	Limit uint64
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
	// labelMatcherSets is a repeated field here in order to enable the representation
	// of labels queries which have not yet been split; the prometheus querier code
	// will eventually split requests like `?match[]=up&match[]=process_start_time_seconds{job="prometheus"}`
	// into separate queries, one for each matcher set
	LabelMatcherSets []string
	// ID of the request used to correlate downstream requests and responses.
	ID int64
	// Limit the number of label values returned. A value of 0 means no limit.
	Limit uint64
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
