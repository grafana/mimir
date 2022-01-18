// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/query_range.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queryrange

import (
	stdjson "encoding/json"
	"fmt"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/timestamp"

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

// WithID clones the current `PrometheusRangeQueryRequest` with the provided ID.
func (q *PrometheusRangeQueryRequest) WithID(id int64) Request {
	new := *q
	new.Id = id
	return &new
}

// WithStartEnd clones the current `PrometheusRangeQueryRequest` with a new `start` and `end` timestamp.
func (q *PrometheusRangeQueryRequest) WithStartEnd(start int64, end int64) Request {
	new := *q
	new.Start = start
	new.End = end
	return &new
}

// WithQuery clones the current `PrometheusRangeQueryRequest` with a new query.
func (q *PrometheusRangeQueryRequest) WithQuery(query string) Request {
	new := *q
	new.Query = query
	return &new
}

// WithQuery clones the current `PrometheusRangeQueryRequest` with new hints.
func (q *PrometheusRangeQueryRequest) WithHints(hints *Hints) Request {
	new := *q
	new.Hints = hints
	return &new
}

// LogToSpan logs the current `PrometheusRangeQueryRequest` parameters to the specified span.
func (q *PrometheusRangeQueryRequest) LogToSpan(sp opentracing.Span) {
	sp.LogFields(
		otlog.String("query", q.GetQuery()),
		otlog.String("start", timestamp.Time(q.GetStart()).String()),
		otlog.String("end", timestamp.Time(q.GetEnd()).String()),
		otlog.Int64("step (ms)", q.GetStep()),
	)
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

func (r *PrometheusInstantQueryRequest) WithID(id int64) Request {
	new := *r
	new.Id = id
	return &new
}

func (r *PrometheusInstantQueryRequest) WithStartEnd(startTime int64, endTime int64) Request {
	new := *r
	new.Time = startTime
	return &new
}

func (r *PrometheusInstantQueryRequest) WithQuery(s string) Request {
	new := *r
	new.Query = s
	return &new
}

func (r *PrometheusInstantQueryRequest) WithHints(hints *Hints) Request {
	new := *r
	new.Hints = hints
	return &new
}

func (r *PrometheusInstantQueryRequest) LogToSpan(sp opentracing.Span) {
	sp.LogFields(
		otlog.String("query", r.GetQuery()),
		otlog.String("time", timestamp.Time(r.GetTime()).String()),
	)
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
	*vs = vectorSampleStream{
		Labels:  mimirpb.FromMetricsToLabelAdapters(s.Metric),
		Samples: []mimirpb.Sample{{TimestampMs: int64(s.Timestamp), Value: float64(s.Value)}},
	}
	return nil
}

func (vs vectorSampleStream) MarshalJSON() ([]byte, error) {
	if len(vs.Samples) != 1 {
		return nil, fmt.Errorf("vector sample stream should have exactly one sample, got %d", len(vs.Samples))
	}
	return json.Marshal(model.Sample{
		Metric:    mimirpb.FromLabelAdaptersToMetric(vs.Labels),
		Timestamp: model.Time(vs.Samples[0].TimestampMs),
		Value:     model.SampleValue(vs.Samples[0].Value),
	})
}

// UnmarshalJSON implements json.Unmarshaler.
func (s *SampleStream) UnmarshalJSON(data []byte) error {
	var stream struct {
		Metric model.Metric     `json:"metric"`
		Values []mimirpb.Sample `json:"values"`
	}
	if err := json.Unmarshal(data, &stream); err != nil {
		return err
	}
	s.Labels = mimirpb.FromMetricsToLabelAdapters(stream.Metric)
	s.Samples = stream.Values
	return nil
}

// MarshalJSON implements json.Marshaler.
func (s *SampleStream) MarshalJSON() ([]byte, error) {
	stream := struct {
		Metric model.Metric     `json:"metric"`
		Values []mimirpb.Sample `json:"values"`
	}{
		Metric: mimirpb.FromLabelAdaptersToMetric(s.Labels),
		Values: s.Samples,
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
