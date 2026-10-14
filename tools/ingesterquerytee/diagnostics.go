// SPDX-License-Identifier: AGPL-3.0-only

package ingesterquerytee

import (
	"cmp"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

const diagnosticLogBytes = 16 << 10
const diagnosticBuckets = 8

type sampleDifference struct {
	labels string
	key    sampleKey
	reason string
}

func firstSampleDifference(a, b samplesBySeries) *sampleDifference {
	var first *sampleDifference
	consider := func(labels string, key sampleKey, reason string) {
		if first == nil || cmp.Or(cmp.Compare(labels, first.labels), cmp.Compare(key.timestamp, first.key.timestamp), cmp.Compare(key.startTimestamp, first.key.startTimestamp)) < 0 {
			first = &sampleDifference{labels: labels, key: key, reason: reason}
		}
	}
	for labels, samples := range a {
		for key, value := range samples {
			other, exists := b[labels][key]
			if !exists {
				consider(labels, key, "missing_from_shadow")
			} else if !equalSample(value, other) {
				reason := "sample_value"
				if value.digest == other.digest {
					reason = "histogram_reset_hint"
				}
				consider(labels, key, reason)
			}
		}
	}
	for labels, samples := range b {
		for key := range samples {
			if _, exists := a[labels][key]; !exists {
				consider(labels, key, "extra_in_shadow")
			}
		}
	}
	return first
}

type diagnosticFloat struct {
	Value string `json:"value"`
	Bits  string `json:"bits"`
}

func floatDiagnostic(v float64) diagnosticFloat {
	return diagnosticFloat{Value: strconv.FormatFloat(v, 'g', -1, 64), Bits: fmt.Sprintf("0x%016x", math.Float64bits(v))}
}

type diagnosticHistogram struct {
	Schema              int32                      `json:"schema"`
	ResetHint           histogram.CounterResetHint `json:"reset_hint"`
	Count               diagnosticFloat            `json:"count"`
	Sum                 diagnosticFloat            `json:"sum"`
	ZeroCount           diagnosticFloat            `json:"zero_count"`
	ZeroThreshold       diagnosticFloat            `json:"zero_threshold"`
	PositiveSpans       []histogram.Span           `json:"positive_spans"`
	NegativeSpans       []histogram.Span           `json:"negative_spans"`
	PositiveBuckets     []diagnosticFloat          `json:"positive_buckets"`
	NegativeBuckets     []diagnosticFloat          `json:"negative_buckets"`
	CustomValues        []diagnosticFloat          `json:"custom_values"`
	PositiveBucketCount int                        `json:"positive_bucket_count"`
	NegativeBucketCount int                        `json:"negative_bucket_count"`
	Truncated           bool                       `json:"truncated"`
}

func histogramDiagnostic(h *histogram.FloatHistogram, hint histogram.CounterResetHint) *diagnosticHistogram {
	floats := func(values []float64) []diagnosticFloat {
		out := make([]diagnosticFloat, 0, min(len(values), diagnosticBuckets))
		for _, v := range values[:min(len(values), diagnosticBuckets)] {
			out = append(out, floatDiagnostic(v))
		}
		return out
	}
	return &diagnosticHistogram{
		Schema: h.Schema, ResetHint: hint, Count: floatDiagnostic(h.Count), Sum: floatDiagnostic(h.Sum),
		ZeroCount: floatDiagnostic(h.ZeroCount), ZeroThreshold: floatDiagnostic(h.ZeroThreshold),
		PositiveSpans:   h.PositiveSpans[:min(len(h.PositiveSpans), diagnosticBuckets)],
		NegativeSpans:   h.NegativeSpans[:min(len(h.NegativeSpans), diagnosticBuckets)],
		PositiveBuckets: floats(h.PositiveBuckets), NegativeBuckets: floats(h.NegativeBuckets), CustomValues: floats(h.CustomValues),
		PositiveBucketCount: len(h.PositiveBuckets), NegativeBucketCount: len(h.NegativeBuckets),
		Truncated: len(h.PositiveSpans) > diagnosticBuckets || len(h.NegativeSpans) > diagnosticBuckets || len(h.PositiveBuckets) > diagnosticBuckets || len(h.NegativeBuckets) > diagnosticBuckets || len(h.CustomValues) > diagnosticBuckets,
	}
}

type diagnosticSample struct {
	Type      string               `json:"type"`
	Encoding  int32                `json:"chunk_encoding"`
	Float     *diagnosticFloat     `json:"float,omitempty"`
	Histogram *diagnosticHistogram `json:"histogram,omitempty"`
	histogram *histogram.FloatHistogram
}

type diagnosticDifference struct {
	Reason                    string                 `json:"reason"`
	Labels                    []mimirpb.LabelAdapter `json:"labels"`
	Timestamp                 int64                  `json:"timestamp_ms"`
	StartTimestamp            int64                  `json:"start_timestamp_ms"`
	Primary                   *diagnosticSample      `json:"primary"`
	Shadow                    *diagnosticSample      `json:"shadow"`
	HistogramBucketDifference any                    `json:"histogram_first_bucket_difference,omitempty"`
	HistogramFields           []string               `json:"histogram_fields_differ,omitempty"`
	Error                     string                 `json:"diagnostic_error,omitempty"`
}

func differenceDiagnostic(d *sampleDifference, primary, shadow []frame) (out *diagnosticDifference) {
	out = &diagnosticDifference{Reason: d.reason, Timestamp: d.key.timestamp, StartTimestamp: d.key.startTimestamp}
	// Diagnostics must not replace the actual comparison outcome if decoding fails.
	defer func() {
		if recover() != nil {
			out.Error = "diagnostic decoding failed"
		}
	}()
	data := []byte(d.labels)
	for len(data) > 0 {
		read := func() string {
			n, size := binary.Uvarint(data)
			if size <= 0 || n > uint64(len(data)-size) {
				panic("invalid diagnostic label key")
			}
			value := string(data[size : size+int(n)])
			data = data[size+int(n):]
			return value
		}
		out.Labels = append(out.Labels, mimirpb.LabelAdapter{Name: read(), Value: read()})
	}
	var err error
	out.Primary, err = sampleDiagnostic(primary, d)
	if err != nil {
		out.Error = "primary: " + err.Error()
		return out
	}
	out.Shadow, err = sampleDiagnostic(shadow, d)
	if err != nil {
		out.Error = "shadow: " + err.Error()
		return out
	}
	if out.Primary != nil && out.Shadow != nil && out.Primary.histogram != nil && out.Shadow.histogram != nil {
		a, b := reflect.ValueOf(*out.Primary.histogram), reflect.ValueOf(*out.Shadow.histogram)
		for i := range a.NumField() {
			if digestValue(a.Field(i).Interface()) != digestValue(b.Field(i).Interface()) {
				out.HistogramFields = append(out.HistogramFields, a.Type().Field(i).Name)
			}
		}
		for _, name := range []string{"PositiveBuckets", "NegativeBuckets", "CustomValues"} {
			left, right := a.FieldByName(name).Interface().([]float64), b.FieldByName(name).Interface().([]float64)
			for i := range max(len(left), len(right)) {
				if i < len(left) && i < len(right) && math.Float64bits(left[i]) == math.Float64bits(right[i]) {
					continue
				}
				var primaryValue, shadowValue *diagnosticFloat
				if i < len(left) {
					value := floatDiagnostic(left[i])
					primaryValue = &value
				}
				if i < len(right) {
					value := floatDiagnostic(right[i])
					shadowValue = &value
				}
				out.HistogramBucketDifference = map[string]any{"field": name, "index": i, "primary": primaryValue, "shadow": shadowValue}
				break
			}
			if out.HistogramBucketDifference != nil {
				break
			}
		}
	}
	return out
}

func sampleDiagnostic(frames []frame, d *sampleDifference) (*diagnosticSample, error) {
	var seriesIndex uint64
	var target uint64
	found := false
	var result *diagnosticSample
	for _, f := range frames {
		var msg client.QueryStreamResponse
		if err := msg.Unmarshal(f); err != nil {
			return nil, err
		}
		for _, series := range msg.StreamingSeries {
			if labelKey(series.Labels) == d.labels {
				target, found = seriesIndex, true
			}
			seriesIndex++
		}
		for _, series := range msg.StreamingSeriesChunks {
			if !found || series.SeriesIndex != target {
				continue
			}
			for _, wire := range series.Chunks {
				chunk, err := chunkenc.FromData(chunkenc.Encoding(wire.Encoding-3), wire.Data)
				if err != nil {
					return nil, err
				}
				it := chunk.Iterator(nil)
				for typ := it.Next(); typ != chunkenc.ValNone; typ = it.Next() {
					if it.AtT() != d.key.timestamp || it.AtST() != d.key.startTimestamp {
						continue
					}
					sample := &diagnosticSample{Encoding: wire.Encoding}
					if typ == chunkenc.ValFloat {
						_, value := it.At()
						detail := floatDiagnostic(value)
						sample.Type, sample.Float = "float", &detail
					} else {
						_, h := it.AtFloatHistogram(nil)
						hint := h.CounterResetHint
						if hint != histogram.GaugeType {
							h.CounterResetHint = histogram.UnknownCounterReset
						}
						h = h.Compact(0)
						sample.Type = "float_histogram"
						if typ == chunkenc.ValHistogram {
							sample.Type = "histogram"
						}
						sample.histogram, sample.Histogram = h, histogramDiagnostic(h, hint)
					}
					// Match normalization's preference for a known hint on duplicate samples.
					if result == nil || sample.Histogram == nil || sample.Histogram.ResetHint != histogram.UnknownCounterReset {
						result = sample
					}
				}
				if err := it.Err(); err != nil {
					return nil, err
				}
			}
		}
	}
	return result, nil
}

func appendDiagnosticJSON(fields []any, name string, value any, budget *int) []any {
	limit := min(*budget, 8192)
	if name == "query" {
		limit = min(limit, 6144)
	}
	if name == "replay" {
		limit = min(limit, 4096)
	}
	if limit < 64 {
		return append(fields, name+"_truncated", true)
	}
	inputBudget := limit
	if !diagnosticInputFits(reflect.ValueOf(value), &inputBudget, 0) {
		data := `{"truncated":true,"reason":"payload exceeds input budget"}`
		*budget -= len(data)
		return append(fields, name, data, name+"_truncated", true)
	}
	data, err := json.Marshal(value)
	if err != nil {
		return append(fields, name+"_error", "diagnostic serialization failed")
	}
	truncated := len(data) > limit
	if truncated {
		// Escaping can expand each prefix byte to six bytes; keep the wrapper within budget.
		prefix := string(data[:max(0, (limit-64)/6)])
		data, err = json.Marshal(struct {
			Truncated bool   `json:"truncated"`
			Prefix    string `json:"prefix"`
		}{true, prefix})
		if err != nil {
			return append(fields, name+"_error", "diagnostic serialization failed")
		}
	}
	*budget -= len(data)
	fields = append(fields, name, string(data))
	if truncated {
		fields = append(fields, name+"_truncated", true)
	}
	return fields
}

// Check raw input before marshaling so huge queries or labels cannot allocate an unbounded JSON buffer.
func diagnosticInputFits(v reflect.Value, budget *int, depth int) bool {
	if depth > 32 || *budget < 0 {
		return false
	}
	if !v.IsValid() {
		*budget -= 4
		return *budget >= 0
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			*budget -= 4
		} else {
			return diagnosticInputFits(v.Elem(), budget, depth+1)
		}
	case reflect.String:
		*budget -= v.Len() + 2
	case reflect.Struct:
		for i := range v.NumField() {
			field := v.Type().Field(i)
			if field.PkgPath != "" || field.Tag.Get("json") == "-" {
				continue
			}
			*budget -= len(field.Name) + 4
			if !diagnosticInputFits(v.Field(i), budget, depth+1) {
				return false
			}
		}
	case reflect.Map:
		it := v.MapRange()
		for it.Next() {
			*budget -= 2
			if !diagnosticInputFits(it.Key(), budget, depth+1) || !diagnosticInputFits(it.Value(), budget, depth+1) {
				return false
			}
		}
	case reflect.Slice, reflect.Array:
		for i := range v.Len() {
			*budget -= 1
			if !diagnosticInputFits(v.Index(i), budget, depth+1) {
				return false
			}
		}
	default:
		*budget -= 32
	}
	return *budget >= 0
}
