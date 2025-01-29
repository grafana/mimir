// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortexpb/compat.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimirpb

import (
	"bytes"
	stdjson "encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/jsonutil"

	"github.com/grafana/mimir/pkg/mimirpb_custom"
)

// ToWriteRequest converts matched slices of Labels, Samples, Exemplars, and Metadata into a WriteRequest
// proto. It gets timeseries from the pool, so ReuseSlice() should be called when done. Note that this
// method implies that only a single sample and optionally exemplar can be set for each series.
//
// For histograms use NewWriteRequest and Add* functions to build write request with Floats and Histograms
func ToWriteRequest(lbls [][]*mimirpb_custom.LabelAdapter, samples []*Sample, exemplars []*Exemplar, metadata []*MetricMetadata, source WriteRequest_SourceEnum) *WriteRequest {
	return NewWriteRequest(metadata, source).AddFloatSeries(lbls, samples, exemplars)
}

// NewWriteRequest creates a new empty WriteRequest with metadata
func NewWriteRequest(metadata []*MetricMetadata, source WriteRequest_SourceEnum) *WriteRequest {
	return &WriteRequest{
		Timeseries: PreallocTimeseriesSliceFromPool(),
		Metadata:   metadata,
		Source:     source,
	}
}

// AddFloatSeries converts matched slices of Labels, Samples, Exemplars into a WriteRequest
// proto. It gets timeseries from the pool, so ReuseSlice() should be called when done. Note that this
// method implies that only a single sample and optionally exemplar can be set for each series.
func (req *WriteRequest) AddFloatSeries(lbls [][]*mimirpb_custom.LabelAdapter, samples []*Sample, exemplars []*Exemplar) *WriteRequest {
	for i, s := range samples {
		ts := TimeseriesFromPool()
		ts.Labels = append(ts.Labels, lbls[i]...)
		ts.Samples = append(ts.Samples, s)

		if exemplars != nil {
			// If provided, we expect a matched entry for exemplars (like labels and samples) but the
			// entry may be nil since not every timeseries is guaranteed to have an exemplar.
			if e := exemplars[i]; e != nil {
				ts.Exemplars = append(ts.Exemplars, e)
			}
		}

		req.Timeseries = append(req.Timeseries, PreallocTimeseries{TimeSeries: ts})
	}
	return req
}

// AddHistogramSeries converts matched slices of Labels, Histograms, Exemplars into a WriteRequest
// proto. It gets timeseries from the pool, so ReuseSlice() should be called when done. Note that this
// method implies that only a single sample and optionally exemplar can be set for each series.
func (req *WriteRequest) AddHistogramSeries(lbls [][]*mimirpb_custom.LabelAdapter, histograms []*Histogram, exemplars []*Exemplar) *WriteRequest {
	for i, s := range histograms {
		ts := TimeseriesFromPool()
		ts.Labels = append(ts.Labels, lbls[i]...)
		ts.Histograms = append(ts.Histograms, s)

		if exemplars != nil {
			// If provided, we expect a matched entry for exemplars (like labels and samples) but the
			// entry may be nil since not every timeseries is guaranteed to have an exemplar.
			if e := exemplars[i]; e != nil {
				ts.Exemplars = append(ts.Exemplars, e)
			}
		}

		req.Timeseries = append(req.Timeseries, PreallocTimeseries{TimeSeries: ts})
	}

	return req
}

// AddExemplarsAt appends exemplars to the timeseries at index i.
// This is needed as the Add*Series functions only allow for a single exemplar
// to be added per time series for simplicity.
func (req *WriteRequest) AddExemplarsAt(i int, exemplars []*Exemplar) *WriteRequest {
	for _, e := range exemplars {
		req.Timeseries[i].Exemplars = append(req.Timeseries[i].Exemplars, e)
	}
	return req
}

// FromLabelAdaptersToMetric converts []LabelAdapter to a model.Metric.
// Don't do this on any performance sensitive paths.
func FromLabelAdaptersToMetric(ls []mimirpb_custom.LabelAdapter) model.Metric {
	m := make(model.Metric, len(ls))
	for _, la := range ls {
		m[model.LabelName(la.Name)] = model.LabelValue(la.Value)
	}
	return m
}

// FromLabelAdaptersToKeyString makes a string to be used as a key to a map.
// It's much simpler than FromLabelAdaptersToString, but not human-readable.
func FromLabelAdaptersToKeyString(ls []mimirpb_custom.LabelAdapter) string {
	buf := make([]byte, 0, 1024)
	for i := range ls {
		buf = append(buf, '\xff')
		buf = append(buf, ls[i].Name...)
		buf = append(buf, '\xff')
		buf = append(buf, ls[i].Value...)
	}
	return string(buf)
}

// FromLabelAdaptersToString formats label adapters as a metric name with labels, while preserving
// label order, and keeping duplicates. If there are multiple "__name__" labels, only
// first one is used as metric name, other ones will be included as regular labels.
func FromLabelAdaptersToString(ls []mimirpb_custom.LabelAdapter) string {
	var space [1024]byte
	var quoteSpace [256]byte
	b := bytes.NewBuffer(space[:0])
	metricNameIndex := -1

	for i, l := range ls {
		if l.Name == model.MetricNameLabel {
			b.WriteString(l.Value)
			metricNameIndex = i
			break
		}
	}

	count := 0
	for i, l := range ls {
		if i == metricNameIndex {
			continue
		}
		if count == 0 {
			b.WriteByte('{')
		} else {
			b.WriteByte(',')
			b.WriteByte(' ')
		}
		b.WriteString(l.Name)
		b.WriteByte('=')
		b.Write(strconv.AppendQuote(quoteSpace[:0], l.Value))
		count++
	}
	if count > 0 {
		b.WriteByte('}')
	}
	if b.Len() == 0 {
		return "{}"
	}
	return b.String()
}

// FromMetricsToLabelAdapters converts model.Metric to []LabelAdapter.
// Don't do this on any performance sensitive paths.
// The result is sorted.
func FromMetricsToLabelAdapters(metric model.Metric) []mimirpb_custom.LabelAdapter {
	result := make([]mimirpb_custom.LabelAdapter, 0, len(metric))
	for k, v := range metric {
		result = append(result, mimirpb_custom.LabelAdapter{
			Name:  string(k),
			Value: string(v),
		})
	}
	sort.Sort(byLabel(result)) // The labels should be sorted upon initialisation.
	return result
}

func FromExemplarsToExemplarProtos(es []exemplar.Exemplar) []Exemplar {
	result := make([]Exemplar, 0, len(es))
	for _, e := range es {
		result = append(result, Exemplar{
			Labels:      FromLabelsToLabelAdapters(e.Labels),
			Value:       e.Value,
			TimestampMs: e.Ts,
		})
	}
	return result
}

func FromExemplarProtosToExemplars(es []Exemplar) []exemplar.Exemplar {
	result := make([]exemplar.Exemplar, 0, len(es))
	for _, e := range es {
		result = append(result, exemplar.Exemplar{
			Labels: FromLabelAdaptersToLabels(e.Labels),
			Value:  e.Value,
			Ts:     e.TimestampMs,
		})
	}
	return result
}

func FromHistogramProtoToHistogram(hp *Histogram) *histogram.Histogram {
	if hp == nil {
		return nil
	}
	if hp.IsFloatHistogram() {
		panic("FromHistogramProtoToHistogram called on float histogram")
	}
	return &histogram.Histogram{
		CounterResetHint: histogram.CounterResetHint(hp.ResetHint),
		Schema:           hp.Schema,
		ZeroThreshold:    hp.ZeroThreshold,
		ZeroCount:        hp.GetZeroCountInt(),
		Count:            hp.GetCountInt(),
		Sum:              hp.Sum,
		PositiveSpans:    fromSpansProtoToSpans(hp.GetPositiveSpans()),
		PositiveBuckets:  hp.GetPositiveDeltas(),
		NegativeSpans:    fromSpansProtoToSpans(hp.GetNegativeSpans()),
		NegativeBuckets:  hp.GetNegativeDeltas(),
	}
}

func FromHistogramProtoToFloatHistogram(hp *Histogram) *histogram.FloatHistogram {
	if hp == nil {
		return nil
	}
	if hp.IsFloatHistogram() {
		panic("FromHistogramProtoToFloatHistogram called on float histogram")
	}
	return &histogram.FloatHistogram{
		CounterResetHint: histogram.CounterResetHint(hp.ResetHint),
		Schema:           hp.Schema,
		ZeroThreshold:    hp.ZeroThreshold,
		ZeroCount:        float64(hp.GetZeroCountInt()),
		Count:            float64(hp.GetCountInt()),
		Sum:              hp.Sum,
		PositiveSpans:    fromSpansProtoToSpans(hp.GetPositiveSpans()),
		PositiveBuckets:  deltasToCounts(hp.GetPositiveDeltas()),
		NegativeSpans:    fromSpansProtoToSpans(hp.GetNegativeSpans()),
		NegativeBuckets:  deltasToCounts(hp.GetNegativeDeltas()),
	}
}

func deltasToCounts(deltas []int64) []float64 {
	counts := make([]float64, len(deltas))
	var cur float64
	for i, d := range deltas {
		cur += float64(d)
		counts[i] = cur
	}
	return counts
}

func FromFloatHistogramProtoToFloatHistogram(hp *Histogram) *histogram.FloatHistogram {
	if hp == nil {
		return nil
	}
	if !hp.IsFloatHistogram() {
		panic("FromFloatHistogramProtoToFloatHistogram called on integer histogram")
	}
	return &histogram.FloatHistogram{
		CounterResetHint: histogram.CounterResetHint(hp.ResetHint),
		Schema:           hp.Schema,
		ZeroThreshold:    hp.ZeroThreshold,
		ZeroCount:        hp.GetZeroCountFloat(),
		Count:            hp.GetCountFloat(),
		Sum:              hp.Sum,
		PositiveSpans:    fromSpansProtoToSpans(hp.GetPositiveSpans()),
		PositiveBuckets:  hp.GetPositiveCounts(),
		NegativeSpans:    fromSpansProtoToSpans(hp.GetNegativeSpans()),
		NegativeBuckets:  hp.GetNegativeCounts(),
	}
}

func FromHistogramProtoToPromHistogram(hp *Histogram) *model.SampleHistogram {
	if hp == nil {
		return nil
	}
	if hp.IsFloatHistogram() {
		return FromFloatHistogramToPromHistogram(FromFloatHistogramProtoToFloatHistogram(hp))
	}
	return FromHistogramToPromHistogram(FromHistogramProtoToHistogram(hp))
}

func fromSpansProtoToSpans(s []BucketSpan) []histogram.Span {
	if len(s) == 0 {
		return nil
	}
	return *(*[]histogram.Span)(unsafe.Pointer(&s))
}

// FromHistogramToHistogramProto does not make a deepcopy, slices are referenced
func FromHistogramToHistogramProto(timestamp int64, h *histogram.Histogram) Histogram {
	if h == nil {
		panic("FromHistogramToHistogramProto called on nil histogram")
	}
	return Histogram{
		Count:          &Histogram_CountInt{CountInt: h.Count},
		Sum:            h.Sum,
		Schema:         h.Schema,
		ZeroThreshold:  h.ZeroThreshold,
		ZeroCount:      &Histogram_ZeroCountInt{ZeroCountInt: h.ZeroCount},
		NegativeSpans:  fromSpansToSpansProto(h.NegativeSpans),
		NegativeDeltas: h.NegativeBuckets,
		// NegativeCounts: nil,  not relevant for integer Histogram
		PositiveSpans:  fromSpansToSpansProto(h.PositiveSpans),
		PositiveDeltas: h.PositiveBuckets,
		// PositiveCounts: nil,  not relevant for integer Histogram
		ResetHint: Histogram_ResetHint(h.CounterResetHint),
		Timestamp: timestamp,
	}
}

// FromFloatHistogramToHistogramProto does not make a deepcopy, slices are referenced
func FromFloatHistogramToHistogramProto(timestamp int64, fh *histogram.FloatHistogram) Histogram {
	if fh == nil {
		panic("FromFloatHistogramToHistogramProto called on nil histogram")
	}
	return Histogram{
		Count:         &Histogram_CountFloat{CountFloat: fh.Count},
		Sum:           fh.Sum,
		Schema:        fh.Schema,
		ZeroThreshold: fh.ZeroThreshold,
		ZeroCount:     &Histogram_ZeroCountFloat{ZeroCountFloat: fh.ZeroCount},
		NegativeSpans: fromSpansToSpansProto(fh.NegativeSpans),
		// NegativeDeltas: nil,  not relevant for float Histograms
		NegativeCounts: fh.NegativeBuckets,
		PositiveSpans:  fromSpansToSpansProto(fh.PositiveSpans),
		// PositiveDeltas: nil,  not relevant for float Histograms
		PositiveCounts: fh.PositiveBuckets,
		ResetHint:      Histogram_ResetHint(fh.CounterResetHint),
		Timestamp:      timestamp,
	}
}

func fromSpansToSpansProto(s []histogram.Span) []BucketSpan {
	if len(s) == 0 {
		return nil
	}
	return *(*[]BucketSpan)(unsafe.Pointer(&s))
}

// FromFPointsToSamples casts []promql.FPoint to []Sample. It uses unsafe.
func FromFPointsToSamples(points []promql.FPoint) []Sample {
	return *(*[]Sample)(unsafe.Pointer(&points))
}

// FromHPointsToHistograms converts []promql.HPoint to []FloatHistogramPair. It uses unsafe.
func FromHPointsToHistograms(points []promql.HPoint) []FloatHistogramPair {
	return *(*[]FloatHistogramPair)(unsafe.Pointer(&points))
}

// FromFloatHistogramToPromHistogram converts histogram.FloatHistogram to model.SampleHistogram.
func FromFloatHistogramToPromHistogram(h *histogram.FloatHistogram) *model.SampleHistogram {
	if h == nil {
		return nil
	}
	buckets := make([]*model.HistogramBucket, 0)
	it := h.AllBucketIterator()
	for it.Next() {
		bucket := it.At()
		if bucket.Count == 0 {
			continue // No need to expose empty buckets in JSON.
		}
		buckets = append(buckets, &model.HistogramBucket{
			Boundaries: getBucketBoundaries(bucket),
			Lower:      model.FloatString(bucket.Lower),
			Upper:      model.FloatString(bucket.Upper),
			Count:      model.FloatString(bucket.Count),
		})
	}
	return &model.SampleHistogram{
		Count:   model.FloatString(h.Count),
		Sum:     model.FloatString(h.Sum),
		Buckets: buckets,
	}
}

func FromHistogramToPromHistogram(h *histogram.Histogram) *model.SampleHistogram {
	return FromFloatHistogramToPromHistogram(h.ToFloat(nil))
}

type byLabel []mimirpb_custom.LabelAdapter

func (s byLabel) Len() int           { return len(s) }
func (s byLabel) Less(i, j int) bool { return s[i].Name < s[j].Name }
func (s byLabel) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// MetricMetadataMetricTypeToMetricType converts a metric type from our internal client
// to a Prometheus one.
func MetricMetadataMetricTypeToMetricType(mt MetricMetadata_MetricType) model.MetricType {
	switch mt {
	case UNKNOWN:
		return model.MetricTypeUnknown
	case COUNTER:
		return model.MetricTypeCounter
	case GAUGE:
		return model.MetricTypeGauge
	case HISTOGRAM:
		return model.MetricTypeHistogram
	case GAUGEHISTOGRAM:
		return model.MetricTypeGaugeHistogram
	case SUMMARY:
		return model.MetricTypeSummary
	case INFO:
		return model.MetricTypeInfo
	case STATESET:
		return model.MetricTypeStateset
	default:
		return model.MetricTypeUnknown
	}
}

// isTesting is only set from tests to get special behaviour to verify that custom sample encode and decode is used,
// both when using jsonitor or standard json package.
var isTesting = false

// MarshalJSON implements json.Marshaler.
func (s Sample) MarshalJSON() ([]byte, error) {
	if isTesting && math.IsNaN(s.Value) {
		return nil, fmt.Errorf("test sample")
	}

	t, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(model.Time(s.TimestampMs))
	if err != nil {
		return nil, err
	}
	v, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(model.SampleValue(s.Value))
	if err != nil {
		return nil, err
	}
	return []byte(fmt.Sprintf("[%s,%s]", t, v)), nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (s *Sample) UnmarshalJSON(b []byte) error {
	var t model.Time
	var v model.SampleValue
	vs := [...]stdjson.Unmarshaler{&t, &v}
	if err := jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(b, &vs); err != nil {
		return err
	}
	s.TimestampMs = int64(t)
	s.Value = float64(v)

	if isTesting && math.IsNaN(float64(v)) {
		return fmt.Errorf("test sample")
	}
	return nil
}

func SampleJsoniterEncode(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	sample := (*Sample)(ptr)

	if isTesting && math.IsNaN(sample.Value) {
		stream.Error = fmt.Errorf("test sample")
		return
	}

	stream.WriteArrayStart()
	jsonutil.MarshalTimestamp(sample.TimestampMs, stream)
	stream.WriteMore()
	jsonutil.MarshalFloat(sample.Value, stream)
	stream.WriteArrayEnd()
}

func SampleJsoniterDecode(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	if !iter.ReadArray() {
		iter.ReportError("mimirpb.Sample", "expected [")
		return
	}

	t := model.Time(iter.ReadFloat64() * float64(time.Second/time.Millisecond))

	if !iter.ReadArray() {
		iter.ReportError("mimirpb.Sample", "expected ,")
		return
	}

	bs := iter.ReadStringAsSlice()
	ss := *(*string)(unsafe.Pointer(&bs))
	v, err := strconv.ParseFloat(ss, 64)
	if err != nil {
		iter.ReportError("mimirpb.Sample", err.Error())
		return
	}

	if isTesting && math.IsNaN(v) {
		iter.Error = fmt.Errorf("test sample")
		return
	}

	if iter.ReadArray() {
		iter.ReportError("mimirpb.Sample", "expected ]")
	}

	*(*Sample)(ptr) = Sample{
		TimestampMs: int64(t),
		Value:       v,
	}
}

func FromPromToMimirSampleHistogram(src *model.SampleHistogram) *SampleHistogram {
	return (*SampleHistogram)(unsafe.Pointer(src))
}

func FromMimirSampleToPromHistogram(src *SampleHistogram) *model.SampleHistogram {
	return (*model.SampleHistogram)(unsafe.Pointer(src))
}

// FromFloatHistogramToSampleHistogram converts histogram.FloatHistogram to SampleHistogram.
func FromFloatHistogramToSampleHistogram(h *histogram.FloatHistogram) *SampleHistogram {
	if h == nil {
		return nil
	}
	// The extra +1 in the capacity is for the zero count bucket (which may optionally exist).
	buckets := make([]*HistogramBucket, 0, len(h.PositiveBuckets)+len(h.NegativeBuckets)+1)

	it := h.AllBucketIterator()
	for it.Next() {
		bucket := it.At()
		if bucket.Count == 0 {
			continue // No need to expose empty buckets in JSON.
		}
		buckets = append(buckets, &HistogramBucket{
			Boundaries: getBucketBoundaries(bucket),
			Lower:      bucket.Lower,
			Upper:      bucket.Upper,
			Count:      bucket.Count,
		})
	}
	return &SampleHistogram{
		Count:   h.Count,
		Sum:     h.Sum,
		Buckets: buckets,
	}
}

func getBucketBoundaries(bucket histogram.Bucket[float64]) int32 {
	var boundaries int32 = 2 // Exclusive on both sides AKA open interval.
	if bucket.LowerInclusive {
		if bucket.UpperInclusive {
			boundaries = 3 // Inclusive on both sides AKA closed interval.
		} else {
			boundaries = 1 // Inclusive only on lower end AKA right open.
		}
	} else {
		if bucket.UpperInclusive {
			boundaries = 0 // Inclusive only on upper end AKA left open.
		}
	}
	return boundaries
}

func (vs *SampleHistogramPair) UnmarshalJSON(b []byte) error {
	s := model.SampleHistogramPair{}
	if err := stdjson.Unmarshal(b, &s); err != nil {
		return err
	}
	vs.Timestamp = int64(s.Timestamp)
	vs.Histogram = FromPromToMimirSampleHistogram(s.Histogram)
	return nil
}

func (vs SampleHistogramPair) MarshalJSON() ([]byte, error) {
	s := model.SampleHistogramPair{
		Timestamp: model.Time(vs.Timestamp),
		Histogram: FromMimirSampleToPromHistogram(vs.Histogram),
	}
	return stdjson.Marshal(s)
}

func init() {
	jsoniter.RegisterTypeEncoderFunc("mimirpb.Sample", SampleJsoniterEncode, func(unsafe.Pointer) bool { return false })
	jsoniter.RegisterTypeDecoderFunc("mimirpb.Sample", SampleJsoniterDecode)
}

// PreallocatingMetric overrides the Unmarshal behaviour of Metric.
type PreallocatingMetric struct {
	Metric
}

// Unmarshal is like Metric.Unmarshal, but it preallocates the slice of labels
// instead of growing it during append(). Unmarshal traverses the dAtA slice and counts the number of
// Metric.Labels elements. Then it preallocates a slice of mimirpb.LabelAdapter with that capacity
// and delegates the actual unmarshalling to Metric.Unmarshal.
//
// Unmarshal should be manually updated when new fields are added to Metric.
// Unmarshal will give up on counting labels if it encounters unknown fields and will
// fall back to Metric.Unmarshal
//
// The implementation of Unmarshal is copied from the implementation of
// Metric.Unmarshal and modified, so it only counts the labels instead of
// also unmarshalling them.
func (m *PreallocatingMetric) Unmarshal(dAtA []byte) error {
	numLabels, ok := m.labelsCount(dAtA)
	if ok && numLabels > 0 {
		m.Labels = make([]*mimirpb_custom.LabelAdapter, 0, numLabels)
	}

	return m.Metric.Unmarshal(dAtA)
}

// The implementation of labelsCount is copied from the implementation of
// Metric.Unmarshal and modified, so it only counts the labels instead of
// also unmarshalling them.
func (m *PreallocatingMetric) labelsCount(dAtA []byte) (int, bool) {
	l := len(dAtA)
	iNdEx := 0
	numLabels := 0
loop:
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, false
			}
			if iNdEx >= l {
				return 0, false
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return 0, false
		}
		if fieldNum <= 0 {
			return 0, false
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return 0, false
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, false
				}
				if iNdEx >= l {
					return 0, false
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return 0, false
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return 0, false
			}
			if postIndex > l {
				return 0, false
			}
			numLabels++
			iNdEx = postIndex
		default:
			// There is a field we don't know about, so we can't make an assured decision based
			break loop
		}
	}

	return numLabels, true
}
