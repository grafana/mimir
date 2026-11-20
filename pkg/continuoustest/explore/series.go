// SPDX-License-Identifier: AGPL-3.0-only

package explore

import (
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/prompb"
)

const (
	// The names of UTF-8 series escape to the names of legacy series, or to escapedUTF8LabelName,
	// regardless of how the OTLP endpoint is configured to handle underscores.
	metricNamePrefix     = "mimir_continuous_test_explore_"
	utf8MetricNamePrefix = "mimir.continuous_test.explore."
	utf8LabelName        = "explore.label.café"
	escapedUTF8LabelName = "explore_label_caf_"
	utf8LabelValue       = "ünïcödé ✓"

	labelFamily        = "family"
	labelCharset       = "charset"
	labelUnit          = "unit"
	labelHelp          = "help"
	labelType          = "type"
	labelSampleType    = "sample_type"
	labelKind          = "kind"
	labelSchema        = "schema"
	labelLayout        = "layout"
	labelSeed          = "seed"
	labelWriteInterval = "write_interval"
	labelProtocol      = "protocol"

	zeroThreshold = 0x1p-128
	// utf8HelpSuffix ends the help texts of HelpUTF8.
	utf8HelpSuffix = " Ünïcödé ✓, \"quoted\", back\\slashed,\ttabbed and\nmultiline."
)

// otlpUnits maps units to the units that OTLP writes, which the OTLP endpoint translates back.
var otlpUnits = map[Unit]string{UnitSeconds: "s", UnitBytes: "By"}

// Series is one combination of dimension values, written to its own series. Every write that
// picks the series appends a sample to it, with the protocol of the write.
type Series struct {
	// Seed determines the bucket layout of the series and the values of its samples.
	Seed uint64

	// Dimensions holds the dimension values of the series as labels, in the order of the
	// dimensions of its family.
	Dimensions []prompb.Label

	Family  Family
	Charset Charset
	Unit    Unit
	Help    Help

	// Type is the metric type of float series.
	Type MetricType

	// SampleType, Kind, Schema and Layout are the dimensions of native histogram series.
	SampleType SampleType
	Kind       Kind
	Schema     int32
	Layout     Layout
}

// newSeries returns the series with the given dimension values. Its seed only depends on the test
// seed and on its dimension values, so the series keeps its layout when other values are added to
// the definition.
func newSeries(seed uint64, dims []dimension, values []string) (Series, error) {
	var s Series
	h := fnv.New64a()
	for i, d := range dims {
		v := values[i]
		s.Dimensions = append(s.Dimensions, prompb.Label{Name: d.name, Value: v})
		_, _ = fmt.Fprintf(h, "%s=%s\x00", d.name, v)
		switch d.name {
		case labelFamily:
			s.Family = Family(v)
		case labelCharset:
			s.Charset = Charset(v)
		case labelUnit:
			s.Unit = Unit(v)
		case labelHelp:
			s.Help = Help(v)
		case labelType:
			s.Type = MetricType(v)
		case labelSampleType:
			s.SampleType = SampleType(v)
		case labelKind:
			s.Kind = Kind(v)
		case labelSchema:
			schema, err := strconv.ParseInt(v, 10, 32)
			if err != nil {
				return Series{}, fmt.Errorf("parsing schema %q: %w", v, err)
			}
			s.Schema = int32(schema)
		case labelLayout:
			s.Layout = Layout(v)
		default:
			return Series{}, fmt.Errorf("unknown dimension %q", d.name)
		}
	}
	s.Seed = mix(seed, h.Sum64())
	return s, nil
}

func (s Series) String() string {
	parts := make([]string, 0, len(s.Dimensions))
	for _, l := range s.Dimensions {
		parts = append(parts, l.Name+"="+l.Value)
	}
	return strings.Join(parts, " ")
}

// MetricName returns the name of the metric of the series.
func (s Series) MetricName() string {
	return s.metricName(s.Charset)
}

// metricName returns the name of the metric of the series with names of the given charset, such
// as mimir_continuous_test_explore_float_gauge_help_ascii_seconds, or
// mimir.continuous_test.explore.float.gauge.help_ascii.seconds. Metadata belongs to a metric, so
// the name has every metadata dimension: the help text, and the unit, as its suffix. The names of
// counters of float samples end with _total. The OTLP endpoint adds neither suffix again.
func (s Series) metricName(charset Charset) string {
	prefix, separator := metricNamePrefix, "_"
	if charset == CharsetUTF8 {
		prefix, separator = utf8MetricNamePrefix, "."
	}
	parts := []string{string(s.Family), string(s.Kind)}
	if s.Family == FamilyFloat {
		parts[1] = string(s.Type)
	}
	parts = append(parts, "help_"+string(s.Help))
	if s.Unit != UnitNone {
		parts = append(parts, string(s.Unit))
	}
	name := prefix + strings.Join(parts, separator)
	if s.Family == FamilyFloat && s.Type == MetricTypeCounter {
		name += "_total"
	}
	return name
}

var floatMetadataTypes = map[MetricType]prompb.MetricMetadata_MetricType{
	MetricTypeCounter:  prompb.MetricMetadata_COUNTER,
	MetricTypeGauge:    prompb.MetricMetadata_GAUGE,
	MetricTypeUnknown:  prompb.MetricMetadata_UNKNOWN,
	MetricTypeInfo:     prompb.MetricMetadata_INFO,
	MetricTypeStateset: prompb.MetricMetadata_STATESET,
}

// Metadata returns the metadata written with the series by the given protocol. A non-empty help
// identifies the protocol, so that reading it back verifies the metadata of each protocol. OTLP
// writes OTLP units.
func (s Series) Metadata(protocol Protocol) prompb.MetricMetadata {
	m := prompb.MetricMetadata{
		Type:             prompb.MetricMetadata_HISTOGRAM,
		MetricFamilyName: s.MetricName(),
		Help:             s.help(protocol),
		Unit:             s.PrometheusUnit(),
	}
	switch {
	case s.Family == FamilyFloat:
		m.Type = floatMetadataTypes[s.Type]
	case s.Kind == KindGauge:
		m.Type = prompb.MetricMetadata_GAUGEHISTOGRAM
	}
	if protocol == ProtocolOTLP {
		m.Unit = otlpUnits[s.Unit]
	}
	return m
}

// PrometheusUnit returns the unit of the metadata of the series as Mimir stores it.
func (s Series) PrometheusUnit() string {
	if s.Unit == UnitNone {
		return ""
	}
	return string(s.Unit)
}

func (s Series) help(protocol Protocol) string {
	if s.Help == HelpNone {
		return ""
	}
	metric := "native histogram " + string(s.Kind)
	if s.Family == FamilyFloat {
		metric = "float " + string(s.Type)
	}
	help := fmt.Sprintf("A %s written by the continuous-test explore test with protocol %s.", metric, protocol)
	if s.Help == HelpUTF8 {
		help += utf8HelpSuffix
	}
	return help
}

// TimeSeries returns the series with its sample at the given time, as it is written. commonLabels
// identify the test instance writing the series.
func (s Series) TimeSeries(ts time.Time, commonLabels []prompb.Label) prompb.TimeSeries {
	series := prompb.TimeSeries{Labels: s.Labels(commonLabels)}
	if s.Family == FamilyFloat {
		series.Samples = []prompb.Sample{{Value: s.Float(ts), Timestamp: ts.UnixMilli()}}
	} else {
		series.Histograms = []prompb.Histogram{s.Sample(ts)}
	}
	return series
}

// Float returns the value of a float series at the given time, which only depends on the second it
// falls in. Values are multiples of a quarter, which are exactly representable, so they survive
// the round trip unchanged.
func (s Series) Float(ts time.Time) float64 {
	sec := ts.Unix()
	switch s.Type {
	case MetricTypeCounter:
		// Like native histogram counters, counters grow at a constant rate per second since the Unix
		// epoch, so any write can append a sample.
		rate := int64(1 + s.Seed%100)
		return float64(rate*sec) / 4
	case MetricTypeInfo:
		return 1
	case MetricTypeStateset:
		return float64(mix(s.Seed, uint64(sec)) % 2)
	default:
		return float64(int64(mix(s.Seed, uint64(sec))%40001)-20000) / 4
	}
}

// Labels returns the sorted labels of the series. commonLabels identify the test instance writing
// the series.
func (s Series) Labels(commonLabels []prompb.Label) []prompb.Label {
	lbls := make([]prompb.Label, 0, len(s.Dimensions)+len(commonLabels)+2)
	lbls = append(lbls, prompb.Label{Name: "__name__", Value: s.MetricName()})
	if s.Charset == CharsetUTF8 {
		lbls = append(lbls, prompb.Label{Name: utf8LabelName, Value: utf8LabelValue})
	}
	lbls = append(lbls, s.Dimensions...)
	lbls = append(lbls, commonLabels...)
	slices.SortFunc(lbls, func(a, b prompb.Label) int { return strings.Compare(a.Name, b.Name) })
	return lbls
}

// Sample returns the sample of a native histogram series at the given time, as it is written.
func (s Series) Sample(ts time.Time) prompb.Histogram {
	h, fh := s.Histogram(ts)
	if h != nil {
		return prompb.FromIntHistogram(ts.UnixMilli(), h)
	}
	return prompb.FromFloatHistogram(ts.UnixMilli(), fh)
}

// Histogram returns the value of a native histogram series at the given time, which only depends on
// the second it falls in. Exactly one of the returned histograms is non-nil, depending on the
// sample type.
func (s Series) Histogram(ts time.Time) (*histogram.Histogram, *histogram.FloatHistogram) {
	sec := ts.Unix()
	l := newLayout(s.Seed, s.Layout, histogram.IsCustomBucketsSchema(s.Schema))

	// Counters grow at a constant rate per bucket and per second since the Unix epoch, so any write
	// can append a sample, while gauges take a random value per bucket every second. Bucket b of
	// the positive side, then of the negative side, and then the zero bucket are drawn in turn.
	// Counts stay far below 2^53, so float histograms hold them exactly.
	value := func(b int, rate int64) int64 {
		if s.Kind == KindGauge {
			return 1 + int64(mix(s.Seed, uint64(sec), uint64(b))%100)
		}
		return rate * sec
	}
	positive := make([]int64, len(l.positiveRates))
	for b, rate := range l.positiveRates {
		positive[b] = value(b, rate)
	}
	negative := make([]int64, len(l.negativeRates))
	for b, rate := range l.negativeRates {
		negative[b] = value(len(positive)+b, rate)
	}
	var zeroCount int64
	if l.zeroRate > 0 {
		zeroCount = value(len(positive)+len(negative), l.zeroRate)
	}
	sum := l.sumRate * sec
	if s.Kind == KindGauge {
		sum = int64(mix(s.Seed, uint64(sec)) % 10000)
	}
	total := zeroCount
	for _, c := range slices.Concat(positive, negative) {
		total += c
	}

	hint := histogram.UnknownCounterReset
	if s.Kind == KindGauge {
		hint = histogram.GaugeType
	}
	threshold := zeroThreshold
	if l.customValues != nil {
		threshold = 0
	}

	if s.SampleType == SampleTypeFloat {
		// Quarters are exactly representable, so float values survive the round trip unchanged.
		quarters := func(counts []int64) []float64 {
			out := make([]float64, len(counts))
			for i, c := range counts {
				out[i] = float64(c) / 4
			}
			return out
		}
		return nil, &histogram.FloatHistogram{
			CounterResetHint: hint,
			Schema:           s.Schema,
			ZeroThreshold:    threshold,
			ZeroCount:        float64(zeroCount) / 4,
			Count:            float64(total) / 4,
			Sum:              float64(sum) / 4,
			PositiveSpans:    l.positiveSpans,
			PositiveBuckets:  quarters(positive),
			NegativeSpans:    l.negativeSpans,
			NegativeBuckets:  quarters(negative),
			CustomValues:     l.customValues,
		}
	}

	return &histogram.Histogram{
		CounterResetHint: hint,
		Schema:           s.Schema,
		ZeroThreshold:    threshold,
		ZeroCount:        uint64(zeroCount),
		Count:            uint64(total),
		Sum:              float64(sum),
		PositiveSpans:    l.positiveSpans,
		PositiveBuckets:  deltas(positive),
		NegativeSpans:    l.negativeSpans,
		NegativeBuckets:  deltas(negative),
		CustomValues:     l.customValues,
	}, nil
}

func deltas(counts []int64) []int64 {
	out := make([]int64, len(counts))
	prev := int64(0)
	for i, c := range counts {
		out[i] = c - prev
		prev = c
	}
	return out
}

// ExpectedHistogram returns the value of the series at the given time as a float histogram, which
// is how it is expected to be read back.
func (s Series) ExpectedHistogram(ts time.Time) *histogram.FloatHistogram {
	h, fh := s.Histogram(ts)
	if h != nil {
		return h.ToFloat(nil)
	}
	return fh
}

// layout is the bucket layout of a series and the rate at which its counter values grow.
type layout struct {
	positiveSpans, negativeSpans []histogram.Span
	positiveRates, negativeRates []int64
	zeroRate, sumRate            int64
	// customValues holds the bucket boundaries of custom buckets, or nil for exponential buckets.
	customValues []float64
}

func newLayout(seed uint64, kind Layout, custom bool) layout {
	rng := newRand(seed, 0)
	var l layout
	switch kind {
	case LayoutContiguous:
		l.positiveSpans, l.positiveRates = randomSpans(rng, 1, 8, custom)
	case LayoutGaps:
		l.positiveSpans, l.positiveRates = randomSpans(rng, 2+rng.IntN(2), 3, custom)
	case LayoutNegative:
		l.positiveSpans, l.positiveRates = randomSpans(rng, 1+rng.IntN(3), 3, custom)
		l.negativeSpans, l.negativeRates = randomSpans(rng, 1+rng.IntN(3), 3, custom)
		l.zeroRate = int64(1 + rng.IntN(10))
	case LayoutZeroOnly:
		l.zeroRate = int64(1 + rng.IntN(10))
	}
	if custom {
		l.customValues = randomCustomValues(rng, l.positiveSpans)
	}
	l.sumRate = int64(1 + rng.IntN(100))
	return l
}

// randomSpans returns n spans of 1 to maxLength buckets each, and the rate of each bucket. The
// first span starts near bucket 0, or at bucket 0 to 2 for custom buckets, whose offsets can't be
// negative. Later spans leave a gap of 1 to 3 buckets.
func randomSpans(rng *rand.Rand, n, maxLength int, custom bool) ([]histogram.Span, []int64) {
	var spans []histogram.Span
	var rates []int64
	for i := range n {
		offset := int32(1 + rng.IntN(3))
		if i == 0 && custom {
			offset = int32(rng.IntN(3))
		} else if i == 0 {
			offset = int32(rng.IntN(9) - 3)
		}
		length := uint32(1 + rng.IntN(maxLength))
		spans = append(spans, histogram.Span{Offset: offset, Length: length})
		for range length {
			rates = append(rates, int64(1+rng.IntN(10)))
		}
	}
	return spans, rates
}

// randomCustomValues returns increasing bucket boundaries for the buckets of spans. The last bucket
// of the spans is sometimes the +Inf bucket, which has no boundary.
func randomCustomValues(rng *rand.Rand, spans []histogram.Span) []float64 {
	buckets := 0
	for _, s := range spans {
		buckets += int(s.Offset) + int(s.Length)
	}
	values := make([]float64, max(0, buckets-1+rng.IntN(3)))
	// Quarters are exactly representable, so boundaries survive the round trip unchanged.
	bound := float64(rng.IntN(9)-4) / 4
	for i := range values {
		values[i] = bound
		bound += float64(1+rng.IntN(8)) / 4
	}
	return values
}
