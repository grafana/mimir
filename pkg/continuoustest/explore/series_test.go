// SPDX-License-Identifier: AGPL-3.0-only

package explore

import (
	"cmp"
	"testing"
	"time"

	"github.com/prometheus/otlptranslator"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
)

var allLayouts = []Layout{LayoutContiguous, LayoutGaps, LayoutNegative, LayoutZeroOnly}

func TestSeries_Histogram(t *testing.T) {
	start := time.Unix(1_800_000_000, 0)
	for _, sampleType := range []SampleType{SampleTypeInt, SampleTypeFloat} {
		for _, kind := range []Kind{KindCounter, KindGauge} {
			for _, schema := range allSchemas() {
				for _, layout := range allLayouts {
					custom := histogram.IsCustomBucketsSchema(schema)
					if custom && (layout == LayoutNegative || layout == LayoutZeroOnly) {
						continue
					}
					for seed := range uint64(3) {
						s := testSeries(t, testSeriesValues{seed: seed, sampleType: sampleType, kind: kind, schema: schema, layout: layout})
						t.Run(s.String(), func(t *testing.T) {
							var prev *histogram.FloatHistogram
							for i := range 20 {
								// Writes pick series at random, so samples can be any number of half seconds apart.
								ts := start.Add(time.Duration(i*i) * 500 * time.Millisecond)
								h, fh := s.Histogram(ts)
								if sampleType == SampleTypeInt {
									require.Nil(t, fh)
									require.NoError(t, h.Validate())
								} else {
									require.Nil(t, h)
									require.NoError(t, fh.Validate())
								}

								expected := s.ExpectedHistogram(ts)
								require.Equal(t, schema, expected.Schema)
								require.Equal(t, custom, expected.UsesCustomBuckets())
								require.Equal(t, kind == KindGauge, expected.CounterResetHint == histogram.GaugeType)
								if kind == KindCounter && prev != nil {
									require.False(t, expected.DetectReset(prev), "counters must never reset")
									require.Greater(t, expected.Count, prev.Count, "counters must grow")
								}
								prev = expected
							}
						})
					}
				}
			}
		}
	}
}

func TestSeries_Sample(t *testing.T) {
	ts := time.Unix(1_800_000_000, 0)

	s := testSeries(t, testSeriesValues{sampleType: SampleTypeInt, schema: 3})
	sample := s.Sample(ts)
	require.Equal(t, ts.UnixMilli(), sample.Timestamp)
	require.False(t, sample.IsFloatHistogram())
	h, _ := s.Histogram(ts)
	require.True(t, h.Equals(sample.ToIntHistogram()))

	s = testSeries(t, testSeriesValues{sampleType: SampleTypeFloat, schema: 3})
	sample = s.Sample(ts)
	require.Equal(t, ts.UnixMilli(), sample.Timestamp)
	require.True(t, sample.IsFloatHistogram())
	_, fh := s.Histogram(ts)
	require.True(t, fh.Equals(sample.ToFloatHistogram()))
}

func TestSeries_Layouts(t *testing.T) {
	ts := time.Unix(1_800_000_000, 0)
	for _, schema := range []int32{3, histogram.CustomBucketsSchema} {
		for _, layout := range allLayouts {
			custom := histogram.IsCustomBucketsSchema(schema)
			if custom && (layout == LayoutNegative || layout == LayoutZeroOnly) {
				continue
			}
			for seed := range uint64(20) {
				s := testSeries(t, testSeriesValues{seed: seed, sampleType: SampleTypeInt, kind: KindCounter, schema: schema, layout: layout})
				h, _ := s.Histogram(ts)
				switch layout {
				case LayoutContiguous:
					require.Len(t, h.PositiveSpans, 1)
					require.Empty(t, h.NegativeSpans)
					require.Zero(t, h.ZeroCount)
				case LayoutGaps:
					require.GreaterOrEqual(t, len(h.PositiveSpans), 2)
					for _, span := range h.PositiveSpans[1:] {
						require.Positive(t, span.Offset, "spans should be separated by empty buckets")
					}
					require.Empty(t, h.NegativeSpans)
					require.Zero(t, h.ZeroCount)
				case LayoutNegative:
					require.NotEmpty(t, h.PositiveSpans)
					require.NotEmpty(t, h.NegativeSpans)
					require.Positive(t, h.ZeroCount)
				case LayoutZeroOnly:
					require.Empty(t, h.PositiveSpans)
					require.Empty(t, h.NegativeSpans)
					require.Positive(t, h.ZeroCount)
					require.Equal(t, h.ZeroCount, h.Count)
				}
				if custom {
					require.Zero(t, h.ZeroThreshold)
					require.NotNil(t, h.CustomValues)
				} else {
					require.Equal(t, zeroThreshold, h.ZeroThreshold)
					require.Nil(t, h.CustomValues)
				}
			}
		}
	}
}

func TestSeries_GaugeValuesChange(t *testing.T) {
	ts := time.Unix(1_800_000_000, 0)
	s := testSeries(t, testSeriesValues{sampleType: SampleTypeInt, kind: KindGauge, schema: 3, layout: LayoutNegative})
	require.NotEqual(t, s.ExpectedHistogram(ts), s.ExpectedHistogram(ts.Add(500*time.Millisecond)))
}

func TestSeries_Float(t *testing.T) {
	start := time.Unix(1_800_000_000, 0)
	for _, metricType := range allMetricTypes {
		for seed := range uint64(3) {
			s := testSeries(t, testSeriesValues{seed: seed, metricType: metricType})
			t.Run(s.String(), func(t *testing.T) {
				values := map[float64]bool{}
				prev := 0.0
				for i := range 20 {
					// Writes pick series at random, so samples can be any number of half seconds apart.
					ts := start.Add(time.Duration(i*i) * 500 * time.Millisecond)
					v := s.Float(ts)
					values[v] = true
					require.Equal(t, v, float64(int64(v*4))/4, "values must be multiples of a quarter")
					switch metricType {
					case MetricTypeCounter:
						require.Greater(t, v, prev, "counters must grow")
					case MetricTypeInfo:
						require.Equal(t, 1.0, v)
					case MetricTypeStateset:
						require.Contains(t, []float64{0, 1}, v)
					}
					prev = v
				}
				if metricType != MetricTypeInfo {
					require.Greater(t, len(values), 1, "values must change")
				}
			})
		}
	}
}

func TestSeries_TimeSeries(t *testing.T) {
	ts := time.Unix(1_800_000_000, 0)
	common := []prompb.Label{{Name: labelWriteInterval, Value: "10s"}}

	s := testSeries(t, testSeriesValues{metricType: MetricTypeGauge})
	series := s.TimeSeries(ts, common)
	require.Equal(t, s.Labels(common), series.Labels)
	require.Equal(t, []prompb.Sample{{Value: s.Float(ts), Timestamp: ts.UnixMilli()}}, series.Samples)
	require.Empty(t, series.Histograms)

	s = testSeries(t, testSeriesValues{schema: 3})
	series = s.TimeSeries(ts, common)
	require.Equal(t, s.Labels(common), series.Labels)
	require.Equal(t, []prompb.Histogram{s.Sample(ts)}, series.Histograms)
	require.Empty(t, series.Samples)
}

// A series keeps its seed, and so its layout and values, when values are added to the definition.
func TestSeries_SeedOnlyDependsOnTheDimensionValues(t *testing.T) {
	s := testSeries(t, testSeriesValues{seed: 1, kind: KindGauge, schema: 3})

	def := Definition{
		Charsets: []Charset{CharsetUTF8, CharsetLegacy},
		Metadata: MetadataDefinition{Units: []Unit{UnitSeconds, UnitNone}, Helps: []Help{HelpUTF8, HelpASCII}},
		Floats:   &FloatsDefinition{Types: allMetricTypes},
		NativeHistograms: &NativeHistogramsDefinition{
			SampleTypes: []SampleType{SampleTypeFloat, SampleTypeInt},
			Kinds:       []Kind{KindCounter, KindGauge},
			Schemas:     []int32{0, 3},
			Layouts:     allLayouts,
		},
	}
	found := false
	for _, dims := range def.families() {
		for _, values := range allCombinations(dims) {
			other, err := newSeries(1, dims, values)
			require.NoError(t, err)
			if other.String() == s.String() {
				found = true
				require.Equal(t, s, other)
			} else {
				require.NotEqual(t, s.Seed, other.Seed)
			}
		}
	}
	require.True(t, found)

	require.NotEqual(t, s.Seed, testSeries(t, testSeriesValues{seed: 2, kind: KindGauge, schema: 3}).Seed, "the seed of the test must change the seed of the series")
}

func TestSeries_Labels(t *testing.T) {
	common := []prompb.Label{{Name: labelWriteInterval, Value: "10s"}, {Name: labelSeed, Value: "1"}}
	s := testSeries(t, testSeriesValues{unit: UnitSeconds, sampleType: SampleTypeFloat, kind: KindCounter, schema: -2, layout: LayoutGaps})
	expected := map[string]string{
		"__name__":         "mimir_continuous_test_explore_native_histogram_counter_help_ascii_seconds",
		labelFamily:        "native_histogram",
		labelCharset:       "legacy",
		labelUnit:          "seconds",
		labelHelp:          "ascii",
		labelSampleType:    "float",
		labelKind:          "counter",
		labelSchema:        "-2",
		labelLayout:        "gaps",
		labelWriteInterval: "10s",
		labelSeed:          "1",
	}
	require.Equal(t, expected, labelMap(t, s.Labels(common)))

	s = testSeries(t, testSeriesValues{charset: CharsetUTF8, unit: UnitSeconds, sampleType: SampleTypeFloat, kind: KindCounter, schema: -2, layout: LayoutGaps})
	expected["__name__"] = "mimir.continuous_test.explore.native_histogram.counter.help_ascii.seconds"
	expected[labelCharset] = "utf8"
	expected[utf8LabelName] = utf8LabelValue
	require.Equal(t, expected, labelMap(t, s.Labels(common)))

	s = testSeries(t, testSeriesValues{help: HelpNone, metricType: MetricTypeCounter})
	require.Equal(t, map[string]string{
		"__name__":         "mimir_continuous_test_explore_float_counter_help_none_total",
		labelFamily:        "float",
		labelCharset:       "legacy",
		labelUnit:          "none",
		labelHelp:          "none",
		labelType:          "counter",
		labelWriteInterval: "10s",
		labelSeed:          "1",
	}, labelMap(t, s.Labels(common)))
}

func TestSeries_MetricName(t *testing.T) {
	for metricType, expected := range map[MetricType]string{
		MetricTypeCounter:  "mimir_continuous_test_explore_float_counter_help_ascii_total",
		MetricTypeGauge:    "mimir_continuous_test_explore_float_gauge_help_ascii",
		MetricTypeUnknown:  "mimir_continuous_test_explore_float_unknown_help_ascii",
		MetricTypeInfo:     "mimir_continuous_test_explore_float_info_help_ascii",
		MetricTypeStateset: "mimir_continuous_test_explore_float_stateset_help_ascii",
	} {
		require.Equal(t, expected, testSeries(t, testSeriesValues{metricType: metricType}).MetricName())
	}
	require.Equal(t, "mimir_continuous_test_explore_float_counter_help_utf8_seconds_total", testSeries(t, testSeriesValues{unit: UnitSeconds, help: HelpUTF8, metricType: MetricTypeCounter}).MetricName())
	require.Equal(t, "mimir.continuous_test.explore.float.counter.help_none.bytes_total", testSeries(t, testSeriesValues{charset: CharsetUTF8, unit: UnitBytes, help: HelpNone, metricType: MetricTypeCounter}).MetricName())
	require.Equal(t, "mimir_continuous_test_explore_native_histogram_gauge_help_ascii", testSeries(t, testSeriesValues{kind: KindGauge}).MetricName())
	require.Equal(t, "mimir.continuous_test.explore.native_histogram.gauge.help_ascii.seconds", testSeries(t, testSeriesValues{charset: CharsetUTF8, unit: UnitSeconds, kind: KindGauge}).MetricName())
}

func TestSeries_Metadata(t *testing.T) {
	s := testSeries(t, testSeriesValues{kind: KindGauge, charset: CharsetUTF8, unit: UnitSeconds})
	require.Equal(t, prompb.MetricMetadata{
		Type:             prompb.MetricMetadata_GAUGEHISTOGRAM,
		MetricFamilyName: "mimir.continuous_test.explore.native_histogram.gauge.help_ascii.seconds",
		Help:             "A native histogram gauge written by the continuous-test explore test with protocol v2.",
		Unit:             "seconds",
	}, s.Metadata(ProtocolV2))

	s = testSeries(t, testSeriesValues{unit: UnitBytes, help: HelpUTF8})
	require.Equal(t, prompb.MetricMetadata{
		Type:             prompb.MetricMetadata_HISTOGRAM,
		MetricFamilyName: "mimir_continuous_test_explore_native_histogram_counter_help_utf8_bytes",
		Help:             "A native histogram counter written by the continuous-test explore test with protocol otlp." + utf8HelpSuffix,
		Unit:             "By",
	}, s.Metadata(ProtocolOTLP))
	require.Equal(t, "bytes", s.PrometheusUnit())

	s = testSeries(t, testSeriesValues{help: HelpNone})
	require.Equal(t, prompb.MetricMetadata{
		Type:             prompb.MetricMetadata_HISTOGRAM,
		MetricFamilyName: "mimir_continuous_test_explore_native_histogram_counter_help_none",
	}, s.Metadata(ProtocolOTLP))
	require.Empty(t, s.PrometheusUnit())

	for metricType, expected := range map[MetricType]prompb.MetricMetadata_MetricType{
		MetricTypeCounter:  prompb.MetricMetadata_COUNTER,
		MetricTypeGauge:    prompb.MetricMetadata_GAUGE,
		MetricTypeUnknown:  prompb.MetricMetadata_UNKNOWN,
		MetricTypeInfo:     prompb.MetricMetadata_INFO,
		MetricTypeStateset: prompb.MetricMetadata_STATESET,
	} {
		s = testSeries(t, testSeriesValues{unit: UnitSeconds, metricType: metricType})
		require.Equal(t, prompb.MetricMetadata{
			Type:             expected,
			MetricFamilyName: s.MetricName(),
			Help:             "A float " + string(metricType) + " written by the continuous-test explore test with protocol v1.",
			Unit:             "seconds",
		}, s.Metadata(ProtocolV1))
	}
}

// The OTLP endpoint escapes UTF-8 names to the expected names however it is configured to handle
// underscores and suffixes, and translates OTLP units back.
func TestSeries_EscapedNames(t *testing.T) {
	otlpTypes := map[MetricType]otlptranslator.MetricType{
		MetricTypeCounter: otlptranslator.MetricTypeMonotonicCounter,
		MetricTypeGauge:   otlptranslator.MetricTypeGauge,
	}
	var series []Series
	for _, unit := range []Unit{UnitNone, UnitSeconds, UnitBytes} {
		for _, help := range []Help{HelpNone, HelpASCII, HelpUTF8} {
			for metricType := range otlpTypes {
				series = append(series, testSeries(t, testSeriesValues{charset: CharsetUTF8, unit: unit, help: help, metricType: metricType}))
			}
			series = append(series, testSeries(t, testSeriesValues{charset: CharsetUTF8, unit: unit, help: help}))
		}
	}
	for _, suffixes := range []bool{false, true} {
		namer := otlptranslator.MetricNamer{WithMetricSuffixes: suffixes}
		for _, s := range series {
			var otlpType otlptranslator.MetricType = otlptranslator.MetricTypeExponentialHistogram
			if s.Family == FamilyFloat {
				otlpType = otlpTypes[s.Type]
			}
			m := s.Metadata(ProtocolOTLP)
			escaped, err := namer.Build(otlptranslator.Metric{Name: s.MetricName(), Unit: m.Unit, Type: otlpType})
			require.NoError(t, err)
			require.Equal(t, s.metricName(CharsetLegacy), escaped, "series %s, suffixes %t", s, suffixes)
		}
	}
	unitNamer := otlptranslator.UnitNamer{}
	for _, unit := range []Unit{UnitNone, UnitSeconds, UnitBytes} {
		s := testSeries(t, testSeriesValues{unit: unit})
		require.Equal(t, s.PrometheusUnit(), unitNamer.Build(s.Metadata(ProtocolOTLP).Unit), "unit %s", unit)
	}
	for _, sanitization := range []bool{false, true} {
		for _, preserve := range []bool{false, true} {
			namer := otlptranslator.LabelNamer{UnderscoreLabelSanitization: sanitization, PreserveMultipleUnderscores: preserve}
			escaped, err := namer.Build(utf8LabelName)
			require.NoError(t, err)
			require.Equal(t, escapedUTF8LabelName, escaped, "sanitization %t, preserve multiple underscores %t", sanitization, preserve)
		}
	}
}

func allSchemas() []int32 {
	var schemas []int32
	for schema := histogram.ExponentialSchemaMin; schema <= histogram.ExponentialSchemaMax; schema++ {
		schemas = append(schemas, schema)
	}
	return append(schemas, histogram.CustomBucketsSchema)
}

func labelMap(t *testing.T, lbls []prompb.Label) map[string]string {
	values := map[string]string{}
	for i, l := range lbls {
		if i > 0 {
			require.Less(t, lbls[i-1].Name, l.Name, "labels must be sorted")
		}
		values[l.Name] = l.Value
	}
	return values
}

var allMetricTypes = []MetricType{MetricTypeCounter, MetricTypeGauge, MetricTypeUnknown, MetricTypeInfo, MetricTypeStateset}

// testSeriesValues holds the test seed and the dimension values of a test series: a float series
// if metricType is set, or else a native histogram series. Unset values default to the first
// value of each dimension, but for the help, which defaults to an ASCII help.
type testSeriesValues struct {
	seed       uint64
	charset    Charset
	unit       Unit
	help       Help
	metricType MetricType
	sampleType SampleType
	kind       Kind
	schema     int32
	layout     Layout
}

func testSeries(t *testing.T, v testSeriesValues) Series {
	def := Definition{
		Charsets: []Charset{cmp.Or(v.charset, CharsetLegacy)},
		Metadata: MetadataDefinition{Units: []Unit{cmp.Or(v.unit, UnitNone)}, Helps: []Help{cmp.Or(v.help, HelpASCII)}},
	}
	if v.metricType != "" {
		def.Floats = &FloatsDefinition{Types: []MetricType{v.metricType}}
	} else {
		def.NativeHistograms = &NativeHistogramsDefinition{
			SampleTypes: []SampleType{cmp.Or(v.sampleType, SampleTypeInt)},
			Kinds:       []Kind{cmp.Or(v.kind, KindCounter)},
			Schemas:     []int32{v.schema},
			Layouts:     []Layout{cmp.Or(v.layout, LayoutContiguous)},
		}
	}
	dims := def.families()[0]
	values := make([]string, len(dims))
	for i, d := range dims {
		values[i] = d.values[0]
	}
	s, err := newSeries(v.seed, dims, values)
	require.NoError(t, err)
	return s
}
