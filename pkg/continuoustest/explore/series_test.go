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
								// Writes pick series at random, so samples can be any number of seconds apart.
								ts := start.Add(time.Duration(i*i) * time.Second)
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
								require.Equal(t, expected, s.ExpectedHistogram(ts.Add(999*time.Millisecond)), "the value must only depend on the second")
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
	require.NotEqual(t, s.ExpectedHistogram(ts), s.ExpectedHistogram(ts.Add(time.Second)))
}

// A series keeps its seed, and so its layout and values, when values are added to the definition.
func TestSeries_SeedOnlyDependsOnTheDimensionValues(t *testing.T) {
	s := testSeries(t, testSeriesValues{seed: 1, kind: KindGauge, schema: 3})

	def := Definition{
		Charsets: []Charset{CharsetUTF8, CharsetLegacy},
		NativeHistograms: NativeHistogramsDefinition{
			SampleTypes: []SampleType{SampleTypeFloat, SampleTypeInt},
			Kinds:       []Kind{KindCounter, KindGauge},
			Schemas:     []int32{0, 3},
			Layouts:     allLayouts,
		},
	}
	found := false
	for _, values := range allCombinations(def.dimensions()) {
		other, err := newSeries(1, def.dimensions(), values)
		require.NoError(t, err)
		if other.String() == s.String() {
			found = true
			require.Equal(t, s, other)
		} else {
			require.NotEqual(t, s.Seed, other.Seed)
		}
	}
	require.True(t, found)

	require.NotEqual(t, s.Seed, testSeries(t, testSeriesValues{seed: 2, kind: KindGauge, schema: 3}).Seed, "the seed of the test must change the seed of the series")
}

func TestSeries_Labels(t *testing.T) {
	common := []prompb.Label{{Name: labelPace, Value: "dev"}, {Name: labelSeed, Value: "1"}}
	s := testSeries(t, testSeriesValues{sampleType: SampleTypeFloat, kind: KindCounter, schema: -2, layout: LayoutGaps})
	expected := map[string]string{
		"__name__":      counterMetricName,
		labelCharset:    "legacy",
		labelSampleType: "float",
		labelKind:       "counter",
		labelSchema:     "-2",
		labelLayout:     "gaps",
		labelPace:       "dev",
		labelSeed:       "1",
	}
	require.Equal(t, expected, labelMap(t, s.Labels(common)))

	s = testSeries(t, testSeriesValues{charset: CharsetUTF8, sampleType: SampleTypeFloat, kind: KindCounter, schema: -2, layout: LayoutGaps})
	expected["__name__"] = utf8CounterMetricName
	expected[labelCharset] = "utf8"
	expected[utf8LabelName] = utf8LabelValue
	require.Equal(t, expected, labelMap(t, s.Labels(common)))
}

func TestSeries_Metadata(t *testing.T) {
	s := testSeries(t, testSeriesValues{kind: KindGauge, charset: CharsetUTF8})
	require.Equal(t, prompb.MetricMetadata{
		Type:             prompb.MetricMetadata_GAUGEHISTOGRAM,
		MetricFamilyName: utf8GaugeMetricName,
		Help:             "A native histogram gauge written by the continuous-test explore test with protocol v2.",
		Unit:             metadataUnit,
	}, s.Metadata(ProtocolV2))

	s = testSeries(t, testSeriesValues{})
	require.Equal(t, prompb.MetricMetadata{
		Type:             prompb.MetricMetadata_HISTOGRAM,
		MetricFamilyName: counterMetricName,
		Help:             "A native histogram counter written by the continuous-test explore test with protocol otlp.",
	}, s.Metadata(ProtocolOTLP))
}

// The OTLP endpoint escapes UTF-8 names to the expected names however it is configured to handle
// underscores.
func TestSeries_EscapedNames(t *testing.T) {
	for _, suffixes := range []bool{false, true} {
		namer := otlptranslator.MetricNamer{WithMetricSuffixes: suffixes}
		for name, expected := range map[string]string{utf8CounterMetricName: counterMetricName, utf8GaugeMetricName: gaugeMetricName} {
			escaped, err := namer.Build(otlptranslator.Metric{Name: name, Type: otlptranslator.MetricTypeExponentialHistogram})
			require.NoError(t, err)
			require.Equal(t, expected, escaped, "suffixes %t", suffixes)
		}
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

// testSeriesValues holds the test seed and the dimension values of a test series. Unset values
// default to the first value of each dimension.
type testSeriesValues struct {
	seed       uint64
	charset    Charset
	sampleType SampleType
	kind       Kind
	schema     int32
	layout     Layout
}

func testSeries(t *testing.T, v testSeriesValues) Series {
	def := Definition{
		Charsets: []Charset{cmp.Or(v.charset, CharsetLegacy)},
		NativeHistograms: NativeHistogramsDefinition{
			SampleTypes: []SampleType{cmp.Or(v.sampleType, SampleTypeInt)},
			Kinds:       []Kind{cmp.Or(v.kind, KindCounter)},
			Schemas:     []int32{v.schema},
			Layouts:     []Layout{cmp.Or(v.layout, LayoutContiguous)},
		},
	}
	dims := def.dimensions()
	values := make([]string, len(dims))
	for i, d := range dims {
		values[i] = d.values[0]
	}
	s, err := newSeries(v.seed, dims, values)
	require.NoError(t, err)
	return s
}
