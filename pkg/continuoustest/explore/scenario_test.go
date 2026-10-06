// SPDX-License-Identifier: AGPL-3.0-only

package explore

import (
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/stretchr/testify/require"
)

func TestScenario_Histogram(t *testing.T) {
	for _, sampleType := range []SampleType{SampleTypeInt, SampleTypeFloat} {
		for _, kind := range []Kind{KindCounter, KindGauge} {
			for schema := histogram.ExponentialSchemaMin; schema <= histogram.ExponentialSchemaMax; schema++ {
				s := testScenario(t, sampleType, kind, schema)
				t.Run(s.String(), func(t *testing.T) {
					for series := range 3 {
						var prev *histogram.FloatHistogram
						for step := range int64(20) {
							h, fh := s.Histogram(series, step)
							if sampleType == SampleTypeInt {
								require.Nil(t, fh)
								require.NoError(t, h.Validate())
							} else {
								require.Nil(t, h)
								require.NoError(t, fh.Validate())
							}

							expected := s.ExpectedHistogram(series, step)
							require.Equal(t, schema, expected.Schema)
							require.Equal(t, kind == KindGauge, expected.CounterResetHint == histogram.GaugeType)
							require.Equal(t, expected, s.ExpectedHistogram(series, step), "generation must be deterministic")
							if kind == KindCounter && prev != nil {
								require.False(t, expected.DetectReset(prev), "counters must never reset")
							}
							prev = expected
						}
					}
				})
			}
		}
	}
}

func TestScenario_SeriesDiffer(t *testing.T) {
	s := testScenario(t, SampleTypeInt, KindGauge, 3)
	require.NotEqual(t, s.ExpectedHistogram(0, 0), s.ExpectedHistogram(1, 0))
	require.NotEqual(t, s.ExpectedHistogram(0, 0), s.ExpectedHistogram(0, 1))
}

func TestScenario_SeriesLabels(t *testing.T) {
	s := testScenario(t, SampleTypeFloat, KindCounter, -2)
	lbls := s.SeriesLabels(1, nil)
	values := map[string]string{}
	for i, l := range lbls {
		if i > 0 {
			require.Less(t, lbls[i-1].Name, l.Name, "labels must be sorted")
		}
		values[l.Name] = l.Value
	}
	require.Equal(t, map[string]string{
		"__name__":        counterMetricName,
		labelScenario:     "7",
		labelScenarioSeed: strconv.FormatUint(s.Seed, 10),
		labelSeriesID:     "1",
		labelSampleType:   "float",
		labelKind:         "counter",
		labelSchema:       "-2",
	}, values)
}

func testScenario(t *testing.T, sampleType SampleType, kind Kind, schema int32) Scenario {
	def := NativeHistogramsDefinition{SampleTypes: []SampleType{sampleType}, Kinds: []Kind{kind}, Schemas: []int32{schema}}
	s, err := newScenario(7, time.Unix(0, 0), mix(uint64(schema), 7), def.dimensions(), []string{string(sampleType), string(kind), strconv.Itoa(int(schema))})
	require.NoError(t, err)
	return s
}
