package tsdb

import (
	"testing"

	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
)

const (
	float               = "float"
	intHistogram        = "integer histogram"
	floatHistogram      = "float histogram"
	gaugeIntHistogram   = "gauge int histogram"
	gaugeFloatHistogram = "gauge float histogram"
)

type tsValue struct {
	Ts                 int64
	V                  int64
	CounterResetHeader histogram.CounterResetHint
}

type sampleTypeScenario struct {
	sampleType string
	appendFunc func(appender storage.Appender, lbls labels.Labels, ts, value int64) (storage.SeriesRef, sample, error)
	sampleFunc func(ts, value int64) sample
}

var sampleTypeScenarios = map[string]sampleTypeScenario{
	float: {
		sampleType: sampleMetricTypeFloat,
		appendFunc: func(appender storage.Appender, lbls labels.Labels, ts, value int64) (storage.SeriesRef, sample, error) {
			s := sample{t: ts, f: float64(value)}
			ref, err := appender.Append(0, lbls, ts, s.f)
			return ref, s, err
		},
		sampleFunc: func(ts, value int64) sample {
			return sample{t: ts, f: float64(value)}
		},
	},
	intHistogram: {
		sampleType: sampleMetricTypeHistogram,
		appendFunc: func(appender storage.Appender, lbls labels.Labels, ts, value int64) (storage.SeriesRef, sample, error) {
			s := sample{t: ts, h: tsdbutil.GenerateTestHistogram(int(value))}
			ref, err := appender.AppendHistogram(0, lbls, ts, s.h, nil)
			return ref, s, err
		},
		sampleFunc: func(ts, value int64) sample {
			return sample{t: ts, h: tsdbutil.GenerateTestHistogram(int(value))}
		},
	},
	floatHistogram: {
		sampleType: sampleMetricTypeHistogram,
		appendFunc: func(appender storage.Appender, lbls labels.Labels, ts, value int64) (storage.SeriesRef, sample, error) {
			s := sample{t: ts, fh: tsdbutil.GenerateTestFloatHistogram(int(value))}
			ref, err := appender.AppendHistogram(0, lbls, ts, nil, s.fh)
			return ref, s, err
		},
		sampleFunc: func(ts, value int64) sample {
			return sample{t: ts, fh: tsdbutil.GenerateTestFloatHistogram(int(value))}
		},
	},
	gaugeIntHistogram: {
		sampleType: sampleMetricTypeHistogram,
		appendFunc: func(appender storage.Appender, lbls labels.Labels, ts, value int64) (storage.SeriesRef, sample, error) {
			s := sample{t: ts, h: tsdbutil.GenerateTestGaugeHistogram(int(value))}
			ref, err := appender.AppendHistogram(0, lbls, ts, s.h, nil)
			return ref, s, err
		},
		sampleFunc: func(ts, value int64) sample {
			return sample{t: ts, h: tsdbutil.GenerateTestGaugeHistogram(int(value))}
		},
	},
	gaugeFloatHistogram: {
		sampleType: sampleMetricTypeHistogram,
		appendFunc: func(appender storage.Appender, lbls labels.Labels, ts, value int64) (storage.SeriesRef, sample, error) {
			s := sample{t: ts, fh: tsdbutil.GenerateTestGaugeFloatHistogram(int(value))}
			ref, err := appender.AppendHistogram(0, lbls, ts, nil, s.fh)
			return ref, s, err
		},
		sampleFunc: func(ts, value int64) sample {
			return sample{t: ts, fh: tsdbutil.GenerateTestGaugeFloatHistogram(int(value))}
		},
	},
}

// requireEqualSamples checks that the actual series are equal to the expected ones. It ignores the counter reset hints for histograms.
func requireEqualSamples(t *testing.T, expected, actual map[string][]chunks.Sample, ignoreCounterResets bool) {
	for name, expectedItem := range expected {
		actualItem, ok := actual[name]
		require.True(t, ok, "Expected series %s not found", name)
		compareSamples(t, name, expectedItem, actualItem, ignoreCounterResets)
	}
	for name := range actual {
		_, ok := expected[name]
		require.True(t, ok, "Unexpected series %s", name)
	}
}

func compareSamples(t *testing.T, name string, expected, actual []chunks.Sample, ignoreCounterResets bool) {
	require.Equal(t, len(expected), len(actual), "Length not expected for %s", name)
	for i, s := range expected {
		expectedSample := s
		actualSample := actual[i]
		require.Equal(t, expectedSample.T(), expectedSample.T(), "Different timestamps for %s[%d]", name, i)
		require.Equal(t, expectedSample.Type().String(), actualSample.Type().String(), "Different types for %s[%d] at ts %d", name, i, expectedSample.T())
		switch {
		case s.H() != nil:
			{
				expectedHist := expectedSample.H()
				actualHist := actualSample.H()
				if ignoreCounterResets && expectedHist.CounterResetHint != histogram.GaugeType {
					expectedHist.CounterResetHint = histogram.UnknownCounterReset
					actualHist.CounterResetHint = histogram.UnknownCounterReset
				} else {
					require.Equal(t, expectedHist.CounterResetHint, actualHist.CounterResetHint, "Sample header doesn't match for %s[%d] at ts %d, expected: %s, actual: %s", name, i, expectedSample.T(), counterResetAsString(expectedHist.CounterResetHint), counterResetAsString(actualHist.CounterResetHint))
				}
				require.Equal(t, expectedHist, actualHist, "Sample doesn't match for %s[%d] at ts %d", name, i, expectedSample.T())
			}
		case s.FH() != nil:
			{
				expectedHist := expectedSample.FH()
				actualHist := actualSample.FH()
				if ignoreCounterResets {
					expectedHist.CounterResetHint = histogram.UnknownCounterReset
					actualHist.CounterResetHint = histogram.UnknownCounterReset
				} else {
					require.Equal(t, expectedHist.CounterResetHint, actualHist.CounterResetHint, "Sample header doesn't match for %s[%d] at ts %d, expected: %s, actual: %s", name, i, expectedSample.T(), counterResetAsString(expectedHist.CounterResetHint), counterResetAsString(actualHist.CounterResetHint))
				}
				require.Equal(t, expectedHist, actualHist, "Sample doesn't match for %s[%d] at ts %d", name, i, expectedSample.T())
			}
		default:
			require.Equal(t, expectedSample, expectedSample, "Sample doesn't match for %s[%d] at ts %d", name, i, expectedSample.T())
		}
	}
}

func counterResetAsString(h histogram.CounterResetHint) string {
	switch h {
	case histogram.UnknownCounterReset:
		return "UnknownCounterReset"
	case histogram.CounterReset:
		return "CounterReset"
	case histogram.NotCounterReset:
		return "NotCounterReset"
	case histogram.GaugeType:
		return "GaugeType"
	}
	panic("Unexpected counter reset type")
}

func samplesFromIterator(t testing.TB, it chunkenc.Iterator) []chunks.Sample {
	var samples []chunks.Sample
	for typ := it.Next(); typ != chunkenc.ValNone; typ = it.Next() {
		switch typ {
		case chunkenc.ValFloat:
			ts, val := it.At()
			samples = append(samples, sample{t: ts, f: val})
		case chunkenc.ValHistogram:
			ts, val := it.AtHistogram(nil)
			samples = append(samples, sample{t: ts, h: val})
		case chunkenc.ValFloatHistogram:
			ts, val := it.AtFloatHistogram(nil)
			samples = append(samples, sample{t: ts, fh: val})
		default:
			t.Fatalf("unknown sample value type %s", typ)
		}
	}
	return samples
}
