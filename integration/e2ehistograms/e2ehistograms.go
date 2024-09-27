// SPDX-License-Identifier: AGPL-3.0-only

package e2ehistograms

import (
	"math/rand"
	"time"

	"github.com/grafana/e2e"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"

	"github.com/grafana/mimir/pkg/util/test"
)

var (
	generateTestHistogram           = test.GenerateTestHistogram
	generateTestFloatHistogram      = test.GenerateTestFloatHistogram
	generateTestGaugeHistogram      = test.GenerateTestGaugeHistogram
	generateTestGaugeFloatHistogram = test.GenerateTestGaugeFloatHistogram
	generateTestSampleHistogram     = test.GenerateTestSampleHistogram
)

// generateHistogramFunc defines what kind of native histograms to generate: float/integer, counter/gauge
type generateHistogramFunc func(tsMillis int64, value int) prompb.Histogram

func GenerateHistogramSeries(name string, ts time.Time, additionalLabels ...prompb.Label) (series []prompb.TimeSeries, vector model.Vector, matrix model.Matrix) {
	return generateHistogramSeriesWrapper(func(tsMillis int64, value int) prompb.Histogram {
		return prompb.FromIntHistogram(tsMillis, generateTestHistogram(value))
	}, name, ts, additionalLabels...)
}

func GenerateFloatHistogramSeries(name string, ts time.Time, additionalLabels ...prompb.Label) (series []prompb.TimeSeries, vector model.Vector, matrix model.Matrix) {
	return generateHistogramSeriesWrapper(func(tsMillis int64, value int) prompb.Histogram {
		return prompb.FromFloatHistogram(tsMillis, generateTestFloatHistogram(value))
	}, name, ts, additionalLabels...)
}

func GenerateGaugeHistogramSeries(name string, ts time.Time, additionalLabels ...prompb.Label) (series []prompb.TimeSeries, vector model.Vector, matrix model.Matrix) {
	return generateHistogramSeriesWrapper(func(tsMillis int64, value int) prompb.Histogram {
		return prompb.FromIntHistogram(tsMillis, generateTestGaugeHistogram(value))
	}, name, ts, additionalLabels...)
}

func GenerateGaugeFloatHistogramSeries(name string, ts time.Time, additionalLabels ...prompb.Label) (series []prompb.TimeSeries, vector model.Vector, matrix model.Matrix) {
	return generateHistogramSeriesWrapper(func(tsMillis int64, value int) prompb.Histogram {
		return prompb.FromFloatHistogram(tsMillis, generateTestGaugeFloatHistogram(value))
	}, name, ts, additionalLabels...)
}

func generateHistogramSeriesWrapper(generateHistogram generateHistogramFunc, name string, ts time.Time, additionalLabels ...prompb.Label) (series []prompb.TimeSeries, vector model.Vector, matrix model.Matrix) {
	tsMillis := e2e.TimeToMilliseconds(ts)

	value := rand.Intn(1000)

	lbls := append(
		[]prompb.Label{
			{Name: labels.MetricName, Value: name},
		},
		additionalLabels...,
	)

	// Generate the series
	series = append(series, prompb.TimeSeries{
		Labels: lbls,
		Exemplars: []prompb.Exemplar{
			{Value: float64(value), Timestamp: tsMillis, Labels: []prompb.Label{
				{Name: "trace_id", Value: "1234"},
			}},
		},
		Histograms: []prompb.Histogram{generateHistogram(tsMillis, value)},
	})

	// Generate the expected vector and matrix when querying it
	metric := model.Metric{}
	metric[labels.MetricName] = model.LabelValue(name)
	for _, lbl := range additionalLabels {
		metric[model.LabelName(lbl.Name)] = model.LabelValue(lbl.Value)
	}

	vector = append(vector, &model.Sample{
		Metric:    metric,
		Timestamp: model.Time(tsMillis),
		Histogram: generateTestSampleHistogram(value),
	})

	matrix = append(matrix, &model.SampleStream{
		Metric: metric,
		Histograms: []model.SampleHistogramPair{
			{
				Timestamp: model.Time(tsMillis),
				Histogram: generateTestSampleHistogram(value),
			},
		},
	})

	return
}

func GenerateNHistogramSeries(nSeries, nExemplars int, name func() string, ts time.Time, additionalLabels func() []prompb.Label) (series []prompb.TimeSeries, vector model.Vector) {
	tsMillis := e2e.TimeToMilliseconds(ts)

	// Generate the series
	for i := 0; i < nSeries; i++ {
		lbls := []prompb.Label{
			{Name: labels.MetricName, Value: name()},
		}
		if additionalLabels != nil {
			lbls = append(lbls, additionalLabels()...)
		}

		exemplars := []prompb.Exemplar{}
		if i < nExemplars {
			exemplars = []prompb.Exemplar{
				{Value: float64(i), Timestamp: tsMillis, Labels: []prompb.Label{{Name: "trace_id", Value: "1234"}}},
			}
		}

		series = append(series, prompb.TimeSeries{
			Labels:     lbls,
			Histograms: []prompb.Histogram{prompb.FromIntHistogram(tsMillis, generateTestHistogram(i))},
			Exemplars:  exemplars,
		})
	}

	// Generate the expected vector when querying it
	for i := 0; i < nSeries; i++ {
		metric := model.Metric{}
		for _, lbl := range series[i].Labels {
			metric[model.LabelName(lbl.Name)] = model.LabelValue(lbl.Value)
		}

		vector = append(vector, &model.Sample{
			Metric:    metric,
			Timestamp: model.Time(tsMillis),
			Histogram: generateTestSampleHistogram(i),
		})
	}
	return
}
