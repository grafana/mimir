package mimirpb

import (
	"fmt"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	util_test "github.com/grafana/mimir/pkg/util/test"
)

func TestUnmarshalWriteRequest(t *testing.T) {
	req := makeWriteRequest(1_000, 2, 2, true, true, "foo", "bar")
	data, err := req.Marshal()
	require.NoError(t, err)

	got := &WriteRequest{}
	require.NoError(t, UnmarshalWriteRequest(got, data))

	gotProto := &WriteRequest{}
	require.NoError(t, gotProto.Unmarshal(data))

	require.Equal(t, got, gotProto)
}

func BenchmarkUnmarshalWriteRequest(b *testing.B) {
	metrics := make([]string, 50)
	for i := range metrics {
		metrics[i] = fmt.Sprintf("metric_%d", i)
	}
	reqs := make([]*WriteRequest, 1000)
	datas := make([][]byte, 1000)
	var err error
	for i := range reqs {
		reqs[i] = makeWriteRequest(1_000, 100, 100, true, true, metrics...)
		datas[i], err = reqs[i].Marshal()
		require.NoError(b, err)
	}

	b.Run("unmarshal=proto", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			w := &WriteRequest{}
			if err := w.Unmarshal(datas[i%len(datas)]); err != nil {
				panic(err)
			}
		}
	})
	b.Run("unmarshal=inlined", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			w := &WriteRequest{}
			if err := UnmarshalWriteRequest(w, datas[i%len(datas)]); err != nil {
				panic(err)
			}
		}
	})
}

// makeWriteRequest is copied from distributor_test.go
func makeWriteRequest(startTimestampMs int64, samples, metadata int, exemplars, histograms bool, metrics ...string) *WriteRequest {
	request := &WriteRequest{}
	for _, metric := range metrics {
		for i := 0; i < samples; i++ {
			req := makeTimeseries(
				[]string{
					model.MetricNameLabel, metric,
					"bar", "baz",
					"sample", fmt.Sprintf("%d", i),
				},
				makeSamples(startTimestampMs+int64(i), float64(i)),
				nil,
			)

			if exemplars {
				req.Exemplars = makeExemplars(
					[]string{
						"traceID", "123456",
						"foo", "bar",
						"exemplar", fmt.Sprintf("%d", i),
					},
					startTimestampMs+int64(i),
					float64(i),
				)
			}

			if histograms {
				if i%2 == 0 {
					req.Histograms = makeHistograms(startTimestampMs+int64(i), util_test.GenerateTestHistogram(i))
				} else {
					req.Histograms = makeFloatHistograms(startTimestampMs+int64(i), util_test.GenerateTestFloatHistogram(i))
				}
			}

			request.Timeseries = append(request.Timeseries, req)
		}

		for i := 0; i < metadata; i++ {
			m := &MetricMetadata{
				MetricFamilyName: fmt.Sprintf("metric_%d", i),
				Type:             COUNTER,
				Help:             fmt.Sprintf("a help for metric_%d", i),
			}
			request.Metadata = append(request.Metadata, m)
		}
	}

	return request
}

func makeTimeseries(seriesLabels []string, samples []Sample, exemplars []Exemplar) PreallocTimeseries {
	return PreallocTimeseries{
		TimeSeries: &TimeSeries{
			Labels:    FromLabelsToLabelAdapters(labels.FromStrings(seriesLabels...)),
			Samples:   samples,
			Exemplars: exemplars,
		},
	}
}

func makeSamples(ts int64, value float64) []Sample {
	return []Sample{{
		Value:       value,
		TimestampMs: ts,
	}}
}

func makeExemplars(exemplarLabels []string, ts int64, value float64) []Exemplar {
	return []Exemplar{{
		Labels:      FromLabelsToLabelAdapters(labels.FromStrings(exemplarLabels...)),
		Value:       value,
		TimestampMs: ts,
	}}
}

func makeHistograms(ts int64, histogram *histogram.Histogram) []Histogram {
	return []Histogram{FromHistogramToHistogramProto(ts, histogram)}
}

func makeFloatHistograms(ts int64, histogram *histogram.FloatHistogram) []Histogram {
	return []Histogram{FromFloatHistogramToHistogramProto(ts, histogram)}
}
