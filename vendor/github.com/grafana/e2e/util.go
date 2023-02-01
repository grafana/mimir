package e2e

import (
	"context"
	"crypto/tls"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
)

func RunCommandAndGetOutput(name string, args ...string) ([]byte, error) {
	cmd := exec.Command(name, args...)
	return cmd.CombinedOutput()
}

func RunCommandWithTimeoutAndGetOutput(timeout time.Duration, name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, name, args...)
	return cmd.CombinedOutput()
}

func EmptyFlags() map[string]string {
	return map[string]string{}
}

func MergeFlags(inputs ...map[string]string) map[string]string {
	output := MergeFlagsWithoutRemovingEmpty(inputs...)

	for k, v := range output {
		if v == "" {
			delete(output, k)
		}
	}

	return output
}

func MergeFlagsWithoutRemovingEmpty(inputs ...map[string]string) map[string]string {
	output := map[string]string{}

	for _, input := range inputs {
		for name, value := range input {
			output[name] = value
		}
	}

	return output
}

func BuildArgs(flags map[string]string) []string {
	args := make([]string, 0, len(flags))

	for name, value := range flags {
		if value != "" {
			args = append(args, name+"="+value)
		} else {
			args = append(args, name)
		}
	}

	return args
}

// DoGet performs a HTTP GET request towards the supplied URL and using a
// timeout of 1 second.
func DoGet(url string) (*http.Response, error) {
	return doRequest("GET", url, nil, nil)
}

// DoGetTLS is like DoGet but allows to configure a TLS config.
func DoGetTLS(url string, tlsConfig *tls.Config) (*http.Response, error) {
	return doRequest("GET", url, nil, tlsConfig)
}

// DoPost performs a HTTP POST request towards the supplied URL with an empty
// body and using a timeout of 1 second.
func DoPost(url string) (*http.Response, error) {
	return doRequest("POST", url, strings.NewReader(""), nil)
}

func doRequest(method, url string, body io.Reader, tlsConfig *tls.Config) (*http.Response, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	client := &http.Client{
		Timeout: time.Second,
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}

	return client.Do(req)
}

// TimeToMilliseconds returns the input time as milliseconds, using the same
// formula used by Prometheus in order to get the same timestamp when asserting
// on query results. The formula we're mimicking here is Prometheus parseTime().
// See: https://github.com/prometheus/prometheus/blob/df80dc4d3970121f2f76cba79050983ffb3cdbb0/web/api/v1/api.go#L1690-L1694
func TimeToMilliseconds(t time.Time) int64 {
	// Convert to seconds.
	sec := float64(t.Unix()) + float64(t.Nanosecond())/1e9

	// Parse seconds.
	s, ns := math.Modf(sec)

	// Round nanoseconds part.
	ns = math.Round(ns*1000) / 1000

	// Convert to millis.
	return (int64(s) * 1e3) + (int64(ns * 1e3))
}

func GenerateSeries(name string, ts time.Time, additionalLabels ...prompb.Label) (series []prompb.TimeSeries, vector model.Vector, matrix model.Matrix) {
	tsMillis := TimeToMilliseconds(ts)
	value := rand.Float64()

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
			{Value: value, Timestamp: tsMillis, Labels: []prompb.Label{
				{Name: "trace_id", Value: "1234"},
			}},
		},
		Samples: []prompb.Sample{
			{Value: value, Timestamp: tsMillis},
		},
	})

	// Generate the expected vector and matrix when querying it
	metric := model.Metric{}
	metric[labels.MetricName] = model.LabelValue(name)
	for _, lbl := range additionalLabels {
		metric[model.LabelName(lbl.Name)] = model.LabelValue(lbl.Value)
	}

	vector = append(vector, &model.Sample{
		Metric:    metric,
		Value:     model.SampleValue(value),
		Timestamp: model.Time(tsMillis),
	})

	matrix = append(matrix, &model.SampleStream{
		Metric: metric,
		Values: []model.SamplePair{
			{
				Timestamp: model.Time(tsMillis),
				Value:     model.SampleValue(value),
			},
		},
	})

	return
}

func GenerateNSeries(nSeries, nExemplars int, name func() string, ts time.Time, additionalLabels func() []prompb.Label) (series []prompb.TimeSeries, vector model.Vector) {
	tsMillis := TimeToMilliseconds(ts)

	// Generate the series
	for i := 0; i < nSeries; i++ {
		lbls := []prompb.Label{
			{Name: labels.MetricName, Value: name()},
		}
		if additionalLabels != nil {
			lbls = append(lbls, additionalLabels()...)
		}

		value := rand.Float64()

		exemplars := []prompb.Exemplar{}
		if i < nExemplars {
			exemplars = []prompb.Exemplar{
				{Value: value, Timestamp: tsMillis, Labels: []prompb.Label{{Name: "trace_id", Value: "1234"}}},
			}
		}

		series = append(series, prompb.TimeSeries{
			Labels: lbls,
			Samples: []prompb.Sample{
				{Value: value, Timestamp: tsMillis},
			},
			Exemplars: exemplars,
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
			Value:     model.SampleValue(series[i].Samples[0].Value),
			Timestamp: model.Time(tsMillis),
		})
	}
	return
}

// GetTempDirectory creates a temporary directory for shared integration
// test files, either in the working directory or a directory referenced by
// the E2E_TEMP_DIR environment variable
func GetTempDirectory() (string, error) {
	var (
		dir string
		err error
	)
	// If a temp dir is referenced, return that
	if os.Getenv("E2E_TEMP_DIR") != "" {
		dir = os.Getenv("E2E_TEMP_DIR")
	} else {
		dir, err = os.Getwd()
		if err != nil {
			return "", err
		}
	}

	tmpDir, err := ioutil.TempDir(dir, "e2e_integration_test")
	if err != nil {
		return "", err
	}
	// Allow use of the temporary directory for testing with non-root
	// users.
	if err := os.Chmod(tmpDir, 0777); err != nil {
		return "", err
	}
	absDir, err := filepath.Abs(tmpDir)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		return "", err
	}

	return absDir, nil
}

// based on GenerateTestHistograms in github.com/prometheus/prometheus/tsdb
func GenerateTestHistogram(i int) *histogram.Histogram {
	return &histogram.Histogram{
		Count:         10 + uint64(i*8),
		ZeroCount:     2 + uint64(i),
		ZeroThreshold: 0.001,
		Sum:           18.4 * float64(i+1),
		Schema:        1,
		PositiveSpans: []histogram.Span{
			{Offset: 0, Length: 2},
			{Offset: 1, Length: 2},
		},
		PositiveBuckets: []int64{int64(i + 1), 1, -1, 0},
		NegativeSpans: []histogram.Span{
			{Offset: 0, Length: 2},
			{Offset: 1, Length: 2},
		},
		NegativeBuckets: []int64{int64(i + 1), 1, -1, 0},
	}
}

// based on GenerateTestFloatHistograms in github.com/prometheus/prometheus/tsdb
func GenerateTestFloatHistogram(i int) *histogram.FloatHistogram {
	return &histogram.FloatHistogram{
		Count:         10 + float64(i*8),
		ZeroCount:     2 + float64(i),
		ZeroThreshold: 0.001,
		Sum:           18.4 * float64(i+1),
		Schema:        1,
		PositiveSpans: []histogram.Span{
			{Offset: 0, Length: 2},
			{Offset: 1, Length: 2},
		},
		PositiveBuckets: []float64{float64(i + 1), float64(i + 2), float64(i + 1), float64(i + 1)},
		NegativeSpans: []histogram.Span{
			{Offset: 0, Length: 2},
			{Offset: 1, Length: 2},
		},
		NegativeBuckets: []float64{float64(i + 1), float64(i + 2), float64(i + 1), float64(i + 1)},
	}
}

// explicit decoded version of GenerateTestHistogram and GenerateTestFloatHistogram
func GenerateTestSampleHistogram(i int) *model.SampleHistogram {
	return &model.SampleHistogram{
		Count: model.FloatString(10 + i*8),
		Sum:   model.FloatString(18.4 * float64(i+1)),
		Buckets: model.HistogramBuckets{
			&model.HistogramBucket{
				Boundaries: 1,
				Lower:      -4,
				Upper:      -2.82842712474619,
				Count:      model.FloatString(1 + i),
			},
			&model.HistogramBucket{
				Boundaries: 1,
				Lower:      -2.82842712474619,
				Upper:      -2,
				Count:      model.FloatString(1 + i),
			},
			&model.HistogramBucket{
				Boundaries: 1,
				Lower:      -1.414213562373095,
				Upper:      -1,
				Count:      model.FloatString(2 + i),
			},
			&model.HistogramBucket{
				Boundaries: 1,
				Lower:      -1,
				Upper:      -0.7071067811865475,
				Count:      model.FloatString(1 + i),
			},
			&model.HistogramBucket{
				Boundaries: 3,
				Lower:      -0.001,
				Upper:      0.001,
				Count:      model.FloatString(2 + i),
			},
			&model.HistogramBucket{
				Boundaries: 0,
				Lower:      0.7071067811865475,
				Upper:      1,
				Count:      model.FloatString(1 + i),
			},
			&model.HistogramBucket{
				Boundaries: 0,
				Lower:      1,
				Upper:      1.414213562373095,
				Count:      model.FloatString(2 + i),
			},
			&model.HistogramBucket{
				Boundaries: 0,
				Lower:      2,
				Upper:      2.82842712474619,
				Count:      model.FloatString(1 + i),
			},
			&model.HistogramBucket{
				Boundaries: 0,
				Lower:      2.82842712474619,
				Upper:      4,
				Count:      model.FloatString(1 + i),
			},
		},
	}
}

func GenerateHistogramSeries(name string, ts time.Time, additionalLabels ...prompb.Label) (series []prompb.TimeSeries, vector model.Vector, matrix model.Matrix) {
	tsMillis := TimeToMilliseconds(ts)

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
		Histograms: []prompb.Histogram{remote.HistogramToHistogramProto(tsMillis, GenerateTestHistogram(value))},
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
		Histogram: GenerateTestSampleHistogram(value),
	})

	matrix = append(matrix, &model.SampleStream{
		Metric: metric,
		Histograms: []model.SampleHistogramPair{
			{
				Timestamp: model.Time(tsMillis),
				Histogram: GenerateTestSampleHistogram(value),
			},
		},
	})

	return
}

func GenerateNHistogramSeries(nSeries, nExemplars int, name func() string, ts time.Time, additionalLabels func() []prompb.Label) (series []prompb.TimeSeries, vector model.Vector) {
	tsMillis := TimeToMilliseconds(ts)

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
			Histograms: []prompb.Histogram{remote.HistogramToHistogramProto(tsMillis, GenerateTestHistogram(i))},
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
			Histogram: GenerateTestSampleHistogram(i),
		})
	}
	return
}
