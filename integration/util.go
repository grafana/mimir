// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/util.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker
// +build requires_docker

package integration

import (
	"bytes"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/grafana/e2e"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
)

var (
	// Expose some utilities from the framework so that we don't have to prefix them
	// with the package name in tests.
	mergeFlags = e2e.MergeFlags
	// generateSeries  = e2e.GenerateSeries
	// generateNSeries = e2e.GenerateNSeries

	// These are the earliest and latest possible timestamps supported by the Prometheus API -
	// the Prometheus API does not support omitting a time range from query requests,
	// so we use these when we want to query over all time.
	// These values are defined in github.com/prometheus/prometheus/web/api/v1/api.go but
	// sadly not exported.
	prometheusMinTime = time.Unix(math.MinInt64/1000+62135596801, 0).UTC()
	prometheusMaxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()
)

func getMimirProjectDir() string {
	if dir := os.Getenv("MIMIR_CHECKOUT_DIR"); dir != "" {
		return dir
	}

	// use the git path if available
	dir, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err == nil {
		return string(bytes.TrimSpace(dir))
	}

	return os.Getenv("GOPATH") + "/src/github.com/grafana/mimir"
}

func writeFileToSharedDir(s *e2e.Scenario, dst string, content []byte) error {
	dst = filepath.Join(s.SharedDir(), dst)

	// Ensure the entire path of directories exist.
	if err := os.MkdirAll(filepath.Dir(dst), os.ModePerm); err != nil {
		return err
	}

	return os.WriteFile(
		dst,
		content,
		os.ModePerm)
}

func copyFileToSharedDir(s *e2e.Scenario, src, dst string) error {
	content, err := os.ReadFile(filepath.Join(getMimirProjectDir(), src))
	if err != nil {
		return errors.Wrapf(err, "unable to read local file %s", src)
	}

	return writeFileToSharedDir(s, dst, content)
}

func getServerTLSFlags() map[string]string {
	return map[string]string{
		"-server.grpc-tls-cert-path":   filepath.Join(e2e.ContainerSharedDir, serverCertFile),
		"-server.grpc-tls-key-path":    filepath.Join(e2e.ContainerSharedDir, serverKeyFile),
		"-server.grpc-tls-client-auth": "RequireAndVerifyClientCert",
		"-server.grpc-tls-ca-path":     filepath.Join(e2e.ContainerSharedDir, caCertFile),
	}
}

func getServerHTTPTLSFlags() map[string]string {
	return map[string]string{
		"-server.http-tls-cert-path":   filepath.Join(e2e.ContainerSharedDir, serverCertFile),
		"-server.http-tls-key-path":    filepath.Join(e2e.ContainerSharedDir, serverKeyFile),
		"-server.http-tls-client-auth": "RequireAndVerifyClientCert",
		"-server.http-tls-ca-path":     filepath.Join(e2e.ContainerSharedDir, caCertFile),
	}
}

func getClientTLSFlagsWithPrefix(prefix string) map[string]string {
	return getTLSFlagsWithPrefix(prefix, "ingester.client", false)
}

func getTLSFlagsWithPrefix(prefix string, servername string, http bool) map[string]string {
	flags := map[string]string{
		"-" + prefix + ".tls-cert-path":   filepath.Join(e2e.ContainerSharedDir, clientCertFile),
		"-" + prefix + ".tls-key-path":    filepath.Join(e2e.ContainerSharedDir, clientKeyFile),
		"-" + prefix + ".tls-ca-path":     filepath.Join(e2e.ContainerSharedDir, caCertFile),
		"-" + prefix + ".tls-server-name": servername,
	}

	if !http {
		flags["-"+prefix+".tls-enabled"] = "true"
	}

	return flags
}

// based on GenerateTestHistograms in github.com/prometheus/prometheus/tsdb
func generateTestHistogram(i int) *histogram.Histogram {
	return &histogram.Histogram{
		Count:         5 + uint64(i*4),
		ZeroCount:     2 + uint64(i),
		ZeroThreshold: 0.001,
		Sum:           18.4 * float64(i+1),
		Schema:        1,
		PositiveSpans: []histogram.Span{
			{Offset: 0, Length: 2},
			{Offset: 1, Length: 2},
		},
		PositiveBuckets: []int64{int64(i + 1), 1, -1, 0},
	}
}

// Copied from e2e GenerateSeries temporarily until we update it to use newer prometheus (or mimir-prometheus?)
func generateSeries(name string, ts time.Time, additionalLabels ...prompb.Label) (series []prompb.TimeSeries, vector model.Vector, matrix model.Matrix) {
	tsMillis := e2e.TimeToMilliseconds(ts)
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
		Histograms: []prompb.Histogram{remote.HistogramToHistogramProto(tsMillis, generateTestHistogram(0))},
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

func generateNSeries(nSeries, nExemplars int, name func() string, ts time.Time, additionalLabels func() []prompb.Label) (series []prompb.TimeSeries, vector model.Vector) {
	tsMillis := e2e.TimeToMilliseconds(ts)

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
			Exemplars:  exemplars,
			Histograms: []prompb.Histogram{remote.HistogramToHistogramProto(tsMillis, generateTestHistogram(i))},
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
