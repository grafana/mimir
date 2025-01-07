// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/util.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/grafana/e2e"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"

	"github.com/grafana/mimir/integration/e2ehistograms"
)

var (
	// Expose some utilities from the framework so that we don't have to prefix them
	// with the package name in tests.
	mergeFlags = e2e.MergeFlags

	generateFloatSeries  = e2e.GenerateSeries
	generateNFloatSeries = e2e.GenerateNSeries

	// These are local, because e2e is used by non metric products that do not have native histograms
	generateHistogramSeries  = e2ehistograms.GenerateHistogramSeries
	generateNHistogramSeries = e2ehistograms.GenerateNHistogramSeries
)

// generateSeriesFunc defines what kind of series (and expected vectors/matrices) to generate - float samples or native histograms
type generateSeriesFunc func(name string, ts time.Time, additionalLabels ...prompb.Label) (series []prompb.TimeSeries, vector model.Vector, matrix model.Matrix)

// Generates different typed series based on an index in i.
// Use with a large enough number of series, e.g. i>100
func generateAlternatingSeries(i int) generateSeriesFunc {
	switch i % 5 {
	case 0:
		return generateFloatSeries
	case 1:
		return generateHistogramSeries
	case 2:
		return e2ehistograms.GenerateFloatHistogramSeries
	case 3:
		return e2ehistograms.GenerateGaugeHistogramSeries
	case 4:
		return e2ehistograms.GenerateGaugeFloatHistogramSeries
	default:
		return nil
	}
}

// generateNSeriesFunc defines what kind of n * series (and expected vectors) to generate - float samples or native histograms
type generateNSeriesFunc func(nSeries, nExemplars int, name func() string, ts time.Time, additionalLabels func() []prompb.Label) (series []prompb.TimeSeries, vector model.Vector)

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

func filterSamplesByTimestamp(input []prompb.Sample, startMs, endMs int64) []prompb.Sample {
	var filtered []prompb.Sample

	for _, sample := range input {
		if sample.Timestamp >= startMs && sample.Timestamp <= endMs {
			filtered = append(filtered, sample)
		}
	}

	return filtered
}

func filterHistogramsByTimestamp(input []prompb.Histogram, startMs, endMs int64) []prompb.Histogram {
	var filtered []prompb.Histogram

	for _, sample := range input {
		if sample.Timestamp >= startMs && sample.Timestamp <= endMs {
			filtered = append(filtered, sample)
		}
	}

	return filtered
}

func prompbLabelsToMetric(pbLabels []prompb.Label) model.Metric {
	metric := make(model.Metric, len(pbLabels))

	for _, l := range pbLabels {
		metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
	}

	return metric
}

func metricToPrompbLabels(metric model.Metric) []prompb.Label {
	lbls := make([]prompb.Label, 0, len(metric))

	for name, value := range metric {
		lbls = append(lbls, prompb.Label{
			Name:  string(name),
			Value: string(value),
		})
	}

	// Sort labels because they're expected to be sorted by contract.
	slices.SortFunc(lbls, func(a, b prompb.Label) int {
		cmp := strings.Compare(a.Name, b.Name)
		if cmp != 0 {
			return cmp
		}

		return strings.Compare(a.Value, b.Value)
	})

	return lbls
}

func vectorToPrompbTimeseries(vector model.Vector) []*prompb.TimeSeries {
	res := make([]*prompb.TimeSeries, 0, len(vector))

	for _, sample := range vector {
		res = append(res, &prompb.TimeSeries{
			Labels: metricToPrompbLabels(sample.Metric),
			Samples: []prompb.Sample{
				{
					Value:     float64(sample.Value),
					Timestamp: int64(sample.Timestamp),
				},
			},
		})
	}

	return res
}

// remoteReadQueryByMetricName generates a prompb.Query to query series by metric name within
// the given start / end interval.
func remoteReadQueryByMetricName(metricName string, start, end time.Time) *prompb.Query {
	return &prompb.Query{
		Matchers:         remoteReadQueryMatchersByMetricName(metricName),
		StartTimestampMs: start.UnixMilli(),
		EndTimestampMs:   end.UnixMilli(),
		Hints: &prompb.ReadHints{
			StepMs:  1,
			StartMs: start.UnixMilli(),
			EndMs:   end.UnixMilli(),
		},
	}
}

func remoteReadQueryMatchersByMetricName(metricName string) []*prompb.LabelMatcher {
	return []*prompb.LabelMatcher{{Type: prompb.LabelMatcher_EQ, Name: labels.MetricName, Value: metricName}}
}
