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
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/pkg/errors"

	"github.com/grafana/e2e"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

var (
	// Expose some utilities from the framework so that we don't have to prefix them
	// with the package name in tests.
	mergeFlags              = e2e.MergeFlags
	generateSeries          = e2e.GenerateSeries // to be deprecated and replaced with either only generateFloatSeries or both that and generateHistogramSeries, depending on the nature of the test
	generateFloatSeries     = e2e.GenerateSeries
	generateNFloatSeries    = e2e.GenerateNSeries
	generateHistogramSeries = e2e.GenerateHistogramSeries
	// generateNHistogramSeries = e2e.GenerateNHistogramSeries

	// These are the earliest and latest possible timestamps supported by the Prometheus API -
	// the Prometheus API does not support omitting a time range from query requests,
	// so we use these when we want to query over all time.
	// These values are defined in github.com/prometheus/prometheus/web/api/v1/api.go but
	// sadly not exported.
	prometheusMinTime = time.Unix(math.MinInt64/1000+62135596801, 0).UTC()
	prometheusMaxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()
)

// generateSeriesFunc defines what kind of series (and expected vectors/matrices) to generate - float samples or native histograms
type generateSeriesFunc func(name string, ts time.Time, additionalLabels ...prompb.Label) (series []prompb.TimeSeries, vector model.Vector, matrix model.Matrix)

// generateNSeriesFunc defines what kind of n * series (and expected vectors) to generate - float samples or native histograms
// type generateNSeriesFunc func(nSeries, nExemplars int, name func() string, ts time.Time, additionalLabels func() []prompb.Label) (series []prompb.TimeSeries, vector model.Vector)

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
