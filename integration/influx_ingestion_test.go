// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"math"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestInfluxIngestion(t *testing.T) {
	t.Helper()

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, blocksBucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	// Start Mimir components.
	require.NoError(t, copyFileToSharedDir(s, "docs/configurations/single-process-config-blocks.yaml", mimirConfigFile))

	// Start Mimir in single binary mode, reading the config from file and overwriting
	// the backend config to make it work with Minio.
	flags := mergeFlags(
		DefaultSingleBinaryFlags(),
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		map[string]string{
			"-distributor.influx-endpoint-enabled": "true",
		},
	)

	mimir := e2emimir.NewSingleBinary("mimir-1", flags, e2emimir.WithConfigFile(mimirConfigFile), e2emimir.WithPorts(9009, 9095))
	require.NoError(t, s.StartAndWaitReady(mimir))

	c, err := e2emimir.NewClient(mimir.HTTPEndpoint(), mimir.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	// Push some series to Mimir.
	now := time.Now()

	series, expectedVector, expectedMatrix := generateFloatSeries("series_f1", now, prompb.Label{Name: "foo", Value: "bar"})
	// Fix up the expectation as Influx values seem to be rounded to millionths
	for _, s := range expectedVector {
		s.Metric[model.LabelName("__mimir_source__")] = model.LabelValue("influx")
		s.Value = model.SampleValue(math.Round(float64(s.Value)*1000000) / 1000000.0)
	}
	// Fix up the expectation as Influx values seem to be rounded to millionths
	for im, s := range expectedMatrix {
		for iv, v := range s.Values {
			expectedMatrix[im].Values[iv].Value = model.SampleValue(math.Round(float64(v.Value)*1000000) / 1000000.0)
		}
	}

	res, err := c.PushInflux(series)
	require.NoError(t, err)
	require.Equal(t, 204, res.StatusCode)

	// Check metric to track Influx requests
	require.NoError(t, mimir.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_distributor_influx_requests_total"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "user", "user-1"))))

	// Query the series.
	result, err := c.Query("series_f1", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector, result.(model.Vector))

	labelValues, err := c.LabelValues("foo", v1.MinTime, v1.MaxTime, nil)
	require.NoError(t, err)
	require.Equal(t, model.LabelValues{"bar"}, labelValues)

	labelNames, err := c.LabelNames(v1.MinTime, v1.MaxTime, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"__mimir_source__", "__name__", "foo"}, labelNames)

	rangeResult, err := c.QueryRange("series_f1", now.Add(-15*time.Minute), now, 15*time.Second)
	require.NoError(t, err)
	require.Equal(t, model.ValMatrix, rangeResult.Type())
	require.Equal(t, expectedMatrix, rangeResult.(model.Matrix))

	// No metadata to query, but we do the query anyway.
	_, err = c.GetPrometheusMetadata()
	require.NoError(t, err)
}
