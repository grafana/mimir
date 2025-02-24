// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"fmt"
	"io"
	"strconv"
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
	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestOTLPIngestion(t *testing.T) {
	t.Run("enabling OTel suffixes", func(t *testing.T) {
		testOTLPIngestion(t, true)
	})

	t.Run("disabling OTel suffixes", func(t *testing.T) {
		testOTLPIngestion(t, false)
	})
}

func testOTLPIngestion(t *testing.T, enableSuffixes bool) {
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
			"-distributor.otel-metric-suffixes-enabled": strconv.FormatBool(enableSuffixes),
		},
	)

	mimir := e2emimir.NewSingleBinary("mimir-1", flags, e2emimir.WithConfigFile(mimirConfigFile), e2emimir.WithPorts(9009, 9095))
	require.NoError(t, s.StartAndWaitReady(mimir))

	c, err := e2emimir.NewClient(mimir.HTTPEndpoint(), mimir.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	sfx := ""
	if enableSuffixes {
		sfx = "_bytes"
	}

	// Push some series to Mimir.
	now := time.Now()
	series, expectedVector, expectedMatrix := generateFloatSeries("series_1", now, prompb.Label{Name: "foo", Value: "bar"})
	// Fix up the expectation wrt. suffix
	for _, s := range expectedVector {
		s.Metric[model.LabelName("__name__")] = model.LabelValue(fmt.Sprintf("series_1%s", sfx))
	}
	metadata := []mimirpb.MetricMetadata{
		{
			Help: "foo",
			Unit: "By",
		},
	}

	res, err := c.PushOTLP(series, metadata)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Check metric to track OTLP requests
	require.NoError(t, mimir.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_distributor_otlp_requests_total"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "user", "user-1"))))

	// Query the series.
	result, err := c.Query(fmt.Sprintf("series_1%s", sfx), now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector, result.(model.Vector))

	labelValues, err := c.LabelValues("foo", v1.MinTime, v1.MaxTime, nil)
	require.NoError(t, err)
	require.Equal(t, model.LabelValues{"bar"}, labelValues)

	labelNames, err := c.LabelNames(v1.MinTime, v1.MaxTime, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"__name__", "foo"}, labelNames)

	rangeResult, err := c.QueryRange(fmt.Sprintf("series_1%s", sfx), now.Add(-15*time.Minute), now, 15*time.Second)
	require.NoError(t, err)
	require.Equal(t, model.ValMatrix, rangeResult.Type())
	require.Equal(t, expectedMatrix, rangeResult.(model.Matrix))

	// Query the metadata
	metadataResult, err := c.GetPrometheusMetadata()
	require.NoError(t, err)
	require.Equal(t, 200, metadataResult.StatusCode)

	metadataResponseBody, err := io.ReadAll(metadataResult.Body)
	require.NoError(t, err)

	expectedJSON := fmt.Sprintf(`
	{
	   "status":"success",
	   "data":{
		  "series_1%s":[
			 {
				"type":"gauge",
				"help":"foo",
				"unit":"By"
			 }
		  ]
	   }
	}
	`, sfx)

	require.JSONEq(t, expectedJSON, string(metadataResponseBody))

	// Push series with histograms to Mimir
	series, expectedVector, _ = generateHistogramSeries("series", now)
	res, err = c.PushOTLP(series, metadata)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Check metric to track OTLP requests
	require.NoError(t, mimir.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_distributor_otlp_requests_total"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "user", "user-1"))))

	result, err = c.Query(fmt.Sprintf("series%s", sfx), now)
	require.NoError(t, err)

	want := expectedVector[0]
	got := result.(model.Vector)[0]
	assert.Equal(t, want.Histogram.Sum, got.Histogram.Sum)
	assert.Equal(t, want.Histogram.Count, got.Histogram.Count)
	// it is not possible to assert with assert.ElementsMatch(t, expectedVector, result.(model.Vector))
	// till https://github.com/open-telemetry/opentelemetry-proto/pull/441 is released. That is only
	// to test setup logic

	expectedJSON = fmt.Sprintf(`
		{
		   "status":"success",
		   "data":{
			  "series%s":[
				 {
					"type":"histogram",
					"help":"foo",
					"unit":"By"
				 }
			  ],
			  "series_1%s":[
				 {
					"type":"gauge",
					"help":"foo",
					"unit":"By"
				 }
			  ]
		   }
		}
	`, sfx, sfx)

	metadataResult, err = c.GetPrometheusMetadata()
	require.NoError(t, err)
	require.Equal(t, 200, metadataResult.StatusCode)

	metadataResponseBody, err = io.ReadAll(metadataResult.Body)
	require.NoError(t, err)
	require.JSONEq(t, expectedJSON, string(metadataResponseBody))
}
