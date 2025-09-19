// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"fmt"
	"net/http"
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

func TestGettingStartedWithGrafanaMimir(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, copyFileToSharedDir(s, "docs/configurations/demo.yaml", "demo.yaml"))

	mimir := e2emimir.NewSingleBinary("mimir", nil, e2emimir.WithPorts(9009, 9095), e2emimir.WithConfigFile("demo.yaml"))
	require.NoError(t, s.StartAndWaitReady(mimir))

	runTestPushSeriesAndQueryBack(t, mimir, "series_1", generateFloatSeries)
	runTestPushSeriesAndQueryBack(t, mimir, "hseries_1", generateHistogramSeries)
}

func TestPlayWithGrafanaMimirTutorial(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, copyFileToSharedDir(s, "docs/sources/mimir/get-started/play-with-grafana-mimir/config/mimir.yaml", "mimir.yaml"))

	// Start dependencies.
	minio := e2edb.NewMinio(9000, mimirBucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	flags := map[string]string{
		// Override storage config as Minio setup is different in integration tests.
		"-common.storage.s3.endpoint":          fmt.Sprintf("%s-minio-9000:9000", networkName),
		"-common.storage.s3.access-key-id":     e2edb.MinioAccessKey,
		"-common.storage.s3.secret-access-key": e2edb.MinioSecretKey,
		"-common.storage.s3.insecure":          "true",
		"-common.storage.s3.bucket-name":       mimirBucketName,

		// Override the list of members to join, setting the hostname we expect within the Docker network created by integration tests.
		"-memberlist.join": networkName + "-mimir-1",
	}

	// Start Mimir (3 replicas).
	mimir1 := e2emimir.NewSingleBinary("mimir-1", flags, e2emimir.WithConfigFile("mimir.yaml"))
	mimir2 := e2emimir.NewSingleBinary("mimir-2", flags, e2emimir.WithConfigFile("mimir.yaml"))
	mimir3 := e2emimir.NewSingleBinary("mimir-3", flags, e2emimir.WithConfigFile("mimir.yaml"))
	require.NoError(t, s.StartAndWaitReady(mimir1, mimir2, mimir3))

	// We need that all Mimir instances see each other in the ingesters ring.
	for _, instance := range []*e2emimir.MimirService{mimir1, mimir2, mimir3} {
		require.NoError(t, instance.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
			labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
			labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))
	}

	runTestPushSeriesAndQueryBack(t, mimir1, "series_1", generateFloatSeries)
	runTestPushSeriesAndQueryBack(t, mimir2, "hseries_1", generateHistogramSeries)
}

func runTestPushSeriesAndQueryBack(t *testing.T, mimir *e2emimir.MimirService, seriesName string, genSeries generateSeriesFunc) {
	c, err := e2emimir.NewClient(mimir.HTTPEndpoint(), mimir.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	// Push some series to Mimir.
	now := time.Now()
	series, expectedVector, expectedMatrix := genSeries(seriesName, now, prompb.Label{Name: "foo", Value: "bar"})

	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	// Query the series.
	result, err := c.Query(seriesName, now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector, result.(model.Vector))

	labelValues, err := c.LabelValues("foo", v1.MinTime, v1.MaxTime, nil)
	require.NoError(t, err)
	require.Equal(t, model.LabelValues{"bar"}, labelValues)

	labelNames, err := c.LabelNames(v1.MinTime, v1.MaxTime, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"__name__", "foo"}, labelNames)

	rangeResult, err := c.QueryRange(seriesName, now.Add(-15*time.Minute), now, 15*time.Second)
	require.NoError(t, err)
	require.Equal(t, model.ValMatrix, rangeResult.Type())
	require.Equal(t, expectedMatrix, rangeResult.(model.Matrix))
}
