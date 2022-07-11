// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker
// +build requires_docker

package integration

import (
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
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

	runTestPushSeriesAndQueryBack(t, mimir)
}

func TestPlayWithGrafanaMimirTutorial(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	require.NoError(t, copyFileToSharedDir(s, "docs/sources/tutorials/play-with-grafana-mimir/config/mimir.yaml", "mimir.yaml"))

	// Start dependencies.
	minio := e2edb.NewMinio(9000, mimirBucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	flags := mergeFlags(
		// Override the storage config.
		CommonStorageBackendFlags(),
		// Override the list of members to join, setting the hostname we expect within the Docker network created by integration tests.
		map[string]string{"-memberlist.join": networkName + "-mimir-1"},
	)

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

	runTestPushSeriesAndQueryBack(t, mimir1)
}

func runTestPushSeriesAndQueryBack(t *testing.T, mimir *e2emimir.MimirService) {
	c, err := e2emimir.NewClient(mimir.HTTPEndpoint(), mimir.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	// Push some series to Mimir.
	now := time.Now()
	series, expectedVector := generateSeries("series_1", now, prompb.Label{Name: "foo", Value: "bar"})

	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	// Query the series.
	result, err := c.Query("series_1", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector, result.(model.Vector))

	// Work around the Prometheus client lib not having a way to omit the start and end params.
	minTime := time.Unix(math.MinInt64/1000+62135596801, 0).UTC()
	maxTime := time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()

	labelValues, err := c.LabelValues("foo", minTime, maxTime, nil)
	require.NoError(t, err)
	require.Equal(t, model.LabelValues{"bar"}, labelValues)

	labelNames, err := c.LabelNames(minTime, maxTime)
	require.NoError(t, err)
	require.Equal(t, []string{"__name__", "foo"}, labelNames)

	// Check that a range query does not return an error to sanity check the queryrange tripperware.
	_, err = c.QueryRange("series_1", now.Add(-15*time.Minute), now, 15*time.Second)
	require.NoError(t, err)
}
