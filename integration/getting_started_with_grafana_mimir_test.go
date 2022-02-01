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
	"github.com/prometheus/common/model"
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

	mimir := e2emimir.NewSingleBinaryWithConfigFile("mimir", "demo.yaml", nil, "", 9009, 9095)
	require.NoError(t, s.StartAndWaitReady(mimir))

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

	// Work round Prometheus client lib not having a way to omit the start&end params
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
