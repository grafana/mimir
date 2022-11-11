// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker
// +build requires_docker

package integration

import (
	"math"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestReadWriteMode(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	minio := e2edb.NewMinio(9000, mimirBucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	flags := mergeFlags(
		CommonStorageBackendFlags(),
		map[string]string{
			"-memberlist.join": "mimir-backend-1",
		},
	)

	readInstance := e2emimir.NewReadInstance("mimir-read-1", flags)
	writeInstance := e2emimir.NewWriteInstance("mimir-write-1", flags)
	backendInstance := e2emimir.NewBackendInstance("mimir-backend-1", flags)
	require.NoError(t, s.StartAndWaitReady(readInstance, writeInstance, backendInstance))

	c, err := e2emimir.NewClient(writeInstance.HTTPEndpoint(), readInstance.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	// Push some data to the cluster.
	now := time.Now()
	series, expectedVector := generateSeries("test_series_1", now, prompb.Label{Name: "foo", Value: "bar"})

	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Verify we can read the data we just pushed, both with an instant query and a range query.
	result, err := c.Query("test_series_1", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	require.Equal(t, expectedVector, result.(model.Vector))

	_, err = c.QueryRange("test_series_1", now.Add(-5*time.Minute), now, 15*time.Second)
	require.NoError(t, err)

	minTime := time.Unix(math.MinInt64/1000+62135596801, 0).UTC()
	maxTime := time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()

	// Verify we can retrieve the labels we just pushed.
	labelValues, err := c.LabelValues("foo", minTime, maxTime, nil)
	require.NoError(t, err)
	require.Equal(t, model.LabelValues{"bar"}, labelValues)

	labelNames, err := c.LabelNames(minTime, maxTime)
	require.NoError(t, err)
	require.Equal(t, []string{"__name__", "foo"}, labelNames)
}
