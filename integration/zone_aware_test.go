// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/zone_aware_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestZoneAwareReplication(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags())
	flags["-ingester.ring.replication-factor"] = "3"
	flags["-ingester.ring.zone-awareness-enabled"] = "true"

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Mimir components.
	ingesterFlags := func(zone string) map[string]string {
		return mergeFlags(flags, map[string]string{
			"-ingester.ring.instance-availability-zone": zone,
		})
	}

	ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-a"))
	ingester2 := e2emimir.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-a"))
	ingester3 := e2emimir.NewIngester("ingester-3", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-b"))
	ingester4 := e2emimir.NewIngester("ingester-4", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-b"))
	ingester5 := e2emimir.NewIngester("ingester-5", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-c"))
	ingester6 := e2emimir.NewIngester("ingester-6", consul.NetworkHTTPEndpoint(), ingesterFlags("zone-c"))
	require.NoError(t, s.StartAndWaitReady(ingester1, ingester2, ingester3, ingester4, ingester5, ingester6))

	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, querier))

	// Wait until distributor and querier have updated the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(6), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(6), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)

	// Push some series
	now := time.Now()
	numSeries := 100
	expectedVectors := map[string]model.Vector{}

	for i := 1; i <= numSeries; i++ {
		metricName := fmt.Sprintf("series_%d", i)
		var genSeries generateSeriesFunc
		if i%2 == 0 {
			genSeries = generateFloatSeries
		} else {
			genSeries = generateHistogramSeries
		}
		series, expectedVector, _ := genSeries(metricName, now)
		res, err := client.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)

		expectedVectors[metricName] = expectedVector
	}

	// Query back series => all good
	for metricName, expectedVector := range expectedVectors {
		result, err := client.Query(metricName, now)
		require.NoError(t, err)
		require.Equal(t, model.ValVector, result.Type())
		assert.Equal(t, expectedVector, result.(model.Vector))
	}

	// SIGKILL 1 ingester in 1st zone
	require.NoError(t, ingester1.Kill())

	// Push 1 more series => all good
	numSeries++
	metricName := fmt.Sprintf("series_%d", numSeries)
	series, expectedVector, _ := generateFloatSeries(metricName, now)
	res, err := client.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	expectedVectors[metricName] = expectedVector

	// Query back series => all good
	for metricName, expectedVector := range expectedVectors {
		result, err := client.Query(metricName, now)
		require.NoError(t, err)
		require.Equal(t, model.ValVector, result.Type())
		assert.Equal(t, expectedVector, result.(model.Vector))
	}

	// SIGKILL 1 more ingester in the 1st zone (all ingesters in 1st zone have been killed)
	require.NoError(t, ingester2.Kill())

	// Push 1 more series => all good
	numSeries++
	metricName = fmt.Sprintf("series_%d", numSeries)
	series, expectedVector, _ = generateHistogramSeries(metricName, now)
	res, err = client.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	expectedVectors[metricName] = expectedVector

	// Query back series => all good
	for metricName, expectedVector := range expectedVectors {
		result, err := client.Query(metricName, now)
		require.NoError(t, err)
		require.Equal(t, model.ValVector, result.Type())
		assert.Equal(t, expectedVector, result.(model.Vector))
	}

	// SIGKILL 1 ingester in the 2nd zone
	require.NoError(t, ingester3.Kill())

	// Query back any series => fail (either because of a timeout or 500)
	result, _, err := client.QueryRaw("series_1")
	if !errIsTimeout(err) {
		require.NoError(t, err)
		require.Equal(t, 500, result.StatusCode)
	}

	// SIGKILL 1 more ingester in the 2nd zone (all ingesters in 2nd zone have been killed)
	require.NoError(t, ingester4.Kill())

	// Push 1 more series => fail
	series, _, _ = generateFloatSeries("series_last", now)
	res, err = client.Push(series)
	require.NoError(t, err)
	require.Equal(t, 500, res.StatusCode)

}

func errIsTimeout(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	if strings.Contains(err.Error(), "Client.Timeout") {
		return true
	}

	return false
}
