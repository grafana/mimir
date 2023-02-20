// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/ingester_limits_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"strconv"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestValidateSeparateMetrics(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
	)

	flags["-validation.separate-metrics-group-label"] = "group_1"

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Mimir components.
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), flags)
	ingester2 := e2emimir.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), flags)
	ingester3 := e2emimir.NewIngester("ingester-3", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester1, ingester2, ingester3))

	// Wait until distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	now := time.Now()
	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", userID)
	require.NoError(t, err)

	// Push invalid metrics to increment cortex_discarded_samples_total
	series, _, _ := generateFloatSeries("TestMetric", now, prompb.Label{
		Name:  "Test|Invalid|Label|Char",
		Value: "123",
	}, prompb.Label{
		Name:  "group_1",
		Value: "test-group",
	})
	pushTimeSeriesWithStatus(t, client, series, 400)

	series, _, _ = generateHistogramSeries("TestMetric", now, prompb.Label{
		Name:  "Test|Invalid|Label|Char",
		Value: "123",
	}, prompb.Label{
		Name:  "group_1",
		Value: "test-group",
	})
	pushTimeSeriesWithStatus(t, client, series, 400)

	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_discarded_samples_total"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "group", "test-group")),
	))

	// Push series with no group label
	series, _, _ = generateFloatSeries("TestMetric", now, prompb.Label{
		Name:  "Test|Invalid|Label|Char",
		Value: "123",
	})
	pushTimeSeriesWithStatus(t, client, series, 400)

	series, _, _ = generateHistogramSeries("TestMetric", now, prompb.Label{
		Name:  "Test|Invalid|Label|Char",
		Value: "123",
	})
	pushTimeSeriesWithStatus(t, client, series, 400)

	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_discarded_samples_total"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "group", "")),
	))

	// Ensure previous series didn't disappear
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_discarded_samples_total"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "group", "test-group")),
	))

	// Push two series, group label only present in second series
	series1, _, _ := generateFloatSeries("TestMetric", now, prompb.Label{
		Name:  "Test|Invalid|Label|Char",
		Value: "123",
	})
	pushTimeSeriesWithStatus(t, client, series1, 400)

	series2, _, _ := generateHistogramSeries("TestMetric", now, prompb.Label{
		Name:  "Test|Invalid|Label|Char",
		Value: "123",
	}, prompb.Label{
		Name:  "group_1",
		Value: "second-series",
	})
	pushTimeSeriesWithStatus(t, client, series2, 400)

	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_discarded_samples_total"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "group", "")),
	))

	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_discarded_samples_total"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "group", "second-series")),
	))
}

func TestPushMultipleInvalidLabels(t *testing.T) {

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
	)

	flags["-validation.separate-metrics-group-label"] = "separate_metrics_group"

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Mimir components.
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), flags)
	ingester2 := e2emimir.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), flags)
	ingester3 := e2emimir.NewIngester("ingester-3", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester1, ingester2, ingester3))

	// Wait until distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", userID)
	require.NoError(t, err)

	// Push invalid series with different groups
	for i := 0; i < 9; i++ {
		var genSeries generateSeriesFunc
		if i%2 == 0 {
			genSeries = generateFloatSeries
		} else {
			genSeries = generateHistogramSeries
		}
		series, _, _ := genSeries("TestMetric", time.Now(), prompb.Label{
			Name:  "separate_metrics_group",
			Value: strconv.Itoa(i % 2),
		}, prompb.Label{
			Name:  "Test|Invalid|Label|Char",
			Value: "123",
		})

		pushTimeSeriesWithStatus(t, client, series, 400)
	}

	// Odd group values
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(4), []string{"cortex_discarded_samples_total"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "group", "1")),
	))

	// Even group values
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(5), []string{"cortex_discarded_samples_total"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "group", "0")),
	))
}

func TestSeparateMetricsGroupLimitExceeded(t *testing.T) {

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
	)

	flags["-validation.separate-metrics-group-label"] = "separate_metrics_group"
	flags["-max-separate-metrics-groups-per-user"] = strconv.Itoa(10)

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start Mimir components.
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), flags)
	ingester2 := e2emimir.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), flags)
	ingester3 := e2emimir.NewIngester("ingester-3", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester1, ingester2, ingester3))

	// Wait until distributor has updated the ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", userID)
	require.NoError(t, err)

	// Push invalid series with different groups
	now := time.Now()
	for i := 0; i < 10; i++ {
		var genSeries generateSeriesFunc
		if i%2 == 0 {
			genSeries = generateFloatSeries
		} else {
			genSeries = generateHistogramSeries
		}
		series, _, _ := genSeries("TestMetric", now, prompb.Label{
			Name:  "separate_metrics_group",
			Value: strconv.Itoa(i),
		}, prompb.Label{
			Name:  "Test|Invalid|Label|Char",
			Value: "123",
		})

		pushTimeSeriesWithStatus(t, client, series, 400)
	}

	// Push another series which should be registered as group "other" as active group limit exceeded
	series, _, _ := generateFloatSeries("TestMetric", now, prompb.Label{
		Name:  "separate_metrics_group",
		Value: "group_limit_exceeded",
	}, prompb.Label{
		Name:  "Test|Invalid|Label|Char",
		Value: "123",
	})

	pushTimeSeriesWithStatus(t, client, series, 400)

	for i := 0; i < 10; i++ {
		require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_discarded_samples_total"},
			e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "group", strconv.Itoa(i))),
		))
	}

	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_discarded_samples_total"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "group", "other")),
	))
}

func pushTimeSeriesWithStatus(t *testing.T, client *e2emimir.Client, timeSeries []prompb.TimeSeries, expectedStatusCode int) {
	res, err := client.Push(timeSeries)
	require.NoError(t, err)
	require.Equal(t, expectedStatusCode, res.StatusCode)
}
