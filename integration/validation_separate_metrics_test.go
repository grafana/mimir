// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/ingester_limits_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker
// +build requires_docker

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
	tests := map[string]struct {
		labelToSearch string
		configFlagSet bool
		metricExists  bool
	}{
		"No separate metrics label present": {
			labelToSearch: "",
			configFlagSet: false,
			metricExists:  true,
		},
		"Check for correct label": {
			labelToSearch: "test-group",
			configFlagSet: true,
			metricExists:  true,
		},
		"Check for incorrect label": {
			labelToSearch: "incorrect-group",
			configFlagSet: true,
			metricExists:  false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			flags := mergeFlags(
				BlocksStorageFlags(),
				BlocksStorageS3Flags(),
			)

			if testData.configFlagSet {
				flags["-validation.separate-metrics-label"] = "group_1"
			}

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

			// Wait until ingesters have heartbeated the ring after all ingesters were active,
			// in order to update the number of instances. Since we have no metric, we have to
			// rely on a ugly sleep.
			time.Sleep(2 * time.Second)

			now := time.Now()
			client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", userID)
			require.NoError(t, err)

			// Push an invalid metric to increment cortex_discarded_samples_total
			series, _, _ := generateSeries("TestMetric", now, prompb.Label{
				Name:  "Test|Invalid|Label|Char",
				Value: "123",
			}, prompb.Label{
				Name:  "group_1",
				Value: "test-group",
			})

			res, err := client.Push(series)
			require.NoError(t, err)
			require.Equal(t, 400, res.StatusCode)

			metricNumSeries, err := distributor.SumMetrics([]string{"cortex_discarded_samples_total"},
				e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "group", testData.labelToSearch)),
				e2e.WaitMissingMetrics)

			if !testData.metricExists {
				require.ErrorContains(t, err, "metric not found")
				// Check the counter was at least updated, regardless of label
				var metricNumSeriesNoLabel []float64
				metricNumSeriesNoLabel, err = distributor.SumMetrics([]string{"cortex_discarded_samples_total"})
				require.NoError(t, err)
				require.Equal(t, []float64{1}, metricNumSeriesNoLabel)
				return
			}

			require.NoError(t, err)
			require.Equal(t, 1, len(metricNumSeries))
			require.Equal(t, float64(1), metricNumSeries[0])
		})
	}
}

func TestPushMultipleInvalidLabels(t *testing.T) {

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
	)

	flags["-validation.separate-metrics-label"] = "separate_metrics_group"

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

	// Wait until ingesters have heartbeated the ring after all ingesters were active,
	// in order to update the number of instances. Since we have no metric, we have to
	// rely on a ugly sleep.
	time.Sleep(2 * time.Second)

	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", userID)
	require.NoError(t, err)

	// Push invalid series with different groups
	for i := 0; i < 10; i++ {
		series, _, _ := generateSeries("TestMetric", time.Now(), prompb.Label{
			Name:  "separate_metrics_group",
			Value: strconv.Itoa(i % 2),
		}, prompb.Label{
			Name:  "Test|Invalid|Label|Char",
			Value: "123",
		})

		res, err := client.Push(series)
		require.NoError(t, err)
		require.Equal(t, 400, res.StatusCode)
	}

	metricNumSeriesOdd, err := distributor.SumMetrics([]string{"cortex_discarded_samples_total"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "group", "1")),
		e2e.WaitMissingMetrics)

	require.NoError(t, err)
	require.Equal(t, 1, len(metricNumSeriesOdd))
	require.Equal(t, float64(5), metricNumSeriesOdd[0])

	metricNumSeriesEven, err := distributor.SumMetrics([]string{"cortex_discarded_samples_total"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "group", "1")),
		e2e.WaitMissingMetrics)

	require.NoError(t, err)
	require.Equal(t, 1, len(metricNumSeriesEven))
	require.Equal(t, float64(5), metricNumSeriesEven[0])
}

func TestSeparateMetricsInactiveGroups(t *testing.T) {

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
	)

	flags["-validation.separate-metrics-label"] = "separate_metrics_group"
	flags["-max-groups-per-user"] = strconv.Itoa(10)

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

	// Wait until ingesters have heartbeated the ring after all ingesters were active,
	// in order to update the number of instances. Since we have no metric, we have to
	// rely on a ugly sleep.
	time.Sleep(2 * time.Second)

	client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", userID)
	require.NoError(t, err)

	// Push invalid series with different groups
	now := time.Now()
	for i := 0; i < 10; i++ {
		series, _, _ := generateSeries("TestMetric", now, prompb.Label{
			Name:  "separate_metrics_group",
			Value: strconv.Itoa(i),
		}, prompb.Label{
			Name:  "Test|Invalid|Label|Char",
			Value: "123",
		})

		res, err := client.Push(series)
		require.NoError(t, err)
		require.Equal(t, 400, res.StatusCode)
	}

	// Push another series which should be registered as group "other" as active group limit exceeded
	series, _, _ := generateSeries("TestMetric", now, prompb.Label{
		Name:  "separate_metrics_group",
		Value: "group_limit_exceeded",
	}, prompb.Label{
		Name:  "Test|Invalid|Label|Char",
		Value: "123",
	})

	res, err := client.Push(series)
	require.NoError(t, err)
	require.Equal(t, 400, res.StatusCode)

	metricNumSeries, err := distributor.SumMetrics([]string{"cortex_discarded_samples_total"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "group", "other")),
		e2e.WaitMissingMetrics)

	require.NoError(t, err)
	require.Equal(t, 1, len(metricNumSeries))
	require.Equal(t, float64(1), metricNumSeries[0])

}
