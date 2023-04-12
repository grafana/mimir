// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/ingester_sharding_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestIngesterSharding(t *testing.T) {
	const numSeriesToPush = 1000
	const queryIngestersWithinSecs = 5

	tests := map[string]struct {
		tenantShardSize             int
		expectedIngestersWithSeries int
	}{
		"zero shard size should spread series across all ingesters": {
			tenantShardSize:             0,
			expectedIngestersWithSeries: 3,
		},
		"non-zero shard size should spread series across the configured shard size number of ingesters": {
			tenantShardSize:             2,
			expectedIngestersWithSeries: 2,
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
			flags["-distributor.ingestion-tenant-shard-size"] = strconv.Itoa(testData.tenantShardSize)
			// We're verifying that shuffle sharding on the read path works so we need to set `query-ingesters-within`
			// to a small enough value that they'll have been part of the ring for long enough by the time we attempt
			// to query back the values we wrote to them. If they _haven't_ been part of the ring for long enough, the
			// query would be sent to all ingesters and our test wouldn't really be testing anything.
			flags["-querier.query-ingesters-within"] = fmt.Sprintf("%ds", queryIngestersWithinSecs)
			flags["-ingester.ring.heartbeat-period"] = "1s"

			// Start dependencies.
			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(consul, minio))

			// Start Mimir components.
			distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
			ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), flags)
			ingester2 := e2emimir.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), flags)
			ingester3 := e2emimir.NewIngester("ingester-3", consul.NetworkHTTPEndpoint(), flags)
			ingesters := e2emimir.NewCompositeMimirService(ingester1, ingester2, ingester3)
			querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
			require.NoError(t, s.StartAndWaitReady(distributor, ingester1, ingester2, ingester3, querier))

			// Wait until distributor and queriers have updated the ring.
			require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
				labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

			require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(3), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
				labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

			// Yes, we're sleeping in this test. We need to make sure that the ingesters have been part
			// of the ring long enough before writing metrics to them to ensure that only the shuffle
			// sharded ingesters will be queried for them when we go to verify the series written.
			time.Sleep((queryIngestersWithinSecs + 1) * time.Second)

			// Push series.
			now := time.Now()
			expectedVectors := map[string]model.Vector{}

			client, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", userID)
			require.NoError(t, err)

			for i := 1; i <= numSeriesToPush; i++ {
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

			// Extract metrics from ingesters.
			numIngestersWithSeries := 0
			totalIngestedSeries := 0

			for _, ing := range []*e2emimir.MimirService{ingester1, ingester2, ingester3} {
				values, err := ing.SumMetrics([]string{"cortex_ingester_memory_series"})
				require.NoError(t, err)

				numMemorySeries := e2e.SumValues(values)
				totalIngestedSeries += int(numMemorySeries)
				if numMemorySeries > 0 {
					numIngestersWithSeries++
				}
			}

			// Verify that the expected number of ingesters had series (write path).
			require.Equal(t, testData.expectedIngestersWithSeries, numIngestersWithSeries)
			require.Equal(t, numSeriesToPush, totalIngestedSeries)

			// Query back series.
			for metricName, expectedVector := range expectedVectors {
				result, err := client.Query(metricName, now)
				require.NoError(t, err)
				require.Equal(t, model.ValVector, result.Type())
				assert.Equal(t, expectedVector, result.(model.Vector))
			}

			// We expect that only ingesters belonging to tenant's shard have been queried if
			// shuffle sharding is enabled.
			expectedIngesters := ingesters.NumInstances()
			if testData.tenantShardSize > 0 {
				expectedIngesters = testData.tenantShardSize
			}

			expectedCalls := expectedIngesters * len(expectedVectors)
			require.NoError(t, ingesters.WaitSumMetricsWithOptions(
				e2e.Equals(float64(expectedCalls)),
				[]string{"cortex_request_duration_seconds"},
				e2e.WithMetricCount,
				e2e.SkipMissingMetrics, // Some ingesters may have received no request at all.
				e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "route", "/cortex.Ingester/QueryStream"))))

			// Ensure no service-specific metrics prefix is used by the wrong service.
			assertServiceMetricsPrefixes(t, Distributor, distributor)
			assertServiceMetricsPrefixes(t, Ingester, ingester1)
			assertServiceMetricsPrefixes(t, Ingester, ingester2)
			assertServiceMetricsPrefixes(t, Ingester, ingester3)
			assertServiceMetricsPrefixes(t, Querier, querier)
		})
	}
}
