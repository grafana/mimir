// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/backward_compatibility_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
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

// previousVersionImages returns a list of previous image version to test backwards compatibility against.
// If MIMIR_PREVIOUS_IMAGES is set to a comma separted list of image versions,
// then those will be used instead of the default versions.
// If MIMIR_PREVIOUS_IMAGES is set to a JSON, it supports mapping flags for those versions,
// see TestParsePreviousImageVersionOverrides for the JSON format to use.
func previousVersionImages(t *testing.T) map[string]e2emimir.FlagMapper {
	if overrides := previousImageVersionOverrides(t); len(overrides) > 0 {
		for key, mapper := range overrides {
			overrides[key] = e2emimir.ChainFlagMappers(mapper, defaultPreviousVersionGlobalOverrides)
		}
		return overrides
	}

	return DefaultPreviousVersionImages
}

func TestBackwardCompatibility(t *testing.T) {
	for previousImage, oldFlagsMapper := range previousVersionImages(t) {
		t.Run(fmt.Sprintf("Backward compatibility upgrading from %s", previousImage), func(t *testing.T) {
			runBackwardCompatibilityTest(t, previousImage, oldFlagsMapper)
		})
	}
}

func TestNewDistributorsCanPushToOldIngestersWithReplication(t *testing.T) {
	for previousImage, oldFlagsMapper := range previousVersionImages(t) {
		t.Run(fmt.Sprintf("Backward compatibility upgrading from %s", previousImage), func(t *testing.T) {
			runNewDistributorsCanPushToOldIngestersWithReplication(t, previousImage, oldFlagsMapper)
		})
	}
}

func runBackwardCompatibilityTest(t *testing.T, previousImage string, oldFlagsMapper e2emimir.FlagMapper) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	const blockRangePeriod = 5 * time.Second

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		map[string]string{
			"-blocks-storage.tsdb.dir":                 e2e.ContainerSharedDir + "/tsdb-shared",
			"-blocks-storage.tsdb.block-ranges-period": blockRangePeriod.String(),
			"-blocks-storage.tsdb.ship-interval":       "1s",
			"-blocks-storage.tsdb.retention-period":    ((blockRangePeriod * 2) - 1).String(),
		},
	)

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start other Mimir components (ingester running on previous version).
	ingester := e2emimir.NewIngester("ingester-old", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithImage(previousImage), e2emimir.WithFlagMapper(oldFlagsMapper))
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	assert.NoError(t, s.StartAndWaitReady(distributor, ingester))

	// Wait until the distributor has updated the ring.
	// The distributor should have 512 tokens for the ingester ring and 1 for the distributor ring
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))

	// Push some series to Mimir.
	series1Timestamp := time.Now()
	series2Timestamp := series1Timestamp.Add(blockRangePeriod * 2)
	series1, expectedVector1, _ := generateFloatSeries("series_1", series1Timestamp, prompb.Label{Name: "label_1", Value: "label_1"})
	series2, expectedVector2, _ := generateFloatSeries("series_2", series2Timestamp, prompb.Label{Name: "label_2", Value: "label_2"})

	c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
	require.NoError(t, err)

	res, err := c.Push(series1)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	res, err = c.Push(series2)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_shipper_uploads_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(2), "cortex_ingester_memory_series_created_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series_removed_total"))

	// Push another series to further compact another block and delete the first block
	// due to expired retention.
	series3Timestamp := series2Timestamp.Add(blockRangePeriod * 2)
	series3, expectedVector3, _ := generateFloatSeries("series_3", series3Timestamp, prompb.Label{Name: "label_3", Value: "label_3"})

	res, err = c.Push(series3)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(2), "cortex_ingester_shipper_uploads_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_memory_series"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(3), "cortex_ingester_memory_series_created_total"))
	require.NoError(t, ingester.WaitSumMetrics(e2e.Equals(2), "cortex_ingester_memory_series_removed_total"))

	// Stop ingester on old version
	require.NoError(t, s.Stop(ingester))

	ingester = e2emimir.NewIngester("ingester-new", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(ingester))

	// Wait until the distributor has updated the ring.
	// The distributor should have 512 tokens for the ingester ring and 1 for the distributor ring
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))

	// Start the compactor to have the bucket index created before querying.
	compactor := e2emimir.NewCompactor("compactor", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(compactor))

	checkQueries(t, consul, previousImage, flags, oldFlagsMapper, s, 1,
		[]instantQueryTest{
			{
				expr:           "series_1",
				time:           series1Timestamp,
				expectedVector: expectedVector1,
			}, {
				expr:           "series_2",
				time:           series2Timestamp,
				expectedVector: expectedVector2,
			}, {
				expr:           "series_3",
				time:           series3Timestamp,
				expectedVector: expectedVector3,
			},
		},
		[]remoteReadRequestTest{
			{
				metricName:         "series_1",
				startTime:          series1Timestamp.Add(-time.Minute),
				endTime:            series1Timestamp.Add(time.Minute),
				expectedTimeseries: vectorToPrompbTimeseries(expectedVector1),
			}, {
				metricName:         "series_2",
				startTime:          series2Timestamp.Add(-time.Minute),
				endTime:            series2Timestamp.Add(time.Minute),
				expectedTimeseries: vectorToPrompbTimeseries(expectedVector2),
			}, {
				metricName:         "series_3",
				startTime:          series3Timestamp.Add(-time.Minute),
				endTime:            series3Timestamp.Add(time.Minute),
				expectedTimeseries: vectorToPrompbTimeseries(expectedVector3),
			},
		},
	)
}

// Check for issues like https://github.com/cortexproject/cortex/issues/2356
func runNewDistributorsCanPushToOldIngestersWithReplication(t *testing.T, previousImage string, oldFlagsMapper e2emimir.FlagMapper) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		map[string]string{
			"-ingester.ring.replication-factor": "3",
		},
	)

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// Start other Mimir components (ingester running on previous version).
	ingester1 := e2emimir.NewIngester("ingester-1", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithImage(previousImage), e2emimir.WithFlagMapper(oldFlagsMapper))
	ingester2 := e2emimir.NewIngester("ingester-2", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithImage(previousImage), e2emimir.WithFlagMapper(oldFlagsMapper))
	ingester3 := e2emimir.NewIngester("ingester-3", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithImage(previousImage), e2emimir.WithFlagMapper(oldFlagsMapper))
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
	require.NoError(t, s.StartAndWaitReady(distributor, ingester1, ingester2, ingester3))

	// Wait until the distributor has updated the ring.
	// The distributor should have 512 tokens for each ingester and 1 for the distributor ring
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(3*512+1), "cortex_ring_tokens_total"))

	// Push some series to Mimir.
	now := time.Now()
	series, expectedVector, _ := generateFloatSeries("series_1", now)

	c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", "user-1")
	require.NoError(t, err)

	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	checkQueries(t, consul, previousImage, flags, oldFlagsMapper, s, 3, []instantQueryTest{{
		time:           now,
		expr:           "series_1",
		expectedVector: expectedVector,
	}}, nil)
}

func checkQueries(
	t *testing.T,
	consul *e2e.HTTPService,
	previousImage string,
	flags map[string]string,
	oldFlagsMapper e2emimir.FlagMapper,
	s *e2e.Scenario,
	numIngesters int,
	instantQueries []instantQueryTest,
	remoteReadRequests []remoteReadRequestTest,
) {
	cases := map[string]struct {
		queryFrontendOptions []e2emimir.Option
		querierOptions       []e2emimir.Option
		storeGatewayOptions  []e2emimir.Option
	}{
		"old query-frontend, new querier and store-gateway": {
			queryFrontendOptions: []e2emimir.Option{
				e2emimir.WithImage(previousImage),
				e2emimir.WithFlagMapper(oldFlagsMapper),
			},
		},
		"new query-frontend and store-gateway, old querier": {
			querierOptions: []e2emimir.Option{
				e2emimir.WithImage(previousImage),
				e2emimir.WithFlagMapper(oldFlagsMapper),
			},
		},
		"new query-frontend and querier, old store-gateway": {
			storeGatewayOptions: []e2emimir.Option{
				e2emimir.WithImage(previousImage),
				e2emimir.WithFlagMapper(oldFlagsMapper),
			},
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			// Start query-frontend.
			queryFrontend := e2emimir.NewQueryFrontend("query-frontend", consul.NetworkHTTPEndpoint(), flags, c.queryFrontendOptions...)
			require.NoError(t, s.Start(queryFrontend))
			defer func() {
				require.NoError(t, s.Stop(queryFrontend))
			}()

			// Start querier and store-gateway.
			querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), e2e.MergeFlagsWithoutRemovingEmpty(flags, map[string]string{
				"-querier.frontend-address": queryFrontend.NetworkGRPCEndpoint(),
			}), c.querierOptions...)
			storeGateway := e2emimir.NewStoreGateway("store-gateway", consul.NetworkHTTPEndpoint(), flags, c.storeGatewayOptions...)

			require.NoError(t, s.Start(querier, storeGateway))
			defer func() {
				require.NoError(t, s.Stop(querier, storeGateway))
			}()

			// Wait until querier, query-frontend and store-gateway are ready, and the querier has updated the ring.
			require.NoError(t, s.WaitReady(querier, queryFrontend, storeGateway))
			require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(float64(numIngesters)), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
				labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))
			require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", "store-gateway-client"),
				labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

			// Query the series.
			for _, endpoint := range []string{queryFrontend.HTTPEndpoint(), querier.HTTPEndpoint()} {
				c, err := e2emimir.NewClient("", endpoint, "", "", "user-1")
				require.NoError(t, err)

				for _, query := range instantQueries {
					t.Run(fmt.Sprintf("%s: instant query: %s", endpoint, query.expr), func(t *testing.T) {
						result, err := c.Query(query.expr, query.time)
						require.NoError(t, err)
						require.Equal(t, model.ValVector, result.Type())
						require.Equal(t, query.expectedVector, result.(model.Vector))
					})
				}

				for _, req := range remoteReadRequests {
					t.Run(fmt.Sprintf("%s: remote read: %s", endpoint, req.metricName), func(t *testing.T) {
						httpRes, result, _, err := c.RemoteRead(remoteReadQueryByMetricName(req.metricName, req.startTime, req.endTime))
						require.NoError(t, err)
						require.Equal(t, http.StatusOK, httpRes.StatusCode)
						require.NotNil(t, result)
						require.Equal(t, req.expectedTimeseries, result.Timeseries)
					})
				}
			}
		})
	}
}

type instantQueryTest struct {
	expr           string
	time           time.Time
	expectedVector model.Vector
}

type remoteReadRequestTest struct {
	metricName         string
	startTime          time.Time
	endTime            time.Time
	expectedTimeseries []*prompb.TimeSeries
}

type testingLogger interface{ Logf(string, ...interface{}) }

func previousImageVersionOverrides(t *testing.T) map[string]e2emimir.FlagMapper {
	overrides, err := parsePreviousImageVersionOverrides(os.Getenv("MIMIR_PREVIOUS_IMAGES"), t)
	require.NoError(t, err)
	return overrides
}

func parsePreviousImageVersionOverrides(env string, logger testingLogger) (map[string]e2emimir.FlagMapper, error) {
	if env == "" {
		return nil, nil
	}

	overrides := map[string]e2emimir.FlagMapper{}
	if strings.TrimSpace(env)[0] != '{' {
		logger.Logf("Overriding previous images with comma separated image names: %s", env)
		for _, image := range strings.Split(env, ",") {
			overrides[image] = e2emimir.NoopFlagMapper
		}
		return overrides, nil
	}
	logger.Logf("Overriding previous images with JSON: %s", env)

	if err := json.Unmarshal([]byte(env), &overrides); err != nil {
		return nil, fmt.Errorf("can't unmarshal previous image version overrides as JSON: %w", err)
	}
	return overrides, nil
}

func TestParsePreviousImageVersionOverrides(t *testing.T) {
	t.Run("empty overrides", func(t *testing.T) {
		overrides, err := parsePreviousImageVersionOverrides("", t)
		require.NoError(t, err)
		require.Empty(t, overrides)
	})

	t.Run("one version override", func(t *testing.T) {
		overrides, err := parsePreviousImageVersionOverrides("first", t)
		require.NoError(t, err)
		require.Len(t, overrides, 1)
		require.NotNil(t, overrides["first"])
	})

	t.Run("comma separated overrides", func(t *testing.T) {
		overrides, err := parsePreviousImageVersionOverrides("first,second", t)
		require.NoError(t, err)
		require.Len(t, overrides, 2)
		require.NotNil(t, overrides["first"])
		require.NotNil(t, overrides["second"])
	})

	t.Run("json overrides with flag mappers", func(t *testing.T) {
		inputFlags := map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
			"d": "4",
			"e": "5",
		}
		expectedFlags := map[string]string{
			"x": "1",
			"y": "2",
			"e": "5",
			"z": "10",
		}

		jsonOverrides := `
			{
				"first": [
					{"remove": ["c", "d"]},
					{"rename": {"a": "x", "b": "y"}},
					{"set": {"z":"10"}}
				],
				"second": []
			}`

		overrides, err := parsePreviousImageVersionOverrides(jsonOverrides, t)
		require.NoError(t, err)
		require.Len(t, overrides, 2)

		require.NotNil(t, overrides["first"])
		require.Equal(t, expectedFlags, overrides["first"](inputFlags))

		require.NotNil(t, overrides["second"])
		require.Equal(t, inputFlags, overrides["second"](inputFlags))
	})
}
