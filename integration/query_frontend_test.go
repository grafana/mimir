// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/query_frontend_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/test"
	"github.com/grafana/e2e"
	e2ecache "github.com/grafana/e2e/cache"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/ca"
	"github.com/grafana/mimir/integration/e2emimir"
	mimirquerier "github.com/grafana/mimir/pkg/querier"
)

type queryFrontendTestConfig struct {
	querySchedulerEnabled       bool
	querySchedulerDiscoveryMode string
	queryStatsEnabled           bool
	setup                       func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string)
	withHistograms              bool
	shardActiveSeriesQueries    bool
}

func TestQueryFrontendWithBlocksStorageViaFlags(t *testing.T) {
	runQueryFrontendTest(t, queryFrontendTestConfig{
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			flags = mergeFlags(
				BlocksStorageFlags(),
				BlocksStorageS3Flags(),
			)

			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))

			return "", flags
		},
		withHistograms: true,
	})
}

func TestQueryFrontendWithBlocksStorageViaCommonFlags(t *testing.T) {
	runQueryFrontendTest(t, queryFrontendTestConfig{
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			flags = mergeFlags(
				CommonStorageBackendFlags(),
				BlocksStorageFlags(),
			)

			minio := e2edb.NewMinio(9000, mimirBucketName)
			require.NoError(t, s.StartAndWaitReady(minio))

			return "", flags
		},
		withHistograms: true,
	})
}

func TestQueryFrontendWithBlocksStorageViaFlagsAndQueryStatsEnabled(t *testing.T) {
	runQueryFrontendTest(t, queryFrontendTestConfig{
		queryStatsEnabled: true,
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			flags = mergeFlags(
				BlocksStorageFlags(),
				BlocksStorageS3Flags(),
			)

			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))

			return "", flags
		},
		withHistograms: true,
	})
}

func TestQueryFrontendWithBlocksStorageViaFlagsAndWithQueryScheduler(t *testing.T) {
	setup := func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
		flags = mergeFlags(
			BlocksStorageFlags(),
			BlocksStorageS3Flags(),
		)

		minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
		require.NoError(t, s.StartAndWaitReady(minio))

		return "", flags
	}

	t.Run("with query-scheduler (DNS-based service discovery)", func(t *testing.T) {
		runQueryFrontendTest(t, queryFrontendTestConfig{
			querySchedulerEnabled:       true,
			querySchedulerDiscoveryMode: "dns",
			setup:                       setup,
			withHistograms:              true,
		})
	})

	t.Run("with query-scheduler (ring-based service discovery)", func(t *testing.T) {
		runQueryFrontendTest(t, queryFrontendTestConfig{
			querySchedulerEnabled:       true,
			querySchedulerDiscoveryMode: "ring",
			setup:                       setup,
			withHistograms:              true,
		})
	})
}

func TestQueryFrontendWithBlocksStorageViaFlagsAndWithQuerySchedulerAndQueryStatsEnabled(t *testing.T) {
	runQueryFrontendTest(t, queryFrontendTestConfig{
		querySchedulerEnabled:       true,
		querySchedulerDiscoveryMode: "dns",
		queryStatsEnabled:           true,
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			flags = mergeFlags(
				BlocksStorageFlags(),
				BlocksStorageS3Flags(),
			)

			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))

			return "", flags
		},
		withHistograms: true,
	})
}

func TestQueryFrontendWithBlocksStorageViaConfigFile(t *testing.T) {
	runQueryFrontendTest(t, queryFrontendTestConfig{
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			require.NoError(t, writeFileToSharedDir(s, mimirConfigFile, []byte(BlocksStorageConfig)))

			minio := e2edb.NewMinio(9000, blocksBucketName)
			require.NoError(t, s.StartAndWaitReady(minio))

			return mimirConfigFile, e2e.EmptyFlags()
		},
		withHistograms: true,
	})
}

func TestQueryFrontendTLSWithBlocksStorageViaFlags(t *testing.T) {
	runQueryFrontendTest(t, queryFrontendTestConfig{
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			flags = mergeFlags(
				BlocksStorageFlags(),
				BlocksStorageS3Flags(),
				getServerTLSFlags(),
				getClientTLSFlagsWithPrefix("ingester.client"),
				getClientTLSFlagsWithPrefix("querier.frontend-client"),
			)

			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))

			// set the ca
			cert := ca.New("Mimir Test")

			// Ensure the entire path of directories exist.
			require.NoError(t, os.MkdirAll(filepath.Join(s.SharedDir(), "certs"), os.ModePerm))

			require.NoError(t, cert.WriteCACertificate(filepath.Join(s.SharedDir(), caCertFile)))

			// server certificate
			require.NoError(t, cert.WriteCertificate(
				&x509.Certificate{
					Subject:     pkix.Name{CommonName: "client"},
					ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
				},
				filepath.Join(s.SharedDir(), clientCertFile),
				filepath.Join(s.SharedDir(), clientKeyFile),
			))
			require.NoError(t, cert.WriteCertificate(
				&x509.Certificate{
					Subject:     pkix.Name{CommonName: "server"},
					DNSNames:    []string{"querier.frontend-client", "ingester.client"},
					ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
				},
				filepath.Join(s.SharedDir(), serverCertFile),
				filepath.Join(s.SharedDir(), serverKeyFile),
			))

			return "", flags
		},
		withHistograms: true,
	})
}

func TestQueryFrontendWithQueryResultPayloadFormats(t *testing.T) {
	formats := []string{"json", "protobuf"}

	for _, format := range formats {
		t.Run(format, func(t *testing.T) {
			runQueryFrontendTest(t, queryFrontendTestConfig{
				setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
					flags = mergeFlags(
						BlocksStorageFlags(),
						BlocksStorageS3Flags(),
					)

					minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
					require.NoError(t, s.StartAndWaitReady(minio))

					return "", flags
				},
				withHistograms: format == "protobuf",
			})
		})
	}
}

func TestQueryFrontendWithIngestStorageViaFlagsAndWithQuerySchedulerAndQueryStatsEnabled(t *testing.T) {
	runQueryFrontendTest(t, queryFrontendTestConfig{
		querySchedulerEnabled:       true,
		querySchedulerDiscoveryMode: "dns",
		queryStatsEnabled:           true,
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			flags = mergeFlags(
				BlocksStorageFlags(),
				BlocksStorageS3Flags(),
				IngestStorageFlags(),
			)

			kafka := e2edb.NewKafka()
			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio, kafka))

			return "", flags
		},
		withHistograms: true,
	})
}

func runQueryFrontendTest(t *testing.T, cfg queryFrontendTestConfig) {
	const numUsers = 10
	const numQueriesPerUser = 10

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	memcached := e2ecache.NewMemcached()
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul, memcached))

	configFile, flags := cfg.setup(t, s)

	flags = mergeFlags(flags, map[string]string{
		"-query-frontend.cache-results":                     "true",
		"-query-frontend.results-cache.backend":             "memcached",
		"-query-frontend.results-cache.memcached.addresses": "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
		"-query-frontend.query-stats-enabled":               strconv.FormatBool(cfg.queryStatsEnabled),
	})

	// Start the query-scheduler if enabled.
	var queryScheduler *e2emimir.MimirService
	if cfg.querySchedulerEnabled && cfg.querySchedulerDiscoveryMode == "dns" {
		queryScheduler = e2emimir.NewQueryScheduler("query-scheduler", flags)
		require.NoError(t, s.StartAndWaitReady(queryScheduler))
		flags["-query-frontend.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
		flags["-querier.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
	} else if cfg.querySchedulerEnabled && cfg.querySchedulerDiscoveryMode == "ring" {
		flags["-query-scheduler.service-discovery-mode"] = "ring"
		flags["-query-scheduler.ring.store"] = "consul"
		flags["-query-scheduler.ring.consul.hostname"] = consul.NetworkHTTPEndpoint()

		queryScheduler = e2emimir.NewQueryScheduler("query-scheduler", flags)
		require.NoError(t, s.StartAndWaitReady(queryScheduler))
	}

	// Start the query-frontend.
	queryFrontend := e2emimir.NewQueryFrontend("query-frontend", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithConfigFile(configFile))
	require.NoError(t, s.Start(queryFrontend))

	if !cfg.querySchedulerEnabled {
		flags["-querier.frontend-address"] = queryFrontend.NetworkGRPCEndpoint()
	}

	// Start all other services.
	ingester := e2emimir.NewIngester("ingester-0", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithConfigFile(configFile))
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithConfigFile(configFile))
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithConfigFile(configFile))

	require.NoError(t, s.StartAndWaitReady(querier, ingester, distributor))
	require.NoError(t, s.WaitReady(queryFrontend))

	// Check if we're discovering memcache or not.
	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "thanos_cache_dns_provider_results"))
	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Greater(0), "thanos_cache_dns_lookups_total"))

	// Wait until distributor and querier have updated the ingesters ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// When using the ingest storage, wait until partitions are ACTIVE in the ring.
	if flags["-ingest-storage.enabled"] == "true" {
		for _, service := range []*e2emimir.MimirService{distributor, queryFrontend, querier} {
			require.NoErrorf(t, service.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_partition_ring_partitions"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", "ingester-partitions"),
				labels.MustNewMatcher(labels.MatchEqual, "state", "Active"))),
				"service: %s", service.Name())
		}

		waitQueryFrontendToSuccessfullyFetchLastProducedOffsets(t, queryFrontend)
	}

	// Push a series for each user to Mimir.
	now := time.Now()
	expectedVectors := make([]model.Vector, numUsers)

	for u := 0; u < numUsers; u++ {
		c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", fmt.Sprintf("user-%d", u))
		require.NoError(t, err)

		var series []prompb.TimeSeries
		genSeries := generateFloatSeries
		if cfg.withHistograms && u%2 > 0 {
			genSeries = generateHistogramSeries
		}
		series, expectedVectors[u], _ = genSeries("series_1", now)

		res, err := c.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)
	}

	// Query the series for each user in parallel.
	wg := sync.WaitGroup{}
	wg.Add(numUsers * numQueriesPerUser)

	for u := 0; u < numUsers; u++ {
		userID := u

		c, err := e2emimir.NewClient("", queryFrontend.HTTPEndpoint(), "", "", fmt.Sprintf("user-%d", userID))
		require.NoError(t, err)

		// No need to repeat the test on start/end time rounding for each user.
		if userID == 0 {
			start := time.Unix(1595846748, 806*1e6)
			end := time.Unix(1595846750, 806*1e6)

			result, err := c.QueryRange("time()", start, end, time.Second)
			require.NoError(t, err)
			require.Equal(t, model.ValMatrix, result.Type())

			matrix := result.(model.Matrix)
			require.Len(t, matrix, 1)
			require.Len(t, matrix[0].Values, 3)
			assert.Equal(t, model.Time(1595846748806), matrix[0].Values[0].Timestamp)
			assert.Equal(t, model.Time(1595846750806), matrix[0].Values[2].Timestamp)
		}

		// No need to repeat the test on Server-Timing header for each user.
		if userID == 0 && cfg.queryStatsEnabled {
			res, _, err := c.QueryRaw("{instance=~\"hello.*\"}")
			require.NoError(t, err)
			require.Regexp(t, "querier_wall_time;dur=[0-9.]*, response_time;dur=[0-9.]*, bytes_processed;val=[0-9.]*, total_samples;val=[0-9.]*$", res.Header.Values("Server-Timing")[0])
		}

		// Beyond the range of -querier.query-ingesters-within should return nothing. No need to repeat it for each user.
		if userID == 0 {
			start := now.Add(-1000 * time.Hour)
			end := now.Add(-999 * time.Hour)

			result, err := c.Series([]string{"series_1"}, start, end)
			require.NoError(t, err)
			require.Len(t, result, 0)
		}

		for q := 0; q < numQueriesPerUser; q++ {
			go func() {
				defer wg.Done()

				result, err := c.Query("series_1", now)
				require.NoError(t, err)
				require.Equal(t, model.ValVector, result.Type())
				assert.Equal(t, expectedVectors[userID], result.(model.Vector))
			}()
		}
	}

	wg.Wait()

	// Compute the expected number of queries.
	expectedQueriesCount := float64(numUsers*numQueriesPerUser) + 2
	expectedIngesterQueriesCount := float64(numUsers * numQueriesPerUser) // The "time()" query and the query with time range < "query ingesters within" are not pushed down to ingesters.
	if cfg.queryStatsEnabled {
		expectedQueriesCount++
		expectedIngesterQueriesCount++
	}

	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(expectedQueriesCount), "cortex_query_frontend_queries_total"))

	// The number of received requests may be greater than the query requests because include
	// requests to /metrics and /ready.
	require.NoError(t, queryFrontend.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(expectedQueriesCount), []string{"cortex_request_duration_seconds"}, e2e.WithMetricCount))
	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(expectedQueriesCount), []string{"cortex_request_duration_seconds"}, e2e.WithMetricCount))
	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.GreaterOrEqual(expectedQueriesCount), []string{"cortex_querier_request_duration_seconds"}, e2e.WithMetricCount))

	// Ensure query stats metrics are tracked only when enabled.
	if cfg.queryStatsEnabled {
		require.NoError(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Greater(0),
			[]string{"cortex_query_seconds_total"},
			e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", "user-1"))))
	} else {
		require.NoError(t, queryFrontend.WaitRemovedMetric("cortex_query_seconds_total"))
	}

	// When the ingest storage is used, we expect that each query issued by this test was processed
	// with strong read consistency.
	if flags["-ingest-storage.enabled"] == "true" {
		require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(expectedQueriesCount), "cortex_ingest_storage_strong_consistency_requests_total"))

		// We expect the offsets to be fetched by query-frontend and then propagated to ingesters.
		require.NoError(t, ingester.WaitSumMetricsWithOptions(e2e.Equals(expectedIngesterQueriesCount), []string{"cortex_ingest_storage_strong_consistency_requests_total"}, e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "with_offset", "true"))))
		require.NoError(t, ingester.WaitSumMetricsWithOptions(e2e.Equals(0), []string{"cortex_ingest_storage_strong_consistency_requests_total"}, e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "with_offset", "false"))))
	} else {
		require.NoError(t, queryFrontend.WaitRemovedMetric("cortex_ingest_storage_strong_consistency_requests_total"))
		require.NoError(t, ingester.WaitRemovedMetric("cortex_ingest_storage_strong_consistency_requests_total"))
	}

	// Ensure no service-specific metrics prefix is used by the wrong service.
	assertServiceMetricsPrefixes(t, Distributor, distributor)
	assertServiceMetricsPrefixes(t, Ingester, ingester)
	assertServiceMetricsPrefixes(t, Querier, querier)
	assertServiceMetricsPrefixes(t, QueryFrontend, queryFrontend)
	assertServiceMetricsPrefixes(t, QueryScheduler, queryScheduler)

	if flags["-ingest-storage.enabled"] == "true" {
		// Ensure cortex_distributor_replication_factor is not exported when ingest storage is enabled
		// because it's how we detect whether a Mimir cluster is running with ingest storage.
		assertServiceMetricsNotMatching(t, "cortex_distributor_replication_factor", queryFrontend, queryScheduler, distributor, ingester, querier)
	} else {
		assertServiceMetricsMatching(t, "cortex_distributor_replication_factor", distributor)
	}
}

// This spins up a minimal query-frontend setup and compares if errors returned
// by QueryRanges are returned in the same way as they are with PromQL
func TestQueryFrontendErrorMessageParity(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	cfg := &queryFrontendTestConfig{
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			flags = mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags())

			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))

			return "", flags
		},
	}

	configFile, flags := cfg.setup(t, s)

	// Write overrides file enabling query-sharding for tenant query-sharding
	runtimeConfig := "runtime-config.yaml"
	require.NoError(t, writeFileToSharedDir(s, runtimeConfig, []byte(`
overrides:
  fake:
    blocked_queries:
    - pattern: ".*blocked_series.*"
      regex: true
    - pattern: "{__name__=\"blocked_selector\"}"
      regex: false
  query-sharding:
    query_sharding_total_shards: 8
    blocked_queries:
    - pattern: ".*blocked_series.*"
      regex: true
    - pattern: "{__name__=\"blocked_selector\"}"
      regex: false
`)))

	flags = mergeFlags(flags, map[string]string{
		"-querier.max-samples":                          "20",                                                 // Very low limit so that we can easily hit it, but high enough to test other features.
		"-querier.max-partial-query-length":             "30d",                                                // To test too long query error (31d)
		"-query-frontend.parallelize-shardable-queries": "true",                                               // Allow queries to be parallized (query-sharding)
		"-query-frontend.query-sharding-total-shards":   "0",                                                  // Disable query-sharding by default
		"-runtime-config.file":                          filepath.Join(e2e.ContainerSharedDir, runtimeConfig), // Read per tenant runtime config
	})
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul))

	queryFrontend := e2emimir.NewQueryFrontend("query-frontend", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithConfigFile(configFile))
	require.NoError(t, s.Start(queryFrontend))

	flags["-querier.frontend-address"] = queryFrontend.NetworkGRPCEndpoint()

	// Start all other services.
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithConfigFile(configFile))
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithConfigFile(configFile))
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithConfigFile(configFile))

	require.NoError(t, s.StartAndWaitReady(distributor, querier, ingester))
	require.NoError(t, s.WaitReady(queryFrontend))

	// Wait until both the distributor and querier have updated the ring.
	// The distributor should have 512 tokens for the ingester ring and 1 for the distributor ring
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512+1), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	now := time.Now()

	// Push some series.
	cWrite, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", "fake")
	require.NoError(t, err)
	cWriteWithQuerySharding, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", "query-sharding")
	require.NoError(t, err)

	for i := 0; i < 50; i++ {
		series, _, _ := generateFloatSeries(
			"metric",
			now,
			prompb.Label{Name: "unique", Value: strconv.Itoa(i)},
			prompb.Label{Name: "group_1", Value: strconv.Itoa(i % 2)},
		)

		res, err := cWrite.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)

		res, err = cWriteWithQuerySharding.Push(series)
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)
	}

	cQueryFrontend, err := e2emimir.NewClient("", queryFrontend.HTTPEndpoint(), "", "", "fake")
	require.NoError(t, err)

	cQueryFrontendWithQuerySharding, err := e2emimir.NewClient("", queryFrontend.HTTPEndpoint(), "", "", "query-sharding")
	require.NoError(t, err)

	cQuerier, err := e2emimir.NewClient("", querier.HTTPEndpoint(), "", "", "fake")
	require.NoError(t, err)

	queryClients := map[string]*e2emimir.Client{
		"query-frontend":               cQueryFrontend,
		"query-frontend with sharding": cQueryFrontendWithQuerySharding,
		"querier":                      cQuerier,
	}

	for _, tc := range []struct {
		name          string
		query         func(*e2emimir.Client) (*http.Response, []byte, error)
		exclude       []string
		expStatusCode int
		expJSON       string
		expBody       string
	}{
		{
			name: "query blocked via regex for instant query",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRaw("blocked_series{foo=\"bar\"}")
			},
			exclude:       []string{"querier"},
			expStatusCode: http.StatusBadRequest,
			expJSON:       `{"error":"the request has been blocked by the cluster administrator (err-mimir-query-blocked)", "errorType":"bad_data", "status":"error"}`,
		},
		{
			name: "query blocked via regex for range query",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRangeRaw("blocked_series{foo=\"bar\"}", now.Add(-time.Hour), now, time.Minute)
			},
			exclude:       []string{"querier"},
			expStatusCode: http.StatusBadRequest,
			expJSON:       `{"error":"the request has been blocked by the cluster administrator (err-mimir-query-blocked)", "errorType":"bad_data", "status":"error"}`,
		},
		{
			name: "query blocked via regex for remote read",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				httpR, _, respBytes, err := c.RemoteRead(remoteReadQueryByMetricName(`blocked_series`, now.Add(-time.Hour*24*32), now))
				return httpR, respBytes, err
			},
			exclude:       []string{"querier"},
			expStatusCode: http.StatusBadRequest,
			expJSON:       `{"error":"remote read error (matchers_0: {__name__=\"blocked_series\"}): the request has been blocked by the cluster administrator (err-mimir-query-blocked)", "errorType":"bad_data", "status":"error"}`,
		},
		{
			name: "query blocked via equality for instant query",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRaw("{__name__=\"blocked_selector\"}")
			},
			exclude:       []string{"querier"},
			expStatusCode: http.StatusBadRequest,
			expJSON:       `{"error":"the request has been blocked by the cluster administrator (err-mimir-query-blocked)", "errorType":"bad_data", "status":"error"}`,
		},
		{
			name: "query blocked via equality for range query",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRangeRaw("{__name__=\"blocked_selector\"}", now.Add(-time.Hour), now, time.Minute)
			},
			exclude:       []string{"querier"},
			expStatusCode: http.StatusBadRequest,
			expJSON:       `{"error":"the request has been blocked by the cluster administrator (err-mimir-query-blocked)", "errorType":"bad_data", "status":"error"}`,
		},
		{
			name: "query blocked via equality for remote read",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				httpR, _, respBytes, err := c.RemoteRead(remoteReadQueryByMetricName(`blocked_selector`, now.Add(-time.Hour*24*32), now))
				return httpR, respBytes, err
			},
			exclude:       []string{"querier"},
			expStatusCode: http.StatusBadRequest,
			expJSON:       `{"error":"remote read error (matchers_0: {__name__=\"blocked_selector\"}): the request has been blocked by the cluster administrator (err-mimir-query-blocked)", "errorType":"bad_data", "status":"error"}`,
		},
		{
			name: "maximum resolution error",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRangeRaw("unknown", now.Add(-time.Hour*24), now, time.Second)
			},
			expStatusCode: http.StatusBadRequest,
			expJSON:       `{"error":"exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)", "errorType":"bad_data", "status":"error"}`,
		},
		{
			name: "negative step",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRangeRaw("unknown", now.Add(-time.Hour), now, -time.Minute)
			},
			expStatusCode: http.StatusBadRequest,
			expJSON:       `{"error":"invalid parameter \"step\": zero or negative query resolution step widths are not accepted. Try a positive integer", "errorType":"bad_data", "status":"error"}`,
		},
		{
			name: "unknown function",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRangeRaw("unknown(up)", now.Add(-time.Hour), now, time.Minute)
			},
			expStatusCode: http.StatusBadRequest,
			expJSON:       `{"error":"invalid parameter \"query\": 1:1: parse error: unknown function with name \"unknown\"", "errorType":"bad_data", "status":"error"}`,
		},
		{
			name: "range vector instead of instant vector",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRangeRaw(`sum by(grpc_method)(grpc_server_handled_total{job="cortex-dedicated-06/etcd"}[1m])`, now.Add(-time.Hour), now, time.Minute)
			},
			expStatusCode: http.StatusBadRequest,
			expJSON:       `{"error":"invalid parameter \"query\": 1:21: parse error: expected type instant vector in aggregation expression, got range vector", "errorType":"bad_data", "status":"error"}`,
		},
		{
			name: "start after end",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRangeRaw("unknown", now, now.Add(-time.Hour), time.Minute)
			},
			expStatusCode: http.StatusBadRequest,
			expJSON:       `{"error":"invalid parameter \"end\": end timestamp must not be before start time", "errorType":"bad_data", "status":"error"}`,
		},
		{
			name: "wrong duration specified in step",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.DoGetBody(fmt.Sprintf(
					"http://%s/prometheus/api/v1/query_range?query=%s&start=%s&end=%s&step=%s",
					c.QuerierAddress(),
					url.QueryEscape("unknown"),
					e2emimir.FormatTime(now.Add(-time.Hour)),
					e2emimir.FormatTime(now),
					"123notafloat",
				))
			},
			expStatusCode: http.StatusBadRequest,
			expJSON:       `{"error":"invalid parameter \"step\": cannot parse \"123notafloat\" to a valid duration", "errorType":"bad_data", "status":"error"}`,
		},
		{
			name: "wrong timestamp in start",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.DoGetBody(fmt.Sprintf(
					"http://%s/prometheus/api/v1/query_range?query=%s&start=%s&end=%s&step=%s",
					c.QuerierAddress(),
					url.QueryEscape("unknown"),
					"depths-of-time",
					e2emimir.FormatTime(now),
					"30",
				))
			},
			expStatusCode: http.StatusBadRequest,
			expJSON:       `{"error":"invalid parameter \"start\": cannot parse \"depths-of-time\" to a valid timestamp", "errorType":"bad_data", "status":"error"}`,
		},
		{
			name: "max samples limit hit",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRangeRaw(`metric`, now.Add(-time.Minute), now, time.Minute)
			},
			expStatusCode: http.StatusUnprocessableEntity,
			expJSON:       `{"error":"query processing would load too many samples into memory in query execution", "errorType":"execution", "status":"error"}`,
		},
		{
			name: "query time range exceeds the limit",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRangeRaw(`sum_over_time(metric[31d:1s])`, now.Add(-time.Minute), now, time.Minute)
			},
			expStatusCode: http.StatusUnprocessableEntity,
			expJSON:       fmt.Sprintf(`{"error":"expanding series: %s", "errorType":"execution", "status":"error"}`, mimirquerier.NewMaxQueryLengthError((744*time.Hour)+(6*time.Minute)-time.Millisecond, 720*time.Hour)),
		},
		{
			name: "query remote read time range exceeds the limit",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				httpR, _, respBytes, err := c.RemoteRead(remoteReadQueryByMetricName(`metric`, now.Add(-time.Hour*24*32), now))
				return httpR, respBytes, err
			},
			expStatusCode: http.StatusBadRequest,
			expBody:       mimirquerier.NewMaxQueryLengthError(time.Hour*24*32, 720*time.Hour).Error(),
		},
		{
			name: "query remote read time range exceeds the limit (streaming chunks)",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				httpR, _, respBytes, err := c.RemoteReadChunks(remoteReadQueryByMetricName(`metric`, now.Add(-time.Hour*24*32), now))
				return httpR, respBytes, err
			},
			expStatusCode: http.StatusBadRequest,
			expBody:       mimirquerier.NewMaxQueryLengthError(time.Hour*24*32, 720*time.Hour).Error(),
		},
		{
			name: "execution error",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRangeRaw(`sum by (group_1) (metric{unique=~"0|1|2|3"}) * on(group_1) group_right(unique) (sum by (group_1,unique) (metric{unique=~"0|1|2|3"}))`, now.Add(-time.Minute), now, time.Minute)
			},
			expStatusCode: http.StatusUnprocessableEntity,
			expJSON:       `{"error":"multiple matches for labels: grouping labels must ensure unique matches", "errorType":"execution", "status":"error"}`,
		},
		{
			name: "range query with range vector",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRangeRaw(`(sum(rate(up[1m])))[5m:]`, now.Add(-time.Hour), now, time.Minute)
			},
			expStatusCode: http.StatusBadRequest,
			expJSON:       `{"error":"invalid parameter \"query\": invalid expression type \"range vector\" for range query, must be Scalar or instant Vector", "errorType":"bad_data", "status":"error"}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.expBody != "" && tc.expJSON != "" {
				t.Fatalf("expected only one of expBody or expJSON to be set")
			}

			for name, c := range queryClients {
				if slices.Contains(tc.exclude, name) {
					continue
				}
				resp, body, err := tc.query(c)
				require.NoError(t, err)
				assert.Equal(t, tc.expStatusCode, resp.StatusCode, "querier returns unexpected statusCode for "+name)
				if tc.expJSON != "" {
					assert.JSONEq(t, tc.expJSON, string(body), "querier returns unexpected body for "+name)
				} else {
					assert.Equal(t, tc.expBody, strings.TrimSpace(string(body)), "querier returns unexpected body for "+name)
				}
			}
		})
	}
}

func TestQueryFrontendWithQueryShardingAndTooLargeEntityRequest(t *testing.T) {
	runQueryFrontendWithQueryShardingHTTPTest(
		t,
		queryFrontendTestConfig{
			querySchedulerEnabled: false,
			setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
				flags = mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
					// The query result payload is 202 bytes, so it will be too large for the configured limit.
					"-querier.frontend-client.grpc-max-send-msg-size": "100",
				})

				minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
				require.NoError(t, s.StartAndWaitReady(minio))

				return "", flags
			},
		},
		http.StatusRequestEntityTooLarge,
		false,
	)
}

func TestQueryFrontendWithQueryShardingAndTooManyRequests(t *testing.T) {
	setupTestWithQueryScheduler := func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
		flags = mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
			"-query-scheduler.max-outstanding-requests-per-tenant": "1",
		})

		minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
		require.NoError(t, s.StartAndWaitReady(minio))

		return "", flags
	}

	t.Run("with query-scheduler (DNS-based service discovery)", func(t *testing.T) {
		runQueryFrontendWithQueryShardingHTTPTest(
			t,
			queryFrontendTestConfig{
				querySchedulerEnabled:       true,
				querySchedulerDiscoveryMode: "dns",
				setup:                       setupTestWithQueryScheduler,
			},
			http.StatusTooManyRequests,
			true,
		)
	})

	t.Run("with query-scheduler (ring-based service discovery)", func(t *testing.T) {
		runQueryFrontendWithQueryShardingHTTPTest(
			t,
			queryFrontendTestConfig{
				querySchedulerEnabled:       true,
				querySchedulerDiscoveryMode: "ring",
				setup:                       setupTestWithQueryScheduler,
			},
			http.StatusTooManyRequests,
			true,
		)
	})

	t.Run("without query-scheduler", func(t *testing.T) {
		runQueryFrontendWithQueryShardingHTTPTest(
			t,
			queryFrontendTestConfig{
				querySchedulerEnabled: false,
				setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
					flags = mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
						"-querier.max-outstanding-requests-per-tenant": "1", // Limit
					})

					minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
					require.NoError(t, s.StartAndWaitReady(minio))

					return "", flags
				},
			},
			http.StatusTooManyRequests,
			true,
		)
	})
}
func runQueryFrontendWithQueryShardingHTTPTest(t *testing.T, cfg queryFrontendTestConfig, expectHTTPSStatus int, checkDiscardedMetrics bool) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	memcached := e2ecache.NewMemcached()
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul, memcached))

	configFile, flags := cfg.setup(t, s)

	flags = mergeFlags(flags, map[string]string{
		"-query-frontend.cache-results":                     "true",
		"-query-frontend.results-cache.backend":             "memcached",
		"-query-frontend.results-cache.memcached.addresses": "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
		"-query-frontend.query-stats-enabled":               strconv.FormatBool(cfg.queryStatsEnabled),
		"-query-frontend.parallelize-shardable-queries":     "true", // Allow queries to be parallized (query-sharding)
		// Allow 16 shards for each query.
		// The test would fail with a lower number too, like 4, but also may succeed if shards are executed sequentially,
		// so we set it to 32 to _ensure_ that more than 1 queries are enqueued at the same time.
		"-query-frontend.query-sharding-total-shards": "32",
	})

	// Start the query-scheduler if enabled.
	var queryScheduler *e2emimir.MimirService
	if cfg.querySchedulerEnabled && cfg.querySchedulerDiscoveryMode == "dns" {
		queryScheduler = e2emimir.NewQueryScheduler("query-scheduler", flags)
		require.NoError(t, s.StartAndWaitReady(queryScheduler))
		flags["-query-frontend.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
		flags["-querier.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
	} else if cfg.querySchedulerEnabled && cfg.querySchedulerDiscoveryMode == "ring" {
		flags["-query-scheduler.service-discovery-mode"] = "ring"
		flags["-query-scheduler.ring.store"] = "consul"
		flags["-query-scheduler.ring.consul.hostname"] = consul.NetworkHTTPEndpoint()

		queryScheduler = e2emimir.NewQueryScheduler("query-scheduler", flags)
		require.NoError(t, s.StartAndWaitReady(queryScheduler))
	}

	// Start the query-frontend.
	queryFrontend := e2emimir.NewQueryFrontend("query-frontend", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithConfigFile(configFile))
	require.NoError(t, s.Start(queryFrontend))

	if !cfg.querySchedulerEnabled {
		flags["-querier.frontend-address"] = queryFrontend.NetworkGRPCEndpoint()
	}

	// Start all other services.
	ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithConfigFile(configFile))
	distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithConfigFile(configFile))
	querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags, e2emimir.WithConfigFile(configFile))

	require.NoError(t, s.StartAndWaitReady(querier, ingester, distributor))
	require.NoError(t, s.WaitReady(queryFrontend))

	// Check if we're discovering memcache or not.
	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "thanos_cache_dns_provider_results"))
	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Greater(0), "thanos_cache_dns_lookups_total"))

	// Wait until distributor and querier have updated the ingesters ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	// Push series for the test user to Mimir.
	now := time.Now()
	c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), queryFrontend.HTTPEndpoint(), "", "", userID)
	require.NoError(t, err)
	var series []prompb.TimeSeries
	series, _, _ = generateFloatSeries("series_1", now, prompb.Label{Name: "group", Value: "a-really-really-really-long-name-that-will-pad-out-the-response-payload-size"})

	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	resp, _, err := c.QueryRawAt("sum by (group) (series_1)", now)
	require.NoError(t, err)
	require.Equal(t, expectHTTPSStatus, resp.StatusCode)

	// Check that query was actually sharded.
	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Greater(0), "cortex_frontend_sharded_queries_total"))

	// Check that we actually discarded the request.
	if checkDiscardedMetrics {
		if cfg.querySchedulerEnabled {
			require.NoError(t, queryScheduler.WaitSumMetrics(e2e.Greater(0), "cortex_query_scheduler_discarded_requests_total"))
		} else {
			require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Greater(0), "cortex_query_frontend_discarded_requests_total"))
		}
	}
}

// waitQueryFrontendToSuccessfullyFetchLastProducedOffsets waits until the query-frontend has successfully fetched
// the last produced offsets at least once. This is required in integration tests to avoid flakiness, because we
// bootstrap a new Mimir and Kafka cluster from scratch in each integration test and the query-frontend may start
// to lookup partitions before the topic is created in Kafka, which will result in a query failure.
func waitQueryFrontendToSuccessfullyFetchLastProducedOffsets(t *testing.T, queryFrontend *e2emimir.MimirService) {
	test.Poll(t, 10*time.Second, true, func() interface{} {
		requests, requestsErr := queryFrontend.SumMetrics([]string{"cortex_ingest_storage_reader_last_produced_offset_request_duration_seconds"}, e2e.WithMetricCount, e2e.WaitMissingMetrics)
		failures, failuresErr := queryFrontend.SumMetrics([]string{"cortex_ingest_storage_reader_last_produced_offset_failures_total"}, e2e.WaitMissingMetrics)

		t.Logf("Waiting query-frontend to successfully fetch last produced offsets – requestsErr: %v requests: %v failuresErr: %v failures: %v", requestsErr, requests, failuresErr, failures)
		return requestsErr == nil && failuresErr == nil && len(requests) == 1 && len(failures) == 1 && requests[0] > failures[0]
	})
}
