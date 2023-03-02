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
	"strconv"
	"sync"
	"testing"
	"time"

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
	"github.com/grafana/mimir/pkg/util/validation"
)

type queryFrontendTestConfig struct {
	querySchedulerEnabled       bool
	querySchedulerDiscoveryMode string
	queryStatsEnabled           bool
	setup                       func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string)
	withHistograms              bool
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

	if cfg.withHistograms {
		flags = mergeFlags(flags, map[string]string{
			"-query-frontend.query-result-response-format": "protobuf",
		})
	}

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
	queryFrontend := e2emimir.NewQueryFrontend("query-frontend", flags, e2emimir.WithConfigFile(configFile))
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
	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "thanos_memcached_dns_provider_results"))
	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Greater(0), "thanos_memcached_dns_lookups_total"))

	// Wait until distributor and querier have updated the ingesters ring.
	require.NoError(t, distributor.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "name", "ingester"),
		labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"))))

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
			require.Regexp(t, "querier_wall_time;dur=[0-9.]*, response_time;dur=[0-9.]*$", res.Header.Values("Server-Timing")[0])
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

	extra := float64(2)
	if cfg.queryStatsEnabled {
		extra++
	}

	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(numUsers*numQueriesPerUser+extra), "cortex_query_frontend_queries_total"))

	// The number of received request is greater then the query requests because include
	// requests to /metrics and /ready.
	require.NoError(t, queryFrontend.WaitSumMetricsWithOptions(e2e.Greater(numUsers*numQueriesPerUser), []string{"cortex_request_duration_seconds"}, e2e.WithMetricCount))
	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Greater(numUsers*numQueriesPerUser), []string{"cortex_request_duration_seconds"}, e2e.WithMetricCount))
	require.NoError(t, querier.WaitSumMetricsWithOptions(e2e.Greater(numUsers*numQueriesPerUser), []string{"cortex_querier_request_duration_seconds"}, e2e.WithMetricCount))

	// Ensure query stats metrics are tracked only when enabled.
	if cfg.queryStatsEnabled {
		require.NoError(t, queryFrontend.WaitSumMetricsWithOptions(
			e2e.Greater(0),
			[]string{"cortex_query_seconds_total"},
			e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", "user-1"))))
	} else {
		require.NoError(t, queryFrontend.WaitRemovedMetric("cortex_query_seconds_total"))
	}

	// Ensure no service-specific metrics prefix is used by the wrong service.
	assertServiceMetricsPrefixes(t, Distributor, distributor)
	assertServiceMetricsPrefixes(t, Ingester, ingester)
	assertServiceMetricsPrefixes(t, Querier, querier)
	assertServiceMetricsPrefixes(t, QueryFrontend, queryFrontend)
	assertServiceMetricsPrefixes(t, QueryScheduler, queryScheduler)
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
  query-sharding:
    query_sharding_total_shards: 8
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

	queryFrontend := e2emimir.NewQueryFrontend("query-frontend", flags, e2emimir.WithConfigFile(configFile))
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

	for _, tc := range []struct {
		name          string
		query         func(*e2emimir.Client) (*http.Response, []byte, error)
		expStatusCode int
		expBody       string
	}{
		{
			name: "maximum resolution error",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRangeRaw("unknown", now.Add(-time.Hour*24), now, time.Second)
			},
			expStatusCode: http.StatusBadRequest,
			expBody:       `{"error":"exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)", "errorType":"bad_data", "status":"error"}`,
		},
		{
			name: "negative step",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRangeRaw("unknown", now.Add(-time.Hour), now, -time.Minute)
			},
			expStatusCode: http.StatusBadRequest,
			expBody:       `{"error":"invalid parameter \"step\": zero or negative query resolution step widths are not accepted. Try a positive integer", "errorType":"bad_data", "status":"error"}`,
		},
		{
			name: "unknown function",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRangeRaw("unknown(up)", now.Add(-time.Hour), now, time.Minute)
			},
			expStatusCode: http.StatusBadRequest,
			expBody:       `{"error":"1:1: parse error: unknown function with name \"unknown\"", "errorType":"bad_data", "status":"error"}`,
		},
		{
			name: "range vector instead of instant vector",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRangeRaw(`sum by(grpc_method)(grpc_server_handled_total{job="cortex-dedicated-06/etcd"}[1m])`, now.Add(-time.Hour), now, time.Minute)
			},
			expStatusCode: http.StatusBadRequest,
			expBody:       `{"error":"1:21: parse error: expected type instant vector in aggregation expression, got range vector", "errorType":"bad_data", "status":"error"}`,
		},
		{
			name: "start after end",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRangeRaw("unknown", now, now.Add(-time.Hour), time.Minute)
			},
			expStatusCode: http.StatusBadRequest,
			expBody:       `{"error":"invalid parameter \"end\": end timestamp must not be before start time", "errorType":"bad_data", "status":"error"}`,
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
			expBody:       `{"error":"invalid parameter \"step\": cannot parse \"123notafloat\" to a valid duration", "errorType":"bad_data", "status":"error"}`,
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
			expBody:       `{"error":"invalid parameter \"start\": cannot parse \"depths-of-time\" to a valid timestamp", "errorType":"bad_data", "status":"error"}`,
		},
		{
			name: "max samples limit hit",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRangeRaw(`metric`, now.Add(-time.Minute), now, time.Minute)
			},
			expStatusCode: http.StatusUnprocessableEntity,
			expBody:       `{"error":"query processing would load too many samples into memory in query execution", "errorType":"execution", "status":"error"}`,
		},
		{
			name: "query time range exceeds the limit",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRangeRaw(`sum_over_time(metric[31d:1s])`, now.Add(-time.Minute), now, time.Minute)
			},
			expStatusCode: http.StatusUnprocessableEntity,
			expBody:       fmt.Sprintf(`{"error":"expanding series: %s", "errorType":"execution", "status":"error"}`, validation.NewMaxQueryLengthError((744*time.Hour)+(6*time.Minute), 720*time.Hour)),
		},
		{
			name: "execution error",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRangeRaw(`sum by (group_1) (metric{unique=~"0|1|2|3"}) * on(group_1) group_right(unique) (sum by (group_1,unique) (metric{unique=~"0|1|2|3"}))`, now.Add(-time.Minute), now, time.Minute)
			},
			expStatusCode: http.StatusUnprocessableEntity,
			expBody:       `{"error":"multiple matches for labels: grouping labels must ensure unique matches", "errorType":"execution", "status":"error"}`,
		},
		{
			name: "range query with range vector",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRangeRaw(`(sum(rate(up[1m])))[5m:]`, now.Add(-time.Hour), now, time.Minute)
			},
			expStatusCode: http.StatusBadRequest,
			expBody:       `{"error":"invalid expression type \"range vector\" for range query, must be Scalar or instant Vector", "errorType":"bad_data", "status":"error"}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, body, err := tc.query(cQuerier)
			require.NoError(t, err)
			assert.Equal(t, tc.expStatusCode, resp.StatusCode, "querier returns unexpected statusCode")
			assert.JSONEq(t, tc.expBody, string(body), "querier returns unexpected body")

			resp, body, err = tc.query(cQueryFrontend)
			require.NoError(t, err)
			assert.Equal(t, tc.expStatusCode, resp.StatusCode, "query-frontend returns unexpected statusCode")
			assert.JSONEq(t, tc.expBody, string(body), "query-frontend returns unexpected body")

			resp, body, err = tc.query(cQueryFrontendWithQuerySharding)
			require.NoError(t, err)
			assert.Equal(t, tc.expStatusCode, resp.StatusCode, "query-frontend with query-sharding returns unexpected statusCode")
			assert.JSONEq(t, tc.expBody, string(body), "query-frontend with query-sharding returns unexpected body")
		})
	}
}

func TestQueryFrontendWithQueryShardingAndTooLargeEntryRequest(t *testing.T) {
	runQueryFrontendWithQueryShardingHTTPTest(
		t,
		queryFrontendTestConfig{
			querySchedulerEnabled: false,
			setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
				flags = mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags(), map[string]string{
					// Set the maximum entry size to 50 byte.
					// The query size is 64 bytes, so it will be a too large entry request.
					"-querier.frontend-client.grpc-max-send-msg-size": "50",
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
	queryFrontend := e2emimir.NewQueryFrontend("query-frontend", flags, e2emimir.WithConfigFile(configFile))
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
	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "thanos_memcached_dns_provider_results"))
	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Greater(0), "thanos_memcached_dns_lookups_total"))

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
	series, _, _ = generateFloatSeries("series_1", now)

	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	resp, _, err := c.QueryRaw("sum(series_1)")
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
