// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/query_frontend_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker
// +build requires_docker

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

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/ca"
	"github.com/grafana/mimir/integration/e2e"
	e2ecache "github.com/grafana/mimir/integration/e2e/cache"
	e2edb "github.com/grafana/mimir/integration/e2e/db"
	"github.com/grafana/mimir/integration/e2emimir"
)

type queryFrontendTestConfig struct {
	querySchedulerEnabled bool
	queryStatsEnabled     bool
	setup                 func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string)
}

func TestQueryFrontendWithBlocksStorageViaFlags(t *testing.T) {
	runQueryFrontendTest(t, queryFrontendTestConfig{
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			flags = BlocksStorageFlags()

			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))

			return "", flags
		},
	})
}

func TestQueryFrontendWithBlocksStorageViaFlagsAndQueryStatsEnabled(t *testing.T) {
	runQueryFrontendTest(t, queryFrontendTestConfig{
		queryStatsEnabled: true,
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			flags = BlocksStorageFlags()

			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))

			return "", flags
		},
	})
}

func TestQueryFrontendWithBlocksStorageViaFlagsAndWithQueryScheduler(t *testing.T) {
	runQueryFrontendTest(t, queryFrontendTestConfig{
		querySchedulerEnabled: true,
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			flags = BlocksStorageFlags()

			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))

			return "", flags
		},
	})
}

func TestQueryFrontendWithBlocksStorageViaFlagsAndWithQuerySchedulerAndQueryStatsEnabled(t *testing.T) {
	runQueryFrontendTest(t, queryFrontendTestConfig{
		querySchedulerEnabled: true,
		queryStatsEnabled:     true,
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			flags = BlocksStorageFlags()

			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))

			return "", flags
		},
	})
}

func TestQueryFrontendWithBlocksStorageViaConfigFile(t *testing.T) {
	runQueryFrontendTest(t, queryFrontendTestConfig{
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			require.NoError(t, writeFileToSharedDir(s, mimirConfigFile, []byte(BlocksStorageConfig)))

			minio := e2edb.NewMinio(9000, BlocksStorageFlags()["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(minio))

			return mimirConfigFile, e2e.EmptyFlags()
		},
	})
}

func TestQueryFrontendTLSWithBlocksStorageViaFlags(t *testing.T) {
	runQueryFrontendTest(t, queryFrontendTestConfig{
		setup: func(t *testing.T, s *e2e.Scenario) (configFile string, flags map[string]string) {
			flags = mergeFlags(
				BlocksStorageFlags(),
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
		"-querier.cache-results":             "true",
		"-querier.split-queries-by-interval": "24h",
		"-querier.query-ingesters-within":    "12h", // Required by the test on query /series out of ingesters time range
		"-frontend.memcached.addresses":      "dns+" + memcached.NetworkEndpoint(e2ecache.MemcachedPort),
		"-frontend.query-stats-enabled":      strconv.FormatBool(cfg.queryStatsEnabled),
	})

	// Start the query-scheduler if enabled.
	var queryScheduler *e2emimir.MimirService
	if cfg.querySchedulerEnabled {
		queryScheduler = e2emimir.NewQueryScheduler("query-scheduler", flags, "")
		require.NoError(t, s.StartAndWaitReady(queryScheduler))
		flags["-frontend.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
		flags["-querier.scheduler-address"] = queryScheduler.NetworkGRPCEndpoint()
	}

	// Start the query-frontend.
	queryFrontend := e2emimir.NewQueryFrontendWithConfigFile("query-frontend", configFile, flags, "")
	require.NoError(t, s.Start(queryFrontend))

	if !cfg.querySchedulerEnabled {
		flags["-querier.frontend-address"] = queryFrontend.NetworkGRPCEndpoint()
	}

	// Start all other services.
	ingester := e2emimir.NewIngesterWithConfigFile("ingester", consul.NetworkHTTPEndpoint(), configFile, flags, "")
	distributor := e2emimir.NewDistributorWithConfigFile("distributor", consul.NetworkHTTPEndpoint(), configFile, flags, "")
	querier := e2emimir.NewQuerierWithConfigFile("querier", consul.NetworkHTTPEndpoint(), configFile, flags, "")

	require.NoError(t, s.StartAndWaitReady(querier, ingester, distributor))
	require.NoError(t, s.WaitReady(queryFrontend))

	// Check if we're discovering memcache or not.
	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Equals(1), "cortex_memcache_client_servers"))
	require.NoError(t, queryFrontend.WaitSumMetrics(e2e.Greater(0), "cortex_dns_lookups_total"))

	// Wait until both the distributor and querier have updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	// Push a series for each user to Mimir.
	now := time.Now()
	expectedVectors := make([]model.Vector, numUsers)

	for u := 0; u < numUsers; u++ {
		c, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", fmt.Sprintf("user-%d", u))
		require.NoError(t, err)

		var series []prompb.TimeSeries
		series, expectedVectors[u] = generateSeries("series_1", now)

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
			flags = BlocksStorageFlags()

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
		"-querier.cache-results":             "true",
		"-querier.split-queries-by-interval": "24h",
		"-querier.max-samples":               "2", // Very low limit so that we can easily hit it.

		"-query-frontend.parallelize-shardable-queries": "true",                                               // Allow queries to be parallized (query-sharding)
		"-frontend.query-sharding-total-shards":         "0",                                                  // Disable query-sharding by default
		"-runtime-config.file":                          filepath.Join(e2e.ContainerSharedDir, runtimeConfig), // Read per tenant runtime config
	})
	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul))

	queryFrontend := e2emimir.NewQueryFrontendWithConfigFile("query-frontend", configFile, flags, "")
	require.NoError(t, s.Start(queryFrontend))

	flags["-querier.frontend-address"] = queryFrontend.NetworkGRPCEndpoint()

	// Start all other services.
	distributor := e2emimir.NewDistributorWithConfigFile("distributor", consul.NetworkHTTPEndpoint(), configFile, flags, "")
	ingester := e2emimir.NewIngesterWithConfigFile("ingester", consul.NetworkHTTPEndpoint(), configFile, flags, "")
	querier := e2emimir.NewQuerierWithConfigFile("querier", consul.NetworkHTTPEndpoint(), configFile, flags, "")

	require.NoError(t, s.StartAndWaitReady(distributor, querier, ingester))
	require.NoError(t, s.WaitReady(queryFrontend))

	// Wait until both the distributor and querier have updated the ring.
	require.NoError(t, distributor.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))
	require.NoError(t, querier.WaitSumMetrics(e2e.Equals(512), "cortex_ring_tokens_total"))

	now := time.Now()

	// Push some series.
	cWrite, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", "fake")
	require.NoError(t, err)
	cWriteWithQuerySharding, err := e2emimir.NewClient(distributor.HTTPEndpoint(), "", "", "", "query-sharding")
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		series, _ := generateSeries(fmt.Sprintf("series_%d", i), now)

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
					"http://%s/api/prom/api/v1/query_range?query=%s&start=%s&end=%s&step=%s",
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
					"http://%s/api/prom/api/v1/query_range?query=%s&start=%s&end=%s&step=%s",
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
				return c.QueryRangeRaw(`{__name__=~"series_.+"}`, now.Add(-time.Minute), now, time.Minute)
			},
			expStatusCode: http.StatusUnprocessableEntity,
			expBody:       `{"error":"query processing would load too many samples into memory in query execution", "errorType":"execution", "status":"error"}`,
		},
		{
			name: "range query with range vector",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRangeRaw(`(sum(rate(up[1m])))[5m:]`, now.Add(-time.Hour), now, time.Minute)
			},
			expStatusCode: http.StatusBadRequest,
			expBody:       `{"error":"invalid expression type \"range vector\" for range query, must be Scalar or instant Vector", "errorType":"bad_data", "status":"error"}`,
		},
		{
			name: "error when negative offset is unsupported",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRangeRaw(`count_over_time(up[1m] offset -1m)`, now.Add(-time.Hour), now, time.Minute)
			},
			expStatusCode: http.StatusBadRequest,
			expBody:       `{"error": "negative offset is disabled, use --enable-feature=promql-negative-offset to enable it", "errorType":"bad_data", "status":"error"}`,
		},
		{
			name: "error when at-modifier is unsupported",
			query: func(c *e2emimir.Client) (*http.Response, []byte, error) {
				return c.QueryRangeRaw(`http_requests_total @ start()`, now.Add(-time.Minute), now, time.Minute)
			},
			expStatusCode: http.StatusBadRequest,
			expBody:       `{"error":"@ modifier is disabled, use -querier.at-modifier-enabled to enable it", "errorType":"bad_data", "status":"error"}`,
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
