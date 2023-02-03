// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"gopkg.in/yaml.v3"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
	"github.com/grafana/mimir/pkg/util/validation"
)

func runPrometheus(name string, args ...string) *e2e.HTTPService {
	port := 9090
	cmd := e2e.NewCommandWithoutEntrypoint(
		"/bin/prometheus",
		append([]string{"--config.file=/etc/prometheus/prometheus.yml", "--storage.tsdb.path=/prometheus", "--enable-feature=native-histograms"}, args...)...,
	)
	readiness := e2e.NewHTTPReadinessProbe(port, "/-/ready", http.StatusOK, http.StatusOK)
	service := e2e.NewHTTPService(name, "prom/prometheus:latest", cmd, readiness, port)

	return service
}

func runPrometheusWithRemoteWrite(name string, args ...string) *e2e.HTTPService {
	return runPrometheus(name, append(args, "--web.enable-remote-write-receiver")...)
}

func TestDistributorForwarding(t *testing.T) {
	const tenant = "tenant1"
	const runtimeConfig = "runtime-config.yaml"
	const metric1 = "metric1"
	const metric2 = "metric2"
	const metric3 = "metric3"

	type testCase struct {
		name                     string
		forwardingRules          validation.ForwardingRules
		submitMetrics            []string
		expectedIngestedMetrics  []string
		expectedForwardedMetrics []string
	}

	tests := []testCase{
		{
			name: "forward a metric, do not ingest it",
			forwardingRules: validation.ForwardingRules{
				metric1: validation.ForwardingRule{Ingest: false},
			},
			submitMetrics:            []string{metric1},
			expectedIngestedMetrics:  []string{},
			expectedForwardedMetrics: []string{metric1},
		}, {
			name: "forward two metrics and also ingest them",
			forwardingRules: validation.ForwardingRules{
				metric1: validation.ForwardingRule{Ingest: true},
				metric2: validation.ForwardingRule{Ingest: true},
			},
			submitMetrics:            []string{metric1, metric2},
			expectedIngestedMetrics:  []string{metric1, metric2},
			expectedForwardedMetrics: []string{metric1, metric2},
		}, {
			name: "submit three metrics, forward two, ingest two",
			forwardingRules: validation.ForwardingRules{
				metric2: validation.ForwardingRule{Ingest: true},
				metric3: validation.ForwardingRule{Ingest: false},
			},
			submitMetrics:            []string{metric1, metric2, metric3},
			expectedIngestedMetrics:  []string{metric1, metric2},
			expectedForwardedMetrics: []string{metric2, metric3},
		}, {
			name:                     "forward nothing and ingest everything",
			forwardingRules:          validation.ForwardingRules{},
			submitMetrics:            []string{metric1, metric2, metric3},
			expectedIngestedMetrics:  []string{metric1, metric2, metric3},
			expectedForwardedMetrics: []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			// Start one Prometheus for each unique forwarding target.
			prometheus := runPrometheusWithRemoteWrite("prometheus")
			require.NoError(t, s.StartAndWaitReady(prometheus))

			// Create client to query Prometheus.
			promAPIClient, err := promapi.NewClient(promapi.Config{Address: "http://" + prometheus.HTTPEndpoint()})
			require.NoError(t, err)

			promClient := promv1.NewAPI(promAPIClient)

			url := "http://" + prometheus.NetworkHTTPEndpoint() + "/api/v1/write"

			// Marshal the forwarding rules into yaml and write them into the runtime config file.
			rules, err := yaml.Marshal(map[string]interface{}{
				"overrides": map[string]interface{}{
					tenant: map[string]interface{}{
						"forwarding_endpoint": url,
						"forwarding_rules":    tc.forwardingRules,
					},
				},
			})
			require.NoError(t, err)
			require.NoError(t, writeFileToSharedDir(s, runtimeConfig, rules))

			// Prepare Mimir flags.
			flags := mergeFlags(
				BlocksStorageFlags(),
				BlocksStorageS3Flags(),
				map[string]string{
					"-ingester.ring.replication-factor": "1",
					"-ingester.ring.heartbeat-period":   "1s",
					"-distributor.forwarding.enabled":   "true",
					"-runtime-config.file":              filepath.Join(e2e.ContainerSharedDir, runtimeConfig),
				},
			)

			// Start dependencies.
			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, flags["-blocks-storage.s3.bucket-name"])
			require.NoError(t, s.StartAndWaitReady(consul, minio))

			// Start Mimir components.
			distributor := e2emimir.NewDistributor("distributor", consul.NetworkHTTPEndpoint(), flags)
			ingester := e2emimir.NewIngester("ingester", consul.NetworkHTTPEndpoint(), flags)
			storeGateway := e2emimir.NewStoreGateway("store-gateway", consul.NetworkHTTPEndpoint(), flags)
			require.NoError(t, s.StartAndWaitReady(distributor, ingester, storeGateway))

			querier := e2emimir.NewQuerier("querier", consul.NetworkHTTPEndpoint(), flags)
			require.NoError(t, s.StartAndWaitReady(querier))

			// Create client to query Mimir.
			mimirClient, err := e2emimir.NewClient(distributor.HTTPEndpoint(), querier.HTTPEndpoint(), "", "", tenant)
			require.NoError(t, err)

			// Submit metrics to Mimir.
			now := time.Now()
			now2 := now.Add(time.Second)
			for _, metric := range tc.submitMetrics {
				series, _, _ := generateFloatSeries(metric, now)
				res, err := mimirClient.Push(series)
				require.NoError(t, err)
				require.Equal(t, 200, res.StatusCode)

				series, _, _ = generateHistogramSeries(metric, now2)
				res, err = mimirClient.Push(series)
				require.NoError(t, err)
				require.Equal(t, 200, res.StatusCode)
			}

			// Query Mimir and the Prometheus servers to check which of the submitted metrics have been sent to where.
			for _, checkMetric := range tc.submitMetrics {

				// Check forwarding targets for the metric.
				expectedInPrometheus := false
				for _, expectedMetric := range tc.expectedForwardedMetrics {
					if checkMetric == expectedMetric {
						expectedInPrometheus = true
						break
					}
				}

				promVal, _, err := promClient.Query(context.Background(), checkMetric, now)
				require.NoError(t, err)

				if expectedInPrometheus {
					// The metric "checkMetric" should be present on this forwarding target.
					require.Contains(t, promVal.String(), checkMetric)
				} else {
					// The metric "checkMetric" should not be present on this forwarding target.
					require.NotContains(t, promVal.String(), checkMetric)
				}

				// Check Mimir for the metric.
				expectedInMimir := false
				for _, expectedMetric := range tc.expectedIngestedMetrics {
					if checkMetric == expectedMetric {
						expectedInMimir = true
						break
					}
				}

				val, err := mimirClient.Query(checkMetric, now)
				require.NoError(t, err)
				if expectedInMimir {
					// The metric "checkMetric" is expected to have been ingested by Mimir.
					require.Contains(t, val.String(), checkMetric)
				} else {
					// The metric "checkMetric" is expected to not have been ingested by Mimir.
					require.NotContains(t, val.String(), checkMetric)
				}
			}
		})
	}
}
