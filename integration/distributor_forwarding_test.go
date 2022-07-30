// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker
// +build requires_docker

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
		append([]string{"--config.file=/etc/prometheus/prometheus.yml", "--storage.tsdb.path=/prometheus"}, args...)...,
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
	const target1 = "target1"
	const target2 = "target2"
	const target3 = "target3"

	type testCase struct {
		name                             string
		forwardingRules                  validation.ForwardingRules
		submitMetrics                    []string
		expectedIngestedMetrics          []string
		expectedForwardedMetricsByTarget map[string][]string
	}

	tests := []testCase{
		{
			name: "forward a metric to one forwarding target, do not ingest it",
			forwardingRules: validation.ForwardingRules{
				metric1: validation.ForwardingRule{Endpoint: target1, Ingest: false},
			},
			submitMetrics:           []string{metric1},
			expectedIngestedMetrics: []string{},
			expectedForwardedMetricsByTarget: map[string][]string{
				target1: {metric1},
			},
		}, {
			name: "forward samples to two forwarding targets and also ingest them",
			forwardingRules: validation.ForwardingRules{
				metric1: validation.ForwardingRule{Endpoint: target1, Ingest: true},
				metric2: validation.ForwardingRule{Endpoint: target2, Ingest: true},
			},
			submitMetrics:           []string{metric1, metric2},
			expectedIngestedMetrics: []string{metric1, metric2},
			expectedForwardedMetricsByTarget: map[string][]string{
				target1: {metric1},
				target2: {metric2},
			},
		}, {
			name: "submit three metrics, forward two of them to two targets, ingest two others",
			forwardingRules: validation.ForwardingRules{
				metric2: validation.ForwardingRule{Endpoint: target2, Ingest: true},
				metric3: validation.ForwardingRule{Endpoint: target3, Ingest: false},
			},
			submitMetrics:           []string{metric1, metric2, metric3},
			expectedIngestedMetrics: []string{metric1, metric2},
			expectedForwardedMetricsByTarget: map[string][]string{
				target1: {},
				target2: {metric2},
				target3: {metric3},
			},
		}, {
			name:                    "forward nothing and ingest everything",
			forwardingRules:         validation.ForwardingRules{},
			submitMetrics:           []string{metric1, metric2, metric3},
			expectedIngestedMetrics: []string{metric1, metric2, metric3},
			expectedForwardedMetricsByTarget: map[string][]string{
				target1: {},
				target2: {},
				target3: {},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			// Start one Prometheus for each unique forwarding target.
			prometheusByTarget := make(map[string]*e2e.HTTPService)
			promClientByTarget := make(map[string]promv1.API)
			for target := range tc.expectedForwardedMetricsByTarget {
				if _, ok := prometheusByTarget[target]; ok {
					continue
				}

				prometheus := runPrometheusWithRemoteWrite(target)
				require.NoError(t, s.StartAndWaitReady(prometheus))
				prometheusByTarget[target] = prometheus

				// Create client to query Prometheus.
				promAPIClient, err := promapi.NewClient(promapi.Config{Address: "http://" + prometheus.HTTPEndpoint()})
				require.NoError(t, err)
				promClientByTarget[target] = promv1.NewAPI(promAPIClient)
			}

			// Replace the targets in the forwarding rules with the URLs of the Prometheus servers we created.
			for ruleIdx, rule := range tc.forwardingRules {
				rule.Endpoint = "http://" + prometheusByTarget[rule.Endpoint].NetworkHTTPEndpoint() + "/api/v1/write"
				tc.forwardingRules[ruleIdx] = rule
			}

			// Marshal the forwarding rules into yaml and write them into the runtime config file.
			rules, err := yaml.Marshal(map[string]interface{}{
				"overrides": map[string]interface{}{
					tenant: map[string]interface{}{
						"forwarding_rules": tc.forwardingRules,
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
			for _, metric := range tc.submitMetrics {
				series, _ := generateSeries(metric, now)
				res, err := mimirClient.Push(series)
				require.NoError(t, err)
				require.Equal(t, 200, res.StatusCode)
			}

			// Query Mimir and the Prometheus servers to check which of the submitted metrics have been sent to where.
			for _, checkMetric := range tc.submitMetrics {

				// Check forwarding targets for the metric.
				for target, expectedMetrics := range tc.expectedForwardedMetricsByTarget {
					promClient := promClientByTarget[target]
					expected := false
					for _, expectedMetric := range expectedMetrics {
						if checkMetric == expectedMetric {
							expected = true
							break
						}
					}

					val, _, err := promClient.Query(context.Background(), checkMetric, now)
					require.NoError(t, err)

					if expected {
						// The metric "checkMetric" should be present on this forwarding target.
						require.Contains(t, val.String(), checkMetric)
					} else {
						// The metric "checkMetric" should not be present on this forwarding target.
						require.NotContains(t, val.String(), checkMetric)
					}
				}

				// Check Mimir for the metric.
				expected := false
				for _, expectedMetric := range tc.expectedIngestedMetrics {
					if checkMetric == expectedMetric {
						expected = true
						break
					}
				}

				val, err := mimirClient.Query(checkMetric, now)
				require.NoError(t, err)
				if expected {
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
