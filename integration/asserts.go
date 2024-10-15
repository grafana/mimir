// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/asserts.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"strings"
	"testing"

	"github.com/grafana/regexp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

type ServiceType int

const (
	Distributor = iota
	Ingester
	Querier
	QueryFrontend
	QueryScheduler
	AlertManager
	Ruler
	StoreGateway
)

var (
	// Service-specific metrics prefixes which shouldn't be used by any other service.
	serviceMetricsPrefixes = map[ServiceType][]string{
		Distributor:    {},
		Ingester:       {"!cortex_ingester_client", "cortex_ingester"},                                                                                   // The metrics prefix cortex_ingester_client may be used by other components so we ignore it.
		Querier:        {"!cortex_querier_storegateway", "!cortex_querier_blocks", "!cortex_querier_queries", "!cortex_querier_query", "cortex_querier"}, // The metrics prefix cortex_querier_storegateway, cortex_querier_blocks, cortex_querier_queries, and cortex_querier_query may be used by other components so we ignore it.
		QueryFrontend:  {"cortex_frontend", "cortex_query_frontend"},
		QueryScheduler: {"cortex_query_scheduler"},
		AlertManager:   {"cortex_alertmanager"},
		Ruler:          {},
		StoreGateway:   {"!cortex_storegateway_client", "cortex_storegateway"}, // The metrics prefix cortex_storegateway_client may be used by other components so we ignore it.
	}

	// Blacklisted metrics prefixes across any Mimir service.
	blacklistedMetricsPrefixes = []string{
		"cortex_alert_manager", // It should be "cortex_alertmanager"
		"cortex_store_gateway", // It should be "cortex_storegateway"
	}
)

func assertServiceMetricsPrefixes(t *testing.T, serviceType ServiceType, service *e2emimir.MimirService) {
	if service == nil {
		return
	}

	metrics, err := service.Metrics()
	require.NoError(t, err)

	// Build the list of blacklisted metrics prefixes for this specific service.
	blacklist := getBlacklistedMetricsPrefixesByService(serviceType)

	// Ensure no metric name matches the blacklisted prefixes.
	for _, metricLine := range strings.Split(metrics, "\n") {
		metricLine = strings.TrimSpace(metricLine)
		if metricLine == "" || strings.HasPrefix(metricLine, "#") {
			continue
		}

		for _, prefix := range blacklist {
			// Skip the metric if it matches an ignored prefix.
			if prefix[0] == '!' && strings.HasPrefix(metricLine, prefix[1:]) {
				break
			}

			assert.Falsef(
				t, strings.HasPrefix(metricLine, prefix),
				"service: %s endpoint: %s prefix: %s metric: %s",
				service.Name(), service.HTTPEndpoint(), prefix, metricLine,
			)
		}
	}
}

func getBlacklistedMetricsPrefixesByService(serviceType ServiceType) []string {
	blacklist := append([]string{}, blacklistedMetricsPrefixes...)

	// Add any service-specific metrics prefix excluding the service itself.
	for t, prefixes := range serviceMetricsPrefixes {
		if t == serviceType {
			continue
		}

		blacklist = append(blacklist, prefixes...)
	}

	return blacklist
}

func assertServiceMetricsNotMatching(t *testing.T, metricName string, services ...*e2emimir.MimirService) {
	for _, service := range services {
		if service == nil {
			continue
		}

		metrics, err := service.Metrics()
		require.NoError(t, err)

		if isRawMetricsContainingMetricName(metricName, metrics) {
			assert.Failf(t, "the service %s exported metrics include the metric name %s but it should not export it", service.Name(), metricName)
		}
	}
}

func assertServiceMetricsMatching(t *testing.T, metricName string, services ...*e2emimir.MimirService) {
	for _, service := range services {
		if service == nil {
			continue
		}

		metrics, err := service.Metrics()
		require.NoError(t, err)

		if !isRawMetricsContainingMetricName(metricName, metrics) {
			assert.Failf(t, "the service %s exported metrics don't include the metric name %s but it should export it", service.Name(), metricName)
		}
	}
}

func isRawMetricsContainingMetricName(metricName string, metrics string) bool {
	metricNameRegex := regexp.MustCompile("^[^ \\{]+")

	// Ensure no metric name matches the input one.
	for _, metricLine := range strings.Split(metrics, "\n") {
		metricLine = strings.TrimSpace(metricLine)
		if metricLine == "" || strings.HasPrefix(metricLine, "#") {
			continue
		}

		actualMetricName := metricNameRegex.FindStringSubmatch(metricLine)
		if len(actualMetricName) != 1 {
			continue
		}

		if actualMetricName[0] == metricName {
			return true
		}
	}

	return false
}
