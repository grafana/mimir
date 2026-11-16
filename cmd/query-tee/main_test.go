// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/cmd/query-tee/main_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMimirReadRoutes(t *testing.T) {
	cardinalityRoutes := []string{
		"/api/v1/cardinality/label_names",
		"/api/v1/cardinality/label_values",
		"/api/v1/cardinality/active_series",
		"/api/v1/cardinality/active_native_histogram_metrics",
	}
	otherRoutes := []string{
		"/api/v1/query",
		"/api/v1/query_range",
		"/api/v1/query_exemplars",
		"/api/v1/labels",
		"/api/v1/label/{name}/values",
		"/api/v1/series",
		"/api/v1/metadata",
		"/prometheus/config/v1/rules",
		"/api/v1/alerts",
		"/api/v1/read",
		"/api/v1/format_query",
		"/api/v1/search/metric_names",
		"/api/v1/search/label_names",
		"/api/v1/search/label_values",
	}

	for _, tc := range []struct {
		name             string
		preferredBackend string
		wantCardinality  bool
	}{
		{name: "without preferred backend"},
		{name: "with preferred backend", preferredBackend: "backend", wantCardinality: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{}
			cfg.ProxyConfig.PreferredBackend = tc.preferredBackend
			routes := mimirReadRoutes(cfg)

			gotPaths := make([]string, 0, len(routes))
			for _, route := range routes {
				gotPaths = append(gotPaths, route.Path)
				assert.True(t, strings.HasPrefix(route.Path, "/api/v1/") || strings.HasPrefix(route.Path, "/prometheus/"))
			}

			wantPaths := append([]string(nil), otherRoutes...)
			if tc.wantCardinality {
				wantPaths = append(wantPaths, cardinalityRoutes...)
			}
			assert.ElementsMatch(t, wantPaths, gotPaths)

			for _, route := range routes {
				if !strings.HasPrefix(route.Path, "/api/v1/cardinality/") {
					continue
				}
				assert.True(t, tc.wantCardinality)
				assert.Equal(t, []string{"GET", "POST"}, route.Methods)
				assert.Nil(t, route.ResponseComparator)
			}
		})
	}

	// With dskit server integration, routes no longer include the path prefix
	// since dskit handles prefix stripping automatically via PathPrefix().Subrouter().
	cfg := Config{}
	cfg.ProxyConfig.Server.PathPrefix = "/some/random/prefix///"
	for _, route := range mimirReadRoutes(cfg) {
		assert.True(t, strings.HasPrefix(route.Path, "/api/v1/") || strings.HasPrefix(route.Path, "/prometheus/"))
	}
}
