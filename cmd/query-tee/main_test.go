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
	cfg := Config{}
	cfg.ProxyConfig.Server.PathPrefix = ""
	routes := mimirReadRoutes(cfg)
	for _, r := range routes {
		assert.True(t, strings.HasPrefix(r.Path, "/api/v1/") || strings.HasPrefix(r.Path, "/prometheus/"))
	}

	// With dskit server integration, routes no longer include the path prefix
	// since dskit handles prefix stripping automatically via PathPrefix().Subrouter()
	cfg = Config{}
	cfg.ProxyConfig.Server.PathPrefix = "/some/random/prefix///"
	routes = mimirReadRoutes(cfg)
	for _, r := range routes {
		assert.True(t, strings.HasPrefix(r.Path, "/api/v1/") || strings.HasPrefix(r.Path, "/prometheus/"))
	}
}
