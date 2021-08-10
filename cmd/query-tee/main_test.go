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
	routes := mimirReadRoutes(Config{PathPrefix: ""})
	for _, r := range routes {
		assert.True(t, strings.HasPrefix(r.Path, "/api/v1/"))
	}

	routes = mimirReadRoutes(Config{PathPrefix: "/some/random/prefix///"})
	for _, r := range routes {
		assert.True(t, strings.HasPrefix(r.Path, "/some/random/prefix/api/v1/"))
	}
}
