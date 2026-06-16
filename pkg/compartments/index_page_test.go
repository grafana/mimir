// SPDX-License-Identifier: AGPL-3.0-only

package compartments

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewIndexPageHandler(t *testing.T) {
	const numCompartments = 3

	rec := httptest.NewRecorder()
	NewIndexPageHandler("Partitions Ring Status", "partition-ring/compartment-<compartment-id>", numCompartments).
		ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	res := rec.Result()
	require.Equal(t, http.StatusOK, res.StatusCode)
	assert.Equal(t, "text/html; charset=utf-8", res.Header.Get("Content-Type"))

	body := rec.Body.String()
	assert.Contains(t, body, "Partitions Ring Status")

	// One link per compartment with the placeholder replaced by the compartment ID, and no extra one.
	assert.Contains(t, body, `<a href="partition-ring/compartment-0">Compartment 0</a>`)
	assert.Contains(t, body, `<a href="partition-ring/compartment-1">Compartment 1</a>`)
	assert.Contains(t, body, `<a href="partition-ring/compartment-2">Compartment 2</a>`)
	assert.NotContains(t, body, "compartment-3")
}
