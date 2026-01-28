// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourceAttributesHandler_ReturnsNotImplemented(t *testing.T) {
	handler := NewResourceAttributesHandler()

	request, err := http.NewRequest("GET", "/api/v1/resources", nil)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	// Should return 501 Not Implemented
	assert.Equal(t, http.StatusNotImplemented, recorder.Result().StatusCode)

	responseBody, err := io.ReadAll(recorder.Result().Body)
	require.NoError(t, err)

	// Verify the response contains an error message
	assert.Contains(t, string(responseBody), "resource attributes querying is not yet supported")
	assert.Contains(t, string(responseBody), `"status":"error"`)
}
