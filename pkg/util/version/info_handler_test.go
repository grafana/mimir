// SPDX-License-Identifier: AGPL-3.0-only

package version

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildInfoHandler(t *testing.T) {
	applicationName := "name"
	features := map[string]interface{}{
		"feature_1": "true",
		"feature_2": "false",
	}

	handler := BuildInfoHandler(applicationName, features)
	request, err := http.NewRequest("GET", "", nil)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Result().StatusCode)

	decoder := json.NewDecoder(recorder.Result().Body)
	var response BuildInfoResponse
	err = decoder.Decode(&response)
	require.NoError(t, err)

	expected := BuildInfoResponse{
		Status: "success",
		BuildInfo: BuildInfo{
			Application: applicationName,
			Version:     Version,
			Revision:    Revision,
			Branch:      Branch,
			GoVersion:   GoVersion,
			Features:    features,
		},
	}

	require.Equal(t, expected, response)
}
