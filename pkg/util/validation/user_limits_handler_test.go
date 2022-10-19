// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
)

func TestUserLimitsHandler(t *testing.T) {
	var d model.Duration
	_ = d.Set("1d") // don't need to check that 1d is a correct value

	defaults := Limits{
		IngestionRate:                  100,
		IngestionBurstSize:             10,
		CompactorBlocksRetentionPeriod: d, // verify this is converted to second as int64
	}

	tenantLimits := make(map[string]*Limits)
	testLimits := defaults
	testLimits.IngestionRate = 200
	tenantLimits["test-with-override"] = &testLimits

	for _, tc := range []struct {
		name               string
		orgID              string
		expectedStatusCode int
		expectedLimits     UserLimitsResponse
	}{
		{
			name:               "Authenticated user with override",
			orgID:              "test-with-override",
			expectedStatusCode: http.StatusOK,
			expectedLimits: UserLimitsResponse{
				IngestionRate:                  200,
				IngestionBurstSize:             10,
				CompactorBlocksRetentionPeriod: 86400,
			},
		},
		{
			name:               "Authenticated user without override",
			orgID:              "test-no-override",
			expectedStatusCode: http.StatusOK,
			expectedLimits: UserLimitsResponse{
				IngestionRate:                  100,
				IngestionBurstSize:             10,
				CompactorBlocksRetentionPeriod: 86400,
			},
		},
		{
			name:               "Unauthenticated user",
			orgID:              "",
			expectedStatusCode: http.StatusUnauthorized,
			expectedLimits:     UserLimitsResponse{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {

			handler := UserLimitsHandler(defaults, NewMockTenantLimits(tenantLimits))
			request := httptest.NewRequest("GET", "/api/v1/user_limits", nil)
			if tc.orgID != "" {
				ctx := user.InjectOrgID(context.Background(), tc.orgID)
				request = request.WithContext(ctx)
			}

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			require.Equal(t, tc.expectedStatusCode, recorder.Result().StatusCode)

			if recorder.Result().StatusCode == http.StatusOK {
				var response UserLimitsResponse
				decoder := json.NewDecoder(recorder.Result().Body)
				require.NoError(t, decoder.Decode(&response))
				require.Equal(t, tc.expectedLimits, response)
			}
		})
	}
}
