// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueryMiddleware_Errors(t *testing.T) {
	tests := map[string]struct {
		err              error
		expectedErrorMsg string
	}{
		"err-mimir-max-total-query-length has a correct message": {
			err:              newMaxTotalQueryLengthError(time.Hour, time.Minute),
			expectedErrorMsg: "the total query time range exceeds the limit (query length: 1h0m0s, limit: 1m0s) (err-mimir-max-total-query-length). To adjust the related per-tenant limit, configure -query-frontend.max-total-query-length, or contact your service administrator.",
		},
		"err-mimir-max-query-expression-size-bytes has a correct message": {
			err:              newMaxQueryExpressionSizeBytesError(10, 20),
			expectedErrorMsg: "the raw query size in bytes exceeds the limit (query size: 10, limit: 20) (err-mimir-max-query-expression-size-bytes). To adjust the related per-tenant limit, configure -query-frontend.max-query-expression-size-bytes, or contact your service administrator.",
		},
		"err-mimir-query-blocked has a correct message": {
			err:              newQueryBlockedError(),
			expectedErrorMsg: "the request has been blocked by the cluster administrator (err-mimir-query-blocked)",
		},
	}
	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expectedErrorMsg, testData.err.Error())
		})
	}
}
