// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQuerier_Errors(t *testing.T) {
	tests := map[string]struct {
		err              error
		expectedErrorMsg string
	}{
		"errBadLookbackConfigs has a correct message": {
			err:              errBadLookbackConfigs,
			expectedErrorMsg: "the -querier.query-ingesters-within setting must be greater than -querier.query-store-after otherwise queries might return partial results",
		},
		"errEmptyTimeRange has a correct message": {
			err:              errEmptyTimeRange,
			expectedErrorMsg: "empty time range",
		},
		"err-mimir-max-query-length has a correct message": {
			err:              NewMaxQueryLengthError(time.Hour, time.Minute),
			expectedErrorMsg: "the query time range exceeds the limit (query length: 1h0m0s, limit: 1m0s) (err-mimir-max-query-length). To adjust the related per-tenant limit, configure -querier.max-partial-query-length, or contact your service administrator.",
		},
	}
	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expectedErrorMsg, testData.err.Error())
		})
	}
}
