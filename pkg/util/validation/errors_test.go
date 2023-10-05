// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewMaxQueryLengthError(t *testing.T) {
	err := NewMaxQueryLengthError(time.Hour, time.Minute)
	assert.Equal(t, "the query time range exceeds the limit (query length: 1h0m0s, limit: 1m0s) (err-mimir-max-query-length). To adjust the related per-tenant limit, configure -querier.max-partial-query-length, or contact your service administrator.", err.Error())
}

func TestNewTotalMaxQueryLengthError(t *testing.T) {
	err := NewMaxTotalQueryLengthError(time.Hour, time.Minute)
	assert.Equal(t, "the total query time range exceeds the limit (query length: 1h0m0s, limit: 1m0s) (err-mimir-max-total-query-length). To adjust the related per-tenant limit, configure -query-frontend.max-total-query-length, or contact your service administrator.", err.Error())
}

func TestFormatRequestRateLimitedMessage(t *testing.T) {
	msg := FormatRequestRateLimitedMessage(10, 5)
	assert.Equal(t, "the request has been rejected because the tenant exceeded the request rate limit, set to 10 requests/s across all distributors with a maximum allowed burst of 5 (err-mimir-tenant-max-request-rate). To adjust the related per-tenant limits, configure -distributor.request-rate-limit and -distributor.request-burst-size, or contact your service administrator.", msg)
}

func TestFormatIngestionRateLimitedMessage(t *testing.T) {
	msg := FormatIngestionRateLimitedMessage(10, 5)
	assert.Equal(t, "the request has been rejected because the tenant exceeded the ingestion rate limit, set to 10 items/s with a maximum allowed burst of 5. This limit is applied on the total number of samples, exemplars and metadata received across all distributors (err-mimir-tenant-max-ingestion-rate). To adjust the related per-tenant limits, configure -distributor.ingestion-rate-limit and -distributor.ingestion-burst-size, or contact your service administrator.", msg)
}
