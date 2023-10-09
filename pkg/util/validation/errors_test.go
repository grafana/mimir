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
