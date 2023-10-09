// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/errors.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package validation

import (
	"fmt"
	"time"

	"github.com/grafana/mimir/pkg/util/globalerror"
)

func NewMaxQueryLengthError(actualQueryLen, maxQueryLength time.Duration) LimitError {
	return LimitError(globalerror.MaxQueryLength.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("the query time range exceeds the limit (query length: %s, limit: %s)", actualQueryLen, maxQueryLength),
		maxPartialQueryLengthFlag))
}

func NewMaxTotalQueryLengthError(actualQueryLen, maxTotalQueryLength time.Duration) LimitError {
	return LimitError(globalerror.MaxTotalQueryLength.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("the total query time range exceeds the limit (query length: %s, limit: %s)", actualQueryLen, maxTotalQueryLength),
		maxTotalQueryLengthFlag))
}

func NewMaxQueryExpressionSizeBytesError(actualSizeBytes, maxQuerySizeBytes int) LimitError {
	return LimitError(globalerror.MaxQueryExpressionSizeBytes.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("the raw query size in bytes exceeds the limit (query size: %d, limit: %d)", actualSizeBytes, maxQuerySizeBytes),
		maxQueryExpressionSizeBytesFlag))
}

func NewQueryBlockedError() LimitError {
	return LimitError(globalerror.QueryBlocked.Message("the request has been blocked by the cluster administrator"))
}
