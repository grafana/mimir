// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/validation"
)

var (
	errBadLookbackConfigs = fmt.Errorf("the -%s setting must be greater than -%s otherwise queries might return partial results", validation.QueryIngestersWithinFlag, queryStoreAfterFlag)
	errEmptyTimeRange     = errors.New("empty time range")
)

func NewMaxQueryLengthError(actualQueryLen, maxQueryLength time.Duration) validation.LimitError {
	return validation.NewLimitError(globalerror.MaxQueryLength.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("the query time range exceeds the limit (query length: %s, limit: %s)", actualQueryLen, maxQueryLength),
		validation.MaxPartialQueryLengthFlag))
}
