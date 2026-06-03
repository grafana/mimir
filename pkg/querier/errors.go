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
	errBadLookbackConfigs              = fmt.Errorf("the -%s setting must be greater than -%s otherwise queries might return partial results", validation.QueryIngestersWithinFlag, queryStoreAfterFlag)
	errStreamingIngesterBufferSize     = fmt.Errorf("the -%s setting must be greater than 0", streamingChunksPerIngesterBufferSizeFlag)
	errStreamingStoreGatewayBufferSize = fmt.Errorf("the -%s setting must be greater than 0", streamingChunksPerStoreGatewayBufferSizeFlag)
	errEmptyTimeRange                  = errors.New("empty time range")
)

func NewMaxQueryLengthError(actualQueryLen, maxQueryLength time.Duration) validation.LimitError {
	return validation.NewLimitError(globalerror.MaxQueryLength.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("the query time range exceeds the limit (query length: %s, limit: %s)", actualQueryLen, maxQueryLength),
		validation.MaxPartialQueryLengthFlag))
}

// MaxLimitError is the typed clamp warning surfaced when a request limit
// is reduced by a per-tenant ceiling. Callers that need to react to the
// clamp specifically — e.g. the streaming search handler deciding
// has_more — type-assert via errors.As so they can read Flag without
// re-parsing the message. The embedded validation.LimitError keeps
// validation.IsLimitError working through errors.As.
type MaxLimitError struct {
	Flag       string
	Limit      int
	Enforced   int
	underlying validation.LimitError
}

func (e *MaxLimitError) Error() string { return e.underlying.Error() }
func (e *MaxLimitError) Unwrap() error { return e.underlying }

func NewMaxLimitError(limit, maxLimit int, flag string) error {
	return &MaxLimitError{
		Flag:     flag,
		Limit:    limit,
		Enforced: maxLimit,
		underlying: validation.NewLimitError(
			fmt.Sprintf("results may be truncated due to %s (requested limit: %d, enforced: %d)", flag, limit, maxLimit),
		),
	}
}
