// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
	"fmt"

	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/validation"
)

var (
	maxSeriesHitMsgFormat = globalerror.MaxSeriesPerQuery.MessageWithStrategyAndPerTenantLimitConfig(
		"the query exceeded the maximum number of series (limit: %d series)",
		cardinalityStrategy,
		validation.MaxSeriesPerQueryFlag,
	)
	maxChunkBytesHitMsgFormat = globalerror.MaxChunkBytesPerQuery.MessageWithStrategyAndPerTenantLimitConfig(
		"the query exceeded the aggregated chunks size limit (limit: %d bytes)",
		cardinalityStrategy,
		validation.MaxChunkBytesPerQueryFlag,
	)
	maxChunksPerQueryLimitMsgFormat = globalerror.MaxChunksPerQuery.MessageWithStrategyAndPerTenantLimitConfig(
		"the query exceeded the maximum number of chunks (limit: %d chunks)",
		cardinalityStrategy,
		validation.MaxChunksPerQueryFlag,
	)
	maxEstimatedChunksPerQueryLimitMsgFormat = globalerror.MaxEstimatedChunksPerQuery.MessageWithStrategyAndPerTenantLimitConfig(
		"the estimated number of chunks for the query exceeded the maximum allowed (limit: %d chunks)",
		cardinalityStrategy,
		validation.MaxEstimatedChunksPerQueryMultiplierFlag,
	)
	maxEstimatedMemoryConsumptionPerQueryLimitMsgFormat = globalerror.MaxEstimatedMemoryConsumptionPerQuery.MessageWithStrategyAndPerTenantLimitConfig(
		"the query exceeded the maximum allowed estimated amount of memory consumed by a single query (limit: %d bytes)",
		cardinalityStrategy,
		validation.MaxEstimatedMemoryConsumptionPerQueryFlag,
	)
)

func limitError(format string, limit uint64) validation.LimitError {
	return validation.NewLimitError(fmt.Sprintf(format, limit))
}

func NewMaxSeriesHitLimitError(maxSeriesPerQuery uint64) validation.LimitError {
	return limitError(maxSeriesHitMsgFormat, maxSeriesPerQuery)
}

func NewMaxChunkBytesHitLimitError(maxChunkBytesPerQuery uint64) validation.LimitError {
	return limitError(maxChunkBytesHitMsgFormat, maxChunkBytesPerQuery)
}

func NewMaxChunksPerQueryLimitError(maxChunksPerQuery uint64) validation.LimitError {
	return limitError(maxChunksPerQueryLimitMsgFormat, maxChunksPerQuery)
}

func NewMaxEstimatedChunksPerQueryLimitError(maxEstimatedChunksPerQuery uint64) validation.LimitError {
	return limitError(maxEstimatedChunksPerQueryLimitMsgFormat, maxEstimatedChunksPerQuery)
}

func NewMaxEstimatedMemoryConsumptionPerQueryLimitError(maxEstimatedMemoryConsumptionPerQuery uint64) validation.LimitError {
	return limitError(maxEstimatedMemoryConsumptionPerQueryLimitMsgFormat, maxEstimatedMemoryConsumptionPerQuery)
}
