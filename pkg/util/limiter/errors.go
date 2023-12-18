// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
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
)

func NewMaxSeriesHitLimitError(maxSeriesPerQuery uint64) validation.LimitError {
	return validation.ErrorFunc(maxSeriesHitMsgFormat)(maxSeriesPerQuery)
}

func NewMaxChunkBytesHitLimitError(maxChunkBytesPerQuery uint64) validation.LimitError {
	return validation.ErrorFunc(maxChunkBytesHitMsgFormat)(maxChunkBytesPerQuery)
}

func NewMaxChunksPerQueryLimitError(maxChunksPerQuery uint64) validation.LimitError {
	return validation.ErrorFunc(maxChunksPerQueryLimitMsgFormat)(maxChunksPerQuery)
}

func NewMaxEstimatedChunksPerQueryLimitError(maxEstimatedChunksPerQuery uint64) validation.LimitError {
	return validation.ErrorFunc(maxEstimatedChunksPerQueryLimitMsgFormat)(maxEstimatedChunksPerQuery)
}
