// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"net/http"
	"time"

	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/util"
)

type UserLimitsResponse struct {
	CompactorBlocksRetentionPeriod int64 `json:"compactor_blocks_retention_period_seconds"` // suffix with second to make it explicit the value is in seconds

	// Write path limits
	IngestionRate             float64 `json:"ingestion_rate"`
	IngestionBurstSize        int     `json:"ingestion_burst_size"`
	IngestionBurstFactor      float64 `json:"ingestion_burst_factor"`
	MaxGlobalSeriesPerUser    int     `json:"max_global_series_per_user"`
	MaxGlobalSeriesPerMetric  int     `json:"max_global_series_per_metric"`
	MaxGlobalExemplarsPerUser int     `json:"max_global_exemplars_per_user"`

	// Read path limits
	MaxChunksPerQuery            int `json:"max_fetched_chunks_per_query"`
	MaxFetchedSeriesPerQuery     int `json:"max_fetched_series_per_query"`
	MaxFetchedChunkBytesPerQuery int `json:"max_fetched_chunk_bytes_per_query"`

	// Ruler limits
	RulerMaxRulesPerRuleGroup   int `json:"ruler_max_rules_per_rule_group"`
	RulerMaxRuleGroupsPerTenant int `json:"ruler_max_rule_groups_per_tenant"`
}

// UserLimitsHandler handles user limits.
func UserLimitsHandler(defaultLimits Limits, tenantLimits TenantLimits) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := tenant.TenantID(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		userLimits := tenantLimits.ByUserID(userID)
		if userLimits == nil {
			userLimits = &defaultLimits
		}

		limits := UserLimitsResponse{
			CompactorBlocksRetentionPeriod: int64(time.Duration(userLimits.CompactorBlocksRetentionPeriod).Seconds()),

			// Write path limits
			IngestionRate:             userLimits.IngestionRate,
			IngestionBurstSize:        userLimits.IngestionBurstSize,
			IngestionBurstFactor:      userLimits.IngestionBurstFactor,
			MaxGlobalSeriesPerUser:    userLimits.MaxGlobalSeriesPerUser,
			MaxGlobalSeriesPerMetric:  userLimits.MaxGlobalSeriesPerMetric,
			MaxGlobalExemplarsPerUser: userLimits.MaxGlobalExemplarsPerUser,

			// Read path limits
			MaxChunksPerQuery:            userLimits.MaxChunksPerQuery,
			MaxFetchedSeriesPerQuery:     userLimits.MaxFetchedSeriesPerQuery,
			MaxFetchedChunkBytesPerQuery: userLimits.MaxFetchedChunkBytesPerQuery,

			// Ruler limits
			RulerMaxRulesPerRuleGroup:   userLimits.RulerMaxRulesPerRuleGroup,
			RulerMaxRuleGroupsPerTenant: userLimits.RulerMaxRuleGroupsPerTenant,
		}

		util.WriteJSONResponse(w, limits)
	}
}
