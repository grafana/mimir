// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"

	promql_stats "github.com/prometheus/prometheus/util/stats"
	prom_api "github.com/prometheus/prometheus/web/api/v1"

	"github.com/grafana/mimir/pkg/querier/stats"
)

func StatsRenderer(ctx context.Context, s *promql_stats.Statistics, param string) promql_stats.QueryStats {
	mimirStats := stats.FromContext(ctx)
	if mimirStats != nil && s != nil {
		mimirStats.AddTotalSamples(uint64(s.Samples.TotalSamples))
	}
	return prom_api.DefaultStatsRenderer(ctx, s, param)
}
