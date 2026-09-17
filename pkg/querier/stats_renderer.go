// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"

	promql_stats "github.com/prometheus/prometheus/util/stats"

	"github.com/grafana/mimir/pkg/querier/stats"
)

func StatsRenderer(ctx context.Context, s *promql_stats.Statistics, param string) promql_stats.QueryStats {
	mimirStats := stats.FromContext(ctx)
	if mimirStats != nil && s != nil {
		mimirStats.AddSamplesProcessed(uint64(s.Samples.TotalSamples))
	}
	// Mimir doesn't use stats from Prometheus Response in favour of mimirStats above.
	return nil
}
