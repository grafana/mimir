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
		mimirStats.AddSamplesProcessedPerStep(totalSamplesPerStepPoints(s.Samples))
	}
	// Mimir doesn't use stats from Prometheus Response in favour of mimirStats above.
	return nil
}

// It's a copy of promql_stats.QuerySamples.totalSamplesPerStepPoints, returning a slice of querier querier/stats.StepStat
// instead of promql_stats.stepStat.
func totalSamplesPerStepPoints(qs *promql_stats.QuerySamples) []stats.StepStat {
	if !qs.EnablePerStepStats {
		return nil
	}

	ts := make([]stats.StepStat, len(qs.TotalSamplesPerStep))
	for i, c := range qs.TotalSamplesPerStep {
		ts[i] = stats.StepStat{Timestamp: qs.StartTimestamp + int64(i)*qs.Interval, Value: c}
	}
	return ts
}
