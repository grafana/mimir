// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	querier_stats "github.com/grafana/mimir/pkg/querier/stats"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

// BenchmarkNativeInfo measures info() with native target_info against the join over the stored target_info, on both
// engines, for the final write-up's cost comparison. What it reports, and over which data and queries, was fixed
// before its first run: besides time and allocations per query, the samples it processed, the series and chunk bytes
// it fetched, and its lookups' response bytes and the series they visited. Phase 4a sets no threshold.
//
// The data is an hour of 50 targets' pushes every minute, each of 20 series, two partitions, and a descriptive change
// of every tenth target's resource halfway through.
func BenchmarkNativeInfo(b *testing.B) {
	const (
		targets        = 50
		seriesPerPush  = 20
		minutes        = 60
		changeAtMinute = 30
	)
	queries := []string{
		`info(requests)`,
		`sum by (host_name) (info(rate(requests[5m])))`,
	}

	for _, engine := range []string{mimir_tsdb.EnginePrometheus, mimir_tsdb.EngineSeriesstore} {
		b.Run("engine="+engine, func(b *testing.B) {
			limits := defaultLimitsTestConfig()
			limits.NativeResourcesEnabled = true
			h := newNativeInfoHarness(b, engine, 2, limits)
			base := time.Now().Truncate(time.Minute).Add(-(minutes + 10) * time.Minute)
			for minute := range minutes {
				md := pmetric.NewMetrics()
				for target := range targets {
					host := fmt.Sprintf("node-%d", target)
					if target%10 == 0 && minute >= changeAtMinute {
						host += "-moved"
					}
					rm := md.ResourceMetrics().AppendEmpty()
					attrs := rm.Resource().Attributes()
					attrs.PutStr("service.name", "api")
					attrs.PutStr("service.instance.id", fmt.Sprintf("instance-%d", target))
					attrs.PutStr("host.name", host)
					attrs.PutStr("k8s.namespace.name", "default")
					gauge := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
					gauge.SetName("requests")
					points := gauge.SetEmptyGauge().DataPoints()
					for s := range seriesPerPush {
						point := points.AppendEmpty()
						point.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(minute) * time.Minute)))
						point.SetDoubleValue(float64(minute))
						point.Attributes().PutStr("pod", fmt.Sprintf("pod-%d", s))
					}
				}
				h.pushOTLP(b, md)
			}
			start, end := base, base.Add((minutes-1)*time.Minute)
			// Once the ingesters have caught up, the queries don't wait for them.
			h.rangeQuery(b, false, `count(requests)`, end, end, time.Minute)

			for _, query := range queries {
				for _, mode := range []string{"join", "native"} {
					b.Run(fmt.Sprintf("mode=%s/query=%s", mode, query), func(b *testing.B) {
						b.ReportAllocs()
						native := mode == "native"
						var samples int64
						var fetchedSeries, chunkBytes, lookupBytes, seriesVisited uint64
						for b.Loop() {
							queryStats, ctx := querier_stats.ContextWithEmptyStats(h.eventualQueryContext(native))
							q, err := h.engine.NewRangeQuery(ctx, h.queryable, nil, query, start, end, time.Minute)
							require.NoError(b, err)
							res := q.Exec(ctx)
							require.NoError(b, res.Err)
							if s := q.Stats(); s != nil {
								samples += s.Samples.TotalSamples
							}
							q.Close()
							fetchedSeries += queryStats.LoadFetchedSeries()
							chunkBytes += queryStats.LoadFetchedChunkBytes()
							lookupBytes += queryStats.LoadNativeLookupResponseBytes()
							seriesVisited += queryStats.LoadNativeLookupSeriesVisited()
						}
						n := float64(b.N)
						b.ReportMetric(float64(samples)/n, "samples/op")
						b.ReportMetric(float64(fetchedSeries)/n, "fetched-series/op")
						b.ReportMetric(float64(chunkBytes)/n, "chunk-bytes/op")
						b.ReportMetric(float64(lookupBytes)/n, "lookup-bytes/op")
						b.ReportMetric(float64(seriesVisited)/n, "lookup-series-visited/op")
						if native {
							require.Positive(b, h.synthesized(b), "native runs synthesize target_info")
						}
						require.Zero(b, h.fallbacks(b))
					})
				}
			}
		})
	}
}
