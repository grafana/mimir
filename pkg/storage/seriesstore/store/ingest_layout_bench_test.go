// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"math/rand/v2"
	"slices"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

// BenchmarkIngestLayouts separates predictable series order, recurring scattered batches, and changing batches.
// The series count keeps random lookups outside the processor caches.
func BenchmarkIngestLayouts(b *testing.B) {
	const batch, prepared = 2000, 200
	series := realisticEnvInt("MIMIR_INGEST_SERIES", 2_000_000)
	for _, order := range []string{"ordered", "recurring", "grouped", "changing"} {
		b.Run(order, func(b *testing.B) {
			withoutRefLeaksInstrumentation(b)
			engine := rw2Engine(b, series)
			random := rand.New(rand.NewPCG(1, 2))
			picks := make([][]int, prepared)
			for i := range picks {
				picks[i] = make([]int, batch)
				for j := range picks[i] {
					if order == "ordered" {
						position, perMetric := i*batch+j, series/300
						picks[i][j] = position%perMetric*300 + position/perMetric%300
					} else {
						picks[i][j] = random.IntN(series)
					}
				}
				if order == "grouped" {
					slices.SortStableFunc(picks[i], func(a, b int) int { return a%300 - b%300 })
				}
			}
			serializer := ingest.RecordSerializerFromVersion(2)
			makeRecords := func(round int) [][]byte {
				out := make([][]byte, 0, prepared)
				for i, pick := range picks {
					req := &mimirpb.WriteRequest{}
					for _, n := range pick {
						if order == "changing" {
							n = random.IntN(series)
						}
						req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
							Labels:  mimirpb.FromLabelsToLabelAdapters(rw2Labels(n)),
							Samples: []mimirpb.Sample{{TimestampMs: int64(round*prepared+i+1) * 1000, Value: float64(n)}},
						}})
					}
					recs, _, err := serializer.ToRecords("ingest", 0, "tenant", req, 64<<20)
					require.NoError(b, err)
					require.Len(b, recs, 1)
					out = append(out, recs[0].Value)
				}
				return out
			}
			var records [][]byte
			sink := &rw2Sink{}
			// Two scrapes warm both the series and any cache admitted on a repeated layout.
			for round := range 2 {
				records = makeRecords(round)
				for _, content := range records {
					appendRW2(b, engine, content, sink)
				}
			}
			round := 2
			records = makeRecords(round)
			b.ReportAllocs()
			b.ResetTimer()
			cpuStart, cpuSeconds := processCPU(), 0.0
			for i := range b.N {
				if i > 0 && i%prepared == 0 {
					cpuSeconds += processCPU() - cpuStart
					b.StopTimer()
					round++
					records = makeRecords(round)
					b.StartTimer()
					cpuStart = processCPU()
				}
				left, count := appendRW2(b, engine, records[i%prepared], sink)
				if len(left) != 0 || count != batch {
					b.Fatalf("ingested %d of %d series; %d leftovers", count, batch, len(left))
				}
			}
			cpuSeconds += processCPU() - cpuStart
			b.StopTimer()
			b.ReportMetric(cpuSeconds*1e9/float64(b.N*batch), "cpu-ns/series")
			require.Zero(b, sink.errors)
			var cacheBytes int
			for _, shard := range engine.store.shards {
				if tenant, ok := shard.tenants[engine.tenantID]; ok && tenant.ingestLayouts != nil {
					cacheBytes += int(unsafe.Sizeof(ingestLayoutCache{})) + tenant.ingestLayouts.used*int(unsafe.Sizeof(refLocation{}))
				}
			}
			b.ReportMetric(float64(cacheBytes), "layout-B")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*batch), "ns/series")
		})
	}
}
