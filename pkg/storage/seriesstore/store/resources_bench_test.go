// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/common/model"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

// BenchmarkEngineResourcesAppend is the steady-state cost of appending a round of samples to every series through
// records, with their resources kept or not: the engine reads the same records either way. A record is one target's
// series, which share its resource of about a kilobyte, like an OTel SDK's with a few dozen attributes.
func BenchmarkEngineResourcesAppend(b *testing.B) {
	const (
		numSeries         = 1_200
		seriesPerResource = 12
		descriptive       = 24
	)
	var series []promlabels.Labels
	for n := range numSeries {
		series = append(series, promlabels.FromStrings(model.MetricNameLabel, fmt.Sprintf("metric_%d", n%seriesPerResource), "instance", fmt.Sprintf("instance-%d", n/seriesPerResource)))
	}
	resources := make([][]mimirpb.ResourceAttribute, numSeries/seriesPerResource)
	for r := range resources {
		resources[r] = []mimirpb.ResourceAttribute{
			{Key: "service.name", StringValue: fmt.Sprintf("service-%d", r%10)},
			{Key: "service.namespace", StringValue: "namespace"},
			{Key: "service.instance.id", StringValue: fmt.Sprintf("instance-%d", r)},
		}
		for i := range descriptive {
			resources[r] = append(resources[r], mimirpb.ResourceAttribute{Key: fmt.Sprintf("attribute.%d", i), StringValue: fmt.Sprintf("value-%d-%012d", i, r)})
		}
	}
	records := func(at int64) [][]byte {
		var out [][]byte
		for r, attributes := range resources {
			out = append(out, recordContent(b, resourceRequest(at, attributes, series[r*seriesPerResource:(r+1)*seriesPerResource]...)))
		}
		return out
	}

	for _, withResources := range []bool{false, true} {
		b.Run(fmt.Sprintf("resources=%t", withResources), func(b *testing.B) {
			e, err := OpenEngine("", "tenant", EngineOptions{Shards: 16, SecondaryHashFunction: secondaryHash})
			require.NoError(b, err)
			b.Cleanup(func() { _ = e.Close() })
			e.SetNativeResources(withResources)
			// The appender creates the series with their resources: a record's path appends only to series the
			// engine has, and leaves the others to the caller.
			app := e.Appender(context.Background())
			for n, l := range series {
				_, err := app.Append(0, l, 0, 1_000, 1, nil, nil, storage.AppendV2Options{Resource: asResourceContext(resources[n/seriesPerResource])})
				require.NoError(b, err)
			}
			require.NoError(b, app.Commit())
			ingested := 0
			for _, content := range records(2_000) {
				record, err := ReadRW2(content, ingest.V2CommonSymbols.GetSlice(), ingest.V2RecordSymbolOffset)
				require.NoError(b, err)
				leftover, n, err := e.AppendRW2(record, 0, 1<<62, 0, false, &rw2Sink{})
				require.NoError(b, err)
				require.Empty(b, leftover)
				ingested += n
				record.Release()
			}
			require.Equal(b, numSeries, ingested, "every series takes the record's path")

			b.ReportAllocs()
			for i := 0; b.Loop(); i++ {
				b.StopTimer()
				contents := records(int64(3_000 + i*1_000))
				b.StartTimer()
				appendRecords(b, e, contents)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*numSeries), "ns/series")
		})
	}
}
