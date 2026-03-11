// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/test"
)

func BenchmarkTSDBBuilder_CompactAndUpload(b *testing.B) {
	benchmarks := []struct {
		numSeries        int
		samplesPerSeries int
		genSparseHeaders bool
		useHistograms    bool
	}{
		{
			numSeries:        1_000,
			samplesPerSeries: 120,
			genSparseHeaders: false,
			useHistograms:    false,
		},
		{
			numSeries:        1_000,
			samplesPerSeries: 120,
			genSparseHeaders: true,
			useHistograms:    false,
		},
		{
			numSeries:        10_000,
			samplesPerSeries: 120,
			genSparseHeaders: false,
			useHistograms:    false,
		},
		{
			numSeries:        10_000,
			samplesPerSeries: 120,
			genSparseHeaders: true,
			useHistograms:    false,
		},
		{
			numSeries:        1_000,
			samplesPerSeries: 120,
			genSparseHeaders: false,
			useHistograms:    true,
		},
		{
			numSeries:        1_000,
			samplesPerSeries: 120,
			genSparseHeaders: true,
			useHistograms:    true,
		},
		{
			numSeries:        10_000,
			samplesPerSeries: 120,
			genSparseHeaders: false,
			useHistograms:    true,
		},
		{
			numSeries:        10_000,
			samplesPerSeries: 120,
			genSparseHeaders: true,
			useHistograms:    true,
		},
	}

	const (
		partitionID = int32(0)
		tenantID    = "1"
	)

	for _, bm := range benchmarks {
		name := fmt.Sprintf(
			"Series=%d/SamplesPerSeries=%d/UseHistograms=%t/GenSparseHeaders=%t",
			bm.numSeries, bm.samplesPerSeries, bm.useHistograms, bm.genSparseHeaders,
		)
		b.Run(name, func(b *testing.B) {
			config, overrides := blockBuilderConfig(b, "kafka:9092", nil)
			config.GenerateSparseIndexHeaders = bm.genSparseHeaders

			logger := log.NewNopLogger()
			registry := prometheus.NewPedanticRegistry()
			tsdbBuilderMetrics := newTSDBBuilderMetrics(registry)
			tsdbMetrics := mimir_tsdb.NewTSDBMetrics(registry, logger)
			builder := NewTSDBBuilder(partitionID, config, overrides, logger, tsdbBuilderMetrics, tsdbMetrics)
			benchmarkCompactAndUpload(b, builder, tenantID, bm.numSeries, bm.samplesPerSeries, bm.useHistograms)
		})
	}
}

func benchmarkCompactAndUpload(
	b *testing.B,
	builder *TSDBBuilder,
	tenantID string,
	numSeries,
	samplesPerSeries int,
	useHistograms bool,
) {

	processingRange := time.Hour.Milliseconds()
	lastEnd := 2 * processingRange
	startTs := lastEnd + 100

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()

		ctx := user.InjectOrgID(b.Context(), tenantID)

		// Generate and ingest samples
		for seriesID := 0; seriesID < numSeries; seriesID++ {
			var req mimirpb.WriteRequest

			if useHistograms {
				histograms := make([]mimirpb.Histogram, samplesPerSeries)
				for j := 0; j < samplesPerSeries; j++ {
					ts := startTs + int64(j*1000)
					histograms[j] = mimirpb.FromHistogramToHistogramProto(ts, test.GenerateTestHistogram(int(ts)))
				}
				req = createWriteRequest(fmt.Sprintf("bench_%d", seriesID), nil, histograms)
			} else {
				samples := make([]mimirpb.Sample, samplesPerSeries)
				for j := 0; j < samplesPerSeries; j++ {
					samples[j] = mimirpb.Sample{
						TimestampMs: startTs + int64(j*1000),
						Value:       float64(seriesID*samplesPerSeries + j),
					}
				}
				req = createWriteRequest(fmt.Sprintf("bench_%d", seriesID), samples, nil)
			}

			err := builder.PushToStorageAndReleaseRequest(ctx, &req)
			require.NoError(b, err)
		}

		shipperDir := b.TempDir()

		b.StartTimer()
		_, err := builder.CompactAndUpload(ctx, mockUploaderFunc(b, shipperDir))
		b.StopTimer()

		require.NoError(b, err)

		err = builder.Close()
		require.NoError(b, err)
	}
}
