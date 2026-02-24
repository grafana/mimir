// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

// BenchmarkTSDBBuilder_CompactAndUpload benchmarks the complete flow of ingesting samples,
// compacting blocks, building sparse index-headers, and uploading.
func BenchmarkTSDBBuilder_CompactAndUpload(b *testing.B) {
	benchmarks := []struct {
		name                      string
		numSeries                 int
		samplesPerSeries          int
		sparseIndexHeaderSampling int
		useHistograms             bool
	}{
		{
			name:                      "1k_series_100_samples_no_sparse_headers",
			numSeries:                 1000,
			samplesPerSeries:          120,
			sparseIndexHeaderSampling: 0, // Disabled
			useHistograms:             false,
		},
		{
			name:                      "1k_series_100_samples_sparse_32",
			numSeries:                 1000,
			samplesPerSeries:          120,
			sparseIndexHeaderSampling: 32,
			useHistograms:             false,
		},
		{
			name:                      "10k_series_100_samples_no_sparse_headers",
			numSeries:                 10000,
			samplesPerSeries:          120,
			sparseIndexHeaderSampling: 0,
			useHistograms:             false,
		},
		{
			name:                      "10k_series_100_samples_sparse_32",
			numSeries:                 10000,
			samplesPerSeries:          120,
			sparseIndexHeaderSampling: 32,
			useHistograms:             false,
		},
		{
			name:                      "1k_histograms_100_samples_no_sparse_headers",
			numSeries:                 1000,
			samplesPerSeries:          120,
			sparseIndexHeaderSampling: 0, // Disabled
			useHistograms:             true,
		},
		{
			name:                      "1k_histograms_100_samples_sparse_32",
			numSeries:                 1000,
			samplesPerSeries:          120,
			sparseIndexHeaderSampling: 32,
			useHistograms:             true,
		},
		{
			name:                      "10k_histograms_100_samples_no_sparse_headers",
			numSeries:                 10000,
			samplesPerSeries:          120,
			sparseIndexHeaderSampling: 0, // Disabled
			useHistograms:             true,
		},
		{
			name:                      "10k_histograms_100_samples_sparse_32",
			numSeries:                 10000,
			samplesPerSeries:          120,
			sparseIndexHeaderSampling: 32,
			useHistograms:             true,
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			benchmarkCompactAndUpload(b, bm.numSeries, bm.samplesPerSeries, bm.sparseIndexHeaderSampling, bm.useHistograms)
		})
	}
}

func benchmarkCompactAndUpload(b *testing.B, numSeries, samplesPerSeries, sparseIndexHeaderSampling int, useHistograms bool) {
	const (
		partitionID = int32(0)
		userID      = "benchmark_user"
	)

	// Setup limits
	limits := map[string]*validation.Limits{
		userID: {
			OutOfOrderTimeWindow:             model.Duration(30 * time.Minute),
			NativeHistogramsIngestionEnabled: true,
		},
	}
	overrides := validation.NewOverrides(defaultLimitsTestConfig(), validation.NewMockTenantLimits(limits))

	processingRange := time.Hour.Milliseconds()
	lastEnd := 2 * processingRange
	startTs := lastEnd + 100

	// Reset the timer before the actual benchmark work
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Setup
		tmpDir := b.TempDir()
		shipperDir := b.TempDir()
		metrics := newTSDBBBuilderMetrics(prometheus.NewRegistry())
		builder := NewTSDBBuilder(log.NewNopLogger(), tmpDir, partitionID, mimir_tsdb.BlocksStorageConfig{}, overrides, metrics, 0, sparseIndexHeaderSampling)

		ctx := user.InjectOrgID(context.Background(), userID)

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

		b.StartTimer()

		// The actual work we're benchmarking: compact and upload
		_, err := builder.CompactAndUpload(ctx, mockUploaderFunc(b, shipperDir))

		b.StopTimer()

		require.NoError(b, err)

		// Cleanup
		err = builder.Close()
		require.NoError(b, err)
	}
}

// BenchmarkSparseIndexHeaderBuild specifically benchmarks just the sparse index-header building phase
func BenchmarkSparseIndexHeaderBuild(b *testing.B) {
	benchmarks := []struct {
		name         string
		numSeries    int
		samplingRate int
	}{
		{
			name:         "1k_series_sampling_32",
			numSeries:    1000,
			samplingRate: 32,
		},
		{
			name:         "1k_series_sampling_16",
			numSeries:    1000,
			samplingRate: 16,
		},
		{
			name:         "10k_series_sampling_32",
			numSeries:    10000,
			samplingRate: 32,
		},
		{
			name:         "10k_series_sampling_16",
			numSeries:    10000,
			samplingRate: 16,
		},
		{
			name:         "50k_series_sampling_32",
			numSeries:    50000,
			samplingRate: 32,
		},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			benchmarkSparseIndexHeaderBuild(b, bm.numSeries, bm.samplingRate)
		})
	}
}

func benchmarkSparseIndexHeaderBuild(b *testing.B, numSeries, samplingRate int) {
	const (
		partitionID      = int32(0)
		userID           = "benchmark_user"
		samplesPerSeries = 100
	)

	// Setup a builder and create a block with the specified number of series
	limits := map[string]*validation.Limits{
		userID: {
			NativeHistogramsIngestionEnabled: true,
		},
	}
	overrides := validation.NewOverrides(defaultLimitsTestConfig(), validation.NewMockTenantLimits(limits))

	tmpDir := b.TempDir()
	metrics := newTSDBBBuilderMetrics(prometheus.NewRegistry())
	builder := NewTSDBBuilder(log.NewNopLogger(), tmpDir, partitionID, mimir_tsdb.BlocksStorageConfig{}, overrides, metrics, 0, 0)
	defer builder.Close()

	ctx := user.InjectOrgID(context.Background(), userID)

	processingRange := time.Hour.Milliseconds()
	lastEnd := 2 * processingRange
	startTs := lastEnd + 100

	// Generate and ingest samples to create a block
	for seriesID := 0; seriesID < numSeries; seriesID++ {
		samples := make([]mimirpb.Sample, samplesPerSeries)
		for j := 0; j < samplesPerSeries; j++ {
			samples[j] = mimirpb.Sample{
				TimestampMs: startTs + int64(j*1000),
				Value:       float64(seriesID*samplesPerSeries + j),
			}
		}
		req := createWriteRequest(fmt.Sprintf("bench_%d", seriesID), samples, nil)
		err := builder.PushToStorageAndReleaseRequest(ctx, &req)
		require.NoError(b, err)
	}

	// Compact to create the block on disk
	shipperDir := b.TempDir()
	_, err := builder.CompactAndUpload(ctx, mockUploaderFunc(b, shipperDir))
	require.NoError(b, err)

	// Get the block directory
	files, err := os.ReadDir(shipperDir)
	require.NoError(b, err)
	require.Len(b, files, 1, "should have exactly one block")

	blockDir := path.Join(shipperDir, files[0].Name())

	// Read the block meta to get the ULID
	meta, err := block.ReadMetaFromDir(blockDir)
	require.NoError(b, err)

	// Now benchmark just the sparse index-header building
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Create a new builder with the specified sampling rate
		benchBuilder := NewTSDBBuilder(log.NewNopLogger(), tmpDir, partitionID, mimir_tsdb.BlocksStorageConfig{}, overrides, metrics, 0, samplingRate)

		// Remove any existing sparse header to force regeneration
		sparseHeaderPath := path.Join(blockDir, block.SparseIndexHeaderFilename)
		os.Remove(sparseHeaderPath)

		// Build the sparse index-header
		err := benchBuilder.prepareSparseIndexHeader(ctx, shipperDir, meta.ULID)
		require.NoError(b, err)

		benchBuilder.Close()
	}
}

//// mockUploaderFunc is a test helper that moves blocks to a destination directory
//func mockUploaderFunc(t testing.TB, destDir string) blockUploader {
//	return func(_ context.Context, _, dbDir string, metas []tsdb.BlockMeta) error {
//		for _, meta := range metas {
//			blockDir := path.Join(dbDir, meta.ULID.String())
//			err := os.Rename(blockDir, path.Join(destDir, path.Base(blockDir)))
//			require.NoError(t, err)
//		}
//		return nil
//	}
//}
//
//func defaultLimitsTestConfig() validation.Limits {
//	limits := validation.Limits{}
//	flagext.DefaultValues(&limits)
//	return limits
//}
