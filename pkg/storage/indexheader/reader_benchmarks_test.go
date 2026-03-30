// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketcache"
	"github.com/grafana/mimir/pkg/util/test"
)

func BenchmarkLookupSymbol(b *testing.B) {
	ctx := context.Background()

	bucketDir := b.TempDir()
	bkt, err := filesystem.NewBucket(filepath.Join(bucketDir, "bkt"))
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, bkt.Close())
	})

	// TODO: are the number of name and value symbols representative?
	nameSymbols := generateSymbols("name", 20)
	valueSymbols := generateSymbols("value", 1000)
	idIndexV2, err := block.CreateBlock(ctx, bucketDir, generateLabels(nameSymbols, valueSymbols), 100, 0, 1000, labels.FromStrings("ext1", "1"))
	require.NoError(b, err)
	_, err = block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(bucketDir, idIndexV2.String()), nil)
	require.NoError(b, err)

	indexName := filepath.Join(bucketDir, idIndexV2.String(), block.IndexHeaderFilename)
	require.NoError(b, WriteBinary(ctx, bkt, idIndexV2, indexName))

	// TODO: are these sensible values for parallelism?
	for _, parallelism := range []int{1, 2, 4, 8, 20, 100} {
		// TODO: are these sensible value for name lookup percentage?
		for _, percentageNameLookups := range []int{20, 40, 50, 60, 80} {
			b.Run(fmt.Sprintf("NameLookups%v%%-Parallelism%v", percentageNameLookups, parallelism), func(b *testing.B) {
				benchmarkLookupSymbol(ctx, b, bucketDir, idIndexV2, parallelism, percentageNameLookups, nameSymbols, valueSymbols)
			})
		}
	}
}

func benchmarkLookupSymbol(ctx context.Context, b *testing.B, bucketDir string, id ulid.ULID, parallelism int, percentageNameLookups int, nameSymbols []string, valueSymbols []string) {
	binaryReader, err := NewStreamBinaryReader(ctx, id, nil, bucketDir, Config{}, 32, log.NewNopLogger(), NewStreamBinaryReaderMetrics(nil))
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, binaryReader.Close()) })

	nameIndices, nameMap := reverseLookup(b, binaryReader, nameSymbols)
	valueIndices, valueMap := reverseLookup(b, binaryReader, valueSymbols)

	b.SetParallelism(parallelism)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		// Use our own random source to avoid contention for the global random number generator.
		random := rand.New(rand.NewSource(time.Now().UnixNano()))
		count := random.Int()

		for pb.Next() {
			var indices []uint32
			var indicesToSymbol map[uint32]string

			if count%100 < percentageNameLookups {
				indices = nameIndices
				indicesToSymbol = nameMap
			} else {
				indices = valueIndices
				indicesToSymbol = valueMap
			}

			index := indices[random.Intn(len(indices))]
			expectedSymbol := indicesToSymbol[index]
			actualSymbol, err := binaryReader.LookupSymbol(context.Background(), index)

			// Why do we wrap require.NoError or require.Equal in an if block here? These methods perform some synchronisation
			// that ends up dominating the benchmark, so we only want to call them if they're needed.
			if err != nil {
				require.NoError(b, err)
			}

			if actualSymbol != expectedSymbol {
				require.Equal(b, expectedSymbol, actualSymbol)
			}

			count++
		}
	})
}

func BenchmarkLabelNames(b *testing.B) {
	ctx := context.Background()

	bucketDir := b.TempDir()
	bkt, err := filesystem.NewBucket(filepath.Join(bucketDir, "bkt"))
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, bkt.Close())
	})

	for _, nameCount := range []int{20, 50, 100, 200} {
		for _, valueCount := range []int{100, 500, 1000} {
			nameSymbols := generateSymbols("name", nameCount)
			valueSymbols := generateSymbols("value", valueCount)
			idIndexV2, err := block.CreateBlock(ctx, bucketDir, generateLabels(nameSymbols, valueSymbols), 100, 0, 1000, labels.FromStrings("ext1", "1"))
			require.NoError(b, err)

			blockDir := filepath.Join(bucketDir, idIndexV2.String())
			_, err = block.Upload(ctx, log.NewNopLogger(), bkt, blockDir, nil)
			require.NoError(b, err)

			indexName := filepath.Join(bucketDir, idIndexV2.String(), block.IndexHeaderFilename)
			require.NoError(b, WriteBinary(ctx, bkt, idIndexV2, indexName))

			binaryReader, err := NewStreamBinaryReader(ctx, idIndexV2, objstore.WithNoopInstr(bkt), blockDir, Config{}, 32, log.NewNopLogger(), NewStreamBinaryReaderMetrics(nil))
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, binaryReader.Close()) })

			slices.Sort(nameSymbols)

			b.ResetTimer()
			b.Run(fmt.Sprintf("%vNames%vValues", nameCount, valueCount), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					actualNames, err := binaryReader.LabelNames(ctx)

					require.NoError(b, err)
					require.Equal(b, nameSymbols, actualNames)
				}

			})
		}
	}
}

func BenchmarkLabelValuesOffsetsIndexV2(b *testing.B) {
	ctx := context.Background()

	dir := b.TempDir()
	bkt, err := filesystem.NewBucket(filepath.Join(dir, "bkt"))
	require.NoError(b, err)
	instrBkt := objstore.WithNoopInstr(bkt)
	b.Cleanup(func() {
		require.NoError(b, bkt.Close())
	})

	for _, nameCount := range []int{50, 100, 200} {
		for _, valueCount := range []int{100, 500, 1000, 5000} {
			nameSymbols := generateSymbols("name", nameCount)
			valueSymbols := generateSymbols("value", valueCount)
			blockID, err := block.CreateBlock(ctx, dir, generateLabels(nameSymbols, valueSymbols), 100, 0, 1000, labels.FromStrings("ext1", "1"))
			require.NoError(b, err)
			_, err = block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(dir, blockID.String()), nil)
			require.NoError(b, err)

			indexName := filepath.Join(dir, blockID.String(), block.IndexHeaderFilename)
			require.NoError(b, WriteBinary(ctx, bkt, blockID, indexName))

			bucketReg := prometheus.NewPedanticRegistry()

			// Initialize the first index-header reader,
			// configured to read all index-header sections from the on-disk index-header.
			// This first call to create a reader also builds the index-header and sparse index-header from the block.
			diskReaderCfg := Config{}
			diskReader, err := NewStreamBinaryReader(ctx, blockID, instrBkt, dir, diskReaderCfg, 32, log.NewNopLogger(), NewStreamBinaryReaderMetrics(nil))
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, diskReader.Close()) })

			// Initialize the second index-header reader,
			// configured to read symbols from disk and postings offsets from bucket.
			bucketCacheCfg := bucketcache.NewCachingBucketConfig() // Caches nothing by default
			cachingBucket, err := bucketcache.NewCachingBucket("test", instrBkt, bucketCacheCfg, log.NewNopLogger(), bucketReg)
			require.NoError(b, err)

			splitReaderCfg := Config{
				BucketReader: BucketReaderConfig{
					Enabled:             true,
					BucketIndexSections: SectionPostingsOffsetsTable,
				},
			}
			splitReader, err := NewStreamBinaryReader(ctx, blockID, cachingBucket, dir, splitReaderCfg, 32, log.NewNopLogger(), NewStreamBinaryReaderMetrics(nil))
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, splitReader.Close()) })

			labelNames, err := diskReader.LabelNames(ctx)
			require.NoError(b, err)

			// Check that disk & bucket readers got the same label names before proceeding
			bucketLabelNames, err := splitReader.LabelNames(ctx)
			require.NoError(b, err)
			require.Equal(b, labelNames, bucketLabelNames)

			rand.Shuffle(len(labelNames), func(i, j int) {
				labelNames[i], labelNames[j] = labelNames[j], labelNames[i]
			})

			readers := []struct {
				name string
				Reader
			}{
				{"disk", diskReader},
				{"split", splitReader},
			}

			b.ResetTimer()
			for _, reader := range readers {
				b.Run(fmt.Sprintf("Names=%d/Values=%d/Reader=%s", len(nameSymbols), len(valueSymbols), reader.name), func(b *testing.B) {
					b.ReportAllocs()

					baselineMetrics := test.RecordBucketMetrics(b, bucketReg, []string{"get_range"})

					for i := 0; i < b.N; i++ {
						name := labelNames[i%len(labelNames)]

						values, err := reader.LabelValuesOffsets(ctx, name, "", func(string) bool {
							return true
						})
						require.NoError(b, err)
						require.NotEmpty(b, values)
					}

					metricsDiff := test.RecordBucketMetricsDiff(b, bucketReg, []string{"get_range"}, baselineMetrics)
					test.ReportBucketMetrics(b, metricsDiff)
				})
			}
		}
	}
}

func BenchmarkLabelValuesOffsetsIndexV2_WithPrefix(b *testing.B) {
	ctx := context.Background()
	tests, blockID, blockDir, bkt := labelValuesTestCases(test.NewTB(b))

	bucketReg := prometheus.NewPedanticRegistry()
	instrBkt := objstore.WrapWithMetrics(objstore.WithNoopInstr(bkt), prometheus.WrapRegistererWithPrefix("thanos_", bucketReg), "")
	b.Cleanup(func() {
		require.NoError(b, bkt.Close())
	})

	// Initialize the first index-header reader,
	// configured to read all index-header sections from the on-disk index-header.
	// This first call to create a reader also builds the index-header and sparse index-header from the block.
	diskReaderCfg := Config{}
	diskReader, err := NewStreamBinaryReader(ctx, blockID, instrBkt, blockDir, diskReaderCfg, 32, log.NewNopLogger(), NewStreamBinaryReaderMetrics(nil))
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, diskReader.Close()) })

	// Initialize the second index-header reader,
	// configured to read symbols from disk and postings offsets from bucket.
	bucketCacheCfg := bucketcache.NewCachingBucketConfig() // Caches nothing by default
	cachingBucket, err := bucketcache.NewCachingBucket("test", instrBkt, bucketCacheCfg, log.NewNopLogger(), bucketReg)
	require.NoError(b, err)

	splitReaderCfg := Config{
		BucketReader: BucketReaderConfig{
			Enabled:             true,
			BucketIndexSections: SectionPostingsOffsetsTable,
		},
	}
	splitReader, err := NewStreamBinaryReader(ctx, blockID, cachingBucket, blockDir, splitReaderCfg, 32, log.NewNopLogger(), NewStreamBinaryReaderMetrics(nil))
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, splitReader.Close()) })

	diskNames, err := diskReader.LabelNames(ctx)
	require.NoError(b, err)
	bucketReaderNames, err := splitReader.LabelNames(ctx)
	require.NoError(b, err)
	require.Equal(b, diskNames, bucketReaderNames)

	readers := []struct {
		name string
		Reader
	}{
		{"disk", diskReader},
		{"split", splitReader},
	}

	b.ResetTimer()

	for lbl, tcs := range tests {
		for _, tc := range tcs {
			for _, reader := range readers {
				b.Run(fmt.Sprintf("Label=%s/Prefix='%s'/Desc=%s/Reader=%s", lbl, tc.prefix, tc.desc, reader.name), func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						baselineMetrics := test.RecordBucketMetrics(b, bucketReg, []string{"get_range"})

						values, err := reader.LabelValuesOffsets(context.Background(), lbl, tc.prefix, tc.filter)
						require.NoError(b, err)
						require.Equal(b, tc.expected, len(values))

						metricsDiff := test.RecordBucketMetricsDiff(b, bucketReg, []string{"get_range"}, baselineMetrics)
						test.ReportBucketMetrics(b, metricsDiff)
					}
				})
			}
		}
	}
}

func BenchmarkPostingsOffset(b *testing.B) {
	ctx := context.Background()

	dir := b.TempDir()
	bkt, err := filesystem.NewBucket(filepath.Join(dir, "bkt"))
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, bkt.Close())
	})

	nameCount := 20

	for _, valueCount := range []int{100, 500, 1000} {
		nameSymbols := generateSymbols("name", nameCount)
		valueSymbols := generateSymbols("value", valueCount)
		idIndexV2, err := block.CreateBlock(ctx, dir, generateLabels(nameSymbols, valueSymbols), 100, 0, 1000, labels.FromStrings("ext1", "1"))
		require.NoError(b, err)
		_, err = block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(dir, idIndexV2.String()), nil)
		require.NoError(b, err)

		indexName := filepath.Join(dir, idIndexV2.String(), block.IndexHeaderFilename)
		require.NoError(b, WriteBinary(ctx, bkt, idIndexV2, indexName))

		b.Run(fmt.Sprintf("%vNames%vValues", nameCount, valueCount), func(b *testing.B) {
			binaryReader, err := NewStreamBinaryReader(ctx, idIndexV2, objstore.WithNoopInstr(bkt), dir, Config{}, 32, log.NewNopLogger(), NewStreamBinaryReaderMetrics(nil))
			require.NoError(b, err)
			b.Cleanup(func() { require.NoError(b, binaryReader.Close()) })

			// Use our own random source to avoid contention for the global random number generator.
			random := rand.New(rand.NewSource(time.Now().UnixNano()))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				name := nameSymbols[random.Intn(nameCount)]
				value := valueSymbols[random.Intn(valueCount)]
				offset, err := binaryReader.PostingsOffset(ctx, name, value)

				require.NoError(b, err)
				require.NotZero(b, offset.Start)
				require.NotZero(b, offset.End)
			}
		})
	}
}

func BenchmarkNewStreamBinaryReader(b *testing.B) {
	ctx := context.Background()

	bucketDir := b.TempDir()
	bkt, err := filesystem.NewBucket(filepath.Join(bucketDir, "bkt"))
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, bkt.Close())
	})

	for _, nameCount := range []int{20, 50, 100, 200} {
		for _, valueCount := range []int{1, 10, 100, 500, 1000, 5000} {
			nameSymbols := generateSymbols("name", nameCount)
			valueSymbols := generateSymbols("value", valueCount)
			idIndexV2, err := block.CreateBlock(ctx, bucketDir, generateLabels(nameSymbols, valueSymbols), 100, 0, 1000, labels.FromStrings("ext1", "1"))
			require.NoError(b, err)

			blockDir := filepath.Join(bucketDir, idIndexV2.String())
			_, err = block.Upload(ctx, log.NewNopLogger(), bkt, blockDir, nil)
			require.NoError(b, err)

			indexName := filepath.Join(bucketDir, idIndexV2.String(), block.IndexHeaderFilename)
			require.NoError(b, WriteBinary(ctx, bkt, idIndexV2, indexName))

			b.Run(fmt.Sprintf("%vNames%vValues", nameCount, valueCount), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					binaryReader, err := NewStreamBinaryReader(ctx, idIndexV2, objstore.WithNoopInstr(bkt), blockDir, Config{}, 32, log.NewNopLogger(), NewStreamBinaryReaderMetrics(nil))
					require.NoError(b, err)
					b.Cleanup(func() { require.NoError(b, binaryReader.Close()) })
				}
			})
		}
	}
}

func generateSymbols(prefix string, count int) []string {
	s := make([]string, 0, count)

	for idx := 0; idx < count; idx++ {
		s = append(s, fmt.Sprintf("%v-%v", prefix, idx))
	}

	return s
}

func generateLabels(names []string, values []string) []labels.Labels {
	l := make([]labels.Labels, 0, len(names)*len(values))

	for _, name := range names {
		for _, value := range values {
			l = append(l, labels.FromStrings(name, value))
		}
	}

	return l
}

func reverseLookup(b *testing.B, reader *StreamBinaryReader, symbols []string) ([]uint32, map[uint32]string) {
	i := make([]uint32, 0, len(symbols))
	m := make(map[uint32]string, len(symbols))

	for _, s := range symbols {
		idx, err := reader.symbolsTable.ReverseLookup(s)
		require.NoError(b, err)
		m[idx] = s
		i = append(i, idx)
	}

	return i, m
}
