// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/mimir/pkg/storage/fixtures"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketcache"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
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
	br, err := NewStreamBinaryReader(ctx, log.NewNopLogger(), nil, bucketDir, id, 32, NewStreamBinaryReaderMetrics(nil), Config{})
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, br.Close())
	})

	nameIndices, nameMap := reverseLookup(b, br, nameSymbols)
	valueIndices, valueMap := reverseLookup(b, br, valueSymbols)

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
			actualSymbol, err := br.LookupSymbol(context.Background(), index)

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
			_, err = block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(bucketDir, idIndexV2.String()), nil)
			require.NoError(b, err)

			indexName := filepath.Join(bucketDir, idIndexV2.String(), block.IndexHeaderFilename)
			require.NoError(b, WriteBinary(ctx, bkt, idIndexV2, indexName))

			b.Run(fmt.Sprintf("%vNames%vValues", nameCount, valueCount), func(b *testing.B) {
				benchmarkReader(b, bucketDir, idIndexV2, func(b *testing.B, br Reader) {
					slices.Sort(nameSymbols)
					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						actualNames, err := br.LabelNames(ctx)

						require.NoError(b, err)
						require.Equal(b, nameSymbols, actualNames)
					}
				})
			})
		}
	}
}

func BenchmarkLabelValuesOffsetsIndexV1(b *testing.B) {
	ctx := context.Background()

	bucketDir := b.TempDir()
	bkt, err := filesystem.NewBucket(filepath.Join(bucketDir, "bkt"))
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, bkt.Close())
	})

	metaIndexV1, err := block.ReadMetaFromDir("./testdata/index_format_v1")
	require.NoError(b, err)
	test.Copy(b, "./testdata/index_format_v1", filepath.Join(bucketDir, metaIndexV1.ULID.String()))

	_, err = block.InjectThanosMeta(log.NewNopLogger(), filepath.Join(bucketDir, metaIndexV1.ULID.String()), block.ThanosMeta{
		Labels: labels.FromStrings("ext1", "1").Map(),
		Source: block.TestSource,
	}, &metaIndexV1.BlockMeta)

	require.NoError(b, err)
	_, err = block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(bucketDir, metaIndexV1.ULID.String()), nil)
	require.NoError(b, err)

	indexName := filepath.Join(bucketDir, metaIndexV1.ULID.String(), block.IndexHeaderFilename)
	require.NoError(b, WriteBinary(ctx, bkt, metaIndexV1.ULID, indexName))

	benchmarkReader(b, bucketDir, metaIndexV1.ULID, func(b *testing.B, br Reader) {
		names, err := br.LabelNames(ctx)
		require.NoError(b, err)

		rand.Shuffle(len(names), func(i, j int) {
			names[i], names[j] = names[j], names[i]
		})

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			name := names[i%len(names)]

			values, err := br.LabelValuesOffsets(ctx, name, "", func(string) bool {
				return true
			})

			require.NoError(b, err)
			require.NotEmpty(b, values)
		}
	})
}

func BenchmarkLabelValuesOffsetsIndexV2(b *testing.B) {
	ctx := context.Background()

	bucketDir := b.TempDir()
	bkt, err := filesystem.NewBucket(filepath.Join(bucketDir, "bkt"))
	require.NoError(b, err)
	instBkt := objstore.WithNoopInstr(bkt)
	b.Cleanup(func() {
		require.NoError(b, bkt.Close())
	})

	nameSymbols := generateSymbols("name", 10)
	valueSymbols := generateSymbols("value", 100)
	idIndexV2, err := block.CreateBlock(ctx, bucketDir, generateLabels(nameSymbols, valueSymbols), 100, 0, 1000, labels.FromStrings("ext1", "1"))
	require.NoError(b, err)
	_, err = block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(bucketDir, idIndexV2.String()), nil)
	require.NoError(b, err)

	indexName := filepath.Join(bucketDir, idIndexV2.String(), block.IndexHeaderFilename)
	require.NoError(b, WriteBinary(ctx, bkt, idIndexV2, indexName))

	diskReader, err := NewStreamBinaryReader(ctx, log.NewNopLogger(), instBkt, bucketDir, idIndexV2, 32, NewStreamBinaryReaderMetrics(nil), Config{})
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, diskReader.Close()) })

	bucketReader, err := NewBucketBinaryReader(ctx, log.NewNopLogger(), instBkt, bucketDir, idIndexV2, 32, Config{})
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, bucketReader.Close()) })

	diskNames, err := diskReader.LabelNames(ctx)
	require.NoError(b, err)
	bucketReaderNames, err := bucketReader.LabelNames(ctx)
	require.NoError(b, err)
	require.Equal(b, diskNames, bucketReaderNames)

	rand.Shuffle(len(diskNames), func(i, j int) {
		diskNames[i], diskNames[j] = diskNames[j], diskNames[i]
	})

	readers := map[string]Reader{
		"disk":   diskReader,
		"bucket": bucketReader,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for readerName, reader := range readers {
		b.Run(fmt.Sprintf("Reader=%s/Names=%d/Values=%d", readerName, len(nameSymbols), len(valueSymbols)), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				name := diskNames[i%len(diskNames)]

				values, err := reader.LabelValuesOffsets(ctx, name, "", func(string) bool {
					return true
				})

				require.NoError(b, err)
				require.NotEmpty(b, values)

				bufStats := reader.BufReaderStats()
				b.ReportMetric(float64(bufStats.BytesDiscarded.Load())/float64(b.N), "buffered-bytes-discarded/op")

			}
		})
	}
}

func BenchmarkExpandedLabelValuesOffsetsIndexV2(b *testing.B) {
	ctx := context.Background()
	const series = 5_000_000

	testBlock := fixtures.SetupTestBlock(b, fixtures.AppendTestSeries(series))
	blockID := testBlock.Meta.ULID
	testCases := fixtures.SeriesSelectorTestCases(b, series)

	indexHeaderFileName := filepath.Join(testBlock.StoreGatewayDir, blockID.String(), block.IndexHeaderFilename)
	require.NoError(b, WriteBinary(ctx, testBlock.Bkt, blockID, indexHeaderFileName))

	diskReader, err := NewStreamBinaryReader(
		ctx, log.NewNopLogger(),
		testBlock.Bkt, testBlock.BktDir, blockID,
		32, NewStreamBinaryReaderMetrics(nil), Config{},
	)
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, diskReader.Close()) })

	bucketReader, err := NewBucketBinaryReader(
		ctx, log.NewNopLogger(),
		testBlock.Bkt, testBlock.BktDir, blockID,
		32, Config{},
	)
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, bucketReader.Close()) })

	diskNames, err := diskReader.LabelNames(ctx)
	require.NoError(b, err)
	bucketReaderNames, err := bucketReader.LabelNames(ctx)
	require.NoError(b, err)
	require.Equal(b, diskNames, bucketReaderNames)

	fmt.Println("OK", diskNames)

	readers := map[string]Reader{
		"disk":   diskReader,
		"bucket": bucketReader,
	}

	b.ResetTimer()
	for readerName, reader := range readers {
		b.Run(fmt.Sprintf("Reader=%s", readerName), func(b *testing.B) {
			b.ReportAllocs()
			for _, testCase := range testCases {
				matchCount := 0
				for _, matcher := range testCase.Matchers {
					values, err := reader.LabelValuesOffsets(
						ctx, matcher.Name, "", matcher.Matches,
					)

					require.NoError(b, err)
					matchCount += len(values)
				}
				// TODO these cases aren't actually correct for this test
				//   because the index reader only solves one matcher at a time;
				//   the expected counts from the test are from the intersection of all matchers
				//require.Equal(
				//	b, testCase.ExpectedCount, matchCount,
				//	"wrong match count",
				//	"case", testCase.Name,
				//	"matchers", testCase.Matchers,
				//)
			}
		})
	}
}

func BenchmarkLabelValuesOffsetsIndexV2_WithPrefix(b *testing.B) {
	ctx := context.Background()
	tests, blockID, bucketDir, bkt := labelValuesTestCases(test.NewTB(b))

	instBkt := objstore.WithNoopInstr(bkt)
	b.Cleanup(func() {
		require.NoError(b, bkt.Close())
	})

	// We reuse cache between tests (!)
	cache := cache.NewMockCache()

	const cfgName = "index"
	subrangeSize := int64(16000)
	cfg := bucketcache.NewCachingBucketConfig()
	cfg.CacheGetRange(cfgName, cache, func(string) bool { return true }, subrangeSize, cache, time.Hour, time.Hour, 0)

	cacheBucketReg := prometheus.NewPedanticRegistry()
	cachingBucket, err := bucketcache.NewCachingBucket("test", instBkt, cfg, log.NewNopLogger(), cacheBucketReg)
	require.NoError(b, err)

	diskReader, err := NewStreamBinaryReader(ctx, log.NewNopLogger(), cachingBucket, bucketDir, blockID, 32, NewStreamBinaryReaderMetrics(nil), Config{})
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, diskReader.Close()) })

	bucketReader, err := NewBucketBinaryReader(ctx, log.NewNopLogger(), cachingBucket, bucketDir, blockID, 32, Config{})
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, bucketReader.Close()) })

	diskNames, err := diskReader.LabelNames(ctx)
	require.NoError(b, err)
	bucketReaderNames, err := bucketReader.LabelNames(ctx)
	require.NoError(b, err)
	require.Equal(b, diskNames, bucketReaderNames)

	readers := map[string]Reader{
		"disk":   diskReader,
		"bucket": bucketReader,
	}

	sortedTestLbls := make([]string, 0, len(tests))
	for k := range tests {
		sortedTestLbls = append(sortedTestLbls, k)
	}
	sort.Strings(sortedTestLbls)

	b.ResetTimer()
	b.ReportAllocs()
	for _, lbl := range sortedTestLbls {
		tcs := tests[lbl]
		for readerName, _ := range readers {
			for _, tc := range tcs {
				b.Run(fmt.Sprintf("Reader=%s/Label=%s/Prefix='%s'/Desc=%s", readerName, lbl, tc.prefix, tc.desc), func(b *testing.B) {
					cacheBucketReg := prometheus.NewPedanticRegistry()
					cachingBucket, err := bucketcache.NewCachingBucket("test", instBkt, cfg, log.NewNopLogger(), cacheBucketReg)
					require.NoError(b, err)

					var reader Reader
					switch readerName {
					case "disk":
						r, err := NewStreamBinaryReader(ctx, log.NewNopLogger(), cachingBucket, bucketDir, blockID, 32, NewStreamBinaryReaderMetrics(nil), Config{})
						require.NoError(b, err)
						b.Cleanup(func() { require.NoError(b, r.Close()) })
						reader = r
					case "bucket":
						r, err := NewBucketBinaryReader(ctx, log.NewNopLogger(), cachingBucket, bucketDir, blockID, 32, Config{})
						require.NoError(b, err)
						b.Cleanup(func() { require.NoError(b, r.Close()) })
						reader = r
					default:
						b.Fatalf("unknown reader: %s", readerName)
					}

					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						startBytesDiscarded := reader.BufReaderStats().BytesDiscarded.Load()
						values, err := reader.LabelValuesOffsets(context.Background(), lbl, tc.prefix, tc.filter)
						require.NoError(b, err)
						require.Equal(b, tc.expected, len(values))

						endBytesDiscarded := reader.BufReaderStats().BytesDiscarded.Load()
						diff := endBytesDiscarded - startBytesDiscarded
						output := float64(diff)
						b.ReportMetric(output, "buffered-bytes-discarded/op")

						metrics, err := cacheBucketReg.Gather()
						require.NoError(b, err)

						promToBenchmarkMetrics := map[string]string{
							"thanos_store_bucket_cache_operation_requests_total":       "get-range-ops",
							"thanos_store_bucket_cache_operation_hits_total":           "get-range-hits",
							"thanos_store_bucket_cache_getrange_requested_bytes_total": "get-range-bytes-requested",
							"thanos_store_bucket_cache_getrange_fetched_bytes_total":   "get-range-bytes-fetched",
							"thanos_store_bucket_cache_getrange_refetched_bytes_total": "get-range-bytes-refetched",
						}
					MetricFamilyLoop:
						for _, mf := range metrics {
						MetricLoop:
							for _, m := range mf.GetMetric() {
								name := mf.GetName()

								benchMetricName, ok := promToBenchmarkMetrics[name]
								if !ok {
									continue MetricFamilyLoop
								}

								for _, l := range m.GetLabel() {
									lName, lValue := l.GetName(), l.GetValue()
									if lName == "operation" && lValue != "get_range" {
										continue MetricLoop
									}
								}

								value := m.GetCounter().GetValue()
								b.ReportMetric(value, benchMetricName+"/op")
							}
						}
					}
				})
			}
		}
	}
}

func BenchmarkPostingsOffset(b *testing.B) {
	ctx := context.Background()

	bucketDir := b.TempDir()
	bkt, err := filesystem.NewBucket(filepath.Join(bucketDir, "bkt"))
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, bkt.Close())
	})

	nameCount := 20

	for _, valueCount := range []int{100, 500, 1000} {
		nameSymbols := generateSymbols("name", nameCount)
		valueSymbols := generateSymbols("value", valueCount)
		idIndexV2, err := block.CreateBlock(ctx, bucketDir, generateLabels(nameSymbols, valueSymbols), 100, 0, 1000, labels.FromStrings("ext1", "1"))
		require.NoError(b, err)
		_, err = block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(bucketDir, idIndexV2.String()), nil)
		require.NoError(b, err)

		indexName := filepath.Join(bucketDir, idIndexV2.String(), block.IndexHeaderFilename)
		require.NoError(b, WriteBinary(ctx, bkt, idIndexV2, indexName))

		b.Run(fmt.Sprintf("%vNames%vValues", nameCount, valueCount), func(b *testing.B) {
			br, err := NewStreamBinaryReader(context.Background(), log.NewNopLogger(), nil, bucketDir, idIndexV2, 32, NewStreamBinaryReaderMetrics(nil), Config{})
			require.NoError(b, err)
			b.Cleanup(func() {
				require.NoError(b, br.Close())
			})

			// Use our own random source to avoid contention for the global random number generator.
			random := rand.New(rand.NewSource(time.Now().UnixNano()))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				name := nameSymbols[random.Intn(nameCount)]
				value := valueSymbols[random.Intn(valueCount)]
				offset, err := br.PostingsOffset(ctx, name, value)

				require.NoError(b, err)
				require.NotZero(b, offset.Start)
				require.NotZero(b, offset.End)
			}
		})
	}
}

func benchmarkReader(b *testing.B, bucketDir string, id ulid.ULID, benchmark func(b *testing.B, br Reader)) {
	br, err := NewStreamBinaryReader(context.Background(), log.NewNopLogger(), nil, bucketDir, id, 32, NewStreamBinaryReaderMetrics(nil), Config{})
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, br.Close())
	})

	benchmark(b, br)
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
			_, err = block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(bucketDir, idIndexV2.String()), nil)
			require.NoError(b, err)

			indexName := filepath.Join(bucketDir, idIndexV2.String(), block.IndexHeaderFilename)
			require.NoError(b, WriteBinary(ctx, bkt, idIndexV2, indexName))

			b.Run(fmt.Sprintf("%vNames%vValues", nameCount, valueCount), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					br, err := NewStreamBinaryReader(ctx, log.NewNopLogger(), nil, bucketDir, idIndexV2, 32, NewStreamBinaryReaderMetrics(nil), Config{})
					require.NoError(b, err)
					b.Cleanup(func() {
						require.NoError(b, br.Close())
					})
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
		idx, err := reader.symbols.ReverseLookup(s)
		require.NoError(b, err)
		m[idx] = s
		i = append(i, idx)
	}

	return i, m
}
