// SPDX-License-Identifier: AGPL-3.0-only

package integration

import (
	"cmp"
	"context"
	"encoding/json"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/bucket/s3"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestCompactBlocksContainingNativeHistograms(t *testing.T) {
	const numBlocks = 2

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, blocksBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// inDir is a staging directory for blocks we upload to the bucket.
	inDir := t.TempDir()
	// outDir is a staging directory for blocks we download from the bucket after compaction.
	outDir := t.TempDir()

	bkt, err := s3.NewBucketClient(s3.Config{
		Endpoint:        minio.HTTPEndpoint(),
		Insecure:        true,
		BucketName:      blocksBucketName,
		AccessKeyID:     e2edb.MinioAccessKey,
		SecretAccessKey: flagext.SecretWithValue(e2edb.MinioSecretKey),
	}, "test", log.NewNopLogger())
	require.NoError(t, err)
	bktClient := bucket.NewPrefixedBucketClient(bkt, userID)
	defer func() {
		require.NoError(t, bktClient.Close())
	}()

	// Create a few blocks to compact and upload them to the bucket.
	metas := make([]*block.Meta, 0, numBlocks)
	expectedSeries := make([]test.Series, numBlocks)

	for i := 0; i < numBlocks; i++ {
		spec := block.SeriesSpec{
			Labels: labels.FromStrings("case", "native_histogram", "i", strconv.Itoa(i)),
			Chunks: []chunks.Meta{
				must(chunks.ChunkFromSamples([]chunks.Sample{
					test.Sample{TS: 10, Hist: test.GenerateTestHistogram(1)},
					test.Sample{TS: 20, Hist: test.GenerateTestHistogram(2)},
				})),
				must(chunks.ChunkFromSamples([]chunks.Sample{
					test.Sample{TS: 30, Hist: test.GenerateTestHistogram(3)},
					test.Sample{TS: 40, Hist: test.GenerateTestHistogram(4)},
				})),
				must(chunks.ChunkFromSamples([]chunks.Sample{
					test.Sample{TS: 50, Hist: test.GenerateTestHistogram(5)},
					test.Sample{TS: 2*time.Hour.Milliseconds() - 1, Hist: test.GenerateTestHistogram(6)},
				})),
			},
		}

		// Populate expectedSeries for comparison w/ compactedSeries later.
		var samples []test.Sample
		for _, chk := range spec.Chunks {
			it := chk.Chunk.Iterator(nil)
			for it.Next() != chunkenc.ValNone {
				ts, h := it.AtHistogram(nil)
				samples = append(samples, test.Sample{TS: ts, Hist: h})
			}
		}
		expectedSeries[i] = test.Series{Labels: spec.Labels, Samples: samples}

		meta, err := block.GenerateBlockFromSpec(inDir, []*block.SeriesSpec{&spec})
		require.NoError(t, err)

		_, err = block.Upload(context.Background(), log.NewNopLogger(), bktClient, filepath.Join(inDir, meta.ULID.String()), meta)
		require.NoError(t, err)

		metas = append(metas, meta)
	}

	// Start compactor.
	compactor := e2emimir.NewCompactor("compactor", consul.NetworkHTTPEndpoint(), mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags()))
	require.NoError(t, s.StartAndWaitReady(compactor))

	// Wait for the compactor to run.
	require.NoError(t, compactor.WaitSumMetrics(e2e.Greater(0), "cortex_compactor_runs_completed_total"))

	var blocks []string
	require.NoError(t, bktClient.Iter(context.Background(), "", func(name string) error {
		baseName := filepath.Base(name)
		if len(baseName) == ulid.EncodedSize {
			blocks = append(blocks, filepath.Base(name))
		}
		return nil
	}))

	var compactedSeries []test.Series

	for _, blockID := range blocks {
		require.NoError(t, block.Download(context.Background(), log.NewNopLogger(), bktClient, ulid.MustParseStrict(blockID), filepath.Join(outDir, blockID)))

		if isMarkedForDeletionDueToCompaction(t, path.Join(outDir, blockID)) {
			// The source blocks are marked for deletion but not deleted yet; skip them.
			continue
		}

		chkReader, err := chunks.NewDirReader(filepath.Join(outDir, blockID, block.ChunksDirname), nil)
		require.NoError(t, err)

		ixReader, err := index.NewFileReader(filepath.Join(outDir, blockID, block.IndexFilename), index.DecodePostingsRaw)
		require.NoError(t, err)

		n, v := index.AllPostingsKey()
		all, err := ixReader.Postings(context.Background(), n, v)
		require.NoError(t, err)

		for p := ixReader.SortedPostings(all); p.Next(); {
			var lbls labels.ScratchBuilder
			var chks []chunks.Meta

			var samples []test.Sample

			require.NoError(t, ixReader.Series(p.At(), &lbls, &chks))

			for _, c := range chks {
				chunk, iter, err := chkReader.ChunkOrIterable(c)
				require.NoError(t, err)
				require.Nil(t, iter)
				c.Chunk = chunk

				it := c.Chunk.Iterator(nil)
				for {
					valType := it.Next()

					if valType == chunkenc.ValNone {
						break
					} else if valType == chunkenc.ValHistogram {
						ts, h := it.AtHistogram(nil)
						samples = append(samples, test.Sample{TS: ts, Hist: h})
					} else {
						t.Error("Unexpected chunkenc.ValueType (we're expecting only histograms): " + string(valType))
					}
				}
			}

			compactedSeries = append(compactedSeries, test.Series{Labels: lbls.Labels(), Samples: samples})
		}

		require.NoError(t, ixReader.Close())
		require.NoError(t, chkReader.Close())

		// This block ULID should not be the same as any of the pre-compacted ones.
		for _, m := range metas {
			require.NotEqual(t, m.ULID.String(), blockID)
		}
	}

	require.Equal(t, expectedSeries, compactedSeries)
}

// TestCompactVerticallyOverlappingBlocks verifies that the compactor vertically
// compacts multiple blocks that cover the same time range and contain the same
// series, merging their samples into a single block. This exercises the
// deduplicating vertical compaction path, which is distinct from the horizontal
// compaction of adjacent time ranges covered by other tests.
func TestCompactVerticallyOverlappingBlocks(t *testing.T) {
	// Each source block carries the same single series, with samples at
	// different timestamps within the same 2h compaction range, so the
	// compactor has to merge them into one block with the union of samples.
	const numBlocks = 3

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, blocksBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	// inDir stages blocks we upload to the bucket; outDir stages blocks we
	// download back after compaction.
	inDir := t.TempDir()
	outDir := t.TempDir()

	bkt, err := s3.NewBucketClient(s3.Config{
		Endpoint:        minio.HTTPEndpoint(),
		Insecure:        true,
		BucketName:      blocksBucketName,
		AccessKeyID:     e2edb.MinioAccessKey,
		SecretAccessKey: flagext.SecretWithValue(e2edb.MinioSecretKey),
	}, "test", log.NewNopLogger())
	require.NoError(t, err)
	bktClient := bucket.NewPrefixedBucketClient(bkt, userID)
	defer func() {
		require.NoError(t, bktClient.Close())
	}()

	// All source blocks share this series. Each block contributes two samples
	// at distinct, non-overlapping timestamps so the merged result is simply
	// the union of all samples sorted by timestamp.
	seriesLabels := labels.FromStrings("case", "vertical_compaction", "series", "a")

	metas := make([]*block.Meta, 0, numBlocks)
	var expectedSamples []test.Sample

	for i := 0; i < numBlocks; i++ {
		// Two samples per block, spaced so blocks overlap within the same 2h
		// range but no two samples share a timestamp.
		ts1 := int64(i*1000 + 100)
		ts2 := int64(i*1000 + 200)

		spec := block.SeriesSpec{
			Labels: seriesLabels,
			Chunks: []chunks.Meta{
				must(chunks.ChunkFromSamples([]chunks.Sample{
					test.Sample{TS: ts1, Val: float64(i) + 0.1},
					test.Sample{TS: ts2, Val: float64(i) + 0.2},
				})),
			},
		}

		expectedSamples = append(expectedSamples,
			test.Sample{TS: ts1, Val: float64(i) + 0.1},
			test.Sample{TS: ts2, Val: float64(i) + 0.2},
		)

		meta, err := block.GenerateBlockFromSpec(inDir, []*block.SeriesSpec{&spec})
		require.NoError(t, err)

		_, err = block.Upload(context.Background(), log.NewNopLogger(), bktClient, filepath.Join(inDir, meta.ULID.String()), meta)
		require.NoError(t, err)

		metas = append(metas, meta)
	}

	// Start compactor.
	compactor := e2emimir.NewCompactor("compactor", consul.NetworkHTTPEndpoint(), mergeFlags(BlocksStorageFlags(), BlocksStorageS3Flags()))
	require.NoError(t, s.StartAndWaitReady(compactor))

	// Wait for the compactor to run at least once.
	require.NoError(t, compactor.WaitSumMetrics(e2e.Greater(0), "cortex_compactor_runs_completed_total"))

	// Collect the blocks currently in the bucket.
	var blocks []string
	require.NoError(t, bktClient.Iter(context.Background(), "", func(name string) error {
		baseName := filepath.Base(name)
		if len(baseName) == ulid.EncodedSize {
			blocks = append(blocks, filepath.Base(name))
		}
		return nil
	}))

	// Read back the samples of the series from every block that is not a
	// source block marked for deletion. After vertical compaction there must
	// be exactly one such block, and it must contain the union of all samples.
	var compactedSeries []test.Series
	compactedBlocks := 0

	for _, blockID := range blocks {
		require.NoError(t, block.Download(context.Background(), log.NewNopLogger(), bktClient, ulid.MustParseStrict(blockID), filepath.Join(outDir, blockID)))

		if isMarkedForDeletionDueToCompaction(t, path.Join(outDir, blockID)) {
			// The source blocks are marked for deletion but not deleted yet; skip them.
			continue
		}

		compactedBlocks++

		chkReader, err := chunks.NewDirReader(filepath.Join(outDir, blockID, block.ChunksDirname), nil)
		require.NoError(t, err)

		ixReader, err := index.NewFileReader(filepath.Join(outDir, blockID, block.IndexFilename), index.DecodePostingsRaw)
		require.NoError(t, err)

		n, v := index.AllPostingsKey()
		all, err := ixReader.Postings(context.Background(), n, v)
		require.NoError(t, err)

		for p := ixReader.SortedPostings(all); p.Next(); {
			var lbls labels.ScratchBuilder
			var chks []chunks.Meta
			var samples []test.Sample

			require.NoError(t, ixReader.Series(p.At(), &lbls, &chks))

			for _, c := range chks {
				chunk, iter, err := chkReader.ChunkOrIterable(c)
				require.NoError(t, err)
				require.Nil(t, iter)
				c.Chunk = chunk

				it := c.Chunk.Iterator(nil)
				for {
					valType := it.Next()
					if valType == chunkenc.ValNone {
						break
					} else if valType == chunkenc.ValFloat {
						ts, val := it.At()
						samples = append(samples, test.Sample{TS: ts, Val: val})
					} else {
						t.Error("Unexpected chunkenc.ValueType (we're expecting only floats): " + string(valType))
					}
				}
				require.NoError(t, it.Err())
			}

			compactedSeries = append(compactedSeries, test.Series{Labels: lbls.Labels(), Samples: samples})
		}

		require.NoError(t, ixReader.Close())
		require.NoError(t, chkReader.Close())

		// The compacted block must be a newly created one, not any of the sources.
		for _, m := range metas {
			require.NotEqual(t, m.ULID.String(), blockID)
		}
	}

	// Vertical compaction must collapse the overlapping source blocks into a
	// single block holding exactly one series with all samples merged.
	require.Equal(t, 1, compactedBlocks)
	require.Len(t, compactedSeries, 1)
	require.Equal(t, seriesLabels, compactedSeries[0].Labels)

	// Compare samples independent of ordering: the merged block must contain
	// exactly the union of samples, but the order in which we read them back
	// across chunks is not part of the behavior under test.
	sortByTS := func(a, b test.Sample) int { return cmp.Compare(a.TS, b.TS) }
	gotSamples := compactedSeries[0].Samples
	slices.SortFunc(gotSamples, sortByTS)
	slices.SortFunc(expectedSamples, sortByTS)
	require.Equal(t, expectedSamples, gotSamples)
}

func isMarkedForDeletionDueToCompaction(t *testing.T, blockPath string) bool {
	deletionMarkFilePath := filepath.Join(blockPath, block.DeletionMarkFilename)
	b, err := os.ReadFile(deletionMarkFilePath)
	if os.IsNotExist(err) {
		return false
	}
	require.NoError(t, err)

	deletionMark := &block.DeletionMark{}
	require.NoError(t, json.Unmarshal(b, deletionMark))

	return deletionMark.Details == "source of compacted block"
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
