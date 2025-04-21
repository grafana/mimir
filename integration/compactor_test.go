// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"context"
	"encoding/json"
	"os"
	"path"
	"path/filepath"
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
	util_test "github.com/grafana/mimir/pkg/util/test"
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
	expectedSeries := make([]util_test.Series, numBlocks)

	for i := 0; i < numBlocks; i++ {
		spec := block.SeriesSpec{
			Labels: labels.FromStrings("case", "native_histogram", "i", strconv.Itoa(i)),
			Chunks: []chunks.Meta{
				must(chunks.ChunkFromSamples([]chunks.Sample{
					util_test.NewSample(10, 0, util_test.GenerateTestHistogram(1), nil),
					util_test.NewSample(20, 0, util_test.GenerateTestHistogram(2), nil),
				})),
				must(chunks.ChunkFromSamples([]chunks.Sample{
					util_test.NewSample(30, 0, util_test.GenerateTestHistogram(3), nil),
					util_test.NewSample(40, 0, util_test.GenerateTestHistogram(4), nil),
				})),
				must(chunks.ChunkFromSamples([]chunks.Sample{
					util_test.NewSample(50, 0, util_test.GenerateTestHistogram(5), nil),
					util_test.NewSample(2*time.Hour.Milliseconds()-1, 0, util_test.GenerateTestHistogram(6), nil),
				})),
			},
		}

		// Populate expectedSeries for comparison w/ compactedSeries later.
		var samples []util_test.Sample
		for _, chk := range spec.Chunks {
			it := chk.Chunk.Iterator(nil)
			for it.Next() != chunkenc.ValNone {
				ts, h := it.AtHistogram(nil)
				samples = append(samples, util_test.NewSample(ts, 0, h, nil))
			}
		}
		expectedSeries[i] = util_test.NewSeries(spec.Labels, samples)

		meta, err := block.GenerateBlockFromSpec(inDir, []*block.SeriesSpec{&spec})
		require.NoError(t, err)

		require.NoError(t, block.Upload(context.Background(), log.NewNopLogger(), bktClient, filepath.Join(inDir, meta.ULID.String()), meta))

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

	var compactedSeries []util_test.Series

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

			var samples []util_test.Sample

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
						samples = append(samples, util_test.NewSample(ts, 0, h, nil))
					} else {
						t.Error("Unexpected chunkenc.ValueType (we're expecting only histograms): " + string(valType))
					}
				}
			}

			compactedSeries = append(compactedSeries, util_test.NewSeries(lbls.Labels(), samples))
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
