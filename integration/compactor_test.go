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
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/histogram"
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
	expectedSeries := make([]series, numBlocks)

	for i := 0; i < numBlocks; i++ {
		spec := block.SeriesSpec{
			Labels: labels.FromStrings("case", "native_histogram", "i", strconv.Itoa(i)),
			Chunks: []chunks.Meta{
				must(chunks.ChunkFromSamples([]chunks.Sample{
					sample{10, 0, test.GenerateTestHistogram(1), nil},
					sample{20, 0, test.GenerateTestHistogram(2), nil},
				})),
				must(chunks.ChunkFromSamples([]chunks.Sample{
					sample{30, 0, test.GenerateTestHistogram(3), nil},
					sample{40, 0, test.GenerateTestHistogram(4), nil},
				})),
				must(chunks.ChunkFromSamples([]chunks.Sample{
					sample{50, 0, test.GenerateTestHistogram(5), nil},
					sample{2*time.Hour.Milliseconds() - 1, 0, test.GenerateTestHistogram(6), nil},
				})),
			},
		}

		// Populate expectedSeries for comparison w/ compactedSeries later.
		var samples []sample
		for _, chk := range spec.Chunks {
			it := chk.Chunk.Iterator(nil)
			for it.Next() != chunkenc.ValNone {
				ts, h := it.AtHistogram(nil)
				samples = append(samples, sample{t: ts, h: h})
			}
		}
		expectedSeries[i] = series{lbls: spec.Labels, samples: samples}

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

	var compactedSeries []series

	for _, blockID := range blocks {
		require.NoError(t, block.Download(context.Background(), log.NewNopLogger(), bktClient, ulid.MustParseStrict(blockID), filepath.Join(outDir, blockID)))

		if isMarkedForDeletionDueToCompaction(t, path.Join(outDir, blockID)) {
			// The source blocks are marked for deletion but not deleted yet; skip them.
			continue
		}

		chkReader, err := chunks.NewDirReader(filepath.Join(outDir, blockID, block.ChunksDirname), nil)
		require.NoError(t, err)

		ixReader, err := index.NewFileReader(filepath.Join(outDir, blockID, block.IndexFilename))
		require.NoError(t, err)

		n, v := index.AllPostingsKey()
		all, err := ixReader.Postings(context.Background(), n, v)
		require.NoError(t, err)

		for p := ixReader.SortedPostings(all); p.Next(); {
			var lbls labels.ScratchBuilder
			var chks []chunks.Meta

			var samples []sample

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
						samples = append(samples, sample{
							t: ts,
							h: h,
						})
					} else {
						t.Error("Unexpected chunkenc.ValueType (we're expecting only histograms): " + string(valType))
					}
				}
			}

			compactedSeries = append(compactedSeries, series{
				lbls:    lbls.Labels(),
				samples: samples,
			})
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

type series struct {
	lbls    labels.Labels
	samples []sample
}

type sample struct {
	t  int64
	v  float64
	h  *histogram.Histogram
	fh *histogram.FloatHistogram
}

func (s sample) T() int64                      { return s.t }
func (s sample) F() float64                    { return s.v }
func (s sample) H() *histogram.Histogram       { return s.h }
func (s sample) FH() *histogram.FloatHistogram { return s.fh }

func (s sample) Type() chunkenc.ValueType {
	switch {
	case s.h != nil:
		return chunkenc.ValHistogram
	case s.fh != nil:
		return chunkenc.ValFloatHistogram
	default:
		return chunkenc.ValFloat
	}
}

func (s sample) Copy() chunks.Sample {
	c := sample{t: s.t, v: s.v}
	if s.h != nil {
		c.h = s.h.Copy()
	}
	if s.fh != nil {
		c.fh = s.fh.Copy()
	}
	return c
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
