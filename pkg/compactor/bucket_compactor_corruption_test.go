// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"
	"errors"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestRunCompactionJob_ChunkChecksumCorruption(t *testing.T) {
	c, job, broken, healthy, bkt := newChunkCorruptionFixture(t, true)
	_, _, err := c.runCompactionJob(t.Context(), job)
	require.ErrorContains(t, err, "checksum mismatch")
	ok, critical := IsCriticalError(err)
	require.True(t, ok, "chunk corruption must identify its broken block for no-compaction handling")
	require.Equal(t, broken.ULID, critical.id)
	require.NoError(t, c.handleKnownCompactionErrors(t.Context(), job, err))
	marked, err := bkt.Exists(t.Context(), path.Join(broken.ULID.String(), block.NoCompactMarkFilename))
	require.NoError(t, err)
	require.True(t, marked)
	marked, err = bkt.Exists(t.Context(), path.Join(healthy.ULID.String(), block.NoCompactMarkFilename))
	require.NoError(t, err)
	require.False(t, marked, "healthy input must not be marked")
}

func TestRunCompactionJob_ValidationDoesNotHideOtherFailures(t *testing.T) {
	for _, scenario := range []string{"healthy inputs", "chunk file unavailable", "canceled", "skipping disabled"} {
		t.Run(scenario, func(t *testing.T) {
			c, job, first, second, bkt := newChunkCorruptionFixture(t, scenario == "skipping disabled")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			expectedErr := errors.New("temporary compaction I/O failure")
			comp := &tsdbCompactorMock{}
			comp.On("Compact", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
				if scenario == "chunk file unavailable" {
					// Simulate a local file-access failure after the index-only check.
					for _, dir := range args.Get(1).([]string) {
						require.NoError(t, os.Remove(filepath.Join(dir, "chunks", "000001")))
					}
				}
				if scenario == "canceled" {
					cancel()
				}
			}).Return([]ulid.ULID(nil), expectedErr)
			c.comp = comp
			c.skipUnhealthyBlocks = scenario != "skipping disabled"
			_, _, err := c.runCompactionJob(ctx, job)
			require.ErrorIs(t, err, expectedErr)
			critical, _ := IsCriticalError(err)
			require.False(t, critical)
			require.ErrorIs(t, c.handleKnownCompactionErrors(t.Context(), job, err), expectedErr)
			for _, meta := range []*block.Meta{first, second} {
				marked, err := bkt.Exists(t.Context(), path.Join(meta.ULID.String(), block.NoCompactMarkFilename))
				require.NoError(t, err)
				require.False(t, marked)
			}
			comp.AssertExpectations(t)
		})
	}
}

func newChunkCorruptionFixture(t *testing.T, corrupt bool) (*BucketCompactor, *Job, *block.Meta, *block.Meta, objstore.Bucket) {
	t.Helper()
	ctx := t.Context()
	logger := log.NewNopLogger()
	storageDir := t.TempDir()
	chunk := chunkenc.NewXORChunk()
	appender, err := chunk.Appender()
	require.NoError(t, err)
	appender.Append(0, 0, 1)
	appender.Append(0, 1, 2)
	specs := block.SeriesSpecs{{
		Labels: labels.FromStrings("__name__", "test_metric"),
		Chunks: []chunks.Meta{{MinTime: 0, MaxTime: 1, Chunk: chunk}},
	}}
	broken, err := block.GenerateBlockFromSpec(storageDir, specs)
	require.NoError(t, err)
	healthy, err := block.GenerateBlockFromSpec(storageDir, specs)
	require.NoError(t, err)

	brokenDir := filepath.Join(storageDir, broken.ULID.String())
	segment := filepath.Join(brokenDir, "chunks", "000001")
	data, err := os.ReadFile(segment)
	require.NoError(t, err)
	require.Greater(t, len(data), 12)
	if corrupt {
		data[12] ^= 0xff
	}
	require.NoError(t, os.WriteFile(segment, data, 0600))
	_, err = block.GatherBlockHealthStats(ctx, logger, brokenDir, broken.MinTime, broken.MaxTime, false)
	require.NoError(t, err, "the existing pre-compaction check misses chunk checksums")
	_, err = block.GatherBlockHealthStats(ctx, logger, brokenDir, broken.MinTime, broken.MaxTime, true)
	if corrupt {
		require.ErrorContains(t, err, "checksum mismatch")
		var chunkErr block.ChunkDataError
		require.ErrorAs(t, err, &chunkErr)
	} else {
		require.NoError(t, err)
	}

	bkt := objstore.NewInMemBucket()
	t.Cleanup(func() { require.NoError(t, bkt.Close()) })
	broken, err = block.Upload(ctx, logger, bkt, brokenDir, broken)
	require.NoError(t, err)
	healthy, err = block.Upload(ctx, logger, bkt, filepath.Join(storageDir, healthy.ULID.String()), healthy)
	require.NoError(t, err)
	job := newJob("test", "checksum-corruption", labels.EmptyLabels(), 0, false, 0, "")
	require.NoError(t, job.AppendMeta(broken))
	require.NoError(t, job.AppendMeta(healthy))
	planner := &tsdbPlannerMock{}
	planner.On("Plan", mock.Anything, mock.Anything).Return([]*block.Meta{broken, healthy}, nil)
	reg := prometheus.NewRegistry()
	comp, err := tsdb.NewLeveledCompactor(ctx, reg, promslog.NewNopLogger(), []int64{1000, 3000}, nil, nil)
	require.NoError(t, err)
	metrics := NewBucketCompactorMetrics(prometheus.NewCounter(prometheus.CounterOpts{Name: "deleted_blocks_total", Help: "Deleted blocks."}), prometheus.NewRegistry())
	c, err := NewBucketCompactor(logger, nil, planner, comp, t.TempDir(), bkt, 1, true, 0, ownAllJobs, sortJobsByNewestBlocksFirst, 0, 0, false, 1, 1, metrics, 32, 1)
	require.NoError(t, err)
	return c, job, broken, healthy, bkt
}
