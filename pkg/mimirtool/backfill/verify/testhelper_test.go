// SPDX-License-Identifier: AGPL-3.0-only

package verify

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	testutil "github.com/grafana/mimir/pkg/util/test"
)

// sampleAt returns a chunks.Sample suitable for chunks.ChunkFromSamples and
// block.GenerateBlockFromSpec. Uses the canonical Mimir test.Sample type
// (pkg/util/test/tsdb.go). This is the convention used across
// pkg/compactor/compactor_test.go and pkg/storegateway/bucket_stores_test.go.
// Shared with wellformed_test.go and singleutcday_test.go (Plan 01-03).
func sampleAt(ts int64, v float64) chunks.Sample {
	return testutil.Sample{TS: ts, Val: v}
}

// generateValidBlock writes a real TSDB block with 3 series to parent and
// returns the block's on-disk directory (parent/<ULID>) and its Meta.
// samples contains the samples for each series; all three series get the
// same samples. The caller picks minTime/maxTime by choosing sample
// timestamps.
func generateValidBlock(t *testing.T, parent string, samples []chunks.Sample) (string, *block.Meta) {
	t.Helper()
	require.GreaterOrEqual(t, len(samples), 1, "need at least one sample")

	makeChunk := func() chunks.Meta {
		c, err := chunks.ChunkFromSamples(samples)
		require.NoError(t, err)
		return c
	}

	specs := []*block.SeriesSpec{
		{Labels: labels.FromStrings("__name__", "metric_a"), Chunks: []chunks.Meta{makeChunk()}},
		{Labels: labels.FromStrings("__name__", "metric_b"), Chunks: []chunks.Meta{makeChunk()}},
		{Labels: labels.FromStrings("__name__", "metric_c"), Chunks: []chunks.Meta{makeChunk()}},
	}
	meta, err := block.GenerateBlockFromSpec(parent, specs)
	require.NoError(t, err)
	return filepath.Join(parent, meta.ULID.String()), meta
}

// corruptChunkSegment truncates chunks/000001 to a very small size so the
// internal format is destroyed. Deep mode should fail; medium mode may or
// may not catch it depending on header position.
func corruptChunkSegment(t *testing.T, blockDir string) {
	t.Helper()
	path := filepath.Join(blockDir, "chunks", "000001")
	require.NoError(t, os.Truncate(path, 10))
}

// mangleIndex overwrites the first 100 bytes of the index file with zeros,
// which destroys the file header. Both deep and medium modes should fail.
func mangleIndex(t *testing.T, blockDir string) {
	t.Helper()
	path := filepath.Join(blockDir, "index")
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	zeros := make([]byte, 100)
	_, err = f.WriteAt(zeros, 0)
	require.NoError(t, err)
}

// flipChunkByte flips a single bit inside chunks/000001 at the given offset
// (measured from the start of the file; caller chooses an offset past the
// segment header so the file format stays valid and only CRC32 is broken).
// This causes deep mode to fail and medium mode to pass.
func flipChunkByte(t *testing.T, blockDir string, offset int64) {
	t.Helper()
	path := filepath.Join(blockDir, "chunks", "000001")
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	var b [1]byte
	_, err = f.ReadAt(b[:], offset)
	require.NoError(t, err)
	b[0] ^= 0x01
	_, err = f.WriteAt(b[:], offset)
	require.NoError(t, err)
}
