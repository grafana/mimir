// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

type inMemoryLogger struct {
	logs []string
}

func (ml *inMemoryLogger) Log(keyvals ...interface{}) error {
	log := ""
	for ix := 0; ix+1 < len(keyvals); ix += 2 {
		k := keyvals[ix]
		v := keyvals[ix+1]

		// Only log level and msg fields for readability.
		if k == "level" || k == "msg" {
			log += fmt.Sprintf("%v=%v ", k, v)
		}
	}
	log = strings.TrimSpace(log)
	ml.logs = append(ml.logs, log)
	return nil
}

// TestStreamBinaryReader_ShouldBuildSamplesFromFile tests if StreamBinaryReady constructs
// and writes samples on first build and reads from disk on the second build.
func TestStreamBinaryReader_ShouldBuildSamplesFromFileSimple(t *testing.T) {
	ctx := context.Background()

	logger := &inMemoryLogger{}

	tmpDir := filepath.Join(t.TempDir(), "test-samples")
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bkt.Close()) })

	// Create block.
	blockID, err := block.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("a", "3"),
	}, 100, 0, 1000, labels.FromStrings("ext1", "1"))
	require.NoError(t, err)
	require.NoError(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), nil))

	// Write samples to disk on first build.
	_, err = NewStreamBinaryReader(ctx, logger, bkt, tmpDir, blockID, 3, NewStreamBinaryReaderMetrics(nil), Config{})
	require.NoError(t, err)
	// Read samples to disk on second build.
	_, err = NewStreamBinaryReader(ctx, logger, bkt, tmpDir, blockID, 3, NewStreamBinaryReaderMetrics(nil), Config{})
	require.NoError(t, err)

	// Checks logs for order of operations: build index-header -> build sample -> read samples.
	t.Log(logger.logs)
	require.Equal(t, []string{
		"level=debug msg=failed to read index-header from disk; recreating",
		"level=debug msg=built index-header file",
		"level=debug msg=constructing index-header samples",
		"level=debug msg=built index-header samples file",
		"level=debug msg=reading from index-header samples file",
	}, logger.logs, "\n")
}

// TestStreamBinaryReader_CheckSamplesCorrectnessExtensive tests if StreamBinaryReader
// reads and writes samples accurately for a variety of index-headers.
func TestStreamBinaryReader_CheckSamplesCorrectnessExtensive(t *testing.T) {
	ctx := context.Background()

	tmpDir := filepath.Join(t.TempDir(), "test-samples")
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bkt.Close()) })

	for _, nameCount := range []int{3, 20, 50} {
		for _, valueCount := range []int{3, 10, 100, 500} {
			logger := &inMemoryLogger{}

			nameSymbols := generateSymbols("name", nameCount)
			valueSymbols := generateSymbols("value", valueCount)

			t.Run(fmt.Sprintf("%vNames%vValues", nameCount, valueCount), func(t *testing.T) {
				t.Parallel()

				blockID, err := block.CreateBlock(ctx, tmpDir, generateLabels(nameSymbols, valueSymbols), 100, 0, 1000, labels.FromStrings("ext1", "1"))
				require.NoError(t, err)
				require.NoError(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), nil))

				indexFile, err := fileutil.OpenMmapFile(filepath.Join(tmpDir, blockID.String(), block.IndexFilename))
				require.NoError(t, err)
				requireCleanup(t, indexFile.Close)

				b := realByteSlice(indexFile.Bytes())

				// Write samples to disk on first build.
				_, err = NewStreamBinaryReader(ctx, logger, bkt, tmpDir, blockID, 3, NewStreamBinaryReaderMetrics(nil), Config{})
				require.NoError(t, err)
				// Read samples to disk on second build.
				r2, err := NewStreamBinaryReader(ctx, logger, bkt, tmpDir, blockID, 3, NewStreamBinaryReaderMetrics(nil), Config{})
				require.NoError(t, err)

				// Check correctness of samples.
				compareIndexToHeader(t, b, r2)
			})
		}
	}
}
