package indexheader

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
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

// TestStreamBinaryReader_ShouldBuildSamplesFromFile should accurately construct and
// write samples on first build and read from disk on the second build.
func TestStreamBinaryReader_ShouldBuildSamplesFromFile(t *testing.T) {
	ctx := context.Background()

	// logs := &concurrency.SyncBuffer{}
	// logger := &componentLogger{component: "compactor", log: log.NewLogfmtLogger(logs)}
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
	r1, err := NewStreamBinaryReader(ctx, logger, bkt, tmpDir, blockID, 3, NewStreamBinaryReaderMetrics(nil), Config{})
	require.NoError(t, err)
	// Read samples to disk on second build.
	r2, err := NewStreamBinaryReader(ctx, logger, bkt, tmpDir, blockID, 3, NewStreamBinaryReaderMetrics(nil), Config{})
	require.NoError(t, err)

	// Checks logs for order of operations: build index-header -> build sample -> read samples.
	require.ElementsMatch(t, []string{
		"level=debug msg=failed to read index-header from disk; recreating",
		"level=debug msg=built index-header file",
		"level=debug msg=failed to read index-header samples from disk; recreating",
		"level=debug msg=built index-header samples file",
		"level=debug msg=reading from index-header samples file",
	}, logger.logs, "\n")

	// Check that the samples are the same.
	require.Equal(t, r1.indexVersion, r2.indexVersion)
	require.Equal(t, r1.version, r2.version)
}
