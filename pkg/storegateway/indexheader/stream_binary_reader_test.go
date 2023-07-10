// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/stream_binary_reader_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

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

// CustomLogger is a custom implementation of log.Logger that captures the logs.
type CustomLogger struct {
	// Store the logs in a slice or any other suitable data structure
	Logs []string
}

func (cl *CustomLogger) Log(keyvals ...interface{}) error {
	str := fmt.Sprint(keyvals...)
	cl.Logs = append(cl.Logs, str)
	return nil
}

// Tests:
// should fail if unable to load sample
// sample should be what is expected
// should rebuild corrupted sample
func TestStreamBinaryReader_ShouldBuildSampleFromFile(t *testing.T) {
	ctx := context.Background()
	// logger := log.NewNopLogger()
	// logger := log.NewLogfmtLogger(os.Stderr)
	logger := &CustomLogger{}

	tmpDir := filepath.Join(t.TempDir(), "test-sample")
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

	// Write sample to disk on first build.
	r1, err := NewStreamBinaryReader(ctx, logger, bkt, tmpDir, blockID, 3, NewStreamBinaryReaderMetrics(nil), Config{})
	require.NoError(t, err)
	// Read sample to disk on second build.
	r2, err := NewStreamBinaryReader(ctx, logger, bkt, tmpDir, blockID, 3, NewStreamBinaryReaderMetrics(nil), Config{})
	require.NoError(t, err)

	// Check that last log confirms we read from index-header sample.
	logStr := strings.Split(logger.Logs[len(logger.Logs)-1], " filepath")[0]
	require.Equal(t, logStr, "leveldebugmsgreading from index-header sample")

	// Check that the samples are the same.
	require.Equal(t, r1.indexVersion, r2.indexVersion)
	require.Equal(t, r1.version, r2.version)
}
